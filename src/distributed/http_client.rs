//! A deliberately tiny HTTP/1.1 client.
//!
//! Peer health probes need one thing: issue a request to another node of *this*
//! engine and read a small response. `reqwest` is already in the tree, but it is
//! a TLS-capable connection-pooling client whose whole surface is irrelevant
//! here, and `hyper`'s client feature would pull more of hyper into the default
//! build than the server does. So this is ~100 lines over `tokio::net`.
//!
//! It is intentionally limited and refuses to guess:
//!
//! * `Connection: close` on every request, and the body is "everything until
//!   EOF". That sidesteps chunked-transfer and keep-alive framing entirely —
//!   correct against any conforming server, and specifically against ours.
//! * No redirects, no TLS, no cookies, no retries. A caller that wants retry
//!   semantics writes the loop, because the right backoff is a policy decision
//!   belonging to the caller (the membership prober has its own).
//! * A single timeout covers connect + write + read. A probe that hangs
//!   half-way through a response is exactly as dead as one that never connected.
//!
//! It is used by the membership prober and by the integration tests, so the
//! tests exercise the same code path the cluster does.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A parsed HTTP response: status code and the complete body.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// The body as UTF-8, lossily. Only for diagnostics and JSON payloads.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Issue a request to `addr` (a `host:port` authority) and read the full response.
///
/// `timeout` bounds the entire exchange, not any single syscall.
pub async fn request(
    addr: &str,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: Option<&[u8]>,
    timeout: Duration,
) -> std::io::Result<HttpResponse> {
    tokio::time::timeout(
        timeout,
        request_inner(addr, method, path, content_type, body),
    )
    .await
    .unwrap_or_else(|_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{} {}{} timed out after {:?}", method, addr, path, timeout),
        ))
    })
}

async fn request_inner(
    addr: &str,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: Option<&[u8]>,
) -> std::io::Result<HttpResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true).ok();

    let body = body.unwrap_or(&[]);
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    head.push_str("\r\n");

    stream.write_all(head.as_bytes()).await?;
    if !body.is_empty() {
        stream.write_all(body).await?;
    }
    stream.flush().await?;

    let mut raw = Vec::with_capacity(4096);
    stream.read_to_end(&mut raw).await?;
    parse_response(&raw)
}

/// `GET addr/path`.
pub async fn get(addr: &str, path: &str, timeout: Duration) -> std::io::Result<HttpResponse> {
    request(addr, "GET", path, None, None, timeout).await
}

/// `POST addr/path` with a text/plain body.
pub async fn post_text(
    addr: &str,
    path: &str,
    body: &str,
    timeout: Duration,
) -> std::io::Result<HttpResponse> {
    request(
        addr,
        "POST",
        path,
        Some("text/plain; charset=utf-8"),
        Some(body.as_bytes()),
        timeout,
    )
    .await
}

fn parse_response(raw: &[u8]) -> std::io::Result<HttpResponse> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| invalid("response has no header terminator"))?;
    let head = &raw[..split];
    let body = raw[split + 4..].to_vec();

    let status_line = head
        .split(|b| *b == b'\n')
        .next()
        .ok_or_else(|| invalid("response has no status line"))?;
    let status_line = String::from_utf8_lossy(status_line);
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| invalid(&format!("unparseable status line: {status_line:?}")))?;

    Ok(HttpResponse { status, body })
}

fn invalid(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_normal_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        let r = parse_response(raw).unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, b"ok");
        assert!(r.is_success());
    }

    #[test]
    fn parses_an_empty_body() {
        let raw = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
        let r = parse_response(raw).unwrap();
        assert_eq!(r.status, 503);
        assert!(r.body.is_empty());
        assert!(!r.is_success());
    }

    #[test]
    fn rejects_a_truncated_response() {
        // A half-written header must be an error, never a zero-length success:
        // silently reporting 200/empty would mark a broken peer healthy.
        assert!(parse_response(b"HTTP/1.1 200 OK\r\nContent-Len").is_err());
    }

    #[tokio::test]
    async fn connect_failure_is_an_error_not_a_hang() {
        // Port 1 on loopback: nothing listens, connection refused immediately.
        let r = get("127.0.0.1:1", "/healthz", Duration::from_millis(500)).await;
        assert!(r.is_err());
    }
}
