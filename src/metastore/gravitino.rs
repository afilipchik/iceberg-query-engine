//! Apache Gravitino catalog client: table discovery from a metastore.
//!
//! # What this integration IS
//!
//! Gravitino (gravitino.apache.org) is a metadata lake: metalakes contain
//! catalogs, catalogs contain schemas, schemas contain **filesets** — named
//! storage locations with free-form properties. This engine maps one
//! Gravitino schema to its table namespace: every fileset in the schema
//! becomes a registered table, with the fileset's `format` property choosing
//! the reader (`parquet`, `iceberg`, `lance`).
//!
//! In a cluster, every `serve` node pulls the same schema from the same
//! metastore at startup, so the nodes agree on what tables exist because a
//! single authority said so — not because N `--tables` flags happened to
//! match. The distributed digest interlock still verifies the DATA agrees;
//! the metastore is what makes the CATALOG agree.
//!
//! # Protocol facts (verified against Gravitino 1.3.0, not assumed)
//!
//! * `GET /api/metalakes/{m}/catalogs/{c}/schemas/{s}/filesets` returns
//!   `{"code":0,"identifiers":[{"namespace":[..],"name":"orders"}]}`.
//! * `GET .../filesets/{name}` returns `{"code":0,"fileset":{...}}` where
//!   `storageLocation` is a **directory** (Gravitino rejects file locations),
//!   echoed back normalized: `file:///a/b` becomes `file:/a/b`.
//! * Errors carry a nonzero `code` plus `type` and `message`.
//! * No auth by default ("simple" authenticator); `Content-Type` matters only
//!   for POSTs, which this client never issues — creation is an operator
//!   action (`scripts/metastore_local.sh populate`), reading is the engine's.
//!
//! # Conventions this engine adds (they live in fileset properties)
//!
//! * `format`: `parquet` | `iceberg` | `lance` — REQUIRED. A fileset without
//!   it is refused by name rather than sniffed: guessing a format is how a
//!   half-converted table gets read as the wrong thing.
//! * `file`: optional file name inside the directory, for a single-file
//!   Parquet table living in a shared directory (Gravitino cannot point a
//!   fileset at a file directly).

use crate::error::{QueryError, Result};
use crate::execution::ExecutionContext;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Duration;

/// Where one schema of tables lives in a Gravitino server.
#[derive(Debug, Clone)]
pub struct GravitinoSource {
    /// `http://host:port` — no trailing slash, no path.
    pub base_url: String,
    pub metalake: String,
    pub catalog: String,
    pub schema: String,
}

/// One fileset, as the engine consumes it.
#[derive(Debug, Clone)]
pub struct Fileset {
    pub name: String,
    pub storage_location: String,
    pub format: String,
    /// Optional file name within the storage location.
    pub file: Option<String>,
}

impl GravitinoSource {
    fn filesets_path(&self) -> String {
        format!(
            "/api/metalakes/{}/catalogs/{}/schemas/{}/filesets",
            self.metalake, self.catalog, self.schema
        )
    }

    /// Names of every fileset in the schema, sorted for determinism.
    pub fn list_filesets(&self) -> Result<Vec<String>> {
        let v = self.get_json(&self.filesets_path())?;
        let mut names: Vec<String> = v["identifiers"]
            .as_array()
            .ok_or_else(|| {
                QueryError::Storage(format!(
                    "metastore {}: fileset list response has no identifiers array",
                    self.base_url
                ))
            })?
            .iter()
            .filter_map(|i| i["name"].as_str().map(|s| s.to_string()))
            .collect();
        names.sort();
        Ok(names)
    }

    /// One fileset, with the engine's conventions enforced.
    pub fn get_fileset(&self, name: &str) -> Result<Fileset> {
        let v = self.get_json(&format!("{}/{name}", self.filesets_path()))?;
        let f = &v["fileset"];
        let storage_location = f["storageLocation"]
            .as_str()
            .ok_or_else(|| {
                QueryError::Storage(format!("metastore fileset `{name}` has no storageLocation"))
            })?
            .to_string();
        let props = &f["properties"];
        let format = props["format"]
            .as_str()
            .map(|s| s.to_lowercase())
            .ok_or_else(|| {
                QueryError::Storage(format!(
                    "metastore fileset `{name}` has no `format` property; set it to \
                 parquet, iceberg or lance — this engine does not sniff formats"
                ))
            })?;
        Ok(Fileset {
            name: name.to_string(),
            storage_location,
            format,
            file: props["file"].as_str().map(|s| s.to_string()),
        })
    }

    /// Register every fileset of the schema as a table in `ctx`. Returns the
    /// registered names. Any single failure fails the whole load: a node
    /// serving a PARTIAL catalog would answer joins with "table not found"
    /// on exactly the tables that matter.
    pub fn register_all(&self, ctx: &mut ExecutionContext) -> Result<Vec<String>> {
        let names = self.list_filesets()?;
        if names.is_empty() {
            return Err(QueryError::Storage(format!(
                "metastore schema {}/{}/{} contains no filesets; nothing to serve",
                self.metalake, self.catalog, self.schema
            )));
        }
        for name in &names {
            let fs = self.get_fileset(name)?;
            let dir = local_path(&fs.storage_location).ok_or_else(|| {
                QueryError::NotImplemented(format!(
                    "fileset `{name}` points at `{}`; only file:// and local paths are \
                     supported",
                    fs.storage_location
                ))
            })?;
            let path = match &fs.file {
                Some(f) => dir.join(f),
                None => dir,
            };
            match fs.format.as_str() {
                "parquet" => ctx.register_parquet(name, &path)?,
                "iceberg" => ctx.register_iceberg(name, &path, None)?,
                #[cfg(feature = "lance")]
                "lance" => ctx.register_lance(name, &path)?,
                #[cfg(not(feature = "lance"))]
                "lance" => {
                    return Err(QueryError::NotImplemented(format!(
                        "fileset `{name}` is a Lance dataset, but this binary was built \
                         without --features lance"
                    )))
                }
                other => {
                    return Err(QueryError::NotImplemented(format!(
                        "fileset `{name}` declares format `{other}`; parquet, iceberg \
                         and lance are supported"
                    )))
                }
            }
        }
        Ok(names)
    }

    /// GET a Gravitino endpoint and return the parsed body after checking
    /// both the HTTP status and Gravitino's own `code` field.
    fn get_json(&self, path: &str) -> Result<serde_json::Value> {
        let body = http_get(&self.base_url, path)?;
        let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
            QueryError::Storage(format!(
                "metastore {}{path}: non-JSON response: {e}",
                self.base_url
            ))
        })?;
        let code = v["code"].as_i64().unwrap_or(-1);
        if code != 0 {
            return Err(QueryError::Storage(format!(
                "metastore {}{path}: code {code} {}: {}",
                self.base_url,
                v["type"].as_str().unwrap_or("?"),
                v["message"].as_str().unwrap_or("?")
            )));
        }
        Ok(v)
    }
}

/// `file:///a`, `file:/a` (Gravitino's normalization) or a plain path → local
/// path. `None` for any other scheme.
fn local_path(uri: &str) -> Option<PathBuf> {
    if let Some(rest) = uri.strip_prefix("file:") {
        let rest = rest.trim_start_matches("//");
        let p = if rest.starts_with('/') {
            rest.to_string()
        } else {
            format!("/{rest}")
        };
        return Some(PathBuf::from(p));
    }
    if uri.contains("://") {
        return None;
    }
    Some(PathBuf::from(uri))
}

/// Minimal synchronous HTTP/1.1 GET.
///
/// Deliberately std-only: the table loader runs inside `spawn_blocking`, where
/// an async client would need its own runtime, and `reqwest::blocking` builds
/// one per call. A metastore fetch is a handful of small local GETs at node
/// startup; a TcpStream and 30 lines is the whole requirement. HTTPS is
/// refused by name rather than half-supported.
fn http_get(base_url: &str, path: &str) -> Result<Vec<u8>> {
    let hostport = base_url
        .strip_prefix("http://")
        .ok_or_else(|| {
            QueryError::NotImplemented(format!(
                "metastore URL `{base_url}` must be http:// (https is not supported)"
            ))
        })?
        .trim_end_matches('/');

    let mut stream = std::net::TcpStream::connect(hostport)
        .map_err(|e| QueryError::Storage(format!("metastore {base_url}: connect: {e}")))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .and_then(|_| stream.set_write_timeout(Some(Duration::from_secs(30))))
        .map_err(|e| QueryError::Storage(format!("metastore {base_url}: socket: {e}")))?;

    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {hostport}\r\nAccept: application/vnd.gravitino.v1+json\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| QueryError::Storage(format!("metastore {base_url}: write: {e}")))?;

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|e| QueryError::Storage(format!("metastore {base_url}: read: {e}")))?;

    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            QueryError::Storage(format!(
                "metastore {base_url}{path}: malformed HTTP response"
            ))
        })?;
    let head = String::from_utf8_lossy(&raw[..header_end]);
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            QueryError::Storage(format!("metastore {base_url}{path}: no HTTP status line"))
        })?;
    let mut body = raw[header_end + 4..].to_vec();

    // `Connection: close` means EOF delimits the body, but chunked responses
    // still carry their framing; strip it when declared.
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body).ok_or_else(|| {
            QueryError::Storage(format!(
                "metastore {base_url}{path}: malformed chunked response"
            ))
        })?;
    }

    // 4xx/5xx bodies are still Gravitino JSON with code/type/message; return
    // them so get_json produces the server's own explanation. Anything
    // non-JSON will fail there with the status preserved in context.
    if status >= 400 && body.is_empty() {
        return Err(QueryError::Storage(format!(
            "metastore {base_url}{path}: HTTP {status} with empty body"
        )));
    }
    Ok(body)
}

/// Decode HTTP/1.1 chunked transfer encoding.
fn dechunk(mut b: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let line_end = b.windows(2).position(|w| w == b"\r\n")?;
        let size =
            usize::from_str_radix(std::str::from_utf8(&b[..line_end]).ok()?.trim(), 16).ok()?;
        b = &b[line_end + 2..];
        if size == 0 {
            return Some(out);
        }
        if b.len() < size + 2 {
            return None;
        }
        out.extend_from_slice(&b[..size]);
        b = &b[size + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_path_accepts_gravitinos_normalizations() {
        for (uri, want) in [
            ("file:///a/b", Some("/a/b")),
            ("file:/a/b", Some("/a/b")),
            ("/a/b", Some("/a/b")),
        ] {
            assert_eq!(local_path(uri), want.map(PathBuf::from), "{uri}");
        }
        assert_eq!(local_path("s3://bucket/k"), None);
        assert_eq!(local_path("hdfs://nn/x"), None);
    }

    #[test]
    fn dechunk_reassembles_a_two_chunk_body() {
        let b = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        assert_eq!(dechunk(b).unwrap(), b"hello world");
    }

    /// End-to-end against a real socket: a hand-rolled HTTP client deserves a
    /// hand-rolled server proving it parses status lines, headers and bodies.
    #[test]
    fn http_get_speaks_http_1_1_to_a_real_socket() {
        use std::io::Write as _;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let t = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut s, &mut buf);
            let body = br#"{"code":0,"identifiers":[]}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            s.write_all(resp.as_bytes()).unwrap();
            s.write_all(body).unwrap();
        });
        let got = http_get(&format!("http://{addr}"), "/api/x").unwrap();
        assert_eq!(got, br#"{"code":0,"identifiers":[]}"#);
        t.join().unwrap();
    }

    #[test]
    fn https_is_refused_by_name() {
        let e = http_get("https://x", "/y").unwrap_err();
        assert!(e.to_string().contains("https"), "{e}");
    }
}
