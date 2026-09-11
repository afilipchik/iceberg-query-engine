use super::*;
use crate::execution::MemoryPool;
use crate::storage::admitted_page_read::PageSource;
use std::{cell::Cell, io};
const HEADER: &[u8] = &[
    0x15, 0, 0x15, 6, 0x15, 6, 0x2c, 0x15, 2, 0x15, 0, 0x15, 6, 0x15, 6, 0, 0,
];
struct Source {
    bytes: Vec<u8>,
    calls: Cell<usize>,
    read: Cell<usize>,
    fail_call: Option<usize>,
}
impl Source {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            calls: Cell::new(0),
            read: Cell::new(0),
            fail_call: None,
        }
    }
}
impl PageSource for Source {
    fn len(&self) -> io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }
    fn read_at(&self, target: &mut [u8], offset: u64) -> io::Result<usize> {
        let call = self.calls.get() + 1;
        self.calls.set(call);
        if self.fail_call == Some(call) {
            return Err(io::Error::other("injected header source failure"));
        }
        let offset = usize::try_from(offset).unwrap();
        let n = target.len().min(self.bytes.len().saturating_sub(offset));
        target[..n].copy_from_slice(&self.bytes[offset..offset + n]);
        self.read.set(self.read.get() + n);
        Ok(n)
    }
}
// Discover the existing buffer owner's fixed charge independently of header logic.
fn buffer_budget(payload: usize) -> usize {
    let pool = MemoryPool::new(4096);
    let empty = crate::execution::ReservedBufferBuilder::<u8>::new(&pool).unwrap();
    let owner = pool.used();
    assert!(owner > 0);
    drop(empty);
    assert_eq!(pool.used(), 0);
    owner.checked_add(payload).unwrap()
}
fn large() -> (Vec<u8>, usize) {
    let mut bytes = HEADER[..HEADER.len() - 1].to_vec();
    // Unknown field9 binary(300 bytes), then field10 list<bool>(200 values).
    bytes.extend_from_slice(&[0x48, 0xac, 2]);
    bytes.extend(std::iter::repeat_n(b'x', 300));
    bytes.extend_from_slice(&[0x19, 0xf1, 0xc8, 1]);
    bytes.extend(std::iter::repeat_n(1, 200));
    bytes.push(0);
    let len = bytes.len();
    bytes.extend_from_slice(&[0, 0, 0]);
    (bytes, len)
}
#[test]
fn tiny_header_admits_without_the_whole_read_window() {
    let mut bytes = HEADER.to_vec();
    bytes.resize(65536, 0);
    let source = Source::new(bytes);
    let pool = MemoryPool::new(buffer_budget(64));
    let header =
        AdmittedPageHeader::read(&source, 0, source.bytes.len() as u64, 32768, &pool).unwrap();
    assert_eq!(header.envelope().header_bytes, HEADER.len());
    assert_eq!(header.subheader(0), Some(&HEADER[7..16]));
    assert!(matches!(
        header.typed().unwrap(),
        TypedPageHeader::DataV1 { values: 1, .. }
    ));
    assert!(source.read.get() <= 64);
    assert!(pool.used() > 0 && pool.used() <= buffer_budget(64));
    drop(header);
    assert_eq!(pool.used(), 0);
}
#[test]
fn header_prefix_growth_releases_the_prior_reservation() {
    let (bytes, len) = large();
    let source = Source::new(bytes);
    let pool = MemoryPool::new(buffer_budget(len));
    let header =
        AdmittedPageHeader::read(&source, 0, source.bytes.len() as u64, 4096, &pool).unwrap();
    assert_eq!(header.envelope().header_bytes, len);
    assert_eq!(header.subheader(0), Some(&HEADER[7..16]));
    assert!(source.calls.get() > 1);
    assert!(pool.used() > 0 && pool.used() <= buffer_budget(len));
    drop(header);
    assert_eq!(pool.used(), 0);
}
#[test]
fn growth_refusal_and_io_failure_leave_no_header_or_charge() {
    let (bytes, len) = large();
    let source = Source::new(bytes.clone());
    let pool = MemoryPool::new(buffer_budget(len - 1));
    let result = AdmittedPageHeader::read(&source, 0, source.bytes.len() as u64, 4096, &pool);
    let error = match result {
        Err(e) => e,
        Ok(_) => panic!("header exceeded its budget"),
    };
    assert!(error.is_memory_limit(), "{error}");
    assert!(source.calls.get() > 0);
    assert_eq!(pool.used(), 0);
    let mut source = Source::new(bytes);
    source.fail_call = Some(2);
    let pool = MemoryPool::new(2 * len);
    let result = AdmittedPageHeader::read(&source, 0, source.bytes.len() as u64, 4096, &pool);
    let error = match result {
        Err(e) => e,
        Ok(_) => panic!("source failure was ignored"),
    };
    assert!(error.to_string().contains("injected header source failure"));
    assert_eq!(source.calls.get(), 2);
    assert_eq!(pool.used(), 0);
}
#[test]
fn malformed_capped_and_empty_headers_do_not_retry() {
    let mut bytes = vec![0x0f];
    bytes.resize(65536, 0);
    let source = Source::new(bytes);
    let pool = MemoryPool::new(4096);
    assert!(AdmittedPageHeader::read(&source, 0, source.bytes.len() as u64, 1024, &pool).is_err());
    assert_eq!(source.calls.get(), 1);
    assert_eq!(pool.used(), 0);
    let mut bytes = HEADER.to_vec();
    bytes.extend_from_slice(&[0, 0, 0]);
    let source = Source::new(bytes);
    assert!(matches!(
        AdmittedPageHeader::read(
            &source,
            0,
            source.bytes.len() as u64,
            HEADER.len() - 1,
            &pool
        ),
        Err(QueryError::NotImplemented(_))
    ));
    assert_eq!(source.calls.get(), 1);
    assert_eq!(pool.used(), 0);
    let source = Source::new(vec![]);
    assert!(AdmittedPageHeader::read(&source, 0, 0, 1024, &pool).is_err());
    assert_eq!(source.calls.get(), 0);
    assert_eq!(pool.used(), 0);
}
#[test]
fn every_truncated_prefix_terminates_and_releases_reservations() {
    let (bytes, _) = large();
    for end in 0..bytes.len() {
        let source = Source::new(bytes[..end].to_vec());
        let pool = MemoryPool::new(2 * bytes.len());
        assert!(
            AdmittedPageHeader::read(&source, 0, end as u64, 4096, &pool).is_err(),
            "prefix={end}"
        );
        assert!(source.calls.get() < 16, "prefix={end}");
        assert_eq!(pool.used(), 0);
    }
}
#[test]
fn allocation_refusal_precedes_io_and_exact_header_cap_succeeds() {
    let mut bytes = HEADER.to_vec();
    bytes.extend_from_slice(&[0, 0, 0]);
    let source = Source::new(bytes);
    let pool = MemoryPool::new(0);
    assert!(
        matches!(AdmittedPageHeader::read(&source,0,source.bytes.len() as u64,1024,&pool),Err(e) if e.is_memory_limit())
    );
    assert_eq!(source.calls.get(), 0);
    let pool = MemoryPool::new(buffer_budget(HEADER.len()));
    let header =
        AdmittedPageHeader::read(&source, 0, source.bytes.len() as u64, HEADER.len(), &pool)
            .unwrap();
    assert_eq!(header.envelope().header_bytes, HEADER.len());
    assert!(header.typed().is_ok());
    drop(header);
    assert_eq!(pool.used(), 0);
}
