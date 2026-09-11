//! Pre-admitted decompressed Parquet page bodies for audited slice codecs.
//! Encoded input and parsed headers remain caller-owned. This does not parse
//! page headers or admit column readers, definition levels or Arrow output.
use crate::{
    execution::{MemoryPool, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::buffer::Buffer;
use parquet::basic::Compression;

fn invalid(message: impl std::fmt::Display) -> QueryError {
    QueryError::Storage(format!("admitted Parquet page: {message}"))
}

fn zstd_decode(encoded: &[u8], output: &mut [u8], pool: &MemoryPool) -> Result<()> {
    if encoded.is_empty() && output.is_empty() {
        return Ok(()); // V2 level-only body.
    }
    // SAFETY: the pinned C function takes no pointers and reports sizeof(DCtx).
    let bytes = unsafe { zstd_sys::ZSTD_estimateDCtxSize() };
    let words = bytes.div_ceil(std::mem::size_of::<u64>());
    let workspace_bytes = words
        .checked_mul(8)
        .ok_or_else(|| invalid("ZSTD workspace overflow"))?;
    let mut workspace = ReservedBufferBuilder::<u64>::with_capacity(pool, words)?;
    workspace.extend_reserved(words, std::iter::repeat(0))?;
    // SAFETY: initialized u64 storage has the reported capacity and remains
    // uniquely owned and live for this whole call. C checks 8-byte alignment
    // before using it (a target with weaker u64 alignment may refuse). Static
    // DCtx initialization never owns/frees the workspace. The single-pass API
    // uses context/destination scratch; legacy frames requiring allocation are
    // rejected by the pinned implementation for a static DCtx. No dictionary or
    // streaming-buffer APIs are used. Input and output are valid disjoint slices.
    let written = unsafe {
        let context = zstd_sys::ZSTD_initStaticDCtx(
            workspace.as_mut_slice().as_mut_ptr().cast(),
            workspace_bytes,
        );
        if context.is_null() {
            return Err(invalid("ZSTD static context initialization failed"));
        }
        let written = zstd_sys::ZSTD_decompressDCtx(
            context,
            output.as_mut_ptr().cast(),
            output.len(),
            encoded.as_ptr().cast(),
            encoded.len(),
        );
        if zstd_sys::ZSTD_isError(written) != 0 {
            let name = std::ffi::CStr::from_ptr(zstd_sys::ZSTD_getErrorName(written));
            return Err(invalid(format!("ZSTD: {}", name.to_string_lossy())));
        }
        written
    };
    if written != output.len() {
        return Err(invalid("ZSTD length differs from page header"));
    }
    Ok(())
}

/// `prefix` is zero for V1; for V2 it is the checked sum of the uncompressed
/// repetition and definition sections. A V2 page with is_compressed=false must
/// use UNCOMPRESSED regardless of its column codec. Other codecs refuse until
/// they have a bounded destination/scratch contract.
pub(crate) fn decode_page_body(
    codec: Compression,
    encoded: &[u8],
    decoded_size: usize,
    prefix: usize,
    pool: &MemoryPool,
) -> Result<Buffer> {
    if prefix > encoded.len() || prefix > decoded_size {
        return Err(invalid("level prefix exceeds page extent"));
    }
    let encoded_values = &encoded[prefix..];
    let value_size = decoded_size - prefix;
    match codec {
        Compression::UNCOMPRESSED => {
            if encoded.len() != decoded_size {
                return Err(invalid("uncompressed extent differs from header"));
            }
        }
        Compression::SNAPPY => {
            // V2 can contain only its uncompressed levels, with no value body.
            let actual = if value_size == 0 && encoded_values.is_empty() {
                0
            } else {
                snap::raw::decompress_len(encoded_values).map_err(invalid)?
            };
            if actual != value_size {
                return Err(invalid("Snappy length differs from page header"));
            }
        }
        Compression::ZSTD(_) => {}
        _ => {
            return Err(QueryError::NotImplemented(format!(
                "admitted page codec {codec:?}"
            )))
        }
    }
    // Admission occurs before initialization. The slice decoder cannot grow the
    // destination, and the pinned snap decoder uses only stack state and slices.
    let mut output = ReservedBufferBuilder::<u8>::with_capacity(pool, decoded_size)?;
    output.extend_reserved(decoded_size, std::iter::repeat(0))?;
    match codec {
        Compression::UNCOMPRESSED => output.as_mut_slice().copy_from_slice(encoded),
        Compression::SNAPPY => {
            output.as_mut_slice()[..prefix].copy_from_slice(&encoded[..prefix]);
            let written = if value_size == 0 && encoded_values.is_empty() {
                0
            } else {
                snap::raw::Decoder::new()
                    .decompress(encoded_values, &mut output.as_mut_slice()[prefix..])
                    .map_err(invalid)?
            };
            if written != value_size {
                return Err(invalid("decoded extent differs"));
            }
        }
        Compression::ZSTD(_) => {
            output.as_mut_slice()[..prefix].copy_from_slice(&encoded[..prefix]);
            zstd_decode(encoded_values, &mut output.as_mut_slice()[prefix..], pool)?;
        }
        _ => unreachable!("codec checked before allocation"),
    }
    Ok(output.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zstd_encoded(input: &[u8]) -> Vec<u8> {
        // Test oracle encoding only, not the admitted production decoder.
        unsafe {
            let mut output = vec![0u8; zstd_sys::ZSTD_compressBound(input.len())];
            let size = zstd_sys::ZSTD_compress(
                output.as_mut_ptr().cast(),
                output.len(),
                input.as_ptr().cast(),
                input.len(),
                3,
            );
            assert_eq!(zstd_sys::ZSTD_isError(size), 0);
            output.truncate(size);
            output
        }
    }

    #[test]
    fn zstd_static_workspace_and_destination_are_admitted_and_exact() {
        // This unsafe-path audit applies to the pinned implementation, including
        // builds that could otherwise select an external library through pkg-config.
        assert_eq!(unsafe { zstd_sys::ZSTD_versionNumber() }, 10506);
        for size in [0, 1, 131071, 131072, 131073, 524288] {
            let input: Vec<u8> = (0..size).map(|i| ((i * 193 + i / 7) % 251) as u8).collect();
            let compressed = zstd_encoded(&input);
            for prefix in [0, 7] {
                let mut encoded = vec![9; prefix];
                encoded.extend_from_slice(&compressed);
                let pool = MemoryPool::new(4 * 1024 * 1024);
                let decoded = decode_page_body(
                    Compression::ZSTD(Default::default()),
                    &encoded,
                    prefix + size,
                    prefix,
                    &pool,
                )
                .unwrap();
                assert_eq!(&decoded[..prefix], vec![9; prefix].as_slice());
                assert_eq!(&decoded[prefix..], input.as_slice());
                assert_eq!(
                    pool.used(),
                    decoded.len() + 512,
                    "workspace must be released after decode"
                );
                let retained = decoded.slice(prefix);
                drop(decoded);
                assert!(pool.used() > 0);
                drop(retained);
                assert_eq!(pool.used(), 0);
            }
        }
    }

    #[test]
    fn zstd_denial_and_corruption_release_every_reservation() {
        let input = vec![42u8; 8192];
        let encoded = zstd_encoded(&input);
        let codec = Compression::ZSTD(Default::default());
        let small = MemoryPool::new(16384);
        assert!(decode_page_body(codec, &encoded, input.len(), 0, &small)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(small.used(), 0);
        let pool = MemoryPool::new(1024 * 1024);
        for body in [&encoded[..encoded.len() - 1], &[0, 1, 2, 3][..]] {
            assert!(decode_page_body(codec, body, input.len(), 0, &pool).is_err());
            assert_eq!(pool.used(), 0);
        }
        for declared in [input.len() - 1, input.len() + 1] {
            assert!(decode_page_body(codec, &encoded, declared, 0, &pool).is_err());
            assert_eq!(pool.used(), 0);
        }
        let empty = decode_page_body(codec, &[4, 5], 2, 2, &pool).unwrap();
        assert_eq!(empty.as_slice(), &[4, 5]);
        drop(empty);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn snappy_prefix_and_slice_owners_are_exact() {
        let values = b"long repeated values long repeated values";
        let mut encoded = vec![9, 8, 7];
        encoded.extend(snap::raw::Encoder::new().compress_vec(values).unwrap());
        let pool = MemoryPool::new(65536);
        let output =
            decode_page_body(Compression::SNAPPY, &encoded, values.len() + 3, 3, &pool).unwrap();
        assert_eq!(&output[..3], &[9, 8, 7]);
        assert_eq!(&output[3..], values);
        let copy = output.clone();
        let slice = output.slice_with_length(3, values.len());
        drop(output);
        drop(copy);
        assert!(pool.used() >= values.len() + 3);
        assert_eq!(slice.as_slice(), values);
        drop(slice);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn length_checks_precede_admission_and_bad_payload_releases_it() {
        let pool = MemoryPool::new(65536);
        let encoded = snap::raw::Encoder::new().compress_vec(b"abc").unwrap();
        assert!(decode_page_body(Compression::SNAPPY, &encoded, 999, 0, &pool).is_err());
        assert!(decode_page_body(Compression::UNCOMPRESSED, b"a", 2, 0, &pool).is_err());
        assert!(decode_page_body(Compression::SNAPPY, &encoded, 3, 10, &pool).is_err());
        assert_eq!(pool.reserved_peak(), 0);
        // A valid Snappy size prefix followed by a truncated literal.
        assert!(decode_page_body(Compression::SNAPPY, &[3, 8, b'a'], 3, 0, &pool).is_err());
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn denial_and_uncompressed_empty_body_are_clean() {
        let encoded = snap::raw::Encoder::new().compress_vec(&[1; 8192]).unwrap();
        let pool = MemoryPool::new(4096);
        assert!(
            decode_page_body(Compression::SNAPPY, &encoded, 8192, 0, &pool)
                .unwrap_err()
                .is_memory_limit()
        );
        assert_eq!(pool.used(), 0);
        let empty = decode_page_body(Compression::UNCOMPRESSED, &[], 0, 0, &pool).unwrap();
        assert!(empty.is_empty());
        drop(empty);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn level_only_pages_and_unsupported_codecs_are_explicit() {
        let pool = MemoryPool::new(4096);
        let levels = [8, 0];
        let page = decode_page_body(Compression::SNAPPY, &levels, 2, 2, &pool).unwrap();
        assert_eq!(page.as_slice(), levels);
        drop(page);
        let encoded_empty = snap::raw::Encoder::new().compress_vec(&[]).unwrap();
        let page = decode_page_body(Compression::SNAPPY, &encoded_empty, 0, 0, &pool).unwrap();
        assert!(page.is_empty());
        drop(page);
        assert!(matches!(
            decode_page_body(Compression::LZ4_RAW, &[], 0, 0, &pool),
            Err(QueryError::NotImplemented(_))
        ));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn decoded_page_and_plain_output_hold_separate_charges() {
        let plain = [3u8, 0, 0, 0, b'o', b'n', b'e', 3, 0, 0, 0, b't', b'w', b'o'];
        let encoded = snap::raw::Encoder::new().compress_vec(&plain).unwrap();
        let pool = MemoryPool::new(65536);
        let page = decode_page_body(Compression::SNAPPY, &encoded, plain.len(), 0, &pool).unwrap();
        let page_charge = pool.used();
        let output = {
            let mut decoder =
                crate::storage::admitted_plain_utf8::PlainUtf8Decoder::new(&page, 2, None).unwrap();
            decoder.next(8, 1024, &pool).unwrap().unwrap()
        };
        assert_eq!(
            output.iter().collect::<Vec<_>>(),
            vec![Some("one"), Some("two")]
        );
        assert!(pool.used() > page_charge);
        drop(page);
        assert!(pool.used() > 0);
        drop(output);
        assert_eq!(pool.used(), 0);
    }
}
