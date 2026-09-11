//! Checked domains for direct-address structures. Statistics can suggest a
//! layout, but narrowing a domain must never wrap it into a smaller allocation.
use crate::error::{QueryError, Result};

pub(crate) fn bounded_i64_width(min: i64, max: i64, limit: usize) -> Option<usize> {
    if min > max {
        return None;
    }
    let width = max as i128 - min as i128 + 1;
    if width as u128 > limit as u128 {
        return None;
    }
    usize::try_from(width).ok()
}

#[inline]
pub(crate) fn checked_i64_index(value: i64, min: i64, width: usize) -> Result<usize> {
    let offset = value as i128 - min as i128;
    if offset < 0 || offset as u128 >= width as u128 {
        return Err(QueryError::Execution(
            "dense aggregate key outside admitted domain".into(),
        ));
    }
    Ok(offset as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_signed_domain_cannot_wrap_to_empty_layout() {
        assert_eq!(bounded_i64_width(i64::MIN, i64::MAX, usize::MAX), None);
        assert_eq!(bounded_i64_width(7, 6, usize::MAX), None);
        assert_eq!(bounded_i64_width(i64::MIN, i64::MIN, 1), Some(1));
        assert_eq!(bounded_i64_width(i64::MAX, i64::MAX, 1), Some(1));
    }
    #[test]
    fn boundary_domains_and_observed_keys_are_checked_independently() {
        for min in [i64::MIN, -2, i64::MAX - 2] {
            assert_eq!(bounded_i64_width(min, min + 2, 3), Some(3));
            assert_eq!(bounded_i64_width(min, min + 2, 2), None);
            for offset in 0..3 {
                assert_eq!(
                    checked_i64_index(min + offset, min, 3).unwrap(),
                    offset as usize
                );
            }
        }
        assert!(checked_i64_index(i64::MAX, i64::MIN, 3).is_err());
        assert!(checked_i64_index(i64::MIN, i64::MAX, 3).is_err());
        assert!(checked_i64_index(7, 7, 0).is_err());
    }
}
