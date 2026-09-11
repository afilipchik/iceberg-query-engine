//! Exact decimal ordering with a scale factor bound once, without allocation.
use std::cmp::Ordering;
#[derive(Clone, Copy, Debug)]
pub(super) struct ScaledComparison {
    swap: bool,
    factor: Option<i128>,
}
impl ScaledComparison {
    pub(super) fn new(left_scale: i8, right_scale: i8) -> Self {
        let delta = i16::from(left_scale) - i16::from(right_scale);
        Self {
            swap: delta < 0,
            factor: 10i128.checked_pow(u32::from(delta.unsigned_abs())),
        }
    }
    #[inline]
    pub(super) fn compare(self, left: i128, right: i128) -> Ordering {
        let (a, b) = if self.swap {
            (right, left)
        } else {
            (left, right)
        };
        let result = if b == 0 {
            a.cmp(&0)
        } else if let Some(scaled) = self.factor.and_then(|f| b.checked_mul(f)) {
            a.cmp(&scaled)
        } else {
            // The scaled nonzero coefficient lies beyond the entire i128
            // domain. Its sign proves ordering; no large integer is needed.
            if b > 0 {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        };
        if self.swap {
            result.reverse()
        } else {
            result
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_scaled_order_covers_signs_extremes_and_fractional_digits() {
        for a in -20i128..=20 {
            for b in -20i128..=20 {
                for sa in -2i8..=2 {
                    for sb in -2i8..=2 {
                        let expected = (a * 10i128.pow((2 - sa) as u32))
                            .cmp(&(b * 10i128.pow((2 - sb) as u32)));
                        assert_eq!(ScaledComparison::new(sa, sb).compare(a, b), expected);
                    }
                }
            }
        }
        for (a, sa, b, sb, expected) in [
            (i128::MAX, 1, i128::MAX, 0, Ordering::Less),
            (i128::MIN, 1, i128::MIN, 0, Ordering::Greater),
            (i128::MIN, 127, -1, -128, Ordering::Greater),
            (i128::MAX, 127, 1, -128, Ordering::Less),
            (0, 127, 0, -128, Ordering::Equal),
            (1, 127, 0, -128, Ordering::Greater),
            (-1, -128, 0, 127, Ordering::Less),
            (12301, 2, 123, 0, Ordering::Greater),
            (-12301, 2, -123, 0, Ordering::Less),
        ] {
            assert_eq!(ScaledComparison::new(sa, sb).compare(a, b), expected);
            assert_eq!(
                ScaledComparison::new(sb, sa).compare(b, a),
                expected.reverse()
            );
        }
    }

    #[test]
    fn admitted_decimal_predicates_preserve_mixed_scales_nulls_and_slices() {
        use crate::{
            execution::MemoryPool,
            physical::compiled_expr::CompiledPredicate,
            planner::{BinaryOp, Expr},
        };
        use arrow::{
            array::{ArrayRef, Decimal128Array, Int64Array},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        let a = [
            Some(12301),
            Some(-12301),
            None,
            Some(i128::MAX / 100),
            Some(-i128::MAX / 100),
            Some(0),
            Some(100),
            Some(-100),
        ];
        let b = [123, -123, 0, i64::MAX, i64::MIN, 0, 1, -1];
        let c = [12, -12, 0, 1, -1, 0, 0, 0];
        let batch = RecordBatch::try_from_iter(vec![
            (
                "a",
                Arc::new(
                    Decimal128Array::from((0..1057).map(|i| a[i % 8]).collect::<Vec<_>>())
                        .with_precision_and_scale(38, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "b",
                Arc::new(Int64Array::from(
                    (0..1057).map(|i| b[i % 8]).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "c",
                Arc::new(
                    Decimal128Array::from((0..1057).map(|i| c[i % 8]).collect::<Vec<_>>())
                        .with_precision_and_scale(35, -1)
                        .unwrap(),
                ) as ArrayRef,
            ),
        ])
        .unwrap()
        .slice(3, 1041);
        for (column, orders) in [
            (
                "b",
                [
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Equal,
                    Ordering::Equal,
                ],
            ),
            (
                "c",
                [
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Greater,
                    Ordering::Less,
                    Ordering::Equal,
                    Ordering::Greater,
                    Ordering::Less,
                ],
            ),
        ] {
            for op in [
                BinaryOp::Eq,
                BinaryOp::NotEq,
                BinaryOp::Lt,
                BinaryOp::LtEq,
                BinaryOp::Gt,
                BinaryOp::GtEq,
            ] {
                let expr = Expr::BinaryExpr {
                    left: Box::new(Expr::column("a")),
                    op,
                    right: Box::new(Expr::column(column)),
                };
                let pool = MemoryPool::new(1024 * 1024);
                let bound = CompiledPredicate::compile_reserved(&expr, &batch.schema(), &pool)
                    .unwrap()
                    .unwrap();
                let mask = bound.evaluate_admitted(&batch, &pool).unwrap().unwrap();
                let expected = (3..1044)
                    .map(|i| {
                        if i % 8 == 2 {
                            return None;
                        }
                        let order = orders[i % 8];
                        Some(match op {
                            BinaryOp::Eq => order == Ordering::Equal,
                            BinaryOp::NotEq => order != Ordering::Equal,
                            BinaryOp::Lt => order == Ordering::Less,
                            BinaryOp::LtEq => order != Ordering::Greater,
                            BinaryOp::Gt => order == Ordering::Greater,
                            BinaryOp::GtEq => order != Ordering::Less,
                            _ => unreachable!(),
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(mask.iter().collect::<Vec<_>>(), expected, "{column} {op:?}");
                drop(bound);
                assert!(pool.used() > 0);
                drop(mask);
                assert_eq!(pool.used(), 0);
            }
        }
    }
}
