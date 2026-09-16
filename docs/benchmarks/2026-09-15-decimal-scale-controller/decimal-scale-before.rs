//! Lossless Arrow Decimal128 scalar: coefficient and scale travel together.
//! rust_decimal has a 96-bit coefficient and cannot represent Arrow's 38 digits.
use crate::error::{QueryError, Result};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

// Precision is array/type metadata. Compute its bounds once at compile time,
// rather than exponentiating once for every validated coefficient.
const DECIMAL_PRECISION_LIMITS: [u128; 39] = {
    let mut limits = [1; 39];
    let mut precision = 1;
    while precision < limits.len() {
        limits[precision] = limits[precision - 1] * 10;
        precision += 1;
    }
    limits
};

#[derive(Debug, Clone, Copy)]
pub struct DecimalValue {
    coefficient: i128,
    scale: i8,
}

impl DecimalValue {
    pub fn new(coefficient: i128, scale: i8) -> Self {
        Self { coefficient, scale }
    }

    pub fn mantissa(self) -> i128 {
        self.coefficient
    }
    pub fn scale(self) -> i8 {
        self.scale
    }
    pub fn to_f64(self) -> f64 {
        self.coefficient as f64 * 10_f64.powi(-i32::from(self.scale))
    }

    /// Rescaling an exact result must not discard fractional digits.
    pub fn rescale(self, scale: i8) -> Result<i128> {
        if scale == self.scale {
            return Ok(self.coefficient);
        }
        if self.coefficient == 0 {
            return Ok(0);
        }
        let delta = i16::from(scale) - i16::from(self.scale);
        let factor = 10_i128
            .checked_pow(delta.unsigned_abs() as u32)
            .ok_or_else(|| QueryError::Execution("decimal rescale overflow".into()))?;
        if delta >= 0 {
            self.coefficient
                .checked_mul(factor)
                .ok_or_else(|| QueryError::Execution("decimal rescale overflow".into()))
        } else if self.coefficient % factor == 0 {
            Ok(self.coefficient / factor)
        } else {
            Err(QueryError::Execution(
                "decimal rescale would lose fractional digits".into(),
            ))
        }
    }

    pub fn validate_precision(coefficient: i128, precision: u8) -> Result<()> {
        if !(1..=38).contains(&precision)
            || coefficient.unsigned_abs() >= DECIMAL_PRECISION_LIMITS[usize::from(precision)]
        {
            return Err(QueryError::Execution("decimal precision overflow".into()));
        }
        Ok(())
    }

    fn normalized(self) -> (i128, i16) {
        let (mut coefficient, mut scale) = (self.coefficient, i16::from(self.scale));
        if coefficient == 0 {
            return (0, 0);
        }
        while coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        (coefficient, scale)
    }
}

impl PartialEq for DecimalValue {
    fn eq(&self, other: &Self) -> bool {
        self.normalized() == other.normalized()
    }
}
impl Eq for DecimalValue {}
impl Hash for DecimalValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.normalized().hash(state);
    }
}
impl PartialOrd for DecimalValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DecimalValue {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.scale == other.scale {
            return self.coefficient.cmp(&other.coefficient);
        }
        let sign = self.coefficient.signum().cmp(&other.coefficient.signum());
        if sign != Ordering::Equal {
            return sign;
        }
        if self.coefficient == 0 {
            return Ordering::Equal;
        }
        // Compare decimal exponents and significands, without overflowing a
        // rescale or rounding through f64 even at the ends of Arrow's range.
        let a = self.coefficient.unsigned_abs().to_string();
        let b = other.coefficient.unsigned_abs().to_string();
        let exponent = (a.len() as i16 - i16::from(self.scale))
            .cmp(&(b.len() as i16 - i16::from(other.scale)));
        let order = exponent.then_with(|| {
            let n = a.len().max(b.len());
            a.bytes()
                .chain(std::iter::repeat(b'0'))
                .take(n)
                .cmp(b.bytes().chain(std::iter::repeat(b'0')).take(n))
        });
        if self.coefficient < 0 {
            order.reverse()
        } else {
            order
        }
    }
}
impl std::ops::Neg for DecimalValue {
    type Output = Self;
    fn neg(self) -> Self {
        Self::new(-self.coefficient, self.scale)
    }
}
impl std::fmt::Display for DecimalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let digits = self.coefficient.unsigned_abs().to_string();
        if self.coefficient < 0 {
            write!(f, "-")?;
        }
        if self.scale <= 0 {
            write!(
                f,
                "{}{}",
                digits,
                "0".repeat((-i16::from(self.scale)) as usize)
            )
        } else {
            let scale = self.scale as usize;
            if scale >= digits.len() {
                write!(f, "0.{}{}", "0".repeat(scale - digits.len()), digits)
            } else {
                let split = digits.len() - scale;
                write!(f, "{}.{}", &digits[..split], &digits[split..])
            }
        }
    }
}
impl std::str::FromStr for DecimalValue {
    type Err = QueryError;
    fn from_str(text: &str) -> Result<Self> {
        let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
        let coefficient = format!("{integer}{fraction}")
            .parse::<i128>()
            .map_err(|_| QueryError::Parse(format!("invalid decimal literal: {text}")))?;
        Self::validate_precision(coefficient, 38)?;
        if fraction.len() > 38 {
            return Err(QueryError::Parse("decimal scale exceeds 38".into()));
        }
        Ok(Self::new(coefficient, fraction.len() as i8))
    }
}

#[cfg(test)]
mod precision_tests {
    use super::DecimalValue;

    #[test]
    fn decimal_float_conversion_preserves_all_scale_bits() {
        for scale in i8::MIN..=i8::MAX {
            for coefficient in [0, 1, -1, (1i128 << 100) + 1, i128::MIN, i128::MAX] {
                let expected = std::hint::black_box(coefficient) as f64
                    * 10_f64.powi(-i32::from(std::hint::black_box(scale)));
                assert_eq!(
                    DecimalValue::new(coefficient, scale).to_f64().to_bits(),
                    expected.to_bits(),
                    "coefficient={coefficient}, scale={scale}"
                );
            }
        }
    }

    #[test]
    #[ignore = "manual conversion cost diagnostic; not a timing acceptance test"]
    fn measure_decimal_float_conversion() {
        use std::{hint::black_box, time::Instant};
        let start = Instant::now();
        let mut sum = 0.0;
        for i in 0..10_000_000i128 {
            // Include positive, zero and negative scales; prevent constant folding.
            let scale = black_box((i % 9) as i8 - 4);
            sum += black_box(DecimalValue::new(black_box(i), scale)).to_f64();
        }
        eprintln!("decimal_float_conversion elapsed_ns={} checksum_bits={}",
            start.elapsed().as_nanos(), black_box(sum).to_bits());
    }

    #[test]
    fn signed_decimal_precision_boundaries_remain_exact() {
        for precision in 1..=38 {
            // Construct independent decimal boundary values from their digits.
            let maximum: i128 = "9".repeat(precision as usize).parse().unwrap();
            let overflow: i128 = format!("1{}", "0".repeat(precision as usize))
                .parse()
                .unwrap();
            for value in [0, maximum, -maximum] {
                DecimalValue::validate_precision(value, precision).unwrap();
            }
            for value in [overflow, -overflow] {
                let error = DecimalValue::validate_precision(value, precision).unwrap_err();
                assert!(error.to_string().contains("decimal precision overflow"));
            }
        }
        for value in [i128::MIN, i128::MAX] {
            assert!(DecimalValue::validate_precision(value, 38).is_err());
        }
        for precision in [0, 39, 127, 255] {
            assert!(DecimalValue::validate_precision(0, precision).is_err());
        }
    }
}
