//! Chunk-local casts with the same rounding operations as Arrow's strict casts.
use super::*;

impl Compiler {
    pub(super) fn coerce_float(&mut self, source: Src, data_type: &DataType) -> Option<Src> {
        let divisor = match data_type {
            DataType::Float64 => return Some(source),
            DataType::Int32 | DataType::Int64 => 1.0,
            DataType::Decimal128(_, scale) => 10_f64.powi(i32::from(*scale)),
            _ => return None,
        };
        Some(match source {
            Src::Col(col) => {
                // Column slots include the exact physical type. Reuse a conversion
                // across BETWEEN bounds without allocating another lookup table.
                if let Some(dst) = self.prog.iter().find_map(|instruction| match instruction {
                    Instr::CoerceF64 {
                        col: existing, dst, ..
                    } if *existing == col => Some(*dst),
                    _ => None,
                }) {
                    return Some(Src::Reg(dst));
                }
                let dst = self.falloc()?;
                self.prog.push(Instr::CoerceF64 { col, divisor, dst });
                Src::Reg(dst)
            }
            Src::LitI32(value) => Src::LitF64(value as f64),
            Src::LitI64(value) => Src::LitF64(value as f64),
            Src::LitI128(value) => Src::LitF64(value as f64 / divisor),
            _ => return None,
        })
    }
}
