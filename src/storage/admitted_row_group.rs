//! Validated adapter from caller-owned footer metadata to admitted flat batches.
//! Footer parsing/cache/schema storage are external contracts, not admitted here.
use super::{
    admitted_batch::AdmittedBatchReader, admitted_column_pages::AdmittedColumnPages,
    admitted_flat_column::AdmittedFlatColumn,
};
use crate::{
    execution::{reserved_vec::ReservedVec, MemoryPool},
    QueryError, Result,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use parquet::{
    arrow::arrow_reader::ArrowReaderMetadata,
    basic::{ConvertedType as C, LogicalType, TimeUnit as PUnit, Type},
    schema::types::ColumnDescriptor,
};
use std::fs::File;
fn unsupported(message: &str) -> QueryError {
    QueryError::NotImplemented(format!("admitted row group: {message}"))
}
fn annotation(column: &ColumnDescriptor, data_type: &DataType) -> bool {
    let converted = column.converted_type();
    match data_type {
        DataType::Utf8 => column.physical_type() == Type::BYTE_ARRAY && converted == C::UTF8,
        DataType::Int32 => {
            matches!(converted, C::NONE | C::INT_32)
                && column.logical_type_ref().is_none_or(|t| {
                    matches!(
                        t,
                        LogicalType::Integer {
                            bit_width: 32,
                            is_signed: true
                        }
                    )
                })
        }
        DataType::Int64 => {
            matches!(converted, C::NONE | C::INT_64)
                && column.logical_type_ref().is_none_or(|t| {
                    matches!(
                        t,
                        LogicalType::Integer {
                            bit_width: 64,
                            is_signed: true
                        }
                    )
                })
        }
        DataType::UInt32 => converted == C::UINT_32,
        DataType::UInt64 => converted == C::UINT_64,
        DataType::Float32 | DataType::Float64 | DataType::Boolean => {
            converted == C::NONE && column.logical_type_ref().is_none()
        }
        DataType::Date32 => converted == C::DATE,
        DataType::Decimal128(p, s) => {
            converted == C::DECIMAL
                && column.type_precision() == i32::from(*p)
                && column.type_scale() == i32::from(*s)
        }
        DataType::Timestamp(unit, zone) => match column.logical_type_ref() {
            Some(LogicalType::Timestamp {
                unit: physical,
                is_adjusted_to_u_t_c,
            }) => {
                let same = matches!(
                    (unit, physical),
                    (TimeUnit::Millisecond, PUnit::MILLIS)
                        | (TimeUnit::Microsecond, PUnit::MICROS)
                        | (TimeUnit::Nanosecond, PUnit::NANOS)
                );
                same && zone.is_some() == *is_adjusted_to_u_t_c
            }
            _ => false,
        },
        _ => false,
    }
}
/// Projection order and repetitions are explicit; no set conversion drops them.
/// Selection must occur before execution; unsupported inputs do not trigger a
/// fallback after a page/column was consumed.
pub(crate) fn open(
    file: &File,
    metadata: &ArrowReaderMetadata,
    group: usize,
    projection: &[usize],
    output_schema: SchemaRef,
    pool: &MemoryPool,
) -> Result<AdmittedBatchReader<FileColumn>> {
    let parquet = metadata.parquet_schema();
    if parquet.num_columns() != metadata.schema().fields().len()
        || projection.len() != output_schema.fields().len()
    {
        return Err(unsupported("nested roots or projection/schema mismatch"));
    }
    let group = metadata
        .metadata()
        .row_groups()
        .get(group)
        .ok_or_else(|| unsupported("row group outside metadata"))?;
    if group.num_columns() != parquet.num_columns() {
        return Err(unsupported("row-group column count differs from schema"));
    }
    let rows = usize::try_from(group.num_rows()).map_err(|_| unsupported("invalid row count"))?;
    // Check every selected column before constructing any page reader.
    for (position, index) in projection.iter().copied().enumerate() {
        let field = metadata
            .schema()
            .fields()
            .get(index)
            .ok_or_else(|| unsupported("projection outside schema"))?;
        let column = parquet.column(index);
        let chunk = group.column(index);
        if !matches!(
            chunk.compression(),
            parquet::basic::Compression::UNCOMPRESSED
                | parquet::basic::Compression::SNAPPY
                | parquet::basic::Compression::ZSTD(_)
        ) || chunk.encodings().any(|e| {
            !matches!(
                e,
                parquet::basic::Encoding::PLAIN
                    | parquet::basic::Encoding::PLAIN_DICTIONARY
                    | parquet::basic::Encoding::RLE_DICTIONARY
                    | parquet::basic::Encoding::RLE
                    | parquet::basic::Encoding::BIT_PACKED
            )
        }) {
            return Err(unsupported("codec or encoding requires another reader"));
        }

        if column.max_rep_level() != 0
            || !(0..=1).contains(&column.max_def_level())
            || column.path().parts().len() != 1
        {
            return Err(unsupported("nested column"));
        }
        if field.data_type() != output_schema.field(position).data_type()
            || !annotation(&column, field.data_type())
        {
            return Err(unsupported("logical type/annotation conversion required"));
        }
        if usize::try_from(group.column(index).num_values()).ok() != Some(rows) {
            return Err(unsupported("flat column count differs from row group"));
        }
    }
    let mut columns = ReservedVec::with_capacity(pool, projection.len())?;
    for index in projection.iter().copied() {
        let column = group.column(index);
        let descriptor = column.column_descr();
        let (start, bytes) = column.byte_range();
        let pages = AdmittedColumnPages::new(
            file.try_clone()?,
            start,
            bytes,
            rows as u64,
            column.compression(),
            65536,
        )?;
        let data_type = metadata.schema().field(index).data_type();
        let optional = descriptor.max_def_level() == 1;
        let reader = if data_type == &DataType::Utf8 {
            AdmittedFlatColumn::new(pages, optional)
        } else {
            AdmittedFlatColumn::new_fixed(
                pages,
                optional,
                column.column_type(),
                descriptor.type_length().max(0) as usize,
                data_type.clone(),
            )?
        };
        columns.extend_reserved(1, [reader])?;
    }
    AdmittedBatchReader::new(columns, output_schema, rows, pool)
}
type FileColumn = AdmittedFlatColumn<File>;

#[cfg(test)]
#[path = "admitted_row_group_working_space_tests.rs"]
mod working_space_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::schema::types::{ColumnPath, Type as SchemaType};
    use std::sync::Arc;
    #[test]
    fn temporal_units_and_decimal_scale_cannot_be_reinterpreted() {
        let timestamp = SchemaType::primitive_type_builder("v", Type::INT64)
            .with_logical_type(Some(LogicalType::Timestamp {
                unit: PUnit::MICROS,
                is_adjusted_to_u_t_c: true,
            }))
            .build()
            .unwrap();
        let column =
            ColumnDescriptor::new(Arc::new(timestamp), 1, 0, ColumnPath::new(vec!["v".into()]));
        assert!(annotation(
            &column,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));
        assert!(!annotation(
            &column,
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        ));
        assert!(!annotation(
            &column,
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(!annotation(&column, &DataType::Int64));
        let decimal = SchemaType::primitive_type_builder("v", Type::INT64)
            .with_logical_type(Some(LogicalType::Decimal {
                precision: 12,
                scale: 4,
            }))
            .with_precision(12)
            .with_scale(4)
            .build()
            .unwrap();
        let column =
            ColumnDescriptor::new(Arc::new(decimal), 1, 0, ColumnPath::new(vec!["v".into()]));
        assert!(annotation(&column, &DataType::Decimal128(12, 4)));
        assert!(!annotation(&column, &DataType::Decimal128(12, 2)));
        assert!(!annotation(&column, &DataType::Decimal128(10, 4)));
        assert!(!annotation(&column, &DataType::Int64));
    }
}
