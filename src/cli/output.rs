//! Output formatting for query results
//!
//! Supports multiple output formats:
//! - Table: Pretty-printed ASCII table (default)
//! - CSV: Comma-separated values
//! - JSON: JSON array of objects
//! - Vertical: One column per line (useful for wide results)

use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::io::{self, Write};

/// Output format for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Pretty-printed ASCII table
    #[default]
    Table,
    /// Comma-separated values
    Csv,
    /// JSON array of objects
    Json,
    /// Vertical format (one column per line)
    Vertical,
}

impl OutputFormat {
    /// Parse format from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "table" | "t" => Some(Self::Table),
            "csv" | "c" => Some(Self::Csv),
            "json" | "j" => Some(Self::Json),
            "vertical" | "v" => Some(Self::Vertical),
            _ => None,
        }
    }

    /// Get format name
    pub fn name(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Vertical => "vertical",
        }
    }

    /// Get all format names for help text
    pub fn all_names() -> &'static [&'static str] {
        &["table", "csv", "json", "vertical"]
    }
}

/// Formatter for outputting query results in various formats
pub struct OutputFormatter {
    format: OutputFormat,
    max_rows: Option<usize>,
}

impl OutputFormatter {
    /// Create a new formatter with the given format
    pub fn new(format: OutputFormat) -> Self {
        Self {
            format,
            max_rows: None,
        }
    }

    /// Set maximum rows to display
    pub fn with_max_rows(mut self, max: usize) -> Self {
        self.max_rows = Some(max);
        self
    }

    /// Get the current format
    pub fn format(&self) -> OutputFormat {
        self.format
    }

    /// Set the format
    pub fn set_format(&mut self, format: OutputFormat) {
        self.format = format;
    }

    /// Format record batches and write to stdout
    pub fn print(&self, batches: &[RecordBatch]) -> io::Result<()> {
        let mut stdout = io::stdout().lock();
        self.write(&mut stdout, batches)
    }

    /// Format record batches and write to the given writer
    pub fn write<W: Write>(&self, writer: &mut W, batches: &[RecordBatch]) -> io::Result<()> {
        match self.format {
            OutputFormat::Table => self.write_table(writer, batches),
            OutputFormat::Csv => self.write_csv(writer, batches),
            OutputFormat::Json => self.write_json(writer, batches),
            OutputFormat::Vertical => self.write_vertical(writer, batches),
        }
    }

    /// Format as string
    pub fn format_to_string(&self, batches: &[RecordBatch]) -> String {
        let mut buffer = Vec::new();
        let _ = self.write(&mut buffer, batches);
        String::from_utf8_lossy(&buffer).into_owned()
    }

    /// Write as pretty-printed table
    fn write_table<W: Write>(&self, writer: &mut W, batches: &[RecordBatch]) -> io::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        // Use Arrow's built-in pretty printer
        let display = arrow::util::pretty::pretty_format_batches(batches)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Apply row limit if set
        if let Some(max) = self.max_rows {
            let display_str = display.to_string();
            let lines: Vec<&str> = display_str.lines().collect();
            // Header + separator = 3 lines, then data rows
            let header_lines = 3;
            let total_lines = header_lines + max + 1; // +1 for bottom border
            for (i, line) in lines.iter().enumerate() {
                if i < total_lines || i == lines.len() - 1 {
                    writeln!(writer, "{}", line)?;
                } else if i == total_lines {
                    writeln!(writer, "... ({} more rows)", lines.len() - total_lines - 1)?;
                }
            }
        } else {
            write!(writer, "{}", display)?;
        }
        Ok(())
    }

    /// Write as CSV
    fn write_csv<W: Write>(&self, writer: &mut W, batches: &[RecordBatch]) -> io::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();

        // Write header
        let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        writeln!(writer, "{}", headers.join(","))?;

        // Write data rows
        let mut row_count = 0;
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Some(max) = self.max_rows {
                    if row_count >= max {
                        return Ok(());
                    }
                }

                let values: Vec<String> = (0..batch.num_columns())
                    .map(|col_idx| {
                        let col = batch.column(col_idx);
                        self.format_csv_value(col, row_idx)
                    })
                    .collect();
                writeln!(writer, "{}", values.join(","))?;
                row_count += 1;
            }
        }
        Ok(())
    }

    /// Write as JSON array of objects
    fn write_json<W: Write>(&self, writer: &mut W, batches: &[RecordBatch]) -> io::Result<()> {
        if batches.is_empty() {
            writeln!(writer, "[]")?;
            return Ok(());
        }

        let schema = batches[0].schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        writeln!(writer, "[")?;

        let mut first = true;
        let mut row_count = 0;
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Some(max) = self.max_rows {
                    if row_count >= max {
                        writeln!(writer, "\n]")?;
                        return Ok(());
                    }
                }

                if !first {
                    writeln!(writer, ",")?;
                }
                first = false;

                write!(writer, "  {{")?;
                for (col_idx, field_name) in field_names.iter().enumerate() {
                    if col_idx > 0 {
                        write!(writer, ", ")?;
                    }
                    let col = batch.column(col_idx);
                    let value = self.format_json_value(col, row_idx);
                    write!(writer, "\"{}\": {}", field_name, value)?;
                }
                write!(writer, "}}")?;
                row_count += 1;
            }
        }

        writeln!(writer, "\n]")?;
        Ok(())
    }

    /// Write in vertical format (one column per line)
    fn write_vertical<W: Write>(&self, writer: &mut W, batches: &[RecordBatch]) -> io::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // Find max field name length for alignment
        let max_name_len = field_names.iter().map(|n| n.len()).max().unwrap_or(0);

        let mut row_count = 0;
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                if let Some(max) = self.max_rows {
                    if row_count >= max {
                        return Ok(());
                    }
                }

                writeln!(writer, "*************************** {} ***************************", row_count + 1)?;
                for (col_idx, field_name) in field_names.iter().enumerate() {
                    let col = batch.column(col_idx);
                    let value = self.format_display_value(col, row_idx);
                    writeln!(writer, "{:>width$}: {}", field_name, value, width = max_name_len)?;
                }
                row_count += 1;
            }
        }
        Ok(())
    }

    /// Format a single value for CSV output
    fn format_csv_value(&self, array: &ArrayRef, row: usize) -> String {
        if array.is_null(row) {
            return String::new();
        }

        let value = self.format_display_value(array, row);

        // Quote if contains comma, quote, or newline
        if value.contains(',') || value.contains('"') || value.contains('\n') {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value
        }
    }

    /// Format a single value for JSON output
    fn format_json_value(&self, array: &ArrayRef, row: usize) -> String {
        if array.is_null(row) {
            return "null".to_string();
        }

        match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                let val = arr.value(row);
                format!("\"{}\"", val.replace('\\', "\\\\").replace('"', "\\\""))
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                let val = arr.value(row);
                format!("\"{}\"", val.replace('\\', "\\\\").replace('"', "\\\""))
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                if arr.value(row) { "true" } else { "false" }.to_string()
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                arr.value(row).to_string()
            }
            _ => {
                // For other types, use display format with quotes
                format!("\"{}\"", self.format_display_value(array, row))
            }
        }
    }

    /// Format a single value for display
    fn format_display_value(&self, array: &ArrayRef, row: usize) -> String {
        if array.is_null(row) {
            return "NULL".to_string();
        }

        match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                arr.value(row).to_string()
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                arr.value(row).to_string()
            }
            DataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                // Days since epoch
                let days = arr.value(row);
                format!("{}", chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default())
            }
            DataType::Date64 => {
                let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
                let millis = arr.value(row);
                format!("{}", chrono::DateTime::from_timestamp_millis(millis).map(|d| d.format("%Y-%m-%d").to_string()).unwrap_or_default())
            }
            DataType::Decimal128(_, scale) => {
                let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let value = arr.value(row);
                if *scale > 0 {
                    let divisor = 10i128.pow(*scale as u32);
                    let int_part = value / divisor;
                    let frac_part = (value % divisor).abs();
                    format!("{}.{:0>width$}", int_part, frac_part, width = *scale as usize)
                } else {
                    value.to_string()
                }
            }
            _ => {
                // Fallback: use Arrow's display
                format!("{:?}", array.as_ref())
            }
        }
    }
}

impl Default for OutputFormatter {
    fn default() -> Self {
        Self::new(OutputFormat::Table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);
        let score_array = Float64Array::from(vec![Some(95.5), Some(87.0), Some(92.3)]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(score_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_output_format_parsing() {
        assert_eq!(OutputFormat::from_str("table"), Some(OutputFormat::Table));
        assert_eq!(OutputFormat::from_str("TABLE"), Some(OutputFormat::Table));
        assert_eq!(OutputFormat::from_str("t"), Some(OutputFormat::Table));
        assert_eq!(OutputFormat::from_str("csv"), Some(OutputFormat::Csv));
        assert_eq!(OutputFormat::from_str("c"), Some(OutputFormat::Csv));
        assert_eq!(OutputFormat::from_str("json"), Some(OutputFormat::Json));
        assert_eq!(OutputFormat::from_str("j"), Some(OutputFormat::Json));
        assert_eq!(OutputFormat::from_str("vertical"), Some(OutputFormat::Vertical));
        assert_eq!(OutputFormat::from_str("v"), Some(OutputFormat::Vertical));
        assert_eq!(OutputFormat::from_str("invalid"), None);
    }

    #[test]
    fn test_csv_output() {
        let batch = create_test_batch();
        let formatter = OutputFormatter::new(OutputFormat::Csv);
        let output = formatter.format_to_string(&[batch]);

        assert!(output.contains("id,name,score"));
        assert!(output.contains("1,Alice,95.5"));
        assert!(output.contains("2,Bob,87"));
        assert!(output.contains("3,,92.3")); // NULL becomes empty
    }

    #[test]
    fn test_csv_quoting() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
        ]));
        let text_array = StringArray::from(vec!["hello, world", "say \"hi\"", "normal"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(text_array)]).unwrap();

        let formatter = OutputFormatter::new(OutputFormat::Csv);
        let output = formatter.format_to_string(&[batch]);

        assert!(output.contains("\"hello, world\"")); // Quoted due to comma
        assert!(output.contains("\"say \"\"hi\"\"\"")); // Escaped quotes
        assert!(output.contains("\nnormal\n")); // Not quoted
    }

    #[test]
    fn test_json_output() {
        let batch = create_test_batch();
        let formatter = OutputFormatter::new(OutputFormat::Json);
        let output = formatter.format_to_string(&[batch]);

        assert!(output.contains("["));
        assert!(output.contains("]"));
        assert!(output.contains("\"id\": 1"));
        assert!(output.contains("\"name\": \"Alice\""));
        assert!(output.contains("\"score\": 95.5"));
        assert!(output.contains("\"name\": null")); // NULL in JSON
    }

    #[test]
    fn test_vertical_output() {
        let batch = create_test_batch();
        let formatter = OutputFormatter::new(OutputFormat::Vertical);
        let output = formatter.format_to_string(&[batch]);

        assert!(output.contains("*** 1 ***"));
        assert!(output.contains("*** 2 ***"));
        assert!(output.contains("*** 3 ***"));
        assert!(output.contains("id: 1"));
        assert!(output.contains("name: Alice"));
        assert!(output.contains("score: 95.5"));
    }

    #[test]
    fn test_table_output() {
        let batch = create_test_batch();
        let formatter = OutputFormatter::new(OutputFormat::Table);
        let output = formatter.format_to_string(&[batch]);

        // Table format uses Arrow's pretty print with borders
        assert!(output.contains("+"));
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("Alice"));
    }

    #[test]
    fn test_max_rows() {
        let batch = create_test_batch();
        let formatter = OutputFormatter::new(OutputFormat::Csv).with_max_rows(2);
        let output = formatter.format_to_string(&[batch]);

        // Should only have header + 2 rows
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 data rows
    }

    #[test]
    fn test_empty_batches() {
        let formatter = OutputFormatter::new(OutputFormat::Csv);
        let output = formatter.format_to_string(&[]);
        assert!(output.is_empty());

        let formatter = OutputFormatter::new(OutputFormat::Json);
        let output = formatter.format_to_string(&[]);
        assert!(output.contains("[]"));
    }

    #[test]
    fn test_format_name() {
        assert_eq!(OutputFormat::Table.name(), "table");
        assert_eq!(OutputFormat::Csv.name(), "csv");
        assert_eq!(OutputFormat::Json.name(), "json");
        assert_eq!(OutputFormat::Vertical.name(), "vertical");
    }

    #[test]
    fn test_set_format() {
        let mut formatter = OutputFormatter::new(OutputFormat::Table);
        assert_eq!(formatter.format(), OutputFormat::Table);

        formatter.set_format(OutputFormat::Csv);
        assert_eq!(formatter.format(), OutputFormat::Csv);
    }
}
