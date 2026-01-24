//! Logical expression types

use crate::planner::schema::{Column, PlanSchema, SchemaField};
use arrow::datatypes::DataType as ArrowDataType;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use std::fmt;
use std::sync::Arc;

/// Scalar value for literals
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    Decimal128(Decimal),
    Utf8(String),
    Date32(i32),
    Date64(i64),
    Timestamp(i64), // microseconds
    Interval(i64),  // days
}

impl ScalarValue {
    pub fn data_type(&self) -> ArrowDataType {
        match self {
            ScalarValue::Null => ArrowDataType::Null,
            ScalarValue::Boolean(_) => ArrowDataType::Boolean,
            ScalarValue::Int8(_) => ArrowDataType::Int8,
            ScalarValue::Int16(_) => ArrowDataType::Int16,
            ScalarValue::Int32(_) => ArrowDataType::Int32,
            ScalarValue::Int64(_) => ArrowDataType::Int64,
            ScalarValue::UInt8(_) => ArrowDataType::UInt8,
            ScalarValue::UInt16(_) => ArrowDataType::UInt16,
            ScalarValue::UInt32(_) => ArrowDataType::UInt32,
            ScalarValue::UInt64(_) => ArrowDataType::UInt64,
            ScalarValue::Float32(_) => ArrowDataType::Float32,
            ScalarValue::Float64(_) => ArrowDataType::Float64,
            ScalarValue::Decimal128(_) => ArrowDataType::Decimal128(38, 10),
            ScalarValue::Utf8(_) => ArrowDataType::Utf8,
            ScalarValue::Date32(_) => ArrowDataType::Date32,
            ScalarValue::Date64(_) => ArrowDataType::Date64,
            ScalarValue::Timestamp(_) => {
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            ScalarValue::Interval(_) => {
                ArrowDataType::Interval(arrow::datatypes::IntervalUnit::DayTime)
            }
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(v) => write!(f, "{}", v),
            ScalarValue::Int8(v) => write!(f, "{}", v),
            ScalarValue::Int16(v) => write!(f, "{}", v),
            ScalarValue::Int32(v) => write!(f, "{}", v),
            ScalarValue::Int64(v) => write!(f, "{}", v),
            ScalarValue::UInt8(v) => write!(f, "{}", v),
            ScalarValue::UInt16(v) => write!(f, "{}", v),
            ScalarValue::UInt32(v) => write!(f, "{}", v),
            ScalarValue::UInt64(v) => write!(f, "{}", v),
            ScalarValue::Float32(v) => write!(f, "{}", v),
            ScalarValue::Float64(v) => write!(f, "{}", v),
            ScalarValue::Decimal128(v) => write!(f, "{}", v),
            ScalarValue::Utf8(v) => write!(f, "'{}'", v),
            ScalarValue::Date32(v) => write!(f, "DATE({})", v),
            ScalarValue::Date64(v) => write!(f, "DATE({})", v),
            ScalarValue::Timestamp(v) => write!(f, "TIMESTAMP({})", v),
            ScalarValue::Interval(v) => write!(f, "INTERVAL({})", v),
        }
    }
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
    // String
    Like,
    NotLike,
    // Other
    StringConcat,
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Subtract => write!(f, "-"),
            BinaryOp::Multiply => write!(f, "*"),
            BinaryOp::Divide => write!(f, "/"),
            BinaryOp::Modulo => write!(f, "%"),
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::NotEq => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::LtEq => write!(f, "<="),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::GtEq => write!(f, ">="),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Like => write!(f, "LIKE"),
            BinaryOp::NotLike => write!(f, "NOT LIKE"),
            BinaryOp::StringConcat => write!(f, "||"),
        }
    }
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Not,
    Negate,
    IsNull,
    IsNotNull,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::Not => write!(f, "NOT"),
            UnaryOp::Negate => write!(f, "-"),
            UnaryOp::IsNull => write!(f, "IS NULL"),
            UnaryOp::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

/// Aggregate function types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    // Statistical aggregates
    Stddev,
    StddevPop,
    StddevSamp,
    Variance,
    VarPop,
    VarSamp,
    // Boolean aggregates
    BoolAnd,
    BoolOr,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT"),
            AggregateFunction::CountDistinct => write!(f, "COUNT DISTINCT"),
            AggregateFunction::Sum => write!(f, "SUM"),
            AggregateFunction::Avg => write!(f, "AVG"),
            AggregateFunction::Min => write!(f, "MIN"),
            AggregateFunction::Max => write!(f, "MAX"),
            AggregateFunction::Stddev => write!(f, "STDDEV"),
            AggregateFunction::StddevPop => write!(f, "STDDEV_POP"),
            AggregateFunction::StddevSamp => write!(f, "STDDEV_SAMP"),
            AggregateFunction::Variance => write!(f, "VARIANCE"),
            AggregateFunction::VarPop => write!(f, "VAR_POP"),
            AggregateFunction::VarSamp => write!(f, "VAR_SAMP"),
            AggregateFunction::BoolAnd => write!(f, "BOOL_AND"),
            AggregateFunction::BoolOr => write!(f, "BOOL_OR"),
        }
    }
}

/// Scalar function types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarFunction {
    // Math
    Abs,
    Ceil,
    Floor,
    Round,
    Power,
    Sqrt,
    Mod,
    Sign,
    Truncate,
    Ln,
    Log,
    Log2,
    Log10,
    Exp,
    Random,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,
    Degrees,
    Radians,
    Pi,
    E,
    Cbrt,
    // String
    Upper,
    Lower,
    Trim,
    Ltrim,
    Rtrim,
    Length,
    Substring,
    Concat,
    Replace,
    Position,
    Strpos,
    Reverse,
    Lpad,
    Rpad,
    SplitPart,
    StartsWith,
    EndsWith,
    Chr,
    Ascii,
    ConcatWs,
    Left,
    Right,
    Repeat,
    // Date/Time
    Year,
    Month,
    Day,
    DateTrunc,
    DatePart,
    CurrentDate,
    CurrentTimestamp,
    Now,
    DateAdd,
    DateDiff,
    Hour,
    Minute,
    Second,
    Quarter,
    Week,
    DayOfWeek,
    DayOfYear,
    // Type conversion
    Cast,
    // Conditional
    Coalesce,
    NullIf,
    Case,
    If,
    Greatest,
    Least,
    // Regex
    RegexpLike,
    RegexpExtract,
    RegexpReplace,
    RegexpSplit,
    RegexpCount,
    // Binary/Encoding
    ToHex,
    FromHex,
    ToBase64,
    FromBase64,
    Md5,
    Sha256,
    Sha1,
    Sha512,
    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseNot,
    BitCount,
    // URL
    UrlExtractHost,
    UrlExtractPath,
    UrlExtractPort,
    UrlExtractProtocol,
    UrlExtractQuery,
    UrlEncode,
    UrlDecode,
    // Other
    Extract,
    Typeof,
    Uuid,
}

impl fmt::Display for ScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Math
            ScalarFunction::Abs => write!(f, "ABS"),
            ScalarFunction::Ceil => write!(f, "CEIL"),
            ScalarFunction::Floor => write!(f, "FLOOR"),
            ScalarFunction::Round => write!(f, "ROUND"),
            ScalarFunction::Power => write!(f, "POWER"),
            ScalarFunction::Sqrt => write!(f, "SQRT"),
            ScalarFunction::Mod => write!(f, "MOD"),
            ScalarFunction::Sign => write!(f, "SIGN"),
            ScalarFunction::Truncate => write!(f, "TRUNCATE"),
            ScalarFunction::Ln => write!(f, "LN"),
            ScalarFunction::Log => write!(f, "LOG"),
            ScalarFunction::Log2 => write!(f, "LOG2"),
            ScalarFunction::Log10 => write!(f, "LOG10"),
            ScalarFunction::Exp => write!(f, "EXP"),
            ScalarFunction::Random => write!(f, "RANDOM"),
            ScalarFunction::Sin => write!(f, "SIN"),
            ScalarFunction::Cos => write!(f, "COS"),
            ScalarFunction::Tan => write!(f, "TAN"),
            ScalarFunction::Asin => write!(f, "ASIN"),
            ScalarFunction::Acos => write!(f, "ACOS"),
            ScalarFunction::Atan => write!(f, "ATAN"),
            ScalarFunction::Atan2 => write!(f, "ATAN2"),
            ScalarFunction::Degrees => write!(f, "DEGREES"),
            ScalarFunction::Radians => write!(f, "RADIANS"),
            ScalarFunction::Pi => write!(f, "PI"),
            ScalarFunction::E => write!(f, "E"),
            ScalarFunction::Cbrt => write!(f, "CBRT"),
            // String
            ScalarFunction::Upper => write!(f, "UPPER"),
            ScalarFunction::Lower => write!(f, "LOWER"),
            ScalarFunction::Trim => write!(f, "TRIM"),
            ScalarFunction::Ltrim => write!(f, "LTRIM"),
            ScalarFunction::Rtrim => write!(f, "RTRIM"),
            ScalarFunction::Length => write!(f, "LENGTH"),
            ScalarFunction::Substring => write!(f, "SUBSTRING"),
            ScalarFunction::Concat => write!(f, "CONCAT"),
            ScalarFunction::Replace => write!(f, "REPLACE"),
            ScalarFunction::Position => write!(f, "POSITION"),
            ScalarFunction::Strpos => write!(f, "STRPOS"),
            ScalarFunction::Reverse => write!(f, "REVERSE"),
            ScalarFunction::Lpad => write!(f, "LPAD"),
            ScalarFunction::Rpad => write!(f, "RPAD"),
            ScalarFunction::SplitPart => write!(f, "SPLIT_PART"),
            ScalarFunction::StartsWith => write!(f, "STARTS_WITH"),
            ScalarFunction::EndsWith => write!(f, "ENDS_WITH"),
            ScalarFunction::Chr => write!(f, "CHR"),
            ScalarFunction::Ascii => write!(f, "ASCII"),
            ScalarFunction::ConcatWs => write!(f, "CONCAT_WS"),
            ScalarFunction::Left => write!(f, "LEFT"),
            ScalarFunction::Right => write!(f, "RIGHT"),
            ScalarFunction::Repeat => write!(f, "REPEAT"),
            // Date/Time
            ScalarFunction::Year => write!(f, "YEAR"),
            ScalarFunction::Month => write!(f, "MONTH"),
            ScalarFunction::Day => write!(f, "DAY"),
            ScalarFunction::DateTrunc => write!(f, "DATE_TRUNC"),
            ScalarFunction::DatePart => write!(f, "DATE_PART"),
            ScalarFunction::CurrentDate => write!(f, "CURRENT_DATE"),
            ScalarFunction::CurrentTimestamp => write!(f, "CURRENT_TIMESTAMP"),
            ScalarFunction::Now => write!(f, "NOW"),
            ScalarFunction::DateAdd => write!(f, "DATE_ADD"),
            ScalarFunction::DateDiff => write!(f, "DATE_DIFF"),
            ScalarFunction::Hour => write!(f, "HOUR"),
            ScalarFunction::Minute => write!(f, "MINUTE"),
            ScalarFunction::Second => write!(f, "SECOND"),
            ScalarFunction::Quarter => write!(f, "QUARTER"),
            ScalarFunction::Week => write!(f, "WEEK"),
            ScalarFunction::DayOfWeek => write!(f, "DAY_OF_WEEK"),
            ScalarFunction::DayOfYear => write!(f, "DAY_OF_YEAR"),
            // Type conversion
            ScalarFunction::Cast => write!(f, "CAST"),
            // Conditional
            ScalarFunction::Coalesce => write!(f, "COALESCE"),
            ScalarFunction::NullIf => write!(f, "NULLIF"),
            ScalarFunction::Case => write!(f, "CASE"),
            ScalarFunction::If => write!(f, "IF"),
            ScalarFunction::Greatest => write!(f, "GREATEST"),
            ScalarFunction::Least => write!(f, "LEAST"),
            // Regex
            ScalarFunction::RegexpLike => write!(f, "REGEXP_LIKE"),
            ScalarFunction::RegexpExtract => write!(f, "REGEXP_EXTRACT"),
            ScalarFunction::RegexpReplace => write!(f, "REGEXP_REPLACE"),
            ScalarFunction::RegexpSplit => write!(f, "REGEXP_SPLIT"),
            ScalarFunction::RegexpCount => write!(f, "REGEXP_COUNT"),
            // Binary/Encoding
            ScalarFunction::ToHex => write!(f, "TO_HEX"),
            ScalarFunction::FromHex => write!(f, "FROM_HEX"),
            ScalarFunction::ToBase64 => write!(f, "TO_BASE64"),
            ScalarFunction::FromBase64 => write!(f, "FROM_BASE64"),
            ScalarFunction::Md5 => write!(f, "MD5"),
            ScalarFunction::Sha256 => write!(f, "SHA256"),
            ScalarFunction::Sha1 => write!(f, "SHA1"),
            ScalarFunction::Sha512 => write!(f, "SHA512"),
            // Bitwise
            ScalarFunction::BitwiseAnd => write!(f, "BITWISE_AND"),
            ScalarFunction::BitwiseOr => write!(f, "BITWISE_OR"),
            ScalarFunction::BitwiseXor => write!(f, "BITWISE_XOR"),
            ScalarFunction::BitwiseNot => write!(f, "BITWISE_NOT"),
            ScalarFunction::BitCount => write!(f, "BIT_COUNT"),
            // URL
            ScalarFunction::UrlExtractHost => write!(f, "URL_EXTRACT_HOST"),
            ScalarFunction::UrlExtractPath => write!(f, "URL_EXTRACT_PATH"),
            ScalarFunction::UrlExtractPort => write!(f, "URL_EXTRACT_PORT"),
            ScalarFunction::UrlExtractProtocol => write!(f, "URL_EXTRACT_PROTOCOL"),
            ScalarFunction::UrlExtractQuery => write!(f, "URL_EXTRACT_QUERY"),
            ScalarFunction::UrlEncode => write!(f, "URL_ENCODE"),
            ScalarFunction::UrlDecode => write!(f, "URL_DECODE"),
            // Other
            ScalarFunction::Extract => write!(f, "EXTRACT"),
            ScalarFunction::Typeof => write!(f, "TYPEOF"),
            ScalarFunction::Uuid => write!(f, "UUID"),
        }
    }
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// Null ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum NullOrdering {
    #[default]
    NullsFirst,
    NullsLast,
}

/// Sort expression
#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    pub expr: Expr,
    pub direction: SortDirection,
    pub nulls: NullOrdering,
}

impl SortExpr {
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            direction: SortDirection::Asc,
            nulls: NullOrdering::NullsFirst,
        }
    }

    pub fn asc(mut self) -> Self {
        self.direction = SortDirection::Asc;
        self
    }

    pub fn desc(mut self) -> Self {
        self.direction = SortDirection::Desc;
        self
    }

    pub fn nulls_first(mut self) -> Self {
        self.nulls = NullOrdering::NullsFirst;
        self
    }

    pub fn nulls_last(mut self) -> Self {
        self.nulls = NullOrdering::NullsLast;
        self
    }
}

/// Logical expression
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference
    Column(Column),

    /// Literal value
    Literal(ScalarValue),

    /// Binary operation
    BinaryExpr {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    /// Unary operation
    UnaryExpr { op: UnaryOp, expr: Box<Expr> },

    /// Aggregate function
    Aggregate {
        func: AggregateFunction,
        args: Vec<Expr>,
        distinct: bool,
    },

    /// Scalar function
    ScalarFunc {
        func: ScalarFunction,
        args: Vec<Expr>,
    },

    /// CAST expression
    Cast {
        expr: Box<Expr>,
        data_type: ArrowDataType,
    },

    /// CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },

    /// IN expression
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    /// BETWEEN expression
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    /// Subquery (scalar)
    ScalarSubquery(Arc<super::LogicalPlan>),

    /// EXISTS subquery
    Exists {
        subquery: Arc<super::LogicalPlan>,
        negated: bool,
    },

    /// IN subquery
    InSubquery {
        expr: Box<Expr>,
        subquery: Arc<super::LogicalPlan>,
        negated: bool,
    },

    /// Alias
    Alias { expr: Box<Expr>, name: String },

    /// Wildcard (*)
    Wildcard,

    /// Qualified wildcard (table.*)
    QualifiedWildcard(String),
}

impl Expr {
    /// Create a column reference
    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column(Column::new(name))
    }

    /// Create a qualified column reference
    pub fn qualified_column(relation: impl Into<String>, name: impl Into<String>) -> Self {
        Expr::Column(Column::new_qualified(relation, name))
    }

    /// Create a literal
    pub fn literal(value: ScalarValue) -> Self {
        Expr::Literal(value)
    }

    /// Create an alias
    pub fn alias(self, name: impl Into<String>) -> Self {
        Expr::Alias {
            expr: Box::new(self),
            name: name.into(),
        }
    }

    /// Binary operation helpers
    pub fn eq(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }

    pub fn not_eq(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::NotEq,
            right: Box::new(other),
        }
    }

    pub fn lt(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Lt,
            right: Box::new(other),
        }
    }

    pub fn lt_eq(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::LtEq,
            right: Box::new(other),
        }
    }

    pub fn gt(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Gt,
            right: Box::new(other),
        }
    }

    pub fn gt_eq(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::GtEq,
            right: Box::new(other),
        }
    }

    pub fn and(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }

    pub fn or(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }

    #[allow(clippy::should_implement_trait)] // Different semantics than std::ops::Add
    pub fn add(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Add,
            right: Box::new(other),
        }
    }

    pub fn subtract(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Subtract,
            right: Box::new(other),
        }
    }

    pub fn multiply(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Multiply,
            right: Box::new(other),
        }
    }

    pub fn divide(self, other: Expr) -> Self {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Divide,
            right: Box::new(other),
        }
    }

    /// Get the output name for this expression
    pub fn output_name(&self) -> String {
        match self {
            Expr::Column(col) => col.name.clone(),
            Expr::Alias { name, .. } => name.clone(),
            Expr::Literal(v) => v.to_string(),
            Expr::BinaryExpr { left, op, right } => {
                format!("{} {} {}", left.output_name(), op, right.output_name())
            }
            Expr::UnaryExpr { op, expr } => format!("{} {}", op, expr.output_name()),
            Expr::Aggregate { func, args, .. } => {
                let arg_names: Vec<_> = args.iter().map(|a| a.output_name()).collect();
                format!("{}({})", func, arg_names.join(", "))
            }
            Expr::ScalarFunc { func, args } => {
                let arg_names: Vec<_> = args.iter().map(|a| a.output_name()).collect();
                format!("{}({})", func, arg_names.join(", "))
            }
            Expr::Cast { expr, data_type } => {
                format!("CAST({} AS {:?})", expr.output_name(), data_type)
            }
            Expr::Case { .. } => "CASE".to_string(),
            Expr::InList { expr, .. } => format!("{} IN (...)", expr.output_name()),
            Expr::Between { expr, .. } => format!("{} BETWEEN ...", expr.output_name()),
            Expr::ScalarSubquery(_) => "(subquery)".to_string(),
            Expr::Exists { .. } => "EXISTS(...)".to_string(),
            Expr::InSubquery { expr, .. } => format!("{} IN (subquery)", expr.output_name()),
            Expr::Wildcard => "*".to_string(),
            Expr::QualifiedWildcard(table) => format!("{}.*", table),
        }
    }

    /// Infer the data type of this expression given an input schema
    pub fn data_type(&self, schema: &PlanSchema) -> crate::error::Result<ArrowDataType> {
        use crate::error::QueryError;

        match self {
            Expr::Column(col) => schema
                .resolve_column(col)
                .map(|(_, field)| field.data_type.clone())
                .ok_or_else(|| QueryError::ColumnNotFound(col.qualified_name())),
            Expr::Literal(v) => Ok(v.data_type()),
            Expr::BinaryExpr { left, op, right } => {
                let left_type = left.data_type(schema)?;
                let right_type = right.data_type(schema)?;

                match op {
                    BinaryOp::And | BinaryOp::Or => Ok(ArrowDataType::Boolean),
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::Like
                    | BinaryOp::NotLike => Ok(ArrowDataType::Boolean),
                    BinaryOp::Add
                    | BinaryOp::Subtract
                    | BinaryOp::Multiply
                    | BinaryOp::Divide
                    | BinaryOp::Modulo => {
                        // Return the wider type
                        Ok(coerce_numeric_types(&left_type, &right_type))
                    }
                    BinaryOp::StringConcat => Ok(ArrowDataType::Utf8),
                }
            }
            Expr::UnaryExpr { op, expr } => match op {
                UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull => Ok(ArrowDataType::Boolean),
                UnaryOp::Negate => expr.data_type(schema),
            },
            Expr::Aggregate { func, args, .. } => match func {
                AggregateFunction::Count | AggregateFunction::CountDistinct => {
                    Ok(ArrowDataType::Int64)
                }
                AggregateFunction::Sum => {
                    if let Some(arg) = args.first() {
                        let arg_type = arg.data_type(schema)?;
                        Ok(promote_sum_type(&arg_type))
                    } else {
                        Ok(ArrowDataType::Int64)
                    }
                }
                AggregateFunction::Avg
                | AggregateFunction::Stddev
                | AggregateFunction::StddevPop
                | AggregateFunction::StddevSamp
                | AggregateFunction::Variance
                | AggregateFunction::VarPop
                | AggregateFunction::VarSamp => Ok(ArrowDataType::Float64),
                AggregateFunction::Min | AggregateFunction::Max => args
                    .first()
                    .map(|a| a.data_type(schema))
                    .unwrap_or(Ok(ArrowDataType::Null)),
                AggregateFunction::BoolAnd | AggregateFunction::BoolOr => {
                    Ok(ArrowDataType::Boolean)
                }
            },
            Expr::ScalarFunc { func, args } => {
                match func {
                    // String functions returning Int64
                    ScalarFunction::Length
                    | ScalarFunction::Position
                    | ScalarFunction::Strpos
                    | ScalarFunction::Ascii => Ok(ArrowDataType::Int64),
                    // String functions returning Utf8
                    ScalarFunction::Upper
                    | ScalarFunction::Lower
                    | ScalarFunction::Trim
                    | ScalarFunction::Ltrim
                    | ScalarFunction::Rtrim
                    | ScalarFunction::Substring
                    | ScalarFunction::Concat
                    | ScalarFunction::Replace
                    | ScalarFunction::Reverse
                    | ScalarFunction::Lpad
                    | ScalarFunction::Rpad
                    | ScalarFunction::SplitPart
                    | ScalarFunction::Chr
                    | ScalarFunction::ConcatWs
                    | ScalarFunction::Left
                    | ScalarFunction::Right
                    | ScalarFunction::Repeat => Ok(ArrowDataType::Utf8),
                    // Boolean functions
                    ScalarFunction::StartsWith
                    | ScalarFunction::EndsWith
                    | ScalarFunction::RegexpLike => Ok(ArrowDataType::Boolean),
                    // Date/time functions returning Int32
                    ScalarFunction::Year
                    | ScalarFunction::Month
                    | ScalarFunction::Day
                    | ScalarFunction::Hour
                    | ScalarFunction::Minute
                    | ScalarFunction::Second
                    | ScalarFunction::Quarter
                    | ScalarFunction::Week
                    | ScalarFunction::DayOfWeek
                    | ScalarFunction::DayOfYear => Ok(ArrowDataType::Int32),
                    // Date/time functions returning Date32
                    ScalarFunction::CurrentDate => Ok(ArrowDataType::Date32),
                    // Date/time functions returning Timestamp
                    ScalarFunction::CurrentTimestamp | ScalarFunction::Now => Ok(
                        ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    ),
                    // Math functions preserving input type
                    ScalarFunction::Abs
                    | ScalarFunction::Ceil
                    | ScalarFunction::Floor
                    | ScalarFunction::Round
                    | ScalarFunction::Truncate => args
                        .first()
                        .map(|a| a.data_type(schema))
                        .unwrap_or(Ok(ArrowDataType::Float64)),
                    // Math functions returning Int32 (sign)
                    ScalarFunction::Sign => Ok(ArrowDataType::Int32),
                    // Math functions returning Int64 (mod, bitwise)
                    ScalarFunction::Mod
                    | ScalarFunction::BitwiseAnd
                    | ScalarFunction::BitwiseOr
                    | ScalarFunction::BitwiseXor
                    | ScalarFunction::BitwiseNot
                    | ScalarFunction::BitCount => Ok(ArrowDataType::Int64),
                    // Math functions returning Float64
                    ScalarFunction::Power
                    | ScalarFunction::Sqrt
                    | ScalarFunction::Ln
                    | ScalarFunction::Log
                    | ScalarFunction::Log2
                    | ScalarFunction::Log10
                    | ScalarFunction::Exp
                    | ScalarFunction::Random
                    | ScalarFunction::Sin
                    | ScalarFunction::Cos
                    | ScalarFunction::Tan
                    | ScalarFunction::Asin
                    | ScalarFunction::Acos
                    | ScalarFunction::Atan
                    | ScalarFunction::Atan2
                    | ScalarFunction::Degrees
                    | ScalarFunction::Radians
                    | ScalarFunction::Pi
                    | ScalarFunction::E
                    | ScalarFunction::Cbrt => Ok(ArrowDataType::Float64),
                    // Conditional functions
                    ScalarFunction::Coalesce
                    | ScalarFunction::NullIf
                    | ScalarFunction::Greatest
                    | ScalarFunction::Least => args
                        .first()
                        .map(|a| a.data_type(schema))
                        .unwrap_or(Ok(ArrowDataType::Null)),
                    // IF function - return type is from second arg (then branch)
                    ScalarFunction::If => args
                        .get(1)
                        .map(|a| a.data_type(schema))
                        .unwrap_or(Ok(ArrowDataType::Null)),
                    // Regex functions
                    ScalarFunction::RegexpExtract | ScalarFunction::RegexpReplace => {
                        Ok(ArrowDataType::Utf8)
                    }
                    ScalarFunction::RegexpCount => Ok(ArrowDataType::Int64),
                    ScalarFunction::RegexpSplit => Ok(ArrowDataType::Utf8), // Returns array, simplified for now
                    // Binary/Encoding functions
                    ScalarFunction::ToHex
                    | ScalarFunction::ToBase64
                    | ScalarFunction::Md5
                    | ScalarFunction::Sha256
                    | ScalarFunction::Sha1
                    | ScalarFunction::Sha512 => Ok(ArrowDataType::Utf8),
                    ScalarFunction::FromHex | ScalarFunction::FromBase64 => {
                        Ok(ArrowDataType::Binary)
                    }
                    // URL functions
                    ScalarFunction::UrlExtractHost
                    | ScalarFunction::UrlExtractPath
                    | ScalarFunction::UrlExtractProtocol
                    | ScalarFunction::UrlExtractQuery
                    | ScalarFunction::UrlEncode
                    | ScalarFunction::UrlDecode => Ok(ArrowDataType::Utf8),
                    ScalarFunction::UrlExtractPort => Ok(ArrowDataType::Int32),
                    // Other
                    ScalarFunction::Typeof => Ok(ArrowDataType::Utf8),
                    ScalarFunction::Uuid => Ok(ArrowDataType::Utf8),
                    ScalarFunction::DateAdd | ScalarFunction::DateTrunc => Ok(
                        ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    ),
                    ScalarFunction::DateDiff | ScalarFunction::DatePart => Ok(ArrowDataType::Int64),
                    _ => Ok(ArrowDataType::Utf8), // Default
                }
            }
            Expr::Cast { data_type, .. } => Ok(data_type.clone()),
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                if let Some((_, then_expr)) = when_then.first() {
                    then_expr.data_type(schema)
                } else if let Some(else_expr) = else_expr {
                    else_expr.data_type(schema)
                } else {
                    Ok(ArrowDataType::Null)
                }
            }
            Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. } => Ok(ArrowDataType::Boolean),
            Expr::ScalarSubquery(plan) => {
                let subquery_schema = plan.schema();
                if let Some(field) = subquery_schema.fields().first() {
                    Ok(field.data_type.clone())
                } else {
                    Ok(ArrowDataType::Null)
                }
            }
            Expr::Alias { expr, .. } => expr.data_type(schema),
            Expr::Wildcard | Expr::QualifiedWildcard(_) => Err(QueryError::Internal(
                "Cannot determine type of wildcard".to_string(),
            )),
        }
    }

    /// Create schema field for this expression
    pub fn to_field(&self, schema: &PlanSchema) -> crate::error::Result<SchemaField> {
        let name = self.output_name();
        let data_type = self.data_type(schema)?;
        Ok(SchemaField::new(name, data_type))
    }

    /// Check if expression contains an aggregate
    pub fn contains_aggregate(&self) -> bool {
        match self {
            Expr::Aggregate { .. } => true,
            Expr::BinaryExpr { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Expr::UnaryExpr { expr, .. } => expr.contains_aggregate(),
            Expr::ScalarFunc { args, .. } => args.iter().any(|a| a.contains_aggregate()),
            Expr::Cast { expr, .. } => expr.contains_aggregate(),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand.as_ref().is_some_and(|e| e.contains_aggregate())
                    || when_then
                        .iter()
                        .any(|(w, t)| w.contains_aggregate() || t.contains_aggregate())
                    || else_expr.as_ref().is_some_and(|e| e.contains_aggregate())
            }
            Expr::Alias { expr, .. } => expr.contains_aggregate(),
            _ => false,
        }
    }

    /// Check if expression contains a subquery
    pub fn contains_subquery(&self) -> bool {
        match self {
            Expr::ScalarSubquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => true,
            Expr::BinaryExpr { left, right, .. } => {
                left.contains_subquery() || right.contains_subquery()
            }
            Expr::UnaryExpr { expr, .. } => expr.contains_subquery(),
            Expr::ScalarFunc { args, .. } => args.iter().any(|a| a.contains_subquery()),
            Expr::Cast { expr, .. } => expr.contains_subquery(),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand.as_ref().is_some_and(|e| e.contains_subquery())
                    || when_then
                        .iter()
                        .any(|(w, t)| w.contains_subquery() || t.contains_subquery())
                    || else_expr.as_ref().is_some_and(|e| e.contains_subquery())
            }
            Expr::Alias { expr, .. } => expr.contains_subquery(),
            Expr::InList { expr, list, .. } => {
                expr.contains_subquery() || list.iter().any(|e| e.contains_subquery())
            }
            Expr::Between {
                expr, low, high, ..
            } => expr.contains_subquery() || low.contains_subquery() || high.contains_subquery(),
            _ => false,
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(col) => write!(f, "{}", col),
            Expr::Literal(v) => write!(f, "{}", v),
            Expr::BinaryExpr { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Expr::UnaryExpr { op, expr } => write!(f, "({} {})", op, expr),
            Expr::Aggregate {
                func,
                args,
                distinct,
            } => {
                let distinct_str = if *distinct { "DISTINCT " } else { "" };
                let args_str: Vec<String> = args.iter().map(|a| a.to_string()).collect();
                write!(f, "{}({}{})", func, distinct_str, args_str.join(", "))
            }
            Expr::ScalarFunc { func, args } => {
                let args_str: Vec<String> = args.iter().map(|a| a.to_string()).collect();
                write!(f, "{}({})", func, args_str.join(", "))
            }
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {:?})", expr, data_type),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                write!(f, "CASE ")?;
                if let Some(op) = operand {
                    write!(f, "{} ", op)?;
                }
                for (when, then) in when_then {
                    write!(f, "WHEN {} THEN {} ", when, then)?;
                }
                if let Some(else_e) = else_expr {
                    write!(f, "ELSE {} ", else_e)?;
                }
                write!(f, "END")
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let not_str = if *negated { "NOT " } else { "" };
                let list_str: Vec<String> = list.iter().map(|e| e.to_string()).collect();
                write!(f, "{} {}IN ({})", expr, not_str, list_str.join(", "))
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let not_str = if *negated { "NOT " } else { "" };
                write!(f, "{} {}BETWEEN {} AND {}", expr, not_str, low, high)
            }
            Expr::ScalarSubquery(_) => write!(f, "(scalar subquery)"),
            Expr::Exists { negated, .. } => {
                let not_str = if *negated { "NOT " } else { "" };
                write!(f, "{}EXISTS(...)", not_str)
            }
            Expr::InSubquery { expr, negated, .. } => {
                let not_str = if *negated { "NOT " } else { "" };
                write!(f, "{} {}IN (subquery)", expr, not_str)
            }
            Expr::Alias { expr, name } => write!(f, "{} AS {}", expr, name),
            Expr::Wildcard => write!(f, "*"),
            Expr::QualifiedWildcard(table) => write!(f, "{}.*", table),
        }
    }
}

/// Coerce numeric types for binary operations
fn coerce_numeric_types(left: &ArrowDataType, right: &ArrowDataType) -> ArrowDataType {
    use ArrowDataType::*;

    match (left, right) {
        (Float64, _) | (_, Float64) => Float64,
        (Float32, _) | (_, Float32) => Float64,
        (Decimal128(_, _), _) | (_, Decimal128(_, _)) => Decimal128(38, 10),
        (Int64, _) | (_, Int64) => Int64,
        (Int32, _) | (_, Int32) => Int64,
        (Int16, _) | (_, Int16) => Int32,
        (Int8, _) | (_, Int8) => Int16,
        _ => Float64,
    }
}

/// Promote type for SUM aggregation
fn promote_sum_type(input: &ArrowDataType) -> ArrowDataType {
    use ArrowDataType::*;

    match input {
        Int8 | Int16 | Int32 | Int64 => Int64,
        UInt8 | UInt16 | UInt32 | UInt64 => UInt64,
        Float32 | Float64 => Float64,
        Decimal128(p, s) => Decimal128(*p, *s),
        _ => Float64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_builders() {
        let col = Expr::column("id");
        let lit = Expr::literal(ScalarValue::Int64(10));
        let expr = col.clone().eq(lit);

        assert!(matches!(
            expr,
            Expr::BinaryExpr {
                op: BinaryOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_display() {
        let expr = Expr::column("a").add(Expr::column("b"));
        assert_eq!(format!("{}", expr), "(a + b)");
    }

    #[test]
    fn test_aggregate_detection() {
        let agg = Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::column("amount")],
            distinct: false,
        };
        assert!(agg.contains_aggregate());

        let col = Expr::column("id");
        assert!(!col.contains_aggregate());
    }
}
