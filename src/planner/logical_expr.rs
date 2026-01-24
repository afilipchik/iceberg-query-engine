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
    /// List/Array type - stores elements and the element data type
    List(Vec<ScalarValue>, Box<ArrowDataType>),
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
            ScalarValue::List(_, elem_type) => ArrowDataType::List(Arc::new(
                arrow::datatypes::Field::new("item", elem_type.as_ref().clone(), true),
            )),
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
            ScalarValue::List(values, _) => {
                write!(f, "ARRAY[")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
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
    // New simple aggregates
    CountIf,
    AnyValue,
    Arbitrary,
    GeometricMean,
    Checksum,
    // Bitwise aggregates
    BitwiseAndAgg,
    BitwiseOrAgg,
    BitwiseXorAgg,
    // String aggregates
    Listagg,
    // Correlation and regression aggregates
    Corr,
    CovarPop,
    CovarSamp,
    Kurtosis,
    Skewness,
    RegrSlope,
    RegrIntercept,
    RegrCount,
    RegrAvgx,
    RegrAvgy,
    // Approximate aggregates
    ApproxPercentile,
    ApproxDistinct,
    // Multi-value aggregates
    MaxBy,
    MinBy,
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
            AggregateFunction::CountIf => write!(f, "COUNT_IF"),
            AggregateFunction::AnyValue => write!(f, "ANY_VALUE"),
            AggregateFunction::Arbitrary => write!(f, "ARBITRARY"),
            AggregateFunction::GeometricMean => write!(f, "GEOMETRIC_MEAN"),
            AggregateFunction::Checksum => write!(f, "CHECKSUM"),
            AggregateFunction::BitwiseAndAgg => write!(f, "BITWISE_AND_AGG"),
            AggregateFunction::BitwiseOrAgg => write!(f, "BITWISE_OR_AGG"),
            AggregateFunction::BitwiseXorAgg => write!(f, "BITWISE_XOR_AGG"),
            AggregateFunction::Listagg => write!(f, "LISTAGG"),
            AggregateFunction::Corr => write!(f, "CORR"),
            AggregateFunction::CovarPop => write!(f, "COVAR_POP"),
            AggregateFunction::CovarSamp => write!(f, "COVAR_SAMP"),
            AggregateFunction::Kurtosis => write!(f, "KURTOSIS"),
            AggregateFunction::Skewness => write!(f, "SKEWNESS"),
            AggregateFunction::RegrSlope => write!(f, "REGR_SLOPE"),
            AggregateFunction::RegrIntercept => write!(f, "REGR_INTERCEPT"),
            AggregateFunction::RegrCount => write!(f, "REGR_COUNT"),
            AggregateFunction::RegrAvgx => write!(f, "REGR_AVGX"),
            AggregateFunction::RegrAvgy => write!(f, "REGR_AVGY"),
            AggregateFunction::ApproxPercentile => write!(f, "APPROX_PERCENTILE"),
            AggregateFunction::ApproxDistinct => write!(f, "APPROX_DISTINCT"),
            AggregateFunction::MaxBy => write!(f, "MAX_BY"),
            AggregateFunction::MinBy => write!(f, "MIN_BY"),
        }
    }
}

/// Scalar function types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarFunction {
    // Math - Basic
    Abs,
    Ceil,
    Floor,
    Round,
    Power,
    Pow, // Alias for Power
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
    Rand, // Alias for Random
    // Math - Trigonometric
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,
    Sinh,
    Cosh,
    Tanh,
    Cot,
    Degrees,
    Radians,
    Pi,
    E,
    Cbrt,
    // Math - Special values
    Infinity,
    Nan,
    IsFinite,
    IsNan,
    IsInfinite,
    // Math - Base conversion
    FromBase,
    ToBase,
    WidthBucket,
    // Math - Statistical distributions
    BetaCdf,
    InverseBetaCdf,
    NormalCdf,
    InverseNormalCdf,
    TCdf,
    TPdf,
    WilsonIntervalLower,
    WilsonIntervalUpper,
    // Math - Vector operations
    CosineSimilarity,
    CosineDistance,
    // String - Basic
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
    Split,
    StartsWith,
    EndsWith,
    Chr,
    Ascii,
    ConcatWs,
    Left,
    Right,
    Repeat,
    // String - Advanced
    Codepoint,
    HammingDistance,
    LevenshteinDistance,
    Soundex,
    Translate,
    LuhnCheck,
    Normalize,
    ToUtf8,
    FromUtf8,
    WordStem,
    // Date/Time - Extraction
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Quarter,
    Week,
    DayOfWeek,
    DayOfYear,
    YearOfWeek,
    TimezoneHour,
    TimezoneMinute,
    // Date/Time - Current
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    CurrentTimezone,
    Localtime,
    Localtimestamp,
    Now,
    // Date/Time - Arithmetic
    DateTrunc,
    DatePart,
    DateAdd,
    DateDiff,
    LastDayOfMonth,
    // Date/Time - Parsing and formatting
    FromUnixtime,
    ToUnixtime,
    FromIso8601Timestamp,
    FromIso8601Date,
    ToIso8601,
    DateFormat,
    DateParse,
    ParseDatetime,
    ParseDuration,
    HumanReadableSeconds,
    // Date/Time - Timezone
    AtTimezone,
    WithTimezone,
    Timezone,
    // Type conversion
    Cast,
    TryCast,
    // Conditional
    Coalesce,
    NullIf,
    Case,
    If,
    Greatest,
    Least,
    Try,
    // Formatting
    Format,
    FormatNumber,
    ParseDataSize,
    // Regex
    RegexpLike,
    RegexpExtract,
    RegexpExtractAll,
    RegexpReplace,
    RegexpSplit,
    RegexpCount,
    RegexpPosition,
    // Binary/Encoding - Hex
    ToHex,
    FromHex,
    // Binary/Encoding - Base64
    ToBase64,
    FromBase64,
    ToBase64Url,
    FromBase64Url,
    // Binary/Encoding - Base32
    ToBase32,
    FromBase32,
    // Binary/Encoding - Hash
    Md5,
    Sha1,
    Sha256,
    Sha512,
    Crc32,
    Xxhash64,
    Murmur3,
    SpookyHashV2_32,
    SpookyHashV2_64,
    // Binary/Encoding - HMAC
    HmacMd5,
    HmacSha1,
    HmacSha256,
    HmacSha512,
    // Binary/Encoding - Endian conversion
    FromBigEndian32,
    ToBigEndian32,
    FromBigEndian64,
    ToBigEndian64,
    // Binary/Encoding - IEEE754
    FromIeee754_32,
    ToIeee754_32,
    FromIeee754_64,
    ToIeee754_64,
    // Bitwise
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseNot,
    BitCount,
    BitwiseLeftShift,
    BitwiseRightShift,
    BitwiseRightShiftArithmetic,
    // URL
    UrlExtractHost,
    UrlExtractPath,
    UrlExtractPort,
    UrlExtractProtocol,
    UrlExtractQuery,
    UrlExtractFragment,
    UrlExtractParameter,
    UrlEncode,
    UrlDecode,
    // Other
    Extract,
    Typeof,
    Uuid,
    // JSON functions
    JsonExtract,
    JsonExtractScalar,
    JsonSize,
    JsonArrayLength,
    JsonArrayGet,
    JsonArrayContains,
    IsJsonScalar,
    JsonFormat,
    JsonParse,
    JsonQuery,
    JsonValue,
    JsonExists,
    JsonObject,
    JsonArray,
    // Array functions
    Cardinality,
    ArrayLength,
    ElementAt,
    ArrayContains,
    ArrayPosition,
    ArrayDistinct,
    ArrayIntersect,
    ArrayUnion,
    ArrayExcept,
    ArrayJoin,
    ArrayMax,
    ArrayMin,
    ArrayRemove,
    ArraySort,
    ArraysOverlap,
    ArrayConcat,
    Flatten,
    ArrayReverse,
    Sequence,
    Shuffle,
    Slice,
    TrimArray,
    ArrayRepeat,
    Ngrams,
    Combinations,
    ArrayFirst,
    ArrayLast,
    ContainsSequence,
    Zip,
}

impl fmt::Display for ScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Math - Basic
            ScalarFunction::Abs => write!(f, "ABS"),
            ScalarFunction::Ceil => write!(f, "CEIL"),
            ScalarFunction::Floor => write!(f, "FLOOR"),
            ScalarFunction::Round => write!(f, "ROUND"),
            ScalarFunction::Power => write!(f, "POWER"),
            ScalarFunction::Pow => write!(f, "POW"),
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
            ScalarFunction::Rand => write!(f, "RAND"),
            // Math - Trigonometric
            ScalarFunction::Sin => write!(f, "SIN"),
            ScalarFunction::Cos => write!(f, "COS"),
            ScalarFunction::Tan => write!(f, "TAN"),
            ScalarFunction::Asin => write!(f, "ASIN"),
            ScalarFunction::Acos => write!(f, "ACOS"),
            ScalarFunction::Atan => write!(f, "ATAN"),
            ScalarFunction::Atan2 => write!(f, "ATAN2"),
            ScalarFunction::Sinh => write!(f, "SINH"),
            ScalarFunction::Cosh => write!(f, "COSH"),
            ScalarFunction::Tanh => write!(f, "TANH"),
            ScalarFunction::Cot => write!(f, "COT"),
            ScalarFunction::Degrees => write!(f, "DEGREES"),
            ScalarFunction::Radians => write!(f, "RADIANS"),
            ScalarFunction::Pi => write!(f, "PI"),
            ScalarFunction::E => write!(f, "E"),
            ScalarFunction::Cbrt => write!(f, "CBRT"),
            // Math - Special values
            ScalarFunction::Infinity => write!(f, "INFINITY"),
            ScalarFunction::Nan => write!(f, "NAN"),
            ScalarFunction::IsFinite => write!(f, "IS_FINITE"),
            ScalarFunction::IsNan => write!(f, "IS_NAN"),
            ScalarFunction::IsInfinite => write!(f, "IS_INFINITE"),
            // Math - Base conversion
            ScalarFunction::FromBase => write!(f, "FROM_BASE"),
            ScalarFunction::ToBase => write!(f, "TO_BASE"),
            ScalarFunction::WidthBucket => write!(f, "WIDTH_BUCKET"),
            // Math - Statistical distributions
            ScalarFunction::BetaCdf => write!(f, "BETA_CDF"),
            ScalarFunction::InverseBetaCdf => write!(f, "INVERSE_BETA_CDF"),
            ScalarFunction::NormalCdf => write!(f, "NORMAL_CDF"),
            ScalarFunction::InverseNormalCdf => write!(f, "INVERSE_NORMAL_CDF"),
            ScalarFunction::TCdf => write!(f, "T_CDF"),
            ScalarFunction::TPdf => write!(f, "T_PDF"),
            ScalarFunction::WilsonIntervalLower => write!(f, "WILSON_INTERVAL_LOWER"),
            ScalarFunction::WilsonIntervalUpper => write!(f, "WILSON_INTERVAL_UPPER"),
            // Math - Vector operations
            ScalarFunction::CosineSimilarity => write!(f, "COSINE_SIMILARITY"),
            ScalarFunction::CosineDistance => write!(f, "COSINE_DISTANCE"),
            // String - Basic
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
            ScalarFunction::Split => write!(f, "SPLIT"),
            ScalarFunction::StartsWith => write!(f, "STARTS_WITH"),
            ScalarFunction::EndsWith => write!(f, "ENDS_WITH"),
            ScalarFunction::Chr => write!(f, "CHR"),
            ScalarFunction::Ascii => write!(f, "ASCII"),
            ScalarFunction::ConcatWs => write!(f, "CONCAT_WS"),
            ScalarFunction::Left => write!(f, "LEFT"),
            ScalarFunction::Right => write!(f, "RIGHT"),
            ScalarFunction::Repeat => write!(f, "REPEAT"),
            // String - Advanced
            ScalarFunction::Codepoint => write!(f, "CODEPOINT"),
            ScalarFunction::HammingDistance => write!(f, "HAMMING_DISTANCE"),
            ScalarFunction::LevenshteinDistance => write!(f, "LEVENSHTEIN_DISTANCE"),
            ScalarFunction::Soundex => write!(f, "SOUNDEX"),
            ScalarFunction::Translate => write!(f, "TRANSLATE"),
            ScalarFunction::LuhnCheck => write!(f, "LUHN_CHECK"),
            ScalarFunction::Normalize => write!(f, "NORMALIZE"),
            ScalarFunction::ToUtf8 => write!(f, "TO_UTF8"),
            ScalarFunction::FromUtf8 => write!(f, "FROM_UTF8"),
            ScalarFunction::WordStem => write!(f, "WORD_STEM"),
            // Date/Time - Extraction
            ScalarFunction::Year => write!(f, "YEAR"),
            ScalarFunction::Month => write!(f, "MONTH"),
            ScalarFunction::Day => write!(f, "DAY"),
            ScalarFunction::Hour => write!(f, "HOUR"),
            ScalarFunction::Minute => write!(f, "MINUTE"),
            ScalarFunction::Second => write!(f, "SECOND"),
            ScalarFunction::Millisecond => write!(f, "MILLISECOND"),
            ScalarFunction::Quarter => write!(f, "QUARTER"),
            ScalarFunction::Week => write!(f, "WEEK"),
            ScalarFunction::DayOfWeek => write!(f, "DAY_OF_WEEK"),
            ScalarFunction::DayOfYear => write!(f, "DAY_OF_YEAR"),
            ScalarFunction::YearOfWeek => write!(f, "YEAR_OF_WEEK"),
            ScalarFunction::TimezoneHour => write!(f, "TIMEZONE_HOUR"),
            ScalarFunction::TimezoneMinute => write!(f, "TIMEZONE_MINUTE"),
            // Date/Time - Current
            ScalarFunction::CurrentDate => write!(f, "CURRENT_DATE"),
            ScalarFunction::CurrentTime => write!(f, "CURRENT_TIME"),
            ScalarFunction::CurrentTimestamp => write!(f, "CURRENT_TIMESTAMP"),
            ScalarFunction::CurrentTimezone => write!(f, "CURRENT_TIMEZONE"),
            ScalarFunction::Localtime => write!(f, "LOCALTIME"),
            ScalarFunction::Localtimestamp => write!(f, "LOCALTIMESTAMP"),
            ScalarFunction::Now => write!(f, "NOW"),
            // Date/Time - Arithmetic
            ScalarFunction::DateTrunc => write!(f, "DATE_TRUNC"),
            ScalarFunction::DatePart => write!(f, "DATE_PART"),
            ScalarFunction::DateAdd => write!(f, "DATE_ADD"),
            ScalarFunction::DateDiff => write!(f, "DATE_DIFF"),
            ScalarFunction::LastDayOfMonth => write!(f, "LAST_DAY_OF_MONTH"),
            // Date/Time - Parsing and formatting
            ScalarFunction::FromUnixtime => write!(f, "FROM_UNIXTIME"),
            ScalarFunction::ToUnixtime => write!(f, "TO_UNIXTIME"),
            ScalarFunction::FromIso8601Timestamp => write!(f, "FROM_ISO8601_TIMESTAMP"),
            ScalarFunction::FromIso8601Date => write!(f, "FROM_ISO8601_DATE"),
            ScalarFunction::ToIso8601 => write!(f, "TO_ISO8601"),
            ScalarFunction::DateFormat => write!(f, "DATE_FORMAT"),
            ScalarFunction::DateParse => write!(f, "DATE_PARSE"),
            ScalarFunction::ParseDatetime => write!(f, "PARSE_DATETIME"),
            ScalarFunction::ParseDuration => write!(f, "PARSE_DURATION"),
            ScalarFunction::HumanReadableSeconds => write!(f, "HUMAN_READABLE_SECONDS"),
            // Date/Time - Timezone
            ScalarFunction::AtTimezone => write!(f, "AT_TIMEZONE"),
            ScalarFunction::WithTimezone => write!(f, "WITH_TIMEZONE"),
            ScalarFunction::Timezone => write!(f, "TIMEZONE"),
            // Type conversion
            ScalarFunction::Cast => write!(f, "CAST"),
            ScalarFunction::TryCast => write!(f, "TRY_CAST"),
            // Conditional
            ScalarFunction::Coalesce => write!(f, "COALESCE"),
            ScalarFunction::NullIf => write!(f, "NULLIF"),
            ScalarFunction::Case => write!(f, "CASE"),
            ScalarFunction::If => write!(f, "IF"),
            ScalarFunction::Greatest => write!(f, "GREATEST"),
            ScalarFunction::Least => write!(f, "LEAST"),
            ScalarFunction::Try => write!(f, "TRY"),
            // Formatting
            ScalarFunction::Format => write!(f, "FORMAT"),
            ScalarFunction::FormatNumber => write!(f, "FORMAT_NUMBER"),
            ScalarFunction::ParseDataSize => write!(f, "PARSE_DATA_SIZE"),
            // Regex
            ScalarFunction::RegexpLike => write!(f, "REGEXP_LIKE"),
            ScalarFunction::RegexpExtract => write!(f, "REGEXP_EXTRACT"),
            ScalarFunction::RegexpExtractAll => write!(f, "REGEXP_EXTRACT_ALL"),
            ScalarFunction::RegexpReplace => write!(f, "REGEXP_REPLACE"),
            ScalarFunction::RegexpSplit => write!(f, "REGEXP_SPLIT"),
            ScalarFunction::RegexpCount => write!(f, "REGEXP_COUNT"),
            ScalarFunction::RegexpPosition => write!(f, "REGEXP_POSITION"),
            // Binary/Encoding - Hex
            ScalarFunction::ToHex => write!(f, "TO_HEX"),
            ScalarFunction::FromHex => write!(f, "FROM_HEX"),
            // Binary/Encoding - Base64
            ScalarFunction::ToBase64 => write!(f, "TO_BASE64"),
            ScalarFunction::FromBase64 => write!(f, "FROM_BASE64"),
            ScalarFunction::ToBase64Url => write!(f, "TO_BASE64URL"),
            ScalarFunction::FromBase64Url => write!(f, "FROM_BASE64URL"),
            // Binary/Encoding - Base32
            ScalarFunction::ToBase32 => write!(f, "TO_BASE32"),
            ScalarFunction::FromBase32 => write!(f, "FROM_BASE32"),
            // Binary/Encoding - Hash
            ScalarFunction::Md5 => write!(f, "MD5"),
            ScalarFunction::Sha1 => write!(f, "SHA1"),
            ScalarFunction::Sha256 => write!(f, "SHA256"),
            ScalarFunction::Sha512 => write!(f, "SHA512"),
            ScalarFunction::Crc32 => write!(f, "CRC32"),
            ScalarFunction::Xxhash64 => write!(f, "XXHASH64"),
            ScalarFunction::Murmur3 => write!(f, "MURMUR3"),
            ScalarFunction::SpookyHashV2_32 => write!(f, "SPOOKY_HASH_V2_32"),
            ScalarFunction::SpookyHashV2_64 => write!(f, "SPOOKY_HASH_V2_64"),
            // Binary/Encoding - HMAC
            ScalarFunction::HmacMd5 => write!(f, "HMAC_MD5"),
            ScalarFunction::HmacSha1 => write!(f, "HMAC_SHA1"),
            ScalarFunction::HmacSha256 => write!(f, "HMAC_SHA256"),
            ScalarFunction::HmacSha512 => write!(f, "HMAC_SHA512"),
            // Binary/Encoding - Endian conversion
            ScalarFunction::FromBigEndian32 => write!(f, "FROM_BIG_ENDIAN_32"),
            ScalarFunction::ToBigEndian32 => write!(f, "TO_BIG_ENDIAN_32"),
            ScalarFunction::FromBigEndian64 => write!(f, "FROM_BIG_ENDIAN_64"),
            ScalarFunction::ToBigEndian64 => write!(f, "TO_BIG_ENDIAN_64"),
            // Binary/Encoding - IEEE754
            ScalarFunction::FromIeee754_32 => write!(f, "FROM_IEEE754_32"),
            ScalarFunction::ToIeee754_32 => write!(f, "TO_IEEE754_32"),
            ScalarFunction::FromIeee754_64 => write!(f, "FROM_IEEE754_64"),
            ScalarFunction::ToIeee754_64 => write!(f, "TO_IEEE754_64"),
            // Bitwise
            ScalarFunction::BitwiseAnd => write!(f, "BITWISE_AND"),
            ScalarFunction::BitwiseOr => write!(f, "BITWISE_OR"),
            ScalarFunction::BitwiseXor => write!(f, "BITWISE_XOR"),
            ScalarFunction::BitwiseNot => write!(f, "BITWISE_NOT"),
            ScalarFunction::BitCount => write!(f, "BIT_COUNT"),
            ScalarFunction::BitwiseLeftShift => write!(f, "BITWISE_LEFT_SHIFT"),
            ScalarFunction::BitwiseRightShift => write!(f, "BITWISE_RIGHT_SHIFT"),
            ScalarFunction::BitwiseRightShiftArithmetic => {
                write!(f, "BITWISE_RIGHT_SHIFT_ARITHMETIC")
            }
            // URL
            ScalarFunction::UrlExtractHost => write!(f, "URL_EXTRACT_HOST"),
            ScalarFunction::UrlExtractPath => write!(f, "URL_EXTRACT_PATH"),
            ScalarFunction::UrlExtractPort => write!(f, "URL_EXTRACT_PORT"),
            ScalarFunction::UrlExtractProtocol => write!(f, "URL_EXTRACT_PROTOCOL"),
            ScalarFunction::UrlExtractQuery => write!(f, "URL_EXTRACT_QUERY"),
            ScalarFunction::UrlExtractFragment => write!(f, "URL_EXTRACT_FRAGMENT"),
            ScalarFunction::UrlExtractParameter => write!(f, "URL_EXTRACT_PARAMETER"),
            ScalarFunction::UrlEncode => write!(f, "URL_ENCODE"),
            ScalarFunction::UrlDecode => write!(f, "URL_DECODE"),
            // Other
            ScalarFunction::Extract => write!(f, "EXTRACT"),
            ScalarFunction::Typeof => write!(f, "TYPEOF"),
            ScalarFunction::Uuid => write!(f, "UUID"),
            // JSON
            ScalarFunction::JsonExtract => write!(f, "JSON_EXTRACT"),
            ScalarFunction::JsonExtractScalar => write!(f, "JSON_EXTRACT_SCALAR"),
            ScalarFunction::JsonSize => write!(f, "JSON_SIZE"),
            ScalarFunction::JsonArrayLength => write!(f, "JSON_ARRAY_LENGTH"),
            ScalarFunction::JsonArrayGet => write!(f, "JSON_ARRAY_GET"),
            ScalarFunction::JsonArrayContains => write!(f, "JSON_ARRAY_CONTAINS"),
            ScalarFunction::IsJsonScalar => write!(f, "IS_JSON_SCALAR"),
            ScalarFunction::JsonFormat => write!(f, "JSON_FORMAT"),
            ScalarFunction::JsonParse => write!(f, "JSON_PARSE"),
            ScalarFunction::JsonQuery => write!(f, "JSON_QUERY"),
            ScalarFunction::JsonValue => write!(f, "JSON_VALUE"),
            ScalarFunction::JsonExists => write!(f, "JSON_EXISTS"),
            ScalarFunction::JsonObject => write!(f, "JSON_OBJECT"),
            ScalarFunction::JsonArray => write!(f, "JSON_ARRAY"),
            // Array
            ScalarFunction::Cardinality => write!(f, "CARDINALITY"),
            ScalarFunction::ArrayLength => write!(f, "ARRAY_LENGTH"),
            ScalarFunction::ElementAt => write!(f, "ELEMENT_AT"),
            ScalarFunction::ArrayContains => write!(f, "ARRAY_CONTAINS"),
            ScalarFunction::ArrayPosition => write!(f, "ARRAY_POSITION"),
            ScalarFunction::ArrayDistinct => write!(f, "ARRAY_DISTINCT"),
            ScalarFunction::ArrayIntersect => write!(f, "ARRAY_INTERSECT"),
            ScalarFunction::ArrayUnion => write!(f, "ARRAY_UNION"),
            ScalarFunction::ArrayExcept => write!(f, "ARRAY_EXCEPT"),
            ScalarFunction::ArrayJoin => write!(f, "ARRAY_JOIN"),
            ScalarFunction::ArrayMax => write!(f, "ARRAY_MAX"),
            ScalarFunction::ArrayMin => write!(f, "ARRAY_MIN"),
            ScalarFunction::ArrayRemove => write!(f, "ARRAY_REMOVE"),
            ScalarFunction::ArraySort => write!(f, "ARRAY_SORT"),
            ScalarFunction::ArraysOverlap => write!(f, "ARRAYS_OVERLAP"),
            ScalarFunction::ArrayConcat => write!(f, "ARRAY_CONCAT"),
            ScalarFunction::Flatten => write!(f, "FLATTEN"),
            ScalarFunction::ArrayReverse => write!(f, "ARRAY_REVERSE"),
            ScalarFunction::Sequence => write!(f, "SEQUENCE"),
            ScalarFunction::Shuffle => write!(f, "SHUFFLE"),
            ScalarFunction::Slice => write!(f, "SLICE"),
            ScalarFunction::TrimArray => write!(f, "TRIM_ARRAY"),
            ScalarFunction::ArrayRepeat => write!(f, "ARRAY_REPEAT"),
            ScalarFunction::Ngrams => write!(f, "NGRAMS"),
            ScalarFunction::Combinations => write!(f, "COMBINATIONS"),
            ScalarFunction::ArrayFirst => write!(f, "ARRAY_FIRST"),
            ScalarFunction::ArrayLast => write!(f, "ARRAY_LAST"),
            ScalarFunction::ContainsSequence => write!(f, "CONTAINS_SEQUENCE"),
            ScalarFunction::Zip => write!(f, "ZIP"),
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
                // New aggregate functions
                AggregateFunction::CountIf | AggregateFunction::RegrCount => {
                    Ok(ArrowDataType::Int64)
                }
                AggregateFunction::AnyValue | AggregateFunction::Arbitrary => args
                    .first()
                    .map(|a| a.data_type(schema))
                    .unwrap_or(Ok(ArrowDataType::Null)),
                AggregateFunction::GeometricMean
                | AggregateFunction::Corr
                | AggregateFunction::CovarPop
                | AggregateFunction::CovarSamp
                | AggregateFunction::Kurtosis
                | AggregateFunction::Skewness
                | AggregateFunction::RegrSlope
                | AggregateFunction::RegrIntercept
                | AggregateFunction::RegrAvgx
                | AggregateFunction::RegrAvgy
                | AggregateFunction::ApproxPercentile => Ok(ArrowDataType::Float64),
                AggregateFunction::Checksum
                | AggregateFunction::BitwiseAndAgg
                | AggregateFunction::BitwiseOrAgg
                | AggregateFunction::BitwiseXorAgg
                | AggregateFunction::ApproxDistinct => Ok(ArrowDataType::Int64),
                AggregateFunction::Listagg => Ok(ArrowDataType::Utf8),
                AggregateFunction::MaxBy | AggregateFunction::MinBy => args
                    .first()
                    .map(|a| a.data_type(schema))
                    .unwrap_or(Ok(ArrowDataType::Null)),
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
                    | ScalarFunction::Millisecond
                    | ScalarFunction::Quarter
                    | ScalarFunction::Week
                    | ScalarFunction::DayOfWeek
                    | ScalarFunction::DayOfYear
                    | ScalarFunction::YearOfWeek
                    | ScalarFunction::TimezoneHour
                    | ScalarFunction::TimezoneMinute => Ok(ArrowDataType::Int32),
                    // Date/time functions returning Timestamp
                    ScalarFunction::CurrentTimestamp
                    | ScalarFunction::Now
                    | ScalarFunction::Localtimestamp
                    | ScalarFunction::FromUnixtime
                    | ScalarFunction::FromIso8601Timestamp
                    | ScalarFunction::DateParse
                    | ScalarFunction::ParseDatetime
                    | ScalarFunction::AtTimezone
                    | ScalarFunction::WithTimezone => Ok(ArrowDataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        None,
                    )),
                    // Date/time functions returning Date32
                    ScalarFunction::CurrentDate
                    | ScalarFunction::FromIso8601Date
                    | ScalarFunction::LastDayOfMonth => Ok(ArrowDataType::Date32),
                    // Time functions
                    ScalarFunction::CurrentTime | ScalarFunction::Localtime => Ok(
                        ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                    ),
                    // Duration/Interval functions
                    ScalarFunction::ParseDuration => Ok(ArrowDataType::Interval(
                        arrow::datatypes::IntervalUnit::DayTime,
                    )),
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
                    // Math functions returning Int64 (mod, bitwise, base conversion)
                    ScalarFunction::Mod
                    | ScalarFunction::BitwiseAnd
                    | ScalarFunction::BitwiseOr
                    | ScalarFunction::BitwiseXor
                    | ScalarFunction::BitwiseNot
                    | ScalarFunction::BitCount
                    | ScalarFunction::BitwiseLeftShift
                    | ScalarFunction::BitwiseRightShift
                    | ScalarFunction::BitwiseRightShiftArithmetic
                    | ScalarFunction::FromBase
                    | ScalarFunction::WidthBucket
                    | ScalarFunction::Codepoint
                    | ScalarFunction::HammingDistance
                    | ScalarFunction::LevenshteinDistance
                    | ScalarFunction::Xxhash64
                    | ScalarFunction::SpookyHashV2_64
                    | ScalarFunction::FromBigEndian64
                    | ScalarFunction::ParseDataSize => Ok(ArrowDataType::Int64),
                    // Functions returning Int32
                    ScalarFunction::Crc32
                    | ScalarFunction::Murmur3
                    | ScalarFunction::SpookyHashV2_32
                    | ScalarFunction::FromBigEndian32 => Ok(ArrowDataType::Int32),
                    // Math functions returning Float64
                    ScalarFunction::Power
                    | ScalarFunction::Pow
                    | ScalarFunction::Sqrt
                    | ScalarFunction::Ln
                    | ScalarFunction::Log
                    | ScalarFunction::Log2
                    | ScalarFunction::Log10
                    | ScalarFunction::Exp
                    | ScalarFunction::Random
                    | ScalarFunction::Rand
                    | ScalarFunction::Sin
                    | ScalarFunction::Cos
                    | ScalarFunction::Tan
                    | ScalarFunction::Asin
                    | ScalarFunction::Acos
                    | ScalarFunction::Atan
                    | ScalarFunction::Atan2
                    | ScalarFunction::Sinh
                    | ScalarFunction::Cosh
                    | ScalarFunction::Tanh
                    | ScalarFunction::Cot
                    | ScalarFunction::Degrees
                    | ScalarFunction::Radians
                    | ScalarFunction::Pi
                    | ScalarFunction::E
                    | ScalarFunction::Cbrt
                    | ScalarFunction::Infinity
                    | ScalarFunction::Nan
                    | ScalarFunction::BetaCdf
                    | ScalarFunction::InverseBetaCdf
                    | ScalarFunction::NormalCdf
                    | ScalarFunction::InverseNormalCdf
                    | ScalarFunction::TCdf
                    | ScalarFunction::TPdf
                    | ScalarFunction::WilsonIntervalLower
                    | ScalarFunction::WilsonIntervalUpper
                    | ScalarFunction::CosineSimilarity
                    | ScalarFunction::CosineDistance => Ok(ArrowDataType::Float64),
                    // Math functions returning Boolean
                    ScalarFunction::IsFinite
                    | ScalarFunction::IsNan
                    | ScalarFunction::IsInfinite
                    | ScalarFunction::LuhnCheck => Ok(ArrowDataType::Boolean),
                    // Conditional functions - find first non-null type
                    ScalarFunction::Coalesce | ScalarFunction::Greatest | ScalarFunction::Least => {
                        // Try to find the first non-null type among arguments
                        for arg in args {
                            let arg_type = arg.data_type(schema)?;
                            if arg_type != ArrowDataType::Null {
                                return Ok(arg_type);
                            }
                        }
                        // If all are null, return Null
                        Ok(ArrowDataType::Null)
                    }
                    ScalarFunction::NullIf => args
                        .first()
                        .map(|a| a.data_type(schema))
                        .unwrap_or(Ok(ArrowDataType::Null)),
                    // IF function - return type is from second arg (then branch)
                    ScalarFunction::If => args
                        .get(1)
                        .map(|a| a.data_type(schema))
                        .unwrap_or(Ok(ArrowDataType::Null)),
                    // Regex functions returning string
                    ScalarFunction::RegexpExtract
                    | ScalarFunction::RegexpExtractAll
                    | ScalarFunction::RegexpReplace
                    | ScalarFunction::RegexpSplit => Ok(ArrowDataType::Utf8),
                    // Regex functions returning Int64
                    ScalarFunction::RegexpCount | ScalarFunction::RegexpPosition => {
                        Ok(ArrowDataType::Int64)
                    }
                    // Binary/Encoding functions returning Utf8 (string hashes)
                    ScalarFunction::ToHex
                    | ScalarFunction::ToBase64
                    | ScalarFunction::ToBase64Url
                    | ScalarFunction::ToBase32
                    | ScalarFunction::Md5
                    | ScalarFunction::Sha1
                    | ScalarFunction::Sha256
                    | ScalarFunction::Sha512
                    | ScalarFunction::HmacMd5
                    | ScalarFunction::HmacSha1
                    | ScalarFunction::HmacSha256
                    | ScalarFunction::HmacSha512
                    | ScalarFunction::ToBase
                    | ScalarFunction::ToIso8601
                    | ScalarFunction::DateFormat
                    | ScalarFunction::HumanReadableSeconds
                    | ScalarFunction::CurrentTimezone
                    | ScalarFunction::Timezone
                    | ScalarFunction::Soundex
                    | ScalarFunction::Translate
                    | ScalarFunction::Normalize
                    | ScalarFunction::FromUtf8
                    | ScalarFunction::WordStem
                    | ScalarFunction::Split
                    | ScalarFunction::Format
                    | ScalarFunction::FormatNumber => Ok(ArrowDataType::Utf8),
                    // Binary/Encoding functions returning Binary
                    ScalarFunction::FromHex
                    | ScalarFunction::FromBase64
                    | ScalarFunction::FromBase64Url
                    | ScalarFunction::FromBase32
                    | ScalarFunction::ToUtf8
                    | ScalarFunction::ToBigEndian32
                    | ScalarFunction::ToBigEndian64
                    | ScalarFunction::ToIeee754_32
                    | ScalarFunction::ToIeee754_64 => Ok(ArrowDataType::Binary),
                    // IEEE754 returning Float
                    ScalarFunction::FromIeee754_32 => Ok(ArrowDataType::Float32),
                    ScalarFunction::FromIeee754_64 => Ok(ArrowDataType::Float64),
                    // URL functions returning Utf8
                    ScalarFunction::UrlExtractHost
                    | ScalarFunction::UrlExtractPath
                    | ScalarFunction::UrlExtractProtocol
                    | ScalarFunction::UrlExtractQuery
                    | ScalarFunction::UrlExtractFragment
                    | ScalarFunction::UrlExtractParameter
                    | ScalarFunction::UrlEncode
                    | ScalarFunction::UrlDecode => Ok(ArrowDataType::Utf8),
                    ScalarFunction::UrlExtractPort => Ok(ArrowDataType::Int32),
                    // Unix timestamp conversion
                    ScalarFunction::ToUnixtime => Ok(ArrowDataType::Int64),
                    // Other
                    ScalarFunction::Typeof => Ok(ArrowDataType::Utf8),
                    ScalarFunction::Uuid => Ok(ArrowDataType::Utf8),
                    ScalarFunction::DateAdd | ScalarFunction::DateTrunc => Ok(
                        ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    ),
                    ScalarFunction::DateDiff => Ok(ArrowDataType::Int64),
                    ScalarFunction::DatePart => Ok(ArrowDataType::Float64),
                    // TryCast and Try preserve input type or return null
                    ScalarFunction::TryCast | ScalarFunction::Try => args
                        .first()
                        .map(|a| a.data_type(schema))
                        .unwrap_or(Ok(ArrowDataType::Null)),
                    // JSON functions
                    ScalarFunction::JsonExtract
                    | ScalarFunction::JsonExtractScalar
                    | ScalarFunction::JsonArrayGet
                    | ScalarFunction::JsonFormat
                    | ScalarFunction::JsonParse
                    | ScalarFunction::JsonQuery
                    | ScalarFunction::JsonValue
                    | ScalarFunction::JsonObject
                    | ScalarFunction::JsonArray => Ok(ArrowDataType::Utf8),
                    ScalarFunction::JsonSize | ScalarFunction::JsonArrayLength => {
                        Ok(ArrowDataType::Int64)
                    }
                    ScalarFunction::JsonArrayContains
                    | ScalarFunction::IsJsonScalar
                    | ScalarFunction::JsonExists => Ok(ArrowDataType::Boolean),
                    // Array functions returning Int64
                    ScalarFunction::Cardinality
                    | ScalarFunction::ArrayLength
                    | ScalarFunction::ArrayPosition => Ok(ArrowDataType::Int64),
                    // Array functions returning Boolean
                    ScalarFunction::ArrayContains
                    | ScalarFunction::ArraysOverlap
                    | ScalarFunction::ContainsSequence => Ok(ArrowDataType::Boolean),
                    // Array functions returning Utf8
                    ScalarFunction::ArrayJoin => Ok(ArrowDataType::Utf8),
                    // Array functions returning element type
                    ScalarFunction::ElementAt
                    | ScalarFunction::ArrayMax
                    | ScalarFunction::ArrayMin
                    | ScalarFunction::ArrayFirst
                    | ScalarFunction::ArrayLast => {
                        // Try to get element type from input array
                        if let Some(arg) = args.first() {
                            let arg_type = arg.data_type(schema)?;
                            if let ArrowDataType::List(field) = arg_type {
                                Ok(field.data_type().clone())
                            } else {
                                Ok(ArrowDataType::Utf8) // Fallback for JSON arrays
                            }
                        } else {
                            Ok(ArrowDataType::Null)
                        }
                    }
                    // Array functions returning a list
                    ScalarFunction::ArrayDistinct
                    | ScalarFunction::ArrayIntersect
                    | ScalarFunction::ArrayUnion
                    | ScalarFunction::ArrayExcept
                    | ScalarFunction::Flatten
                    | ScalarFunction::ArraySort
                    | ScalarFunction::ArrayRemove
                    | ScalarFunction::ArrayReverse
                    | ScalarFunction::Shuffle
                    | ScalarFunction::Slice
                    | ScalarFunction::TrimArray
                    | ScalarFunction::Ngrams
                    | ScalarFunction::Combinations
                    | ScalarFunction::ArrayConcat
                    | ScalarFunction::Zip => {
                        // Return the same list type as input
                        if let Some(arg) = args.first() {
                            arg.data_type(schema)
                        } else {
                            Ok(ArrowDataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                ArrowDataType::Utf8,
                                true,
                            ))))
                        }
                    }
                    // Sequence and ArrayRepeat return a list of int64
                    ScalarFunction::Sequence => Ok(ArrowDataType::List(Arc::new(
                        arrow::datatypes::Field::new("item", ArrowDataType::Int64, true),
                    ))),
                    ScalarFunction::ArrayRepeat => {
                        // Return list of element type
                        if let Some(arg) = args.first() {
                            let elem_type = arg.data_type(schema)?;
                            Ok(ArrowDataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item", elem_type, true,
                            ))))
                        } else {
                            Ok(ArrowDataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                ArrowDataType::Utf8,
                                true,
                            ))))
                        }
                    }
                    // Default for remaining string functions
                    _ => Ok(ArrowDataType::Utf8),
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
