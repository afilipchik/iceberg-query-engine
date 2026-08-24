//! Native table manifest: a JSON sidecar (`_manifest.json`) that gives a
//! native table its own identity, versioning and zone-map statistics,
//! independent of any source file's path/mtime/size. Format and layout
//! decision, with full reasoning:
//! `.claude/epics/native-tables-foundation/001.md`'s Outcome section
//! (Decision 2: JSON sidecar, not Arrow IPC/Parquet, not Iceberg's Avro;
//! Decision 3: this file is a NEW sibling module, `ipc_cache.rs` untouched).
//!
//! # What this module owns
//!
//! - The manifest schema (`NativeManifest`/`Snapshot`/`Segment`/
//!   `ColumnStats`) and its JSON (de)serialization, mirroring
//!   `iceberg.rs`'s `TableMetadata`/`Snapshot` STRUCTURE (a version marker
//!   plus a list of data files with per-file stats) without adopting
//!   Iceberg's Avro wire format or its partition/snapshot-lineage model.
//! - Table identity (`table_id`, generated once via UUID v4, meant to stay
//!   stable across full-table replaces) and versioning (`snapshot.version`
//!   — this module computes the NEXT version via `next_version`, but a
//!   replace itself is task 003's job, out of scope here).
//! - Per-segment AND table-level rollup statistics, computed straight from
//!   Arrow arrays (`compute_batch_stats`) — no parquet footer involved,
//!   unlike `ipc_cache.rs`'s v2 dictionary-candidate detection which reads
//!   parquet column-chunk metadata (not reusable for this purpose; see
//!   `002.md`'s Technical Details).
//! - Atomic publication of a whole table directory (manifest + segments
//!   together), mirroring `ipc_cache.rs::build_sidecar`'s own
//!   staging-dir-then-rename pattern. Plus (native-tables-mutation epic,
//!   task 002) atomic publication of a SINGLE manifest file
//!   (`write_manifest_atomic`) for an INCREMENTAL mutation that must
//!   leave a directory's existing segment files untouched — see that
//!   function's doc for why `publish_table_dir`'s whole-directory replace
//!   is not safe to reuse for that case.
//! - (native-tables-mutation epic, task 003) `Segment::deleted_rows`: a
//!   sorted, deduplicated `Vec<u32>` of tombstoned LOCAL row positions,
//!   `#[serde(default)]` so every manifest phase 1/task 002 ever wrote
//!   still reads back unchanged (empty = no deletions). This module owns
//!   only the FIELD, its serialization, and `validate()`'s bounds/sort
//!   checks; consultation at read time lives in `native_table.rs::scan`
//!   and editing lives in `native_delete.rs` (both task 003, neither
//!   touches this file beyond what's described here).
//!
//! # What this module does NOT own
//!
//! Segment data itself is unchanged Arrow IPC, read back exactly the way
//! `ipc_cache.rs`'s sidecars already are: `ipc_cache::read_row_group` and
//! `ipc_cache::sidecar_dict_cols` are called UNCHANGED by whichever task
//! implements the `TableProvider` (004) — this module needs no read-path
//! code of its own. `read_row_group`'s compatibility with this module's
//! output is proven directly by this module's own tests (see
//! `read_row_group_reads_a_manifest_described_segment_unchanged` below),
//! not merely asserted.
//!
//! # Load-bearing finding: segment file names are NOT a free choice
//!
//! `ipc_cache::read_row_group(dir, rg_idx, ..)` does not accept a path —
//! it computes one ITSELF, via a private `rg_path` helper hard-coded to
//! `rg_{rg_idx:05}.arrow`. For task 004's provider to call that function
//! UNCHANGED (the whole point of reusing it), every segment file MUST be
//! physically named exactly `rg_{id:05}.arrow`, using the segment's `id`
//! as the index — NOT the `seg_00000.arrow` example name earlier planning
//! notes used illustratively; that name would silently fail to read via
//! `ipc_cache::read_row_group` (`NotFound`, since the file it actually
//! opens is named differently). `Segment::expected_file_name` is the
//! single source of truth for the required name, and
//! `NativeManifest::validate` REJECTS any manifest whose declared `path`
//! disagrees with it, so this mistake becomes a loud error at write/read
//! time rather than a silent "segment fails to read" bug discovered later.
//!
//! This finding required NO changes to `ipc_cache.rs` — its existing `pub
//! fn`s are sufficient exactly as task 001 predicted; the constraint is
//! satisfied entirely by this module choosing the right file name.

use crate::error::{QueryError, Result};
use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
use arrow::compute::kernels::aggregate::{max as arr_max, min as arr_min};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The manifest's file name inside a table directory.
pub const MANIFEST_FILE_NAME: &str = "_manifest.json";

/// The manifest format this build reads and writes. A manifest whose
/// `format_version` differs is refused by name (see `NativeManifest::
/// validate`) rather than guessed at — the same discipline
/// `iceberg.rs::open_table` applies to Iceberg's own `format-version`.
pub const FORMAT_VERSION: u32 = 1;

// ============================================================================
// Schema representation
//
// Arrow's `DataType` doesn't derive `Serialize`/`Deserialize` for every
// variant (task 001's finding), so this is a small, deliberately CURATED
// subset — every type TPC-H's own generator uses (`src/tpch/schema.rs`:
// Int64, Int32, Utf8, Float64, Date32) plus the reasonable headroom a
// Parquet/Iceberg/Lance source might carry (Boolean, more integer widths,
// Decimal, Timestamp, Dictionary — the last because `ipc_cache.rs` v2's own
// dictionary coercion produces `Dictionary(Int32, Utf8)` columns, and this
// module's schema must track that consistently with a segment's actual IPC
// schema, per 002.md's own note). Nested/opaque types (List, Struct, Map,
// FixedSizeList) are refused BY NAME with the offending type printed, never
// silently dropped or guessed.
// ============================================================================

/// JSON-serializable mirror of `arrow::datatypes::TimeUnit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<TimeUnit> for ManifestTimeUnit {
    fn from(u: TimeUnit) -> Self {
        match u {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

impl From<ManifestTimeUnit> for TimeUnit {
    fn from(u: ManifestTimeUnit) -> Self {
        match u {
            ManifestTimeUnit::Second => Self::Second,
            ManifestTimeUnit::Millisecond => Self::Millisecond,
            ManifestTimeUnit::Microsecond => Self::Microsecond,
            ManifestTimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

/// JSON-serializable mirror of the (curated) subset of `arrow::datatypes::
/// DataType` this manifest format supports. See the module doc's "Schema
/// representation" section for why this is curated rather than exhaustive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ManifestDataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp {
        unit: ManifestTimeUnit,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tz: Option<String>,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Dictionary {
        key: Box<ManifestDataType>,
        value: Box<ManifestDataType>,
    },
}

impl ManifestDataType {
    /// Convert from an Arrow `DataType`. Unsupported (nested/opaque) types
    /// are refused by name — never silently dropped or approximated.
    pub fn from_arrow(dt: &DataType) -> Result<Self> {
        Ok(match dt {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Timestamp(unit, tz) => Self::Timestamp {
                unit: (*unit).into(),
                tz: tz.as_ref().map(|s| s.to_string()),
            },
            DataType::Decimal128(precision, scale) => Self::Decimal128 {
                precision: *precision,
                scale: *scale,
            },
            DataType::Dictionary(key, value) => Self::Dictionary {
                key: Box::new(Self::from_arrow(key)?),
                value: Box::new(Self::from_arrow(value)?),
            },
            other => {
                return Err(QueryError::NotImplemented(format!(
                    "native table manifest: Arrow type {other:?} has no manifest \
                     representation (supported: bool, int8-64, uint8-64, float32/64, \
                     utf8/large_utf8, binary/large_binary, date32/64, timestamp, \
                     decimal128, dictionary)"
                )))
            }
        })
    }

    /// Convert back to an Arrow `DataType`. Infallible: every
    /// `ManifestDataType` variant is, by construction, one this build can
    /// represent as Arrow (the inverse of `from_arrow`'s curated subset).
    pub fn to_arrow(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::LargeUtf8 => DataType::LargeUtf8,
            Self::Binary => DataType::Binary,
            Self::LargeBinary => DataType::LargeBinary,
            Self::Date32 => DataType::Date32,
            Self::Date64 => DataType::Date64,
            Self::Timestamp { unit, tz } => {
                DataType::Timestamp((*unit).into(), tz.clone().map(|s| s.into()))
            }
            Self::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::Dictionary { key, value } => {
                DataType::Dictionary(Box::new(key.to_arrow()), Box::new(value.to_arrow()))
            }
        }
    }
}

/// One column of the manifest's declared schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestField {
    pub name: String,
    pub data_type: ManifestDataType,
    pub nullable: bool,
}

impl ManifestField {
    pub fn from_arrow(field: &Field) -> Result<Self> {
        Ok(Self {
            name: field.name().clone(),
            data_type: ManifestDataType::from_arrow(field.data_type()).map_err(|e| {
                QueryError::NotImplemented(format!("column `{}`: {e}", field.name()))
            })?,
            nullable: field.is_nullable(),
        })
    }

    pub fn to_arrow(&self) -> Field {
        Field::new(&self.name, self.data_type.to_arrow(), self.nullable)
    }
}

/// Convert a full Arrow schema to the manifest's field list.
pub fn schema_to_manifest_fields(schema: &Schema) -> Result<Vec<ManifestField>> {
    schema
        .fields()
        .iter()
        .map(|f| ManifestField::from_arrow(f))
        .collect()
}

/// Convert a manifest's field list back to an Arrow schema.
pub fn manifest_fields_to_schema(fields: &[ManifestField]) -> SchemaRef {
    Arc::new(Schema::new(
        fields.iter().map(|f| f.to_arrow()).collect::<Vec<_>>(),
    ))
}

// ============================================================================
// Statistics
//
// `ColumnStats` mirrors `src/physical/operators/scan.rs`'s existing
// `ColumnStatistics` field-for-field (`min_i64`/`max_i64`/`null_count`/
// `min_f64`/`max_f64`) so task 004's `statistics()` and task 005's
// dense-direct-address consumer need no translation layer — just a
// field-by-field copy, plus `ndv_est` DERIVED at that point from
// `min_i64`/`max_i64`/`row_count` exactly the way `ParquetTable` and
// `LanceTable` already derive it, rather than stored redundantly here.
// ============================================================================

/// Per-column statistics, computed directly from Arrow arrays (no parquet
/// footer, no external file involved). Every field is optional: `None`
/// means "no such stat recorded" (either the column's type isn't
/// zone-mappable, e.g. plain `Utf8`, or the column/segment had no non-null
/// values to bound) — never "unknown/absent due to missing footer data",
/// which is a distinction `ParquetTable`'s equivalent accumulator has to
/// make (see `storage/parquet.rs::compute_statistics`'s "poisons null_count
/// accuracy" comment) but this format does not, since every stat here is
/// always computed fresh from data this module's own writer controls.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_i64: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_i64: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_f64: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_f64: Option<f64>,
}

impl ColumnStats {
    /// Fold `other` into `self`: min-of-mins, max-of-maxes, sum-of-nulls,
    /// treating a `None` side as identity (ignored) rather than poisoning
    /// the result — see the struct doc for why that's correct here. Used
    /// both to build the table-level rollup from segments and, by task 003,
    /// to accumulate a segment's stats across multiple incoming batches.
    pub fn merge(&mut self, other: &ColumnStats) {
        self.min_i64 = merge_min_i64(self.min_i64, other.min_i64);
        self.max_i64 = merge_max_i64(self.max_i64, other.max_i64);
        self.min_f64 = merge_min_f64(self.min_f64, other.min_f64);
        self.max_f64 = merge_max_f64(self.max_f64, other.max_f64);
        self.null_count = merge_sum_u64(self.null_count, other.null_count);
    }
}

fn merge_min_i64(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn merge_max_i64(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn merge_min_f64(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn merge_max_f64(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn merge_sum_u64(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x + y),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

/// Bit-pattern equality for two `ColumnStats`, used only by
/// `NativeManifest::validate`'s rollup-consistency check. Deliberately NOT
/// `derive(PartialEq)`'s plain `==`: `f64`'s `PartialEq` says `NaN != NaN`,
/// which would make validation of a perfectly legitimate manifest (whose
/// data genuinely contains a NaN) fail spuriously, since the same
/// deterministic fold recomputed twice produces the identical NaN bit
/// pattern both times but `NaN == NaN` is still `false` under `PartialEq`.
fn column_stats_bit_eq(a: &ColumnStats, b: &ColumnStats) -> bool {
    fn f64_opt_bits_eq(a: Option<f64>, b: Option<f64>) -> bool {
        match (a, b) {
            (Some(x), Some(y)) => x.to_bits() == y.to_bits(),
            (None, None) => true,
            _ => false,
        }
    }
    a.min_i64 == b.min_i64
        && a.max_i64 == b.max_i64
        && a.null_count == b.null_count
        && f64_opt_bits_eq(a.min_f64, b.min_f64)
        && f64_opt_bits_eq(a.max_f64, b.max_f64)
}

fn stats_maps_bit_eq(a: &BTreeMap<String, ColumnStats>, b: &BTreeMap<String, ColumnStats>) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(k, v)| b.get(k).is_some_and(|v2| column_stats_bit_eq(v, v2)))
}

/// Merge every entry of `other` into `into`, per-column (see
/// `ColumnStats::merge`). The building block both `NativeManifest::rollup`
/// (fold segments into the table-level rollup) and task 003 (fold batches
/// into a running per-segment accumulator, streamed, never a second pass)
/// are expected to use.
pub fn merge_stats_into(
    into: &mut BTreeMap<String, ColumnStats>,
    other: &BTreeMap<String, ColumnStats>,
) {
    for (k, v) in other {
        into.entry(k.clone()).or_default().merge(v);
    }
}

/// Compute one column's stats from an Arrow array. `null_count` is always
/// populated (cheap and type-agnostic — and, per the Lance lesson recorded
/// in this repo's CLAUDE.md, load-bearing for EVERY column type, not just
/// integers: restricting it to integer columns once made `EagerAggregation`
/// silently decline a valid float pre-aggregation). `min_i64`/`max_i64`
/// populate for integer-classed and date columns (matching
/// `src/storage/lance.rs::compute_column_stats`'s own established
/// technique: cast to `Int64` and use the shared aggregate kernels;
/// deliberately excludes `UInt64`, since casting a value above `i64::MAX`
/// would silently wrap — the same exclusion Lance's version makes and for
/// the same reason). `min_f64`/`max_f64` populate for float columns. Every
/// other type (`Utf8`, `Dictionary`, `Binary`, ...) gets `null_count` only.
pub fn column_stats_for_array(array: &ArrayRef) -> ColumnStats {
    let mut stats = ColumnStats {
        null_count: Some(array.null_count() as u64),
        ..Default::default()
    };

    match array.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Date32
        | DataType::Date64 => {
            if let Ok(as_i64) = cast(array, &DataType::Int64) {
                if let Some(a) = as_i64.as_any().downcast_ref::<Int64Array>() {
                    stats.min_i64 = arr_min(a);
                    stats.max_i64 = arr_max(a);
                }
            }
        }
        DataType::Float32 => {
            if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
                stats.min_f64 = arr_min(a).map(|v| v as f64);
                stats.max_f64 = arr_max(a).map(|v| v as f64);
            }
        }
        DataType::Float64 => {
            if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                stats.min_f64 = arr_min(a);
                stats.max_f64 = arr_max(a);
            }
        }
        _ => {}
    }
    stats
}

/// Compute stats for every column of a batch, keyed by (unqualified,
/// lowercase) field name — the SAME convention `TableStatistics::
/// column_stats` (`src/physical/operators/scan.rs`) and `disjoint_group_
/// hint` (`src/physical/planner.rs`) already use, so a manifest's rollup
/// drops straight into `TableStatistics` with no key translation either.
pub fn compute_batch_stats(batch: &RecordBatch) -> BTreeMap<String, ColumnStats> {
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| (f.name().to_lowercase(), column_stats_for_array(c)))
        .collect()
}

// ============================================================================
// Manifest structures
// ============================================================================

/// The version/snapshot marker. `version` is bumped by whoever performs a
/// full-table replace (task 003); `created_at_ms` is this snapshot's
/// creation wall-clock time; `row_count` is the exact total across all
/// segments (validated equal to their sum — see `NativeManifest::
/// validate`). Task 007 hashes this struct's serialized form for
/// GPU-cache identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: u64,
    pub row_count: u64,
    pub created_at_ms: i64,
}

/// One Arrow IPC segment file and its statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    /// Also the index `ipc_cache::read_row_group`/`sidecar_dict_cols` use
    /// to locate this segment's file — see the module doc's "Load-bearing
    /// finding" section.
    pub id: u32,
    /// Relative file name inside the table directory. MUST equal
    /// `Segment::expected_file_name(id)` — `NativeManifest::validate`
    /// enforces this.
    pub path: String,
    /// The PHYSICAL row count actually written to this segment's `.arrow`
    /// file. Deliberately UNAFFECTED by `deleted_rows` (native-tables-
    /// mutation epic, task 001's Decision 1, task 003's Outcome): this
    /// keeps `NativeManifest::validate`'s existing
    /// `snapshot.row_count == sum(segments[].row_count)` invariant true
    /// with zero changes, and keeps `NativeManifest::build` callable
    /// unchanged after a DELETE. Use [`Segment::live_row_count`] for the
    /// LOGICAL (post-delete, visible) count.
    pub row_count: u64,
    pub byte_size: u64,
    /// Per-column stats for JUST this segment's rows. Keyed the same way
    /// as `compute_batch_stats`'s output (unqualified, lowercase).
    /// Deliberately NOT recomputed on DELETE (task 003's Outcome) — always
    /// safe (a wider bound never causes a wrong answer), never chased to
    /// an exact post-delete bound.
    #[serde(default)]
    pub column_stats: BTreeMap<String, ColumnStats>,
    /// Sorted, deduplicated LOCAL row positions (within this segment's own
    /// on-disk row order, i.e. the same 0-based indexing `ipc_cache::
    /// read_row_group`'s returned batches use, concatenated in on-disk
    /// block order) that a `DELETE`/`UPDATE` has tombstoned (native-
    /// tables-mutation epic, task 003, task 001's Decision 1). Read/
    /// consulted at scan time by `NativeTable::scan` (`native_table.rs`),
    /// EDITED (unioned via a `BTreeSet`, so re-deleting an
    /// already-deleted position is a structural no-op) by
    /// `native_delete::apply_deletions`. `#[serde(default)]` means every
    /// manifest phase 1 or task 002 (INSERT) ever wrote deserializes with
    /// an empty `Vec` here — no behavior change for a never-deleted-from
    /// table, and no manifest-format migration needed. A plain `Vec<u32>`,
    /// not `roaring` or a sibling file — see task 001's Outcome for the
    /// full tradeoff analysis (segments are capped at ~1,000,000 rows by
    /// construction, an order of magnitude below where a compressed
    /// bitmap format starts winning, and this format's own established
    /// "one inline, human-inspectable JSON file" discipline). NEVER
    /// contains a value `>= row_count` and is always sorted/deduplicated —
    /// `NativeManifest::validate` enforces both.
    #[serde(default)]
    pub deleted_rows: Vec<u32>,
}

impl Segment {
    /// The ONLY file name `ipc_cache::read_row_group(dir, id, ..)` will
    /// actually open for segment `id` — it computes this same pattern
    /// itself, internally, and does not accept an alternative. See the
    /// module doc's "Load-bearing finding" section.
    pub fn expected_file_name(id: u32) -> String {
        format!("rg_{id:05}.arrow")
    }

    /// The LOGICAL (post-delete, visible) row count: physical `row_count`
    /// minus however many local positions `deleted_rows` currently
    /// tombstones. `0` for a fully-tombstoned segment (which, per task
    /// 001's Decision 3, `native_delete::apply_deletions` drops from the
    /// manifest entirely rather than ever persisting one — so `0` from
    /// this method is reachable only transiently, mid-computation, never
    /// in a published manifest). Used by `NativeTable::statistics()` (a
    /// NEW, separate computation from the physical `row_count` above —
    /// see that field's own doc) and by `native_delete`'s own result
    /// reporting.
    pub fn live_row_count(&self) -> u64 {
        self.row_count
            .saturating_sub(self.deleted_rows.len() as u64)
    }
}

/// `table_dir.join(Segment::expected_file_name(id))` — where a segment's
/// Arrow IPC file must physically live for `ipc_cache::read_row_group` to
/// find it.
pub fn segment_full_path(table_dir: &Path, id: u32) -> PathBuf {
    table_dir.join(Segment::expected_file_name(id))
}

/// The whole-table manifest: identity, schema, version/snapshot marker,
/// segment list, and a table-level statistics rollup. Serialized as
/// `_manifest.json` (see `write_manifest`/`read_manifest`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeManifest {
    pub format_version: u32,
    /// Stable identity generated once at table creation (UUID v4) — NOT
    /// derived from any source file's path/mtime/size. Meant to survive a
    /// full-table replace unchanged (only `snapshot.version` bumps); see
    /// `existing_table_id`.
    pub table_id: String,
    pub schema: Vec<ManifestField>,
    pub snapshot: Snapshot,
    #[serde(default)]
    pub segments: Vec<Segment>,
    /// Table-level rollup of every segment's `column_stats` (min-of-mins,
    /// max-of-maxes, sum-of-null-counts), computed ONCE when the manifest
    /// is finalized — see `rollup`. `TableProvider::statistics()` (task
    /// 004) is meant to read this directly: an O(1) lookup, no recompute.
    #[serde(default)]
    pub table_stats: BTreeMap<String, ColumnStats>,
}

impl NativeManifest {
    /// A fresh, stable table identity. Not derived from any source file's
    /// path/mtime/size — the epic's decoupled-identity requirement.
    pub fn generate_table_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Table-level rollup of `segments`' `column_stats`: a cheap fold over
    /// already-computed, small, in-memory per-segment stats — never a
    /// second pass over segment DATA.
    pub fn rollup(segments: &[Segment]) -> BTreeMap<String, ColumnStats> {
        let mut out = BTreeMap::new();
        for seg in segments {
            merge_stats_into(&mut out, &seg.column_stats);
        }
        out
    }

    /// Build a new, fully-finalized, validated manifest. `row_count` and
    /// `table_stats` are DERIVED from `segments` (never taken on faith from
    /// a caller), so a manifest built this way is internally consistent by
    /// construction — `validate()`'s consistency checks exist to catch
    /// corruption/tampering AFTER this point (e.g. on read-back), not to
    /// second-guess this constructor.
    pub fn build(
        schema: &Schema,
        table_id: impl Into<String>,
        version: u64,
        segments: Vec<Segment>,
        created_at_ms: i64,
    ) -> Result<Self> {
        let row_count: u64 = segments.iter().map(|s| s.row_count).sum();
        let table_stats = Self::rollup(&segments);
        let manifest = Self {
            format_version: FORMAT_VERSION,
            table_id: table_id.into(),
            schema: schema_to_manifest_fields(schema)?,
            snapshot: Snapshot {
                version,
                row_count,
                created_at_ms,
            },
            segments,
            table_stats,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// The manifest's declared schema as an Arrow `SchemaRef`.
    pub fn arrow_schema(&self) -> SchemaRef {
        manifest_fields_to_schema(&self.schema)
    }

    /// Semantic validation beyond "valid JSON": the checks that turn a
    /// structurally-parseable-but-wrong manifest into a clear `Err` instead
    /// of a silently wrong answer downstream (a partial/stale write, a
    /// hand-edited file, a future bug in whatever writes these). Called by
    /// both `build` (so a manifest can never be constructed inconsistent in
    /// the first place) and `read_manifest` (so a manifest that became
    /// inconsistent after the fact — e.g. on disk corruption — is refused
    /// at load time, not at some arbitrary later point).
    pub fn validate(&self) -> Result<()> {
        if self.format_version != FORMAT_VERSION {
            return Err(QueryError::Storage(format!(
                "native table manifest format_version {} is not supported by this build \
                 (supports {FORMAT_VERSION})",
                self.format_version
            )));
        }
        if self.table_id.trim().is_empty() {
            return Err(QueryError::Storage(
                "native table manifest has an empty table_id".into(),
            ));
        }

        let mut known_columns = HashSet::new();
        for f in &self.schema {
            if f.name.trim().is_empty() {
                return Err(QueryError::Storage(
                    "native table manifest schema has a column with an empty name".into(),
                ));
            }
            if !known_columns.insert(f.name.to_lowercase()) {
                return Err(QueryError::Storage(format!(
                    "native table manifest schema has a duplicate column name \
                     (case-insensitive): `{}`",
                    f.name
                )));
            }
        }

        let mut seen_ids = HashSet::new();
        let mut seen_paths = HashSet::new();
        let mut computed_row_count: u64 = 0;
        for seg in &self.segments {
            if !seen_ids.insert(seg.id) {
                return Err(QueryError::Storage(format!(
                    "native table manifest has duplicate segment id {}",
                    seg.id
                )));
            }
            if !seen_paths.insert(seg.path.clone()) {
                return Err(QueryError::Storage(format!(
                    "native table manifest has duplicate segment path `{}`",
                    seg.path
                )));
            }
            let expected = Segment::expected_file_name(seg.id);
            if seg.path != expected {
                return Err(QueryError::Storage(format!(
                    "native table manifest segment {} declares path `{}`, but \
                     ipc_cache::read_row_group always opens `{expected}` for segment {} — \
                     this mismatch would make the segment silently fail to read",
                    seg.id, seg.path, seg.id
                )));
            }
            for name in seg.column_stats.keys() {
                if !known_columns.contains(name) {
                    return Err(QueryError::Storage(format!(
                        "native table manifest segment {} has statistics for unknown \
                         column `{name}`",
                        seg.id
                    )));
                }
            }
            // `deleted_rows` (native-tables-mutation epic, task 003) must be
            // sorted, strictly increasing (implies deduplicated — the
            // invariant `native_delete::apply_deletions`' `BTreeSet` union
            // always produces) and every position must name an actual row
            // of this segment (`< row_count`) — catches corruption/a
            // hand-edited manifest at load time rather than an
            // out-of-bounds panic or a silently wrong scan later.
            for pair in seg.deleted_rows.windows(2) {
                if pair[0] >= pair[1] {
                    return Err(QueryError::Storage(format!(
                        "native table manifest segment {} has an unsorted or duplicate \
                         deleted_rows entry ({} then {}) — deleted_rows must be sorted and \
                         strictly increasing",
                        seg.id, pair[0], pair[1]
                    )));
                }
            }
            if let Some(&max_deleted) = seg.deleted_rows.last() {
                if max_deleted as u64 >= seg.row_count {
                    return Err(QueryError::Storage(format!(
                        "native table manifest segment {} has a deleted_rows entry ({}) \
                         that is out of range for its row_count ({})",
                        seg.id, max_deleted, seg.row_count
                    )));
                }
            }
            computed_row_count = computed_row_count.saturating_add(seg.row_count);
        }
        if computed_row_count != self.snapshot.row_count {
            return Err(QueryError::Storage(format!(
                "native table manifest snapshot.row_count is {} but its {} segment(s) sum \
                 to {computed_row_count} — the manifest is internally inconsistent \
                 (partial write or corruption)",
                self.snapshot.row_count,
                self.segments.len()
            )));
        }

        for name in self.table_stats.keys() {
            if !known_columns.contains(name) {
                return Err(QueryError::Storage(format!(
                    "native table manifest table_stats has an entry for unknown column \
                     `{name}`"
                )));
            }
        }
        let recomputed = Self::rollup(&self.segments);
        if !stats_maps_bit_eq(&recomputed, &self.table_stats) {
            return Err(QueryError::Storage(
                "native table manifest table_stats does not match the fold of its \
                 segments' column_stats — the manifest is internally inconsistent \
                 (partial write or corruption)"
                    .into(),
            ));
        }

        Ok(())
    }
}

// ============================================================================
// Read / write
// ============================================================================

/// `dir/_manifest.json`.
pub fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_FILE_NAME)
}

/// Serialize `manifest` to `_manifest.json` inside `dir`, after validating
/// it. `dir` is typically a staging directory that `publish_table_dir` will
/// atomically rename into place afterward — this function itself performs
/// no locking or atomicity; it is one participant (writing the manifest
/// half of a table directory) in the caller's larger atomic-publish
/// sequence, alongside whatever writes the segment files (task 003).
pub fn write_manifest(dir: &Path, manifest: &NativeManifest) -> Result<()> {
    manifest.validate()?;
    std::fs::create_dir_all(dir)?;
    let text = serde_json::to_string_pretty(manifest)
        .map_err(|e| QueryError::Storage(format!("serialize native table manifest: {e}")))?;
    std::fs::write(manifest_path(dir), text)?;
    Ok(())
}

/// Read and fully validate `_manifest.json` from `dir`. Every failure mode
/// (missing file, truncated/invalid JSON, a structurally-valid-but-
/// internally-inconsistent manifest) returns a clear `Err` naming the file
/// and the problem — never a panic, never a default/guessed value standing
/// in for missing data.
pub fn read_manifest(dir: &Path) -> Result<NativeManifest> {
    let path = manifest_path(dir);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| QueryError::Storage(format!("read {}: {e}", path.display())))?;
    let manifest: NativeManifest = serde_json::from_str(&text)
        .map_err(|e| QueryError::Storage(format!("parse {}: {e}", path.display())))?;
    manifest
        .validate()
        .map_err(|e| QueryError::Storage(format!("{}: {e}", path.display())))?;
    Ok(manifest)
}

/// Does `dir` look like a native table? (Mirrors `iceberg::is_iceberg_dir`
/// for `--tables`-style auto-detection, which task 004 wires up.)
pub fn is_native_table_dir(dir: &Path) -> bool {
    manifest_path(dir).is_file()
}

/// The `table_id` an existing manifest at `dir` already carries, if any —
/// so a full-table replace (task 003) can preserve identity across the
/// replace rather than accidentally minting a new one. `Ok(None)` means no
/// manifest exists yet (a legitimately fresh table); a manifest that exists
/// but fails to parse/validate is a hard `Err`, never silently treated as
/// "absent" (which would risk silently changing a table's identity).
pub fn existing_table_id(dir: &Path) -> Result<Option<String>> {
    if !is_native_table_dir(dir) {
        return Ok(None);
    }
    Ok(Some(read_manifest(dir)?.table_id))
}

/// The version a new snapshot at `dir` should use: 1 for a fresh table,
/// otherwise the existing manifest's `snapshot.version + 1`. Same
/// missing-vs-corrupt distinction as `existing_table_id`.
pub fn next_version(dir: &Path) -> Result<u64> {
    if !is_native_table_dir(dir) {
        return Ok(1);
    }
    Ok(read_manifest(dir)?.snapshot.version + 1)
}

// ============================================================================
// Atomic publication
//
// Mirrors `ipc_cache.rs::build_sidecar`'s existing staging-directory-then-
// atomic-rename pattern wholesale, generalized from "one sidecar" to "a
// whole table directory" (manifest + segments together) — the exact
// mechanism that already makes the sidecar safe against concurrent
// builders/readers across threads AND processes, reused rather than
// reinvented.
// ============================================================================

/// A fresh staging directory for a build targeting `final_dir`, named after
/// this process's pid so concurrent builders (threads or processes) never
/// collide — mirrors `ipc_cache.rs::build_sidecar`'s own `.<pid>.building`
/// staging convention. (This appends to `final_dir`'s existing name rather
/// than replacing its extension the way `build_sidecar` does — deliberately
/// safer here since a table directory's own naming convention isn't fixed
/// by this module; appending can never clobber a meaningful suffix.)
pub fn staging_dir_for(final_dir: &Path) -> PathBuf {
    let mut name = final_dir.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.building", std::process::id()));
    final_dir.with_file_name(name)
}

/// Atomically publish a fully-written staging directory (manifest AND
/// segments together) as `final_dir`. A reader can never observe a
/// half-written table: `final_dir` either has the OLD complete contents or
/// the NEW ones, never a partial mix.
///
/// Unlike `ipc_cache.rs::build_sidecar` (which defers to whatever's already
/// there if a rename loses a race, since it is only an optimization layer
/// over a parquet source of truth), a losing rename here is a hard `Err`:
/// a native table IS the table, not a cache over one, so a publish failure
/// must be loud rather than silently discarded.
pub fn publish_table_dir(staging: &Path, final_dir: &Path) -> Result<()> {
    if let Some(parent) = final_dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::remove_dir_all(final_dir);
    std::fs::rename(staging, final_dir).map_err(|e| {
        QueryError::Storage(format!(
            "publish native table {} -> {}: {e}",
            staging.display(),
            final_dir.display()
        ))
    })
}

/// Atomically publish `manifest` as `dir`'s `_manifest.json`, replacing
/// ONLY that one file — the sibling primitive [`publish_table_dir`] needs
/// for INCREMENTAL mutations (native-tables-mutation epic, task 002's
/// `Append`; a future DELETE/UPDATE). Load-bearing distinction, per task
/// 001's design-spike Outcome (Decision 4): `publish_table_dir` does
/// `remove_dir_all(final_dir)` then `rename(staging, final_dir)` — correct
/// only when `staging` is a COMPLETE, self-sufficient replacement for the
/// whole directory (true for `Create`/`Overwrite`). An incremental
/// mutation instead writes new segment file(s) DIRECTLY into the already-
/// live `dir` (see `native_write::write_append_segments`) and only the
/// MANIFEST changes — calling `publish_table_dir` for that case would
/// `remove_dir_all` away every segment file the new manifest didn't just
/// write, corrupting the table. This function is the correct primitive
/// instead: it touches nothing in `dir` except `_manifest.json` itself.
///
/// Mechanism: validates `manifest` (same as [`write_manifest`]), then
/// writes it to a process-unique temporary file INSIDE `dir`
/// (`_manifest.json.tmp-<pid>`) and `std::fs::rename`s that temp file onto
/// [`manifest_path`]`(dir)` — ONE atomic file-level rename, the same POSIX
/// guarantee `publish_table_dir`'s directory-level rename already relies
/// on (same filesystem, same primitive, narrowed to a single file). A
/// reader opening `_manifest.json` at any instant sees either the fully-
/// old manifest or the fully-new one, never a torn/partial write, and a
/// crash between this function and whatever wrote the new segment
/// file(s) first leaves those segments as harmless, unreferenced orphans
/// (the existing manifest is untouched until this call's rename lands).
///
/// Performs NO locking of its own — callers serializing concurrent
/// writers (e.g. `native_write::lock_table_for_write`) must hold that
/// lock across their ENTIRE read-modify-write span, not just this call;
/// see that function's doc for why a per-call lock here would not be
/// sufficient (a lost update between two independently-computed "next"
/// manifests is possible even though each individual rename is atomic).
pub fn write_manifest_atomic(dir: &Path, manifest: &NativeManifest) -> Result<()> {
    manifest.validate()?;
    let text = serde_json::to_string_pretty(manifest)
        .map_err(|e| QueryError::Storage(format!("serialize native table manifest: {e}")))?;
    let tmp_path = dir.join(format!("{MANIFEST_FILE_NAME}.tmp-{}", std::process::id()));
    std::fs::write(&tmp_path, text)?;
    std::fs::rename(&tmp_path, manifest_path(dir)).map_err(|e| {
        QueryError::Storage(format!(
            "publish native table manifest update {} -> {}: {e}",
            tmp_path.display(),
            manifest_path(dir).display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as DT, Field as ArrowField, TimeUnit as TU};

    fn sample_schema() -> Schema {
        Schema::new(vec![
            ArrowField::new("id", DT::Int64, true),
            ArrowField::new("name", DT::Utf8, true),
            ArrowField::new("price", DT::Float64, true),
        ])
    }

    fn sample_segment(id: u32, row_count: u64, min: i64, max: i64) -> Segment {
        let mut stats = BTreeMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStats {
                min_i64: Some(min),
                max_i64: Some(max),
                null_count: Some(0),
                ..Default::default()
            },
        );
        Segment {
            id,
            path: Segment::expected_file_name(id),
            row_count,
            byte_size: 1024,
            column_stats: stats,
            deleted_rows: Vec::new(),
        }
    }

    // ---------- schema conversion ----------

    #[test]
    fn schema_round_trips_through_manifest_fields() {
        let schema = Schema::new(vec![
            ArrowField::new("a", DT::Boolean, false),
            ArrowField::new("b", DT::Int8, true),
            ArrowField::new("c", DT::Int16, true),
            ArrowField::new("d", DT::Int32, true),
            ArrowField::new("e", DT::Int64, false),
            ArrowField::new("f", DT::UInt8, true),
            ArrowField::new("g", DT::UInt16, true),
            ArrowField::new("h", DT::UInt32, true),
            ArrowField::new("i", DT::UInt64, true),
            ArrowField::new("j", DT::Float32, true),
            ArrowField::new("k", DT::Float64, true),
            ArrowField::new("l", DT::Utf8, true),
            ArrowField::new("m", DT::LargeUtf8, true),
            ArrowField::new("n", DT::Binary, true),
            ArrowField::new("o", DT::Date32, true),
            ArrowField::new("p", DT::Date64, true),
            ArrowField::new(
                "q",
                DT::Timestamp(TU::Microsecond, Some("UTC".into())),
                true,
            ),
            ArrowField::new("r", DT::Timestamp(TU::Nanosecond, None), true),
            ArrowField::new("s", DT::Decimal128(38, 10), true),
            ArrowField::new(
                "t",
                DT::Dictionary(Box::new(DT::Int32), Box::new(DT::Utf8)),
                true,
            ),
        ]);

        let fields = schema_to_manifest_fields(&schema).expect("known-supported types convert");
        let back = manifest_fields_to_schema(&fields);
        assert_eq!(*back, schema, "round trip must reproduce the exact schema");
    }

    #[test]
    fn unsupported_arrow_type_is_a_clear_error_not_a_panic() {
        let nested = DT::List(Arc::new(ArrowField::new("item", DT::Int32, true)));
        let err = ManifestDataType::from_arrow(&nested).unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
        assert!(
            err.to_string().contains("List"),
            "error should name the offending type: {err}"
        );

        // The field-level wrapper adds the column name for a precise error.
        let field = ArrowField::new("weird_col", nested, true);
        let err = ManifestField::from_arrow(&field).unwrap_err();
        assert!(err.to_string().contains("weird_col"), "{err}");
    }

    // ---------- statistics on a known synthetic dataset ----------

    #[test]
    fn column_stats_for_known_synthetic_dataset_are_correct() {
        let schema = Arc::new(sample_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(5), Some(1), None, Some(9)])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("c"),
                    Some("d"),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(2.5),
                    Some(-1.0),
                    Some(7.25),
                    None,
                ])),
            ],
        )
        .unwrap();

        let stats = compute_batch_stats(&batch);

        let id = stats.get("id").expect("id stats present");
        assert_eq!(id.min_i64, Some(1));
        assert_eq!(id.max_i64, Some(9));
        assert_eq!(id.null_count, Some(1));
        assert_eq!(id.min_f64, None, "int column must not populate float stats");

        let name = stats.get("name").expect("name stats present");
        assert_eq!(name.min_i64, None, "string column has no numeric zone-map");
        assert_eq!(name.max_i64, None);
        assert_eq!(name.null_count, Some(1), "null_count is type-agnostic");

        let price = stats.get("price").expect("price stats present");
        assert_eq!(price.min_f64, Some(-1.0));
        assert_eq!(price.max_f64, Some(7.25));
        assert_eq!(price.null_count, Some(1));
        assert_eq!(
            price.min_i64, None,
            "float column must not populate int stats"
        );
    }

    #[test]
    fn date32_and_date64_populate_i64_zone_map_stats() {
        use arrow::array::{Date32Array, Date64Array};
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("d32", DT::Date32, true),
            ArrowField::new("d64", DT::Date64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(100), Some(50), None])),
                Arc::new(Date64Array::from(vec![Some(200_000), Some(-5_000), None])),
            ],
        )
        .unwrap();
        let stats = compute_batch_stats(&batch);
        let d32 = stats.get("d32").unwrap();
        assert_eq!(d32.min_i64, Some(50));
        assert_eq!(d32.max_i64, Some(100));
        assert_eq!(d32.null_count, Some(1));
        let d64 = stats.get("d64").unwrap();
        assert_eq!(d64.min_i64, Some(-5_000));
        assert_eq!(d64.max_i64, Some(200_000));
    }

    // ---------- rollup folding ----------

    #[test]
    fn table_rollup_folds_segment_stats_correctly() {
        // Ranges deliberately NOT monotonic in segment id, so the fold has
        // to actually compare across all three rather than e.g. only ever
        // looking at the first or last segment.
        let segments = vec![
            sample_segment(0, 10, 1, 100),
            sample_segment(1, 5, 50, 500),
            sample_segment(2, 3, 10, 200),
        ];
        let rollup = NativeManifest::rollup(&segments);
        let id = rollup.get("id").unwrap();
        // min-of-mins across {1, 50, 10} = 1; max-of-maxes across
        // {100, 500, 200} = 500.
        assert_eq!(id.min_i64, Some(1));
        assert_eq!(id.max_i64, Some(500));
        assert_eq!(id.null_count, Some(0));
    }

    #[test]
    fn rollup_treats_a_missing_side_as_identity_not_poison() {
        let mut with_stats = BTreeMap::new();
        with_stats.insert(
            "x".to_string(),
            ColumnStats {
                min_i64: Some(5),
                max_i64: Some(5),
                null_count: Some(2),
                ..Default::default()
            },
        );
        let seg_a = Segment {
            id: 0,
            path: Segment::expected_file_name(0),
            row_count: 3,
            byte_size: 8,
            column_stats: with_stats,
            deleted_rows: Vec::new(),
        };
        // Segment B has no entry for "x" at all (e.g. an all-null batch, or
        // a writer that only tracks columns with a real value).
        let seg_b = Segment {
            id: 1,
            path: Segment::expected_file_name(1),
            row_count: 1,
            byte_size: 8,
            column_stats: BTreeMap::new(),
            deleted_rows: Vec::new(),
        };
        let rollup = NativeManifest::rollup(&[seg_a, seg_b]);
        let x = rollup.get("x").unwrap();
        assert_eq!(x.min_i64, Some(5), "missing side must not become None");
        assert_eq!(x.max_i64, Some(5));
        assert_eq!(x.null_count, Some(2));
    }

    #[test]
    fn rollup_with_nan_validates_via_bit_pattern_not_derived_partialeq() {
        // A column whose real data legitimately contains NaN must not make
        // `validate()` spuriously report corruption (see `column_stats_bit_eq`'s
        // doc for why plain `PartialEq` would get this wrong).
        let mut stats = BTreeMap::new();
        stats.insert(
            "x".to_string(),
            ColumnStats {
                min_f64: Some(f64::NAN),
                max_f64: Some(f64::NAN),
                null_count: Some(0),
                ..Default::default()
            },
        );
        let schema = Schema::new(vec![ArrowField::new("x", DT::Float64, true)]);
        let segments = vec![Segment {
            id: 0,
            path: Segment::expected_file_name(0),
            row_count: 1,
            byte_size: 8,
            column_stats: stats,
            deleted_rows: Vec::new(),
        }];
        let manifest =
            NativeManifest::build(&schema, "tid", 1, segments, 0).expect("NaN stats must validate");
        assert!(manifest
            .table_stats
            .get("x")
            .unwrap()
            .min_f64
            .unwrap()
            .is_nan());
    }

    // ---------- manifest JSON round trip ----------

    #[test]
    fn manifest_round_trips_through_json() {
        let dir = tempfile::tempdir().unwrap();
        let schema = sample_schema();
        let segments = vec![sample_segment(0, 3, 1, 9), sample_segment(1, 2, 2, 5)];
        let manifest =
            NativeManifest::build(&schema, "table-abc-123", 1, segments, 1_700_000_000_000)
                .expect("build a valid manifest");

        write_manifest(dir.path(), &manifest).expect("write");
        assert!(manifest_path(dir.path()).is_file());

        let read_back = read_manifest(dir.path()).expect("read back");
        assert_eq!(read_back, manifest, "round trip must reproduce every field");
        assert_eq!(read_back.table_id, "table-abc-123");
        assert_eq!(read_back.snapshot.version, 1);
        assert_eq!(read_back.snapshot.row_count, 5);
        assert_eq!(read_back.segments.len(), 2);
        assert_eq!(
            read_back.table_stats.get("id").unwrap().min_i64,
            Some(1),
            "table-level rollup must round-trip too"
        );

        // Human-readable/greppable: the file is plain, parseable JSON with
        // the documented top-level keys.
        let raw = std::fs::read_to_string(manifest_path(dir.path())).unwrap();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        for key in [
            "format_version",
            "table_id",
            "schema",
            "snapshot",
            "segments",
            "table_stats",
        ] {
            assert!(
                value.get(key).is_some(),
                "manifest JSON missing `{key}`: {raw}"
            );
        }
    }

    #[test]
    fn corrupted_manifest_json_is_a_clear_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(manifest_path(dir.path()), b"{ this is not valid json").unwrap();
        let err = read_manifest(dir.path()).unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
        assert!(
            err.to_string().contains("_manifest.json"),
            "error should name the file: {err}"
        );
    }

    #[test]
    fn missing_manifest_file_is_a_clear_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let err = read_manifest(dir.path()).unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
    }

    #[test]
    fn manifest_missing_a_required_field_is_a_clear_error() {
        let dir = tempfile::tempdir().unwrap();
        // Valid JSON, but no `table_id` -- a required (non-defaulted) field.
        let partial = serde_json::json!({
            "format_version": FORMAT_VERSION,
            "schema": [],
            "snapshot": {"version": 1, "row_count": 0, "created_at_ms": 0},
        });
        std::fs::write(manifest_path(dir.path()), partial.to_string()).unwrap();
        let err = read_manifest(dir.path()).unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
    }

    #[test]
    fn unsupported_format_version_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let doc = serde_json::json!({
            "format_version": FORMAT_VERSION + 1,
            "table_id": "t",
            "schema": [],
            "snapshot": {"version": 1, "row_count": 0, "created_at_ms": 0},
            "segments": [],
            "table_stats": {},
        });
        std::fs::write(manifest_path(dir.path()), doc.to_string()).unwrap();
        let err = read_manifest(dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("format_version"), "{msg}");
        assert!(msg.contains(&(FORMAT_VERSION + 1).to_string()), "{msg}");
    }

    #[test]
    fn partial_manifest_inconsistent_row_count_is_a_clear_error() {
        let schema = sample_schema();
        let segments = vec![sample_segment(0, 3, 1, 9)];
        let mut manifest =
            NativeManifest::build(&schema, "t", 1, segments, 0).expect("valid to start");
        // Simulate a partial write: segments say 3 rows, snapshot disagrees.
        manifest.snapshot.row_count = 999;
        let err = manifest.validate().unwrap_err();
        assert!(err.to_string().contains("row_count"), "{err}");
    }

    // ---------- deleted_rows (native-tables-mutation epic, task 003) ----------

    #[test]
    fn deleted_rows_defaults_to_empty_and_manifests_without_it_round_trip_unchanged() {
        // Every manifest phase 1 or task 002 (INSERT) ever wrote has no
        // `deleted_rows` key at all in its JSON -- `#[serde(default)]` must
        // deserialize that as an empty Vec, not an error, and a segment
        // built via `sample_segment` (this test file's own helper, used
        // throughout tasks 001/002) must already carry an empty one.
        let seg = sample_segment(0, 3, 1, 9);
        assert!(seg.deleted_rows.is_empty());
        assert_eq!(
            seg.live_row_count(),
            3,
            "no deletions -- physical == logical"
        );

        let dir = tempfile::tempdir().unwrap();
        let schema = sample_schema();
        let doc = serde_json::json!({
            "format_version": FORMAT_VERSION,
            "table_id": "pre-task-003",
            "schema": schema_to_manifest_fields(&schema).unwrap(),
            "snapshot": {"version": 1, "row_count": 3, "created_at_ms": 0},
            "segments": [{
                "id": 0,
                "path": "rg_00000.arrow",
                "row_count": 3,
                "byte_size": 8,
                "column_stats": {},
            }],
            "table_stats": {},
        });
        std::fs::write(manifest_path(dir.path()), doc.to_string()).unwrap();
        let read_back = read_manifest(dir.path()).expect("a pre-task-003 manifest must still read");
        assert_eq!(read_back.segments[0].deleted_rows, Vec::<u32>::new());
        assert_eq!(read_back.segments[0].live_row_count(), 3);
    }

    #[test]
    fn live_row_count_subtracts_deleted_rows() {
        let mut seg = sample_segment(0, 10, 1, 9);
        seg.deleted_rows = vec![2, 5, 7];
        assert_eq!(seg.live_row_count(), 7);
    }

    #[test]
    fn deleted_rows_out_of_range_is_a_clear_validation_error() {
        let schema = sample_schema();
        let mut seg = sample_segment(0, 3, 1, 9);
        seg.deleted_rows = vec![0, 3]; // row_count is 3 -- valid positions are 0,1,2
        let err = NativeManifest::build(&schema, "t", 1, vec![seg], 0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("out of range"), "{msg}");
        assert!(msg.contains('3'), "{msg}");
    }

    #[test]
    fn deleted_rows_unsorted_or_duplicated_is_a_clear_validation_error() {
        let schema = sample_schema();

        let mut unsorted = sample_segment(0, 5, 1, 9);
        unsorted.deleted_rows = vec![2, 1];
        let err = NativeManifest::build(&schema, "t", 1, vec![unsorted], 0).unwrap_err();
        assert!(err.to_string().contains("unsorted"), "{err}");

        let mut duplicated = sample_segment(0, 5, 1, 9);
        duplicated.deleted_rows = vec![1, 1];
        let err = NativeManifest::build(&schema, "t", 1, vec![duplicated], 0).unwrap_err();
        assert!(err.to_string().contains("unsorted or duplicate"), "{err}");
    }

    #[test]
    fn deleted_rows_do_not_affect_row_count_consistency_validation() {
        // Decision 1: Segment.row_count / Snapshot.row_count stay PHYSICAL,
        // unaffected by deleted_rows -- a partially-tombstoned segment must
        // still validate cleanly with its ORIGINAL physical row_count.
        let schema = sample_schema();
        let mut seg = sample_segment(0, 5, 1, 9);
        seg.deleted_rows = vec![0, 4];
        let manifest = NativeManifest::build(&schema, "t", 1, vec![seg], 0)
            .expect("a partially-tombstoned segment must still validate");
        assert_eq!(manifest.snapshot.row_count, 5, "physical count unaffected");
        assert_eq!(manifest.segments[0].live_row_count(), 3);
    }

    #[test]
    fn segment_path_mismatch_with_read_mechanism_naming_is_rejected() {
        let schema = sample_schema();
        let mut segments = vec![sample_segment(0, 3, 1, 9)];
        // The name earlier planning notes used illustratively -- and
        // exactly the mistake `validate()` exists to catch, since
        // `ipc_cache::read_row_group` would never find this file.
        segments[0].path = "seg_00000.arrow".to_string();
        let err = NativeManifest::build(&schema, "t", 1, segments, 0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("seg_00000.arrow"), "{msg}");
        assert!(msg.contains("rg_00000.arrow"), "{msg}");
    }

    #[test]
    fn duplicate_segment_id_is_a_clear_error() {
        let schema = sample_schema();
        let mut b = sample_segment(1, 3, 1, 9);
        b.id = 0; // collides with the other segment's id
        b.path = Segment::expected_file_name(0);
        let segments = vec![sample_segment(0, 3, 1, 9), b];
        let err = NativeManifest::build(&schema, "t", 1, segments, 0).unwrap_err();
        assert!(err.to_string().contains("duplicate segment id"), "{err}");
    }

    #[test]
    fn unknown_column_in_stats_is_a_clear_error() {
        let schema = sample_schema();
        let mut seg = sample_segment(0, 3, 1, 9);
        seg.column_stats
            .insert("not_a_real_column".to_string(), ColumnStats::default());
        let err = NativeManifest::build(&schema, "t", 1, vec![seg], 0).unwrap_err();
        assert!(err.to_string().contains("not_a_real_column"), "{err}");
    }

    #[test]
    fn empty_table_zero_segments_is_valid() {
        let schema = sample_schema();
        let manifest =
            NativeManifest::build(&schema, "t", 1, vec![], 0).expect("zero segments is valid");
        assert_eq!(manifest.snapshot.row_count, 0);
        assert!(manifest.table_stats.is_empty());
    }

    // ---------- identity / versioning helpers ----------

    #[test]
    fn existing_table_id_and_next_version_on_a_fresh_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(existing_table_id(dir.path()).unwrap(), None);
        assert_eq!(next_version(dir.path()).unwrap(), 1);

        let schema = sample_schema();
        let manifest = NativeManifest::build(
            &schema,
            NativeManifest::generate_table_id(),
            1,
            vec![sample_segment(0, 3, 1, 9)],
            0,
        )
        .unwrap();
        write_manifest(dir.path(), &manifest).unwrap();

        assert_eq!(
            existing_table_id(dir.path()).unwrap(),
            Some(manifest.table_id.clone())
        );
        assert_eq!(next_version(dir.path()).unwrap(), 2);
    }

    #[test]
    fn is_native_table_dir_detects_and_rejects() {
        let native = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        assert!(!is_native_table_dir(native.path()), "no manifest yet");
        std::fs::write(manifest_path(native.path()), b"{}").unwrap();
        assert!(is_native_table_dir(native.path()));
        assert!(!is_native_table_dir(other.path()));
    }

    // ---------- atomic publish ----------

    #[test]
    fn publish_table_dir_makes_a_staged_manifest_appear_atomically() {
        let root = tempfile::tempdir().unwrap();
        let final_dir = root.path().join("mytable");
        let staging = staging_dir_for(&final_dir);
        assert_ne!(staging, final_dir);
        assert!(staging.to_string_lossy().contains("building"));

        let schema = sample_schema();
        let manifest =
            NativeManifest::build(&schema, "t1", 1, vec![sample_segment(0, 3, 1, 9)], 0).unwrap();
        write_manifest(&staging, &manifest).unwrap();
        assert!(!final_dir.exists(), "must not exist before publish");

        publish_table_dir(&staging, &final_dir).unwrap();
        assert!(!staging.exists(), "staging dir is consumed by the rename");
        assert!(manifest_path(&final_dir).is_file());
        assert_eq!(read_manifest(&final_dir).unwrap(), manifest);
    }

    #[test]
    fn publish_table_dir_replaces_a_stale_target_wholesale() {
        let root = tempfile::tempdir().unwrap();
        let final_dir = root.path().join("mytable");
        let schema = sample_schema();

        // First publish: version 1.
        let staging1 = staging_dir_for(&final_dir);
        let m1 = NativeManifest::build(&schema, "same-id", 1, vec![sample_segment(0, 3, 1, 9)], 0)
            .unwrap();
        write_manifest(&staging1, &m1).unwrap();
        // An extra stray file in the old final dir must NOT survive a
        // whole-directory replace.
        std::fs::create_dir_all(&final_dir).unwrap();
        std::fs::write(final_dir.join("stray.txt"), b"leftover").unwrap();
        publish_table_dir(&staging1, &final_dir).unwrap();
        assert!(!final_dir.join("stray.txt").exists());

        // Second publish (simulating a full-table replace): version 2,
        // same table_id, different segment set.
        let staging2 = staging_dir_for(&final_dir);
        let m2 =
            NativeManifest::build(&schema, "same-id", 2, vec![sample_segment(0, 7, 10, 20)], 1)
                .unwrap();
        write_manifest(&staging2, &m2).unwrap();
        publish_table_dir(&staging2, &final_dir).unwrap();

        let read_back = read_manifest(&final_dir).unwrap();
        assert_eq!(read_back.snapshot.version, 2);
        assert_eq!(read_back.snapshot.row_count, 7);
        assert_eq!(read_back.table_id, "same-id", "identity survives a replace");
    }

    // ---------- write_manifest_atomic: single-FILE publish, task 002 ----------

    #[test]
    fn write_manifest_atomic_replaces_only_the_manifest_file() {
        let root = tempfile::tempdir().unwrap();
        let final_dir = root.path().join("mytable");
        let schema = sample_schema();

        // Publish an initial table (whole-directory path) with one segment.
        // `sample_segment` builds only the manifest-level `Segment` entry
        // (no real Arrow IPC bytes), so write a stand-in segment file
        // ourselves inside the staging dir to actually exercise "does an
        // existing segment file survive" below.
        let staging1 = staging_dir_for(&final_dir);
        std::fs::create_dir_all(&staging1).unwrap();
        std::fs::write(staging1.join("rg_00000.arrow"), b"segment-0-bytes").unwrap();
        let m1 = NativeManifest::build(&schema, "same-id", 1, vec![sample_segment(0, 3, 1, 9)], 0)
            .unwrap();
        write_manifest(&staging1, &m1).unwrap();
        publish_table_dir(&staging1, &final_dir).unwrap();
        assert!(
            final_dir.join("rg_00000.arrow").is_file(),
            "sanity: the first publish's stand-in segment file must exist before we test \
             that a manifest-only update leaves it alone"
        );

        // Simulate a segment file having been written DIRECTLY into the
        // live directory by an Append (native_write::write_append_segments'
        // job, not this module's) plus an unrelated sibling file that must
        // never be touched.
        std::fs::write(final_dir.join("rg_00001.arrow"), b"pretend-arrow-bytes").unwrap();
        std::fs::write(final_dir.join("unrelated.txt"), b"must survive").unwrap();

        // Now publish an updated manifest describing BOTH segments via the
        // single-file primitive -- must NOT remove/rename the directory,
        // must NOT touch the sibling file, must leave the segment files
        // exactly as they were.
        let mut segments = m1.segments.clone();
        segments.push(sample_segment(1, 4, 2, 20));
        let m2 = NativeManifest::build(&schema, "same-id", 2, segments, 1).unwrap();
        write_manifest_atomic(&final_dir, &m2).unwrap();

        assert!(
            final_dir.join("unrelated.txt").is_file(),
            "write_manifest_atomic must never touch sibling files"
        );
        assert_eq!(
            std::fs::read(final_dir.join("rg_00001.arrow")).unwrap(),
            b"pretend-arrow-bytes",
            "an existing segment file must survive a manifest-only publish byte-for-byte"
        );
        assert_eq!(
            std::fs::read(final_dir.join("rg_00000.arrow")).unwrap(),
            b"segment-0-bytes",
            "the FIRST publish's segment file must also still be present, byte-for-byte"
        );
        // No leftover temp file.
        let tmp_leftover = std::fs::read_dir(&final_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".tmp-"));
        assert!(!tmp_leftover, "no temp manifest file must be left behind");

        let read_back = read_manifest(&final_dir).unwrap();
        assert_eq!(read_back.snapshot.version, 2);
        assert_eq!(read_back.snapshot.row_count, 7);
        assert_eq!(read_back.segments.len(), 2);
        assert_eq!(read_back.table_id, "same-id");
    }

    #[test]
    fn write_manifest_atomic_refuses_an_invalid_manifest_without_publishing() {
        let dir = tempfile::tempdir().unwrap();
        let schema = sample_schema();
        let mut manifest =
            NativeManifest::build(&schema, "t", 1, vec![sample_segment(0, 3, 1, 9)], 0).unwrap();
        // Corrupt it post-construction the same way the existing
        // `partial_manifest_inconsistent_row_count_is_a_clear_error` test
        // does, so `validate()` inside `write_manifest_atomic` catches it.
        manifest.snapshot.row_count = 999;

        let err = write_manifest_atomic(dir.path(), &manifest).unwrap_err();
        assert!(err.to_string().contains("row_count"), "{err}");
        assert!(
            !manifest_path(dir.path()).exists(),
            "an invalid manifest must never be published"
        );
    }

    // ---------- the load-bearing proof: ipc_cache::read_row_group reads a
    // manifest-described segment UNCHANGED, with zero refactor ----------

    fn write_ipc_segment(dir: &Path, id: u32, batch: &RecordBatch) -> u64 {
        std::fs::create_dir_all(dir).unwrap();
        let path = segment_full_path(dir, id);
        let file = std::fs::File::create(&path).unwrap();
        let mut w = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
        std::fs::metadata(&path).unwrap().len()
    }

    #[test]
    fn read_row_group_reads_a_manifest_described_segment_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", DT::Int64, false),
            ArrowField::new("name", DT::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y"), None::<&str>])),
            ],
        )
        .unwrap();

        let byte_size = write_ipc_segment(dir.path(), 0, &batch);

        let segment = Segment {
            id: 0,
            path: Segment::expected_file_name(0),
            row_count: 3,
            byte_size,
            column_stats: compute_batch_stats(&batch),
            deleted_rows: Vec::new(),
        };
        let manifest = NativeManifest::build(&schema, "seg-test", 1, vec![segment], 0).unwrap();
        write_manifest(dir.path(), &manifest).unwrap();

        // The actual, UNCHANGED, already-`pub` read entrypoint every later
        // task (004) will call. This is called here with zero modification
        // to ipc_cache.rs -- proof, not assertion, that its `pub fn`s are
        // sufficient exactly as task 001 predicted.
        let read_back = crate::storage::ipc_cache::read_row_group(dir.path(), 0, None, None)
            .expect("ipc_cache::read_row_group must read a manifest-described segment");
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].num_rows(), 3);
        let id_col = read_back[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.values(), &[10, 20, 30]);

        // Projection pushdown, another already-`pub` parameter, also works
        // unchanged against a manifest-described segment.
        let projected = crate::storage::ipc_cache::read_row_group(dir.path(), 0, Some(&[1]), None)
            .expect("projected read");
        assert_eq!(projected[0].num_columns(), 1);
    }

    #[test]
    fn sidecar_dict_cols_reads_a_manifest_described_dictionary_segment_unchanged() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            "status",
            DT::Dictionary(Box::new(DT::Int32), Box::new(DT::Utf8)),
            false,
        )]));
        let keys = Int32Array::from(vec![0, 1, 0]);
        let values = StringArray::from(vec!["OPEN", "CLOSED"]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dict)]).unwrap();

        let byte_size = write_ipc_segment(dir.path(), 0, &batch);
        let segment = Segment {
            id: 0,
            path: Segment::expected_file_name(0),
            row_count: 3,
            byte_size,
            column_stats: compute_batch_stats(&batch),
            deleted_rows: Vec::new(),
        };
        let manifest = NativeManifest::build(&schema, "dict-test", 1, vec![segment], 0).unwrap();
        write_manifest(dir.path(), &manifest).unwrap();

        let dict_cols = crate::storage::ipc_cache::sidecar_dict_cols(dir.path());
        assert!(
            dict_cols.contains("status"),
            "sidecar_dict_cols must see the manifest-described segment's dictionary column: {dict_cols:?}"
        );
    }
}
