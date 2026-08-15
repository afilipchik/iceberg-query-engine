//! Splits: the atoms of distributed work, and how they are divided.
//!
//! # Why not whole files
//!
//! TPC-H at SF=10 is one ~2.8GB `lineitem.parquet` and a 1.8KB `nation.parquet`.
//! Any assignment whose atom is a *file* hands one node ~99.9% of the bytes, so
//! "divide the work equally" fails by construction before a single row is read.
//! The atom here is therefore a **row range inside a row group**:
//! `(file, row_group, row_offset, num_rows)`. Parquet can read exactly that —
//! `with_row_groups(vec![rg])` plus a `RowSelection` — and arrow-rs skips the
//! pages it does not need without decompressing them.
//!
//! # Why not `hash(path) % N`
//!
//! Hashing balances split *counts*. That is the wrong quantity: counts equal
//! bytes only when every split is the same size, which is never true across a
//! real table (the last row group of `lineitem` at SF=10 is 14MB against 59MB
//! for the other 57). [`assign_lpt`] balances **bytes** with greedy
//! longest-processing-time-first: sort descending by size, and repeatedly give
//! the next split to the least-loaded node. LPT's worst case is
//! `(4/3 - 1/(3N))` of optimal, and on split sets of this shape it lands within
//! ~1% — measured, see the tests at the bottom of this file.
//!
//! # Why every node can compute the same answer
//!
//! Enumeration reads only Parquet footers, sorts by a **canonical key** (the
//! file's *name*, not its path, so a different mount point cannot reorder
//! anything), and assignment is a pure function of `(splits, node_count)`.
//! So there is no assignment service and no leader: the initiator sends
//! `(shard_index, shard_count, digest)` and each worker recomputes its own
//! share. The [`SplitSet::digest`] is the safety interlock — if a worker's copy
//! of the data differs in any byte-relevant way, its digest differs and the
//! query FAILS instead of quietly returning an answer over the wrong rows.

use crate::error::{QueryError, Result};
use std::path::{Path, PathBuf};

/// One unit of assignable work: a contiguous row range within one row group of
/// one Parquet file.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Split {
    /// Table this split belongs to.
    pub table: String,
    /// Local path used to read it. Deliberately NOT part of the canonical key:
    /// two nodes may mount the same data at different paths.
    #[serde(skip)]
    pub path: PathBuf,
    /// Canonical file identity — the file name. Part of the digest.
    pub file: String,
    pub row_group: usize,
    /// First row of the range, relative to the start of the row group.
    pub row_offset: i64,
    pub num_rows: i64,
    /// Uncompressed size attributable to this range. Uncompressed rather than
    /// on-disk because the dominant cost of a scan is decode and downstream
    /// processing, not the read; on-disk size would under-weight a
    /// well-compressed column.
    pub bytes: u64,
}

impl Split {
    /// Ordering key that is identical on every node.
    fn canonical_key(&self) -> (&str, &str, usize, i64) {
        (&self.table, &self.file, self.row_group, self.row_offset)
    }

    /// True when this split covers its whole row group, so the reader can skip
    /// building a `RowSelection` at all.
    pub fn is_whole_row_group(&self, row_group_rows: i64) -> bool {
        self.row_offset == 0 && self.num_rows == row_group_rows
    }
}

/// Every split of one table, in canonical order.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SplitSet {
    pub table: String,
    pub splits: Vec<Split>,
    pub total_bytes: u64,
    pub total_rows: i64,
    /// Target size the enumeration aimed at, reported so the number that
    /// produced a given balance is visible rather than folklore.
    pub target_split_bytes: u64,
}

/// Smallest split we will cut. Below this the per-split overhead (a reader
/// build, a page-header walk) starts to dominate the work itself.
pub const MIN_SPLIT_BYTES: u64 = 4 * 1024 * 1024;
/// Largest split we will leave whole. A row group bigger than this is cut even
/// when the cluster is small, because one 2GB atom is a straggler waiting to
/// happen.
pub const MAX_SPLIT_BYTES: u64 = 64 * 1024 * 1024;
/// How many splits per node the enumeration aims for. LPT's error shrinks with
/// the ratio of splits to nodes; 32 puts TPC-H `lineitem` comfortably under the
/// 1.10 imbalance gate at both 3 and 8 nodes with margin to spare.
const SPLITS_PER_NODE: u64 = 32;

/// Choose the target split size for `total_bytes` spread over `nodes`.
///
/// Clamped at both ends: a huge table must not be left as a handful of boulders
/// (`MAX_SPLIT_BYTES`), and a large one must not be shredded into thousands of
/// microscopic splits (`MIN_SPLIT_BYTES`).
///
/// The lower clamp yields, though, when the whole table is smaller than
/// `nodes * MIN_SPLIT_BYTES`. Holding the floor there would hand the entire
/// table to one node and leave the rest idle, which is the opposite of the
/// requirement — better a few small splits than an unused cluster. So the floor
/// becomes "one split per node" for tables that small.
pub fn target_split_bytes(total_bytes: u64, nodes: usize) -> u64 {
    let nodes = nodes.max(1) as u64;
    let floor = MIN_SPLIT_BYTES.min(total_bytes.div_ceil(nodes)).max(1);
    let ideal = total_bytes / (SPLITS_PER_NODE * nodes).max(1);
    ideal.clamp(floor, MAX_SPLIT_BYTES.max(floor))
}

/// Enumerate the splits of a Parquet-backed table.
///
/// `files` may be in any order; the result is canonically ordered. Reads only
/// footers (through the global metadata cache), so this is metadata-cost, not
/// data-cost.
pub fn enumerate_parquet(table: &str, files: &[PathBuf], nodes: usize) -> Result<SplitSet> {
    // Pass 1: row-group inventory, in canonical file order.
    let mut ordered: Vec<&PathBuf> = files.iter().collect();
    ordered.sort_by_key(|p| file_key(p));

    struct RowGroup<'a> {
        path: &'a PathBuf,
        file: String,
        index: usize,
        rows: i64,
        bytes: u64,
    }

    let mut inventory: Vec<RowGroup> = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut total_rows: i64 = 0;
    for path in ordered {
        let md = crate::storage::metadata_cache::cached_metadata(path).map_err(|e| {
            QueryError::Execution(format!(
                "cannot read parquet footer for {}: {e}",
                path.display()
            ))
        })?;
        let meta = md.metadata();
        for (index, rg) in meta.row_groups().iter().enumerate() {
            let rows = rg.num_rows();
            if rows <= 0 {
                continue;
            }
            let bytes = rg.total_byte_size().max(0) as u64;
            total_bytes += bytes;
            total_rows += rows;
            inventory.push(RowGroup {
                path,
                file: file_key(path),
                index,
                rows,
                bytes,
            });
        }
    }

    // Pass 2: cut each row group into as many equal row ranges as the target
    // size asks for. Equal in ROWS, with the remainder spread over the leading
    // pieces, so the arithmetic is integral and identical on every node.
    let target = target_split_bytes(total_bytes, nodes);
    let mut splits = Vec::with_capacity(inventory.len() * 2);
    for rg in &inventory {
        let pieces = if rg.bytes <= target {
            1
        } else {
            // ceil(bytes / target), never more pieces than rows.
            (rg.bytes.div_ceil(target) as i64).min(rg.rows).max(1)
        };
        let base = rg.rows / pieces;
        let remainder = rg.rows % pieces;
        let mut offset = 0i64;
        let mut bytes_left = rg.bytes;
        for piece in 0..pieces {
            let n = base + if piece < remainder { 1 } else { 0 };
            if n == 0 {
                continue;
            }
            // Attribute bytes proportionally to rows, giving the last piece
            // whatever integer division left over. Without that the pieces sum
            // to slightly LESS than the row group, and the reported per-node
            // bytes no longer add up to the table — a balance metric whose
            // denominator is quietly wrong is worse than no metric.
            let bytes = if piece + 1 == pieces {
                bytes_left
            } else {
                (rg.bytes as u128 * n as u128 / rg.rows as u128) as u64
            };
            bytes_left = bytes_left.saturating_sub(bytes);
            splits.push(Split {
                table: table.to_string(),
                path: rg.path.clone(),
                file: rg.file.clone(),
                row_group: rg.index,
                row_offset: offset,
                num_rows: n,
                bytes,
            });
            offset += n;
        }
    }

    splits.sort_by(|a, b| a.canonical_key().cmp(&b.canonical_key()));
    Ok(SplitSet {
        table: table.to_string(),
        splits,
        total_bytes,
        total_rows,
        target_split_bytes: target,
    })
}

/// The canonical name of a file: its final path component. Two nodes that mount
/// the same dataset at `/data` and `/mnt/tpch` must agree, and they do.
fn file_key(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string_lossy().into_owned())
}

impl SplitSet {
    /// A stable fingerprint of the split universe.
    ///
    /// Two nodes that agree on this agree on exactly what work exists, which is
    /// the precondition for each computing its own share independently. FNV-1a
    /// over the canonical fields — not cryptographic, and does not need to be:
    /// it defends against *divergence*, not against an adversary.
    pub fn digest(&self) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        let mut feed = |bytes: &[u8]| {
            for b in bytes {
                h ^= *b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
        };
        feed(self.table.as_bytes());
        for s in &self.splits {
            feed(s.file.as_bytes());
            feed(&(s.row_group as u64).to_le_bytes());
            feed(&s.row_offset.to_le_bytes());
            feed(&s.num_rows.to_le_bytes());
            feed(&s.bytes.to_le_bytes());
        }
        h
    }

    pub fn is_empty(&self) -> bool {
        self.splits.is_empty()
    }

    pub fn len(&self) -> usize {
        self.splits.len()
    }
}

/// Which node owns which splits, plus the per-node load that decided it.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Assignment {
    pub nodes: usize,
    /// `per_node[i]` = indices into the `SplitSet` owned by node `i`.
    pub per_node: Vec<Vec<usize>>,
    pub node_bytes: Vec<u64>,
    pub node_rows: Vec<i64>,
    pub node_splits: Vec<usize>,
    pub total_bytes: u64,
}

impl Assignment {
    /// `max_node_bytes / mean_node_bytes`. 1.0 is perfect; the acceptance gate
    /// for TPC-H `lineitem` is <= 1.10 at both 3 and 8 nodes.
    ///
    /// An empty table has no work to divide, so its imbalance is 1.0 by
    /// definition rather than 0/0 — reporting NaN there would make a
    /// legitimately balanced cluster look broken.
    pub fn imbalance(&self) -> f64 {
        if self.nodes == 0 || self.total_bytes == 0 {
            return 1.0;
        }
        let mean = self.total_bytes as f64 / self.nodes as f64;
        let max = self.node_bytes.iter().copied().max().unwrap_or(0) as f64;
        max / mean
    }

    /// Nodes that were given nothing. Not an error — a table with fewer splits
    /// than nodes cannot fill everyone — but the caller must know, because a
    /// node with no splits must still be skipped rather than sent a fragment
    /// whose empty answer would be merged as if it were data.
    pub fn idle_nodes(&self) -> Vec<usize> {
        (0..self.nodes)
            .filter(|i| self.node_splits[*i] == 0)
            .collect()
    }
}

/// Greedy longest-processing-time-first assignment by BYTES.
///
/// Deterministic: ties in size are broken by canonical key, and ties in node
/// load by the lowest node index. Running this on two nodes with the same
/// `SplitSet` therefore produces the same answer, bit for bit.
pub fn assign_lpt(set: &SplitSet, nodes: usize) -> Assignment {
    let nodes = nodes.max(1);
    let mut order: Vec<usize> = (0..set.splits.len()).collect();
    order.sort_by(|&a, &b| {
        let (x, y) = (&set.splits[a], &set.splits[b]);
        y.bytes
            .cmp(&x.bytes)
            .then_with(|| x.canonical_key().cmp(&y.canonical_key()))
    });

    let mut per_node = vec![Vec::new(); nodes];
    let mut node_bytes = vec![0u64; nodes];
    let mut node_rows = vec![0i64; nodes];
    for idx in order {
        // Node count is small (a cluster, not a fleet of millions), so a linear
        // scan beats a heap and keeps the tie-break trivially the lowest index.
        let mut best = 0usize;
        for n in 1..nodes {
            if node_bytes[n] < node_bytes[best] {
                best = n;
            }
        }
        per_node[best].push(idx);
        node_bytes[best] += set.splits[idx].bytes;
        node_rows[best] += set.splits[idx].num_rows;
    }

    // Give each node its splits in canonical order so a shard scan reads the
    // file roughly forwards rather than in size order.
    for owned in per_node.iter_mut() {
        owned.sort_by_key(|&i| set.splits[i].canonical_key());
    }

    let node_splits = per_node.iter().map(|v| v.len()).collect();
    Assignment {
        nodes,
        per_node,
        node_bytes,
        node_rows,
        node_splits,
        total_bytes: set.total_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic(sizes: &[u64]) -> SplitSet {
        let splits: Vec<Split> = sizes
            .iter()
            .enumerate()
            .map(|(i, &b)| Split {
                table: "t".into(),
                path: PathBuf::from("t.parquet"),
                file: "t.parquet".into(),
                row_group: i,
                row_offset: 0,
                num_rows: 1000,
                bytes: b,
            })
            .collect();
        SplitSet {
            table: "t".into(),
            total_bytes: sizes.iter().sum(),
            total_rows: 1000 * sizes.len() as i64,
            target_split_bytes: MAX_SPLIT_BYTES,
            splits,
        }
    }

    #[test]
    fn lpt_beats_round_robin_on_uneven_splits() {
        // One boulder and a pile of pebbles: round-robin by count would put
        // the boulder and a third of the pebbles on the same node.
        let mut sizes = vec![1000u64];
        sizes.extend(std::iter::repeat(10).take(30));
        let set = synthetic(&sizes);
        let a = assign_lpt(&set, 3);
        // Round-robin's imbalance here would be ~2.3; LPT cannot do better
        // than the boulder's own share, and should reach exactly that.
        let mean = set.total_bytes as f64 / 3.0;
        assert!(
            (a.imbalance() - 1000.0 / mean).abs() < 1e-9,
            "LPT should be optimal here: {:?}",
            a.node_bytes
        );
        assert_eq!(a.node_bytes.iter().sum::<u64>(), set.total_bytes);
    }

    #[test]
    fn every_split_is_assigned_exactly_once() {
        let set = synthetic(&(1..=57).map(|i| i * 1_000_000).collect::<Vec<_>>());
        for nodes in [1usize, 2, 3, 5, 8, 16, 64] {
            let a = assign_lpt(&set, nodes);
            let mut seen: Vec<usize> = a.per_node.iter().flatten().copied().collect();
            seen.sort_unstable();
            assert_eq!(
                seen,
                (0..set.splits.len()).collect::<Vec<_>>(),
                "nodes={nodes}: splits lost or duplicated"
            );
            assert_eq!(a.node_bytes.iter().sum::<u64>(), set.total_bytes);
        }
    }

    #[test]
    fn assignment_is_deterministic_and_identical_across_callers() {
        let set = synthetic(&[5, 5, 5, 5, 3, 3, 1]);
        let a = assign_lpt(&set, 3);
        let b = assign_lpt(&set, 3);
        assert_eq!(a.per_node, b.per_node);
        assert_eq!(a.node_bytes, b.node_bytes);
    }

    #[test]
    fn equal_splits_divide_evenly_when_they_can() {
        let set = synthetic(&[10; 12]);
        let a = assign_lpt(&set, 4);
        assert_eq!(a.node_bytes, vec![30, 30, 30, 30]);
        assert!((a.imbalance() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn more_nodes_than_splits_leaves_nodes_idle_and_says_so() {
        let set = synthetic(&[10, 10]);
        let a = assign_lpt(&set, 5);
        assert_eq!(a.idle_nodes(), vec![2, 3, 4]);
        // The metric must still be finite and meaningful.
        assert!((a.imbalance() - 2.5).abs() < 1e-12);
    }

    #[test]
    fn an_empty_table_has_imbalance_one_not_nan() {
        let set = synthetic(&[]);
        let a = assign_lpt(&set, 3);
        assert_eq!(a.imbalance(), 1.0);
        assert_eq!(a.idle_nodes(), vec![0, 1, 2]);
    }

    #[test]
    fn tpch_lineitem_shaped_splits_meet_the_ten_percent_gate() {
        // The real shape at SF=10: 57 row groups of ~59MB plus a 14MB tail.
        // With whole row groups as the atom this FAILS at 8 nodes (1.118),
        // which is exactly why the atom is a row range, not a row group.
        let mut rg: Vec<u64> = vec![59_000_000; 57];
        rg.push(14_193_988);
        let total: u64 = rg.iter().sum();

        for nodes in [3usize, 8] {
            let target = target_split_bytes(total, nodes);
            let mut sizes = Vec::new();
            for b in &rg {
                let pieces = if *b <= target { 1 } else { b.div_ceil(target) };
                for _ in 0..pieces {
                    sizes.push(b / pieces);
                }
            }
            let set = synthetic(&sizes);
            let a = assign_lpt(&set, nodes);
            assert!(
                a.imbalance() <= 1.10,
                "nodes={nodes}: imbalance {:.4} exceeds the 1.10 gate ({} splits of ~{}MB)",
                a.imbalance(),
                sizes.len(),
                target / (1024 * 1024)
            );
        }
    }

    #[test]
    fn target_size_is_clamped_at_both_ends() {
        assert_eq!(target_split_bytes(u64::MAX / 2, 3), MAX_SPLIT_BYTES);
        // A table far larger than nodes * MIN keeps the floor...
        assert_eq!(target_split_bytes(64 * MIN_SPLIT_BYTES, 3), MIN_SPLIT_BYTES);
        // ...but a small one yields it, so every node still gets a piece
        // instead of one node getting everything.
        assert_eq!(target_split_bytes(300, 3), 100);
        assert_eq!(target_split_bytes(0, 3), 1);
    }

    #[test]
    fn a_table_smaller_than_the_floor_is_still_divided_across_nodes() {
        // The regression this guards: with a hard 4MB floor, a 300KB table is
        // one split, one node does all the work and the other two idle — while
        // the imbalance metric cheerfully reports 3.0.
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/data/tpch-1mb");
        let path = PathBuf::from(format!("{dir}/lineitem.parquet"));
        let set = enumerate_parquet("lineitem", std::slice::from_ref(&path), 3).unwrap();
        assert!(
            set.len() >= 3,
            "a 3-node cluster needs at least 3 splits, got {}",
            set.len()
        );
        let a = assign_lpt(&set, 3);
        assert!(a.idle_nodes().is_empty(), "no node should be idle");
        assert!(
            a.imbalance() <= 1.10,
            "imbalance {:.4} on a small table",
            a.imbalance()
        );
    }

    #[test]
    fn split_bytes_sum_to_the_table_exactly() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/data/tpch-1mb");
        for table in ["lineitem", "orders", "nation"] {
            let path = PathBuf::from(format!("{dir}/{table}.parquet"));
            for nodes in [1usize, 3, 8] {
                let set = enumerate_parquet(table, std::slice::from_ref(&path), nodes).unwrap();
                assert_eq!(
                    set.splits.iter().map(|s| s.bytes).sum::<u64>(),
                    set.total_bytes,
                    "{table} at {nodes} nodes: split bytes must sum to the table"
                );
                assert_eq!(
                    set.splits.iter().map(|s| s.num_rows).sum::<i64>(),
                    set.total_rows
                );
            }
        }
    }

    #[test]
    fn enumerating_a_real_table_is_canonical_and_reproducible() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/data/tpch-1mb");
        let path = PathBuf::from(format!("{dir}/lineitem.parquet"));
        if !path.exists() {
            panic!("data/tpch-1mb is required; CI regenerates it");
        }
        let a = enumerate_parquet("lineitem", std::slice::from_ref(&path), 3).unwrap();
        let b = enumerate_parquet("lineitem", std::slice::from_ref(&path), 3).unwrap();
        assert_eq!(a.digest(), b.digest());
        assert!(a.total_rows > 0);
        assert_eq!(
            a.splits.iter().map(|s| s.num_rows).sum::<i64>(),
            a.total_rows,
            "splits must cover every row exactly once"
        );
        // A different file name must change the digest: that is the whole
        // point of the interlock.
        let other = PathBuf::from(format!("{dir}/orders.parquet"));
        let c = enumerate_parquet("lineitem", std::slice::from_ref(&other), 3).unwrap();
        assert_ne!(a.digest(), c.digest());
    }
}
