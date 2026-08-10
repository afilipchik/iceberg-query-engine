//! Hardware topology detection and topology-aware worker placement.
//!
//! # Why this exists
//!
//! Every parallel site in the engine used to call `rayon::current_num_threads()`
//! and treat the resulting threads as interchangeable. That assumption is false
//! on two kinds of machine the engine is expected to run on:
//!
//! 1. **Multi-socket (NUMA) servers.** A worker that touches memory homed on
//!    another socket pays 1.5-2.5x the latency and burns interconnect bandwidth.
//!    The fix is to keep a worker, its hash table, and the data it scans on the
//!    same node.
//! 2. **Hybrid client CPUs** (Intel P/E-core, ARM big.LITTLE). Threads are *not*
//!    equal: on the development box the E-cores top out at 4.3 GHz against the
//!    P-cores' 5.8 GHz, and their IPC is lower still. In any barrier-synchronised
//!    phase — and this engine has many: parallel aggregate merge, join build,
//!    `(0..num_threads)` morsel fan-outs — the slowest worker sets the wall clock.
//!
//! Both problems are the same problem: *placement*. This module answers "what
//! does this machine actually look like?" once, at startup, and exposes an
//! ordering of CPUs from best to worst so that a pool of N workers occupies the
//! N best CPUs rather than an arbitrary N.
//!
//! # NUMA status — read this before trusting the NUMA path
//!
//! The development machine reports **one** NUMA node (`numactl --hardware`:
//! `available: 1 nodes (0)`). Every NUMA-specific branch below is therefore
//! **unit-tested against synthetic sysfs fixtures but never executed on real
//! multi-socket silicon.** The tests in this file construct fake `/sys` trees
//! (two-socket server, hybrid client, empty container) and assert the parse and
//! the derived orderings. That validates the *parsing and policy*, not the
//! *performance claim*. Do not quote a NUMA speedup from this repository until
//! somebody runs it on a real two-socket box.
//!
//! Correspondingly, every NUMA branch is written so that a one-node machine
//! takes exactly the pre-existing code path: [`Topology::num_numa_nodes`]
//! returns 1, [`node_pool`] returns `None`, and callers fall through to the
//! global pool with no extra allocation, no extra indirection, and no extra
//! synchronisation.
//!
//! # Detection sources (all optional, all degrade gracefully)
//!
//! | fact | sysfs path | fallback |
//! |---|---|---|
//! | NUMA nodes | `node/node%d/cpulist` | single node holding every online CPU |
//! | node distances | `node/node%d/distance` | `[10]` self-distance only |
//! | SMT siblings | `cpu/cpu%d/topology/thread_siblings_list` | every CPU is a physical core |
//! | core / package id | `cpu/cpu%d/topology/{core_id,physical_package_id}` | id = CPU id, package 0 |
//! | performance class | `cpu/cpu%d/cpufreq/cpuinfo_max_freq`, else `cpu/cpu%d/cpu_capacity` | all cores equal |
//!
//! A container with `/sys` masked (or a non-Linux host) yields a uniform,
//! single-node topology whose CPU order is `0..n`, i.e. the historical
//! behaviour. Nothing in the engine requires any of these files to exist.
//!
//! Note that the performance class is **derived**, never hardcoded: no CPU model
//! numbers appear anywhere in this file. A future hybrid part, or an ARM
//! big.LITTLE SoC exposing `cpu_capacity`, is classified by the same code.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::OnceLock;

/// Relative performance weight of the fastest CPU class. Slower classes are
/// scaled against this, so a 4.3 GHz E-core next to a 5.8 GHz P-core scores
/// `4300 * 1000 / 5800 = 741`.
pub const MAX_WEIGHT: u32 = 1000;

/// One logical CPU (a hardware thread).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuInfo {
    /// Logical CPU id as the kernel numbers it.
    pub id: usize,
    /// NUMA node this CPU belongs to (0 when the machine is not NUMA).
    pub node: usize,
    /// Physical core id within the package.
    pub core_id: usize,
    /// Physical package (socket) id.
    pub package_id: usize,
    /// True when this CPU shares a physical core with a lower-numbered CPU,
    /// i.e. it is the second (or later) SMT thread of that core. Scheduling a
    /// worker here yields far less than a full core.
    pub is_smt_sibling: bool,
    /// Raw capability reading in whatever unit sysfs offered (kHz for
    /// `cpuinfo_max_freq`, unitless for `cpu_capacity`). `None` when unknown.
    pub raw_capacity: Option<u64>,
    /// Capability normalised so the fastest CPU on the machine scores
    /// [`MAX_WEIGHT`]. Always [`MAX_WEIGHT`] when nothing could be read, which
    /// makes an unknown machine uniform rather than arbitrarily ordered.
    pub weight: u32,
}

/// One NUMA node and the CPUs homed on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumaNode {
    pub id: usize,
    /// Logical CPUs on this node, ascending.
    pub cpus: Vec<usize>,
    /// ACPI SLIT row for this node: `distances[j]` is the relative cost of
    /// reaching node `j`, with 10 meaning "local". Empty when unavailable.
    pub distances: Vec<u32>,
}

/// A snapshot of the machine, taken once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    /// NUMA nodes, ascending by id. Always at least one entry.
    pub nodes: Vec<NumaNode>,
    /// Every CPU this process is allowed to run on, ascending by id.
    ///
    /// This is intersected with the process affinity mask at detection time, so
    /// running under `taskset -c 0-7` yields an 8-CPU topology. That keeps the
    /// core-scaling harness (`scripts/scaling_bench.py --mode cores`) honest:
    /// the engine sizes its pool to what it was actually given.
    pub cpus: Vec<CpuInfo>,
}

impl Topology {
    /// The process-wide topology, detected once and cached.
    pub fn get() -> &'static Topology {
        static TOPOLOGY: OnceLock<Topology> = OnceLock::new();
        TOPOLOGY.get_or_init(|| {
            let allowed = current_affinity();
            Topology::from_sysfs(Path::new("/sys"), allowed.as_deref())
        })
    }

    /// Parse a topology out of a sysfs tree. `root` is the mount point (`/sys`
    /// in production, a fixture directory in tests). `allowed`, when given,
    /// restricts the result to those logical CPUs.
    ///
    /// This function never fails: anything it cannot read it fills in with the
    /// uniform, single-node default.
    pub fn from_sysfs(root: &Path, allowed: Option<&[usize]>) -> Topology {
        let cpu_root = root.join("devices/system/cpu");
        let node_root = root.join("devices/system/node");

        // ── Which CPUs exist ────────────────────────────────────────────────
        let mut online: Vec<usize> = read_cpulist(&cpu_root.join("online")).unwrap_or_default();
        if online.is_empty() {
            // No `online` file: enumerate cpuN directories instead.
            online = enumerate_cpu_dirs(&cpu_root);
        }
        if online.is_empty() {
            // No sysfs at all (container, non-Linux). Fall back to the count
            // the standard library reports.
            let n = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            online = (0..n).collect();
        }
        if let Some(mask) = allowed {
            online.retain(|c| mask.contains(c));
            if online.is_empty() {
                online = mask.to_vec();
            }
        }
        online.sort_unstable();
        online.dedup();

        // ── NUMA nodes ──────────────────────────────────────────────────────
        let mut node_of: BTreeMap<usize, usize> = BTreeMap::new();
        let mut nodes: Vec<NumaNode> = Vec::new();
        for node_id in enumerate_node_dirs(&node_root) {
            let dir = node_root.join(format!("node{}", node_id));
            let mut cpus = read_cpulist(&dir.join("cpulist")).unwrap_or_default();
            cpus.retain(|c| online.contains(c));
            let distances = read_file(&dir.join("distance"))
                .map(|s| {
                    s.split_whitespace()
                        .filter_map(|t| t.parse::<u32>().ok())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for c in &cpus {
                node_of.insert(*c, node_id);
            }
            nodes.push(NumaNode {
                id: node_id,
                cpus,
                distances,
            });
        }
        // Drop nodes that hold none of our allowed CPUs — under `taskset` onto
        // one socket, the other socket must not appear as a usable node.
        nodes.retain(|n| !n.cpus.is_empty());
        if nodes.is_empty() {
            nodes.push(NumaNode {
                id: 0,
                cpus: online.clone(),
                distances: vec![10],
            });
            for c in &online {
                node_of.insert(*c, 0);
            }
        }

        // ── Per-CPU core identity and capability ────────────────────────────
        let mut raw: BTreeMap<usize, Option<u64>> = BTreeMap::new();
        let mut cpus: Vec<CpuInfo> = Vec::with_capacity(online.len());
        for &id in &online {
            let dir = cpu_root.join(format!("cpu{}", id));
            let topo = dir.join("topology");
            let core_id = read_file(&topo.join("core_id"))
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(id);
            let package_id = read_file(&topo.join("physical_package_id"))
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(0);
            // An SMT sibling is any CPU that is not the lowest-numbered thread
            // of its physical core. Absent the file, assume no SMT.
            let siblings = read_cpulist(&topo.join("thread_siblings_list")).unwrap_or_default();
            let is_smt_sibling = siblings
                .iter()
                .filter(|c| online.contains(c))
                .min()
                .map(|&lowest| lowest < id)
                .unwrap_or(false);
            let capacity = read_file(&dir.join("cpufreq/cpuinfo_max_freq"))
                .and_then(|s| s.trim().parse::<u64>().ok())
                .or_else(|| {
                    // ARM big.LITTLE / DynamIQ exposes a unitless capacity.
                    read_file(&dir.join("cpu_capacity")).and_then(|s| s.trim().parse::<u64>().ok())
                });
            raw.insert(id, capacity);
            cpus.push(CpuInfo {
                id,
                node: node_of.get(&id).copied().unwrap_or(0),
                core_id,
                package_id,
                is_smt_sibling,
                raw_capacity: capacity,
                weight: MAX_WEIGHT,
            });
        }

        // Normalise capability against the fastest CPU present. If any CPU is
        // missing a reading we refuse to rank at all and call the machine
        // uniform — a partial ranking is worse than none, because it would sort
        // the unknown cores to an arbitrary end.
        let all_known = raw.values().all(|v| v.is_some());
        let max_cap = raw.values().filter_map(|v| *v).max().unwrap_or(0);
        if all_known && max_cap > 0 {
            for cpu in &mut cpus {
                let c = cpu.raw_capacity.unwrap_or(max_cap);
                cpu.weight = ((c as u128 * MAX_WEIGHT as u128) / max_cap as u128) as u32;
            }
        }

        Topology { nodes, cpus }
    }

    /// Number of NUMA nodes visible to this process. `1` means there is no NUMA
    /// locality to exploit and every NUMA branch in the engine must no-op.
    pub fn num_numa_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Logical CPUs homed on `node`, or an empty slice for an unknown node.
    pub fn cpus_for_node(&self, node: usize) -> &[usize] {
        self.nodes
            .iter()
            .find(|n| n.id == node)
            .map(|n| n.cpus.as_slice())
            .unwrap_or(&[])
    }

    /// Relative cost of reaching `to` from `from` (ACPI SLIT units, 10 = local).
    /// Returns 10 for the self-distance and when no SLIT is available.
    pub fn distance(&self, from: usize, to: usize) -> u32 {
        if from == to {
            return 10;
        }
        self.nodes
            .iter()
            .find(|n| n.id == from)
            .and_then(|n| n.distances.get(to).copied())
            .unwrap_or(10)
    }

    /// Total performance weight of every usable CPU. A work-splitting policy
    /// that wants proportional shares divides by this.
    pub fn total_weight(&self) -> u64 {
        self.cpus.iter().map(|c| c.weight as u64).sum()
    }

    /// True when every CPU has the same performance weight, i.e. there is no
    /// fast/slow core distinction to exploit. Note that SMT siblings are still
    /// "uniform" by this definition — they have the same weight, they just
    /// cannot all be busy at once.
    pub fn is_uniform(&self) -> bool {
        match self.cpus.first() {
            None => true,
            Some(first) => self.cpus.iter().all(|c| c.weight == first.weight),
        }
    }

    /// True when the machine has both SMT siblings and more than one weight
    /// class, or more than one NUMA node — i.e. placement can matter at all.
    pub fn is_heterogeneous(&self) -> bool {
        !self.is_uniform()
            || self.num_numa_nodes() > 1
            || self.cpus.iter().any(|c| c.is_smt_sibling)
    }

    /// CPUs ordered best-to-worst for occupancy by a pool of workers.
    ///
    /// The ordering rule, in priority order:
    ///
    /// 1. **Physical cores before SMT siblings.** A second thread on a busy core
    ///    adds maybe 20-30% throughput; a distinct core adds ~100%. This is the
    ///    dominant term and it is measurable on the development box: 8 physical
    ///    P-cores plus 8 E-cores ran the TPC-H suite in 8.12 s, while the 16
    ///    hardware threads of those same 8 P-cores took 8.42 s. Slow *cores*
    ///    beat fast *hyperthreads*.
    /// 2. **Higher weight first**, so a pool smaller than the machine lands on
    ///    the fast class. Restricting the same suite to 16 P-threads cost 8.42 s
    ///    against 11.19 s for the 16 E-cores.
    /// 3. **Round-robin across NUMA nodes**, so a pool smaller than the machine
    ///    still spreads over sockets and gets the aggregate memory bandwidth of
    ///    all of them rather than saturating one controller. (Unvalidated: this
    ///    box has one node, where the rule is a no-op.)
    /// 4. CPU id, purely for determinism.
    pub fn preferred_cpu_order(&self) -> Vec<usize> {
        // Bucket by (smt, -weight) then deal round-robin across nodes.
        let mut buckets: BTreeMap<(bool, u32), Vec<&CpuInfo>> = BTreeMap::new();
        for cpu in &self.cpus {
            buckets
                .entry((cpu.is_smt_sibling, MAX_WEIGHT.saturating_sub(cpu.weight)))
                .or_default()
                .push(cpu);
        }
        let node_ids: Vec<usize> = self.nodes.iter().map(|n| n.id).collect();
        let mut order = Vec::with_capacity(self.cpus.len());
        for (_, bucket) in buckets {
            // Within a bucket, interleave nodes: n0c0, n1c0, n0c1, n1c1, ...
            let mut per_node: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for cpu in bucket {
                per_node.entry(cpu.node).or_default().push(cpu.id);
            }
            for v in per_node.values_mut() {
                v.sort_unstable();
            }
            let deepest = per_node.values().map(|v| v.len()).max().unwrap_or(0);
            for i in 0..deepest {
                for node in &node_ids {
                    if let Some(list) = per_node.get(node) {
                        if let Some(&cpu) = list.get(i) {
                            order.push(cpu);
                        }
                    }
                }
            }
        }
        order
    }

    /// The CPUs in the top performance class (the "P-cores" on a hybrid part),
    /// including their SMT siblings. Equals every CPU on a uniform machine.
    pub fn fast_cpus(&self) -> Vec<usize> {
        let max_w = self.cpus.iter().map(|c| c.weight).max().unwrap_or(0);
        self.cpus
            .iter()
            .filter(|c| c.weight == max_w)
            .map(|c| c.id)
            .collect()
    }

    /// Number of distinct physical cores usable by this process.
    pub fn num_physical_cores(&self) -> usize {
        self.cpus.iter().filter(|c| !c.is_smt_sibling).count()
    }

    /// Every usable CPU sharing a physical core with `cpu` (including `cpu`).
    ///
    /// Confining a worker to this set instead of to a single CPU keeps its L1/L2
    /// working set and its NUMA node fixed while still letting the kernel slide
    /// it onto the idle SMT thread when something else — a tokio I/O thread, the
    /// query's own driver — lands on top of it.
    pub fn core_siblings(&self, cpu: usize) -> Vec<usize> {
        let Some(info) = self.cpus.iter().find(|c| c.id == cpu) else {
            return vec![cpu];
        };
        self.cpus
            .iter()
            .filter(|c| c.core_id == info.core_id && c.package_id == info.package_id)
            .map(|c| c.id)
            .collect()
    }

    /// Default worker count: one per usable logical CPU, honouring
    /// `RAYON_NUM_THREADS` when the operator set it explicitly.
    pub fn default_worker_threads(&self) -> usize {
        if let Ok(v) = std::env::var("RAYON_NUM_THREADS") {
            if let Ok(n) = v.trim().parse::<usize>() {
                if n > 0 {
                    return n;
                }
            }
        }
        self.cpus.len().max(1)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// sysfs parsing helpers
// ─────────────────────────────────────────────────────────────────────────────

fn read_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Parse a kernel cpulist such as `"0-3,8,10-11"`.
pub(crate) fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in s.trim().split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) {
                    for c in a..=b {
                        out.push(c);
                    }
                }
            }
            None => {
                if let Ok(a) = part.parse::<usize>() {
                    out.push(a);
                }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn read_cpulist(path: &Path) -> Option<Vec<usize>> {
    read_file(path).map(|s| parse_cpulist(&s))
}

fn enumerate_numbered_dirs(root: &Path, prefix: &str) -> Vec<usize> {
    let mut ids: Vec<usize> = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return ids;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix(prefix) else {
            continue;
        };
        if let Ok(id) = rest.parse::<usize>() {
            if entry.path().is_dir() {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids
}

fn enumerate_cpu_dirs(cpu_root: &Path) -> Vec<usize> {
    enumerate_numbered_dirs(cpu_root, "cpu")
}

fn enumerate_node_dirs(node_root: &Path) -> Vec<usize> {
    enumerate_numbered_dirs(node_root, "node")
}

/// The set of CPUs this process may run on, or `None` when it cannot be read
/// (non-Linux, or a kernel that refuses). `None` means "no restriction".
#[cfg(target_os = "linux")]
pub fn current_affinity() -> Option<Vec<usize>> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return None;
        }
        let mut out = Vec::new();
        for cpu in 0..libc::CPU_SETSIZE as usize {
            if libc::CPU_ISSET(cpu, &set) {
                out.push(cpu);
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn current_affinity() -> Option<Vec<usize>> {
    None
}

/// Pin the calling thread to a single logical CPU. Returns false when the
/// platform or the kernel refused; callers treat that as "carry on unpinned".
#[cfg(target_os = "linux")]
pub fn pin_current_thread_to(cpu: usize) -> bool {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
    }
}

#[cfg(not(target_os = "linux"))]
pub fn pin_current_thread_to(_cpu: usize) -> bool {
    false
}

/// Restrict the calling thread to a set of logical CPUs (used to keep a
/// NUMA-node pool's workers on that node without pinning them to one CPU, so
/// the kernel can still balance within the node).
#[cfg(target_os = "linux")]
pub fn set_thread_affinity(cpus: &[usize]) -> bool {
    if cpus.is_empty() {
        return false;
    }
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_thread_affinity(_cpus: &[usize]) -> bool {
    false
}

/// The logical CPU the calling thread is running on right now, if the platform
/// can say. With pinned workers this is exact, which lets a worker ask "am I on
/// a slow core?" and size its own bite of work accordingly.
#[cfg(target_os = "linux")]
pub fn current_cpu() -> Option<usize> {
    let c = unsafe { libc::sched_getcpu() };
    if c < 0 {
        None
    } else {
        Some(c as usize)
    }
}

#[cfg(not(target_os = "linux"))]
pub fn current_cpu() -> Option<usize> {
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Global pool placement
// ─────────────────────────────────────────────────────────────────────────────

/// Set by [`init_global_pool`]: worker index -> logical CPU it was pinned to.
static PINNED_ORDER: OnceLock<Vec<usize>> = OnceLock::new();

/// Was topology-aware placement requested and successfully installed?
static PLACEMENT_ACTIVE: OnceLock<bool> = OnceLock::new();

fn placement_disabled() -> bool {
    matches!(
        std::env::var("QE_TOPOLOGY").ok().as_deref(),
        Some("0") | Some("off") | Some("false")
    )
}

/// How tightly a worker is bound to the CPU chosen for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// Worker *i* may only run on `preferred_cpu_order()[i]`.
    Cpu,
    /// Worker *i* may run on any SMT thread of that CPU's physical core.
    Core,
    /// Worker *i* may run on any CPU of that CPU's NUMA node. On a single-node
    /// machine this is equivalent to no binding at all.
    Node,
}

/// The default binding policy, and why it is the loosest one.
///
/// Measured on the development box (TPC-H SF=10 parquet, solo per query, five
/// interleaved A/B pairs against an unplaced pool):
///
/// | policy | result |
/// |---|---|
/// | [`Placement::Cpu`] | Q02 **-16.2%** (5/5) but Q11 **+22.7%** (0/5), Q06 +8.2%, Q09 +4.3%, Q01 +4.2% |
/// | [`Placement::Core`] | Q02 -4.5%, Q01 -0.7%, Q09 +1.6%, Q11 +3.8%, but Q06 **+16.2%** (1/5) |
/// | [`Placement::Node`] | Q06 +4.6% (3/5), Q02 -3.2% (3/5), Q01 +0.9% — noise |
///
/// Tight binding wins exactly one query and loses several. The reason is that
/// the engine's rayon workers share the machine with tokio I/O threads and the
/// query driver; a worker bound to one CPU cannot step aside when one of those
/// lands on top of it, and on a hybrid part half the workers end up permanently
/// stuck on E-cores with no way for the kernel to rebalance.
///
/// So the default is the loosest policy that still expresses the thing worth
/// expressing: **stay on your NUMA node**. On this single-node box that is a
/// no-op (the mask covers every usable CPU, and [`set_thread_affinity`] is
/// skipped entirely). On a multi-socket box it is the one placement rule with a
/// real, well-understood payoff — and it is the one this hardware cannot test.
fn placement_policy() -> Placement {
    match std::env::var("QE_PLACEMENT").ok().as_deref() {
        Some("cpu") => Placement::Cpu,
        Some("core") => Placement::Core,
        _ => Placement::Node,
    }
}

/// Install the process-wide rayon pool with topology-aware worker placement.
///
/// Worker *i* is pinned to the *i*-th CPU of [`Topology::preferred_cpu_order`],
/// so the pool fills fast physical cores first, then the remaining physical
/// cores, then SMT siblings. Two things follow:
///
/// * A worker never migrates, so its thread-local hash table stays in the cache
///   (and, on a NUMA box, in the memory homed on its node).
/// * "The first N workers" now means "the N best CPUs", which is what makes a
///   reduced fan-out width for small inputs meaningful rather than arbitrary.
///
/// Safe to call more than once and safe to call after rayon has already built
/// its default pool — in that case it reports failure and the engine keeps the
/// pool it has. Set `QE_TOPOLOGY=0` to opt out entirely.
pub fn init_global_pool() -> bool {
    *PLACEMENT_ACTIVE.get_or_init(|| {
        if placement_disabled() {
            return false;
        }
        let topo = Topology::get();
        let order = topo.preferred_cpu_order();
        if order.is_empty() {
            return false;
        }
        let threads = topo.default_worker_threads();
        // Workers beyond the CPU count wrap around the preference order rather
        // than piling onto CPU 0 — an oversubscribed pool should still be
        // spread out.
        let pin_order: Vec<usize> = (0..threads).map(|i| order[i % order.len()]).collect();
        // Expand each worker's chosen CPU into the affinity mask the policy
        // asks for, once, up front — the start handler must not touch sysfs.
        let policy = placement_policy();
        let masks: Vec<Vec<usize>> = pin_order
            .iter()
            .map(|&cpu| match policy {
                Placement::Cpu => vec![cpu],
                Placement::Core => topo.core_siblings(cpu),
                Placement::Node => {
                    let node = topo
                        .cpus
                        .iter()
                        .find(|c| c.id == cpu)
                        .map(|c| c.node)
                        .unwrap_or(0);
                    topo.cpus_for_node(node).to_vec()
                }
            })
            .collect();
        // A mask covering every CPU the process may use restricts nothing, so
        // do not spend the syscall — this is what makes the default policy
        // literally free on a single-node machine.
        let trivial = masks.iter().all(|m| m.len() == topo.cpus.len());
        let built = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("qe-worker-{}", i))
            .start_handler(move |i| {
                if trivial {
                    return;
                }
                if let Some(mask) = masks.get(i) {
                    set_thread_affinity(mask);
                }
            })
            .build_global()
            .is_ok();
        if built {
            let _ = PINNED_ORDER.set(pin_order);
        }
        built
    })
}

/// True when [`init_global_pool`] installed a pinned pool.
pub fn placement_active() -> bool {
    *PLACEMENT_ACTIVE.get_or_init(|| false)
}

// ─────────────────────────────────────────────────────────────────────────────
// NUMA-node pools  (inert on a single-node machine)
// ─────────────────────────────────────────────────────────────────────────────

static NODE_POOLS: OnceLock<Vec<std::sync::Arc<rayon::ThreadPool>>> = OnceLock::new();

/// A rayon pool whose workers are confined to `node`'s CPUs, for work that
/// should stay node-local (a partitioned join build, a per-node aggregate).
///
/// Returns `None` — always, immediately, with no allocation — when the machine
/// has a single NUMA node, which is the guard that keeps this whole feature a
/// no-op on non-NUMA hardware. **The multi-node path is unit-tested but has
/// never run on real multi-socket silicon; see the module docs.**
pub fn node_pool(node: usize) -> Option<std::sync::Arc<rayon::ThreadPool>> {
    let topo = Topology::get();
    if topo.num_numa_nodes() <= 1 || placement_disabled() {
        return None;
    }
    let pools = NODE_POOLS.get_or_init(|| {
        topo.nodes
            .iter()
            .filter_map(|n| {
                let cpus = n.cpus.clone();
                if cpus.is_empty() {
                    return None;
                }
                rayon::ThreadPoolBuilder::new()
                    .num_threads(cpus.len())
                    .thread_name(move |i| format!("qe-node-worker-{}", i))
                    .start_handler(move |_| {
                        set_thread_affinity(&cpus);
                    })
                    .build()
                    .ok()
                    .map(std::sync::Arc::new)
            })
            .collect()
    });
    pools.get(node).cloned()
}

/// Which NUMA node a unit of work should prefer, given a stable work index.
///
/// This is the single hook a NUMA-aware morsel scheduler needs: hash the file
/// or partition index here, and every consumer of that partition — scan,
/// build-side hash table, spill file — lands on the same node. On a one-node
/// machine it is a constant 0, so the caller's arithmetic folds away.
pub fn preferred_node_for(work_index: usize) -> usize {
    let topo = Topology::get();
    let n = topo.num_numa_nodes();
    if n <= 1 {
        return 0;
    }
    topo.nodes[work_index % n].id
}

/// Fault in the pages backing `buf` from the calling thread.
///
/// Linux allocates a page on the node of whichever CPU first *touches* it, not
/// whichever thread called `malloc`. A large hash table or aggregation state
/// built by a pinned worker therefore wants its pages touched by that same
/// worker before the real work starts; otherwise the pages can end up homed on
/// whichever node happened to run the allocation, and every subsequent probe
/// crosses the interconnect.
///
/// On a single-node machine this is pure cost, so it returns immediately —
/// there is exactly one node and every page is local by construction.
pub fn first_touch<T>(buf: &mut [T]) {
    if Topology::get().num_numa_nodes() <= 1 {
        return;
    }
    if buf.is_empty() {
        return;
    }
    let page = 4096usize;
    let bytes = std::mem::size_of_val(buf);
    let base = buf.as_mut_ptr() as *mut u8;
    let mut off = 0usize;
    while off < bytes {
        // Read-modify-write one byte per page: a pure read of a
        // freshly-mapped anonymous page maps the shared zero page instead of
        // allocating, which would defeat the purpose.
        unsafe {
            let p = base.add(off);
            std::ptr::write_volatile(p, std::ptr::read_volatile(p));
        }
        off += page;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Work-proportional fan-out
// ─────────────────────────────────────────────────────────────────────────────

/// How many workers to fan out over, given how many indivisible units of work
/// there are.
///
/// Spawning 32 workers to consume 3 row groups does not make those row groups
/// finish sooner; it allocates 29 thread-local aggregation states, shards the
/// merge 32 ways, and exposes the query to a straggler on the slowest core.
/// Callers that know their work count should size the fan-out to it.
///
/// `max` is normally [`Topology::default_worker_threads`] or
/// `rayon::current_num_threads()`.
pub fn workers_for(work_units: usize, max: usize) -> usize {
    work_units.clamp(1, max.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    /// Build a synthetic sysfs tree. `cpus` maps logical cpu -> (node, core_id,
    /// package, siblings, max_khz).
    struct FakeSys {
        root: tempfile::TempDir,
    }

    impl FakeSys {
        fn new() -> Self {
            FakeSys {
                root: tempfile::tempdir().unwrap(),
            }
        }
        fn path(&self) -> PathBuf {
            self.root.path().to_path_buf()
        }
        fn write(&self, rel: &str, contents: &str) {
            let p = self.root.path().join(rel);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, contents).unwrap();
        }
        fn cpu(&self, id: usize, core_id: usize, package: usize, siblings: &str, khz: Option<u64>) {
            let base = format!("devices/system/cpu/cpu{}", id);
            self.write(&format!("{}/topology/core_id", base), &core_id.to_string());
            self.write(
                &format!("{}/topology/physical_package_id", base),
                &package.to_string(),
            );
            self.write(&format!("{}/topology/thread_siblings_list", base), siblings);
            if let Some(k) = khz {
                self.write(
                    &format!("{}/cpufreq/cpuinfo_max_freq", base),
                    &k.to_string(),
                );
            }
        }
        fn node(&self, id: usize, cpulist: &str, distance: &str) {
            self.write(&format!("devices/system/node/node{}/cpulist", id), cpulist);
            self.write(
                &format!("devices/system/node/node{}/distance", id),
                distance,
            );
        }
        fn online(&self, list: &str) {
            self.write("devices/system/cpu/online", list);
        }
    }

    /// A two-socket server: 2 nodes x 4 physical cores x 2 SMT threads,
    /// all cores identical. This is the shape the NUMA path targets and the
    /// only place it is exercised — no such machine was available to run on.
    fn two_socket() -> FakeSys {
        let s = FakeSys::new();
        s.online("0-15");
        // node 0 -> cpus 0-3 (cores) + 8-11 (their siblings)
        // node 1 -> cpus 4-7 (cores) + 12-15 (their siblings)
        for core in 0..4 {
            let a = core;
            let b = core + 8;
            let sib = format!("{},{}", a, b);
            s.cpu(a, core, 0, &sib, Some(3_000_000));
            s.cpu(b, core, 0, &sib, Some(3_000_000));
        }
        for core in 0..4 {
            let a = core + 4;
            let b = core + 12;
            let sib = format!("{},{}", a, b);
            s.cpu(a, core, 1, &sib, Some(3_000_000));
            s.cpu(b, core, 1, &sib, Some(3_000_000));
        }
        s.node(0, "0-3,8-11", "10 21");
        s.node(1, "4-7,12-15", "21 10");
        s
    }

    /// A hybrid client part shaped like the development box: 4 P-cores with
    /// SMT (cpus 0-7) at 5.8 GHz plus 8 E-cores (cpus 8-15) at 4.3 GHz, one
    /// NUMA node.
    fn hybrid() -> FakeSys {
        let s = FakeSys::new();
        s.online("0-15");
        for core in 0..4 {
            let a = core * 2;
            let b = core * 2 + 1;
            let sib = format!("{},{}", a, b);
            s.cpu(a, core, 0, &sib, Some(5_800_000));
            s.cpu(b, core, 0, &sib, Some(5_800_000));
        }
        for i in 0..8 {
            let id = 8 + i;
            s.cpu(id, 4 + i, 0, &id.to_string(), Some(4_300_000));
        }
        s.node(0, "0-15", "10");
        s
    }

    #[test]
    fn cpulist_parsing_handles_ranges_singletons_and_junk() {
        assert_eq!(parse_cpulist("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpulist("0-3,8,10-11"), vec![0, 1, 2, 3, 8, 10, 11]);
        assert_eq!(parse_cpulist(" 5 \n"), vec![5]);
        assert_eq!(parse_cpulist(""), Vec::<usize>::new());
        assert_eq!(parse_cpulist("garbage"), Vec::<usize>::new());
        // Duplicates and overlapping ranges collapse.
        assert_eq!(parse_cpulist("1,1,0-2"), vec![0, 1, 2]);
    }

    #[test]
    fn two_socket_numa_is_parsed() {
        let s = two_socket();
        let t = Topology::from_sysfs(&s.path(), None);
        assert_eq!(t.num_numa_nodes(), 2);
        assert_eq!(t.cpus.len(), 16);
        assert_eq!(t.cpus_for_node(0), &[0, 1, 2, 3, 8, 9, 10, 11]);
        assert_eq!(t.cpus_for_node(1), &[4, 5, 6, 7, 12, 13, 14, 15]);
        assert_eq!(t.distance(0, 1), 21);
        assert_eq!(t.distance(1, 0), 21);
        assert_eq!(t.distance(0, 0), 10);
        // Homogeneous cores: uniform weights, but SMT makes it heterogeneous
        // for placement purposes.
        assert!(t.is_uniform());
        assert!(t.is_heterogeneous());
        assert_eq!(t.num_physical_cores(), 8);
        // cpus 8-15 are the second thread of each core.
        for c in &t.cpus {
            assert_eq!(c.is_smt_sibling, c.id >= 8, "cpu {}", c.id);
        }
    }

    #[test]
    fn two_socket_order_fills_physical_cores_alternating_sockets() {
        let s = two_socket();
        let t = Topology::from_sysfs(&s.path(), None);
        let order = t.preferred_cpu_order();
        // First 8 entries must be the 8 physical cores, alternating node 0 / 1
        // so that a half-size pool still uses both memory controllers.
        assert_eq!(&order[..8], &[0, 4, 1, 5, 2, 6, 3, 7]);
        // Remaining 8 are the SMT siblings, same interleave.
        assert_eq!(&order[8..], &[8, 12, 9, 13, 10, 14, 11, 15]);
    }

    #[test]
    fn two_socket_restricted_to_one_socket_hides_the_other_node() {
        let s = two_socket();
        // Like `taskset -c 0-3,8-11`: only socket 0.
        let t = Topology::from_sysfs(&s.path(), Some(&[0, 1, 2, 3, 8, 9, 10, 11]));
        assert_eq!(t.num_numa_nodes(), 1);
        assert_eq!(t.cpus.len(), 8);
        assert_eq!(t.cpus_for_node(0), &[0, 1, 2, 3, 8, 9, 10, 11]);
        assert_eq!(t.preferred_cpu_order(), vec![0, 1, 2, 3, 8, 9, 10, 11]);
        assert_eq!(preferred_node_for_topology(&t, 7), 0);
    }

    #[test]
    fn hybrid_weights_are_derived_not_hardcoded() {
        let s = hybrid();
        let t = Topology::from_sysfs(&s.path(), None);
        assert_eq!(t.num_numa_nodes(), 1);
        assert!(!t.is_uniform());
        // 4.3/5.8 == 741 thousandths. No model number anywhere.
        let p = t.cpus.iter().find(|c| c.id == 0).unwrap();
        let e = t.cpus.iter().find(|c| c.id == 8).unwrap();
        assert_eq!(p.weight, 1000);
        assert_eq!(e.weight, 741);
        assert_eq!(t.fast_cpus(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(t.num_physical_cores(), 12);
        // 4 P physical * 1000 + 4 P siblings * 1000 + 8 E * 741
        assert_eq!(t.total_weight(), 8 * 1000 + 8 * 741);
    }

    #[test]
    fn hybrid_order_is_fast_cores_then_slow_cores_then_hyperthreads() {
        let s = hybrid();
        let t = Topology::from_sysfs(&s.path(), None);
        let order = t.preferred_cpu_order();
        // Measured on the development box: slow physical cores beat fast
        // hyperthreads (8P+8E = 8.12 s vs 16 P-threads = 8.42 s over the
        // TPC-H suite), so E-cores must sort ahead of the P-core siblings.
        assert_eq!(
            order,
            vec![0, 2, 4, 6, 8, 9, 10, 11, 12, 13, 14, 15, 1, 3, 5, 7]
        );
    }

    #[test]
    fn container_without_sysfs_degrades_to_uniform_single_node() {
        let empty = tempfile::tempdir().unwrap();
        let t = Topology::from_sysfs(empty.path(), Some(&[0, 1, 2, 3]));
        assert_eq!(t.num_numa_nodes(), 1);
        assert_eq!(t.cpus.len(), 4);
        assert!(t.is_uniform());
        // Nothing known -> no SMT claims, no ranking, order is plain ascending.
        assert!(!t.is_heterogeneous());
        assert_eq!(t.preferred_cpu_order(), vec![0, 1, 2, 3]);
        assert_eq!(t.distance(0, 0), 10);
        assert_eq!(t.total_weight(), 4 * MAX_WEIGHT as u64);
    }

    #[test]
    fn partial_frequency_information_refuses_to_rank() {
        // One CPU is missing cpuinfo_max_freq (some kernels hide it for
        // offline-capable cores). A partial ranking would sort that core to an
        // arbitrary end, so the machine must be reported uniform instead.
        let s = FakeSys::new();
        s.online("0-3");
        s.cpu(0, 0, 0, "0", Some(5_000_000));
        s.cpu(1, 1, 0, "1", Some(5_000_000));
        s.cpu(2, 2, 0, "2", Some(3_000_000));
        s.cpu(3, 3, 0, "3", None);
        s.node(0, "0-3", "10");
        let t = Topology::from_sysfs(&s.path(), None);
        assert!(t.is_uniform());
        assert_eq!(t.preferred_cpu_order(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn arm_cpu_capacity_is_used_when_cpufreq_is_absent() {
        let s = FakeSys::new();
        s.online("0-3");
        for id in 0..4 {
            s.cpu(id, id, 0, &id.to_string(), None);
        }
        // big.LITTLE: two big cores at capacity 1024, two little at 512.
        s.write("devices/system/cpu/cpu0/cpu_capacity", "1024");
        s.write("devices/system/cpu/cpu1/cpu_capacity", "1024");
        s.write("devices/system/cpu/cpu2/cpu_capacity", "512");
        s.write("devices/system/cpu/cpu3/cpu_capacity", "512");
        s.node(0, "0-3", "10");
        let t = Topology::from_sysfs(&s.path(), None);
        assert!(!t.is_uniform());
        assert_eq!(t.fast_cpus(), vec![0, 1]);
        assert_eq!(t.cpus.iter().find(|c| c.id == 2).unwrap().weight, 500);
    }

    #[test]
    fn no_online_file_falls_back_to_enumerating_cpu_dirs() {
        let s = hybrid();
        std::fs::remove_file(s.path().join("devices/system/cpu/online")).unwrap();
        let t = Topology::from_sysfs(&s.path(), None);
        assert_eq!(t.cpus.len(), 16);
    }

    #[test]
    fn work_proportional_fanout_never_exceeds_work_or_pool() {
        assert_eq!(workers_for(0, 32), 1);
        assert_eq!(workers_for(1, 32), 1);
        assert_eq!(workers_for(3, 32), 3);
        assert_eq!(workers_for(64, 32), 32);
        assert_eq!(workers_for(64, 0), 1);
    }

    #[test]
    fn real_machine_topology_is_self_consistent() {
        // Whatever this box is, the invariants must hold.
        let t = Topology::get();
        assert!(!t.cpus.is_empty());
        assert!(t.num_numa_nodes() >= 1);
        let order = t.preferred_cpu_order();
        assert_eq!(order.len(), t.cpus.len());
        let mut sorted = order.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), order.len(), "preference order has duplicates");
        // Every physical core must precede every SMT sibling.
        let smt: std::collections::HashMap<usize, bool> =
            t.cpus.iter().map(|c| (c.id, c.is_smt_sibling)).collect();
        let first_sibling = order.iter().position(|c| smt[c]);
        if let Some(pos) = first_sibling {
            assert!(order[pos..].iter().all(|c| smt[c]));
        }
        // first_touch must be a no-op that does not corrupt data.
        let mut v = vec![7u64; 4096];
        first_touch(&mut v);
        assert!(v.iter().all(|&x| x == 7));
    }

    /// Test-only mirror of [`preferred_node_for`] that takes an explicit
    /// topology, since the real one reads the cached process topology.
    fn preferred_node_for_topology(t: &Topology, work_index: usize) -> usize {
        let n = t.num_numa_nodes();
        if n <= 1 {
            return 0;
        }
        t.nodes[work_index % n].id
    }

    #[test]
    fn preferred_node_round_robins_across_sockets() {
        let s = two_socket();
        let t = Topology::from_sysfs(&s.path(), None);
        assert_eq!(preferred_node_for_topology(&t, 0), 0);
        assert_eq!(preferred_node_for_topology(&t, 1), 1);
        assert_eq!(preferred_node_for_topology(&t, 2), 0);
        // And on this actual machine it is pinned to 0 (one node).
        assert_eq!(preferred_node_for(0), 0);
        assert_eq!(preferred_node_for(12345), 0);
    }
}
