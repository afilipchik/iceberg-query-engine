//! Functional-dependency group-key reduction.
//!
//! `GROUP BY k, d1..dn` where `k` is a unique key of its base table (footer
//! NDV == row count, null-free) and every `d_i` is functionally dependent on
//! `k` through the join tree collapses to `GROUP BY k` with the dependent
//! columns carried as `ANY_VALUE(d_i)`, plus a Project restoring the original
//! column order. A single-int group key runs on the raw u64 aggregation path
//! instead of allocating a multi-column GroupKey per row — Q18's final
//! aggregate groups 2M rows by 5 mixed columns keyed by unique o_orderkey,
//! Q10's by 7 columns keyed by unique c_custkey.
//!
//! FD reasoning: k unique in T makes every T column constant per group; an
//! inner-join edge `a = b` where `a` belongs to an FD-closed table and `b` is
//! a unique key of its table extends the closure to b's table. Only
//! Inner/Semi/Anti joins are allowed in the subtree (no null-extension), and
//! any self-join edge (both endpoints resolving to the same base table)
//! disqualifies the plan because column-to-table resolution is by name.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{
    AggregateFunction, AggregateNode, Column, Expr, JoinType, LogicalPlan, PlanSchema, ProjectNode,
    SchemaField,
};
use arrow::datatypes::DataType;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct GroupKeyReduction {
    table_stats: HashMap<String, TableStatistics>,
}

impl GroupKeyReduction {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
        }
    }

    pub fn with_table_statistics(table_stats: HashMap<String, TableStatistics>) -> Self {
        Self { table_stats }
    }
}

impl Default for GroupKeyReduction {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for GroupKeyReduction {
    fn name(&self) -> &str {
        "GroupKeyReduction"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.rewrite(plan)
    }
}

impl GroupKeyReduction {
    fn rewrite(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let children: Vec<Arc<LogicalPlan>> = plan
            .children()
            .iter()
            .map(|c| self.rewrite(c).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let plan = plan.with_new_children(children);

        if let LogicalPlan::Aggregate(agg) = &plan {
            if let Some(rewritten) = self.try_reduce(agg) {
                return Ok(rewritten);
            }
        }
        if let LogicalPlan::Limit(l) = &plan {
            if let Some(rewritten) = self.try_defer_decorations(l) {
                return Ok(rewritten);
            }
        }
        Ok(plan)
    }

    /// Resolve a plain column to its base table via footer statistics.
    /// Only unambiguous single-table name matches count.
    fn column_table(&self, name: &str) -> Option<&str> {
        let lname = name.to_lowercase();
        let mut found: Option<&str> = None;
        for (table, stats) in &self.table_stats {
            if stats.column_stats.contains_key(&lname) {
                if found.is_some() {
                    return None;
                }
                found = Some(table.as_str());
            }
        }
        found
    }

    /// Is `name` a unique, null-free key of its base table?
    fn is_unique_key(&self, table: &str, name: &str) -> bool {
        let Some(stats) = self.table_stats.get(table) else {
            return false;
        };
        let Some(cs) = stats.column_stats.get(&name.to_lowercase()) else {
            return false;
        };
        cs.null_count == Some(0)
            && cs
                .ndv_est
                .map(|ndv| ndv as usize >= stats.row_count)
                .unwrap_or(false)
    }

    /// Collect inner-join equi edges and verify the subtree only contains
    /// join types that cannot null-extend or duplicate decoration columns.
    /// Returns None if a disallowed join type or self-join edge is present.
    fn collect_edges(&self, plan: &LogicalPlan, edges: &mut Vec<(String, String)>) -> Option<()> {
        match plan {
            LogicalPlan::Join(j) => {
                match j.join_type {
                    JoinType::Inner => {
                        for (l, r) in &j.on {
                            let (Expr::Column(lc), Expr::Column(rc)) = (l, r) else {
                                continue;
                            };
                            let lt = self.column_table(&lc.name)?;
                            let rt = self.column_table(&rc.name)?;
                            if lt == rt {
                                return None; // self-join: name resolution unsafe
                            }
                            edges.push((lc.name.to_lowercase(), rc.name.to_lowercase()));
                        }
                        self.collect_edges(&j.left, edges)?;
                        self.collect_edges(&j.right, edges)?;
                    }
                    JoinType::Semi | JoinType::Anti => {
                        // Filters rows only; right side exports no columns.
                        self.collect_edges(&j.left, edges)?;
                    }
                    _ => return None,
                }
                Some(())
            }
            LogicalPlan::Scan(_) => Some(()),
            LogicalPlan::Filter(n) => self.collect_edges(&n.input, edges),
            LogicalPlan::SubqueryAlias(n) => self.collect_edges(&n.input, edges),
            // Project/Aggregate/anything else between joins: bail out —
            // derived columns break by-name FD reasoning.
            _ => None,
        }
    }

    /// Deferred decoration join: `Limit n (Sort s (Projects... (reduced
    /// Aggregate)))` where the sort keys resolve to the group key or real
    /// aggregate outputs becomes
    ///
    ///   Projects... ( Project[restore reduced-agg schema] (
    ///     Join on key:
    ///       Limit n (Sort s' (Aggregate[key, real aggs] (pruned input)))
    ///       decoration tree (FD-table scans re-joined) ))
    ///
    /// so the wide ANY_VALUE decoration columns are decoded and gathered for
    /// only n rows instead of every group (Q10 gathers 1.5M customer comment
    /// strings for a 20-row answer). Row-preserving joins (bare unfiltered
    /// scan, unique join key, no needed columns) are pruned from the
    /// aggregation branch, remapping the key to its edge partner.
    fn try_defer_decorations(&self, limit: &crate::planner::LimitNode) -> Option<LogicalPlan> {
        use crate::planner::{JoinNode, JoinType, LimitNode, SortNode};

        let n = limit.fetch?;
        if limit.skip != 0 || n > 10_000 || self.table_stats.is_empty() {
            return None;
        }
        let LogicalPlan::Sort(sort) = &*limit.input else {
            return None;
        };

        // Walk the Project chain down to the reduced Aggregate
        let mut projects: Vec<&ProjectNode> = Vec::new();
        let mut cur: &LogicalPlan = &sort.input;
        while let LogicalPlan::Project(p) = cur {
            projects.push(p);
            cur = &p.input;
        }
        let LogicalPlan::Aggregate(agg) = cur else {
            return None;
        };
        if agg.group_by.len() != 1 {
            return None;
        }
        let Expr::Column(key_col) = &agg.group_by[0] else {
            return None;
        };

        // Split aggregates into real ones and __fd decorations
        let mut real_aggs: Vec<Expr> = Vec::new();
        let mut fd_cols: Vec<(String, Column)> = Vec::new(); // (alias, source col)
        for a in &agg.aggregates {
            if let Expr::Alias { expr, name } = a {
                if name.starts_with("__fd_") {
                    if let Expr::Aggregate { func, args, .. } = &**expr {
                        if matches!(
                            func,
                            AggregateFunction::AnyValue | AggregateFunction::Arbitrary
                        ) {
                            if let Some(Expr::Column(c)) = args.first() {
                                fd_cols.push((name.clone(), c.clone()));
                                continue;
                            }
                        }
                    }
                    return None;
                }
            }
            real_aggs.push(a.clone());
        }
        if fd_cols.is_empty() || real_aggs.is_empty() {
            return None;
        }

        // Field names of key + real aggs in the reduced agg schema
        let fields = agg.schema.fields();
        let n_real = real_aggs.len();
        let keep_names: HashSet<String> = fields
            .iter()
            .take(1 + n_real)
            .map(|f| f.name.clone())
            .collect();

        // Sort exprs must resolve (through the project chain) to kept fields
        let mut translated: Vec<crate::planner::SortExpr> = Vec::new();
        for se in &sort.order_by {
            let Expr::Column(c) = &se.expr else {
                return None;
            };
            let mut name = c.name.clone();
            for p in &projects {
                let mut next = None;
                for e in &p.exprs {
                    let (out, inner) = match e {
                        Expr::Alias { expr, name: alias } => (alias.clone(), &**expr),
                        Expr::Column(pc) => (pc.name.clone(), e),
                        _ => continue,
                    };
                    if out == name {
                        if let Expr::Column(ic) = inner {
                            next = Some(ic.name.clone());
                        }
                        break;
                    }
                }
                name = next?;
            }
            if !keep_names.contains(&name) {
                return None;
            }
            let mut t = se.clone();
            t.expr = Expr::Column(Column {
                relation: None,
                name,
            });
            translated.push(t);
        }

        // FD closure with parent edges (how each table was reached)
        let mut edges = Vec::new();
        self.collect_edges(&agg.input, &mut edges)?;
        let key_name = key_col.name.to_lowercase();
        let key_table = self.column_table(&key_name)?.to_string();
        let mut parent: HashMap<String, (String, String, String)> = HashMap::new(); // table -> (parent_table_or_key, near_col, far_col)
        let mut closure: HashSet<String> = HashSet::new();
        if self.is_unique_key(&key_table, &key_name) {
            closure.insert(key_table.clone());
        }
        loop {
            let mut grew = false;
            for (a, b) in &edges {
                for (from, to) in [(a, b), (b, a)] {
                    let ft = self.column_table(from)?.to_string();
                    let tt = self.column_table(to)?.to_string();
                    let from_det = *from == key_name || closure.contains(&ft);
                    if from_det && !closure.contains(&tt) && self.is_unique_key(&tt, to) {
                        closure.insert(tt.clone());
                        parent.insert(tt.clone(), (ft, from.clone(), to.clone()));
                        grew = true;
                    }
                }
            }
            if !grew {
                break;
            }
        }

        // Decoration tables + the path tables that connect them to the key
        let mut dec_tables: Vec<String> = Vec::new();
        for (_, c) in &fd_cols {
            let mut t = self.column_table(&c.name)?.to_string();
            loop {
                if !dec_tables.contains(&t) {
                    dec_tables.push(t.clone());
                }
                if t == key_table {
                    break;
                }
                let Some((pt, _, _)) = parent.get(&t) else {
                    break;
                };
                let pt = pt.clone();
                if pt == key_table && !closure.contains(&key_table) {
                    break; // parent is the key column itself, not a table in closure
                }
                if dec_tables.contains(&pt) {
                    break;
                }
                t = pt;
            }
        }

        // Decoration tables are folded one by one onto the TopK output so
        // every decoration join probes only ~n rows (building them into a
        // standalone tree first materialized 1.5M wide customer rows before
        // the 20-key filter could apply — slower than no deferral at all).
        let mut placed: HashSet<String> = HashSet::new();
        // Place tables in dependency order (parents before children)
        let mut ordered = dec_tables.clone();
        ordered.sort_by_key(|t| {
            let mut depth = 0;
            let mut cur = t.clone();
            while let Some((p, _, _)) = parent.get(&cur) {
                depth += 1;
                cur = p.clone();
                if depth > 8 {
                    break;
                }
            }
            depth
        });

        // Aggregation branch: prune row-preserving joins, remapping the key
        let mut needed: HashSet<String> = HashSet::new();
        needed.insert(key_name.clone());
        for a in &real_aggs {
            collect_expr_column_names(a, &mut needed);
        }
        let mut renames: HashMap<String, String> = HashMap::new();
        let mut pruned = self.prune_row_preserving(&agg.input, &needed, &mut renames);
        // Child simplification exposes new prune opportunities (dropping
        // nation turns Join(nation, customer) into a bare customer scan that
        // the parent join can then drop) — iterate to a fixpoint.
        for _ in 0..4 {
            let mut needed2: HashSet<String> = HashSet::new();
            for k in &needed {
                needed2.insert(renames.get(k).cloned().unwrap_or_else(|| k.clone()));
            }
            let next = self.prune_row_preserving(&pruned, &needed2, &mut renames);
            if next == pruned {
                break;
            }
            pruned = next;
        }
        let topk_key_name = renames.get(&key_name).cloned().unwrap_or(key_name.clone());

        // Rebuild the slim aggregate. The output key field gets a unique
        // name: the decoration join would otherwise contain two fields named
        // like the key (TopK output + the decoration table's own column) and
        // by-name resolution in later rules can collapse the join condition
        // into a tautology (observed as cross-join semantics on Q10).
        let key_field = fields[0].clone();
        let unique_key_name = "__topk_key".to_string();
        let mut slim_fields = vec![SchemaField {
            name: unique_key_name.clone(),
            data_type: key_field.data_type.clone(),
            nullable: key_field.nullable,
            relation: None,
        }];
        slim_fields.extend(fields.iter().skip(1).take(n_real).cloned());
        let slim_group_inner = if topk_key_name == key_name {
            agg.group_by[0].clone()
        } else {
            Expr::Column(Column {
                relation: None,
                name: topk_key_name.clone(),
            })
        };
        let slim_group = Expr::Alias {
            expr: Box::new(slim_group_inner),
            name: unique_key_name.clone(),
        };
        let slim_real_aggs: Vec<Expr> = real_aggs
            .iter()
            .map(|a| rename_columns(a, &renames))
            .collect();
        let slim_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(pruned),
            group_by: vec![slim_group],
            aggregates: slim_real_aggs,
            schema: PlanSchema::new(slim_fields),
        });

        // Translated sort exprs may reference the (renamed) key
        let translated: Vec<crate::planner::SortExpr> = translated
            .into_iter()
            .map(|mut se| {
                if let Expr::Column(c) = &se.expr {
                    if c.name.to_lowercase() == key_name {
                        se.expr = Expr::Column(Column {
                            relation: None,
                            name: unique_key_name.clone(),
                        });
                    }
                }
                se
            })
            .collect();

        let topk = LogicalPlan::Limit(LimitNode {
            input: Arc::new(LogicalPlan::Sort(SortNode {
                input: Arc::new(slim_agg),
                order_by: translated,
            })),
            skip: 0,
            fetch: Some(n),
        });

        // Fold each decoration table onto the TopK rows in dependency order
        let mut dec_join = topk;
        for t in &ordered {
            let scan = find_scan(&agg.input, t)?;
            // How this table links to what is already on the left
            let (near, far) = if *t == key_table && closure.contains(&key_table) {
                (unique_key_name.clone(), key_name.clone())
            } else {
                let (pt, near, far) = parent.get(t)?;
                if !placed.contains(pt) && pt.to_lowercase() != key_name && *pt != key_table {
                    return None;
                }
                // The group key itself lives on the TopK side under its
                // unique alias
                let near_name = if near.to_lowercase() == key_name {
                    unique_key_name.clone()
                } else {
                    near.clone()
                };
                (near_name, far.clone())
            };
            let schema = dec_join.schema().merge(&scan.schema);
            dec_join = LogicalPlan::Join(JoinNode {
                left: Arc::new(dec_join),
                right: Arc::new(LogicalPlan::Scan(scan.clone())),
                join_type: JoinType::Inner,
                on: vec![(
                    Expr::Column(Column {
                        relation: None,
                        name: near,
                    }),
                    Expr::Column(Column {
                        relation: None,
                        name: far,
                    }),
                )],
                filter: None,
                schema,
            });
            placed.insert(t.clone());
        }

        // Restore the reduced-agg output schema
        let mut restore_exprs: Vec<Expr> = Vec::with_capacity(fields.len());
        for (i, f) in fields.iter().enumerate() {
            if i == 0 {
                restore_exprs.push(Expr::Alias {
                    expr: Box::new(Expr::Column(Column {
                        relation: None,
                        name: unique_key_name.clone(),
                    })),
                    name: f.name.clone(),
                });
            } else if i <= n_real {
                restore_exprs.push(Expr::Column(Column {
                    relation: None,
                    name: f.name.clone(),
                }));
            } else {
                let (alias, src) = &fd_cols[i - 1 - n_real];
                restore_exprs.push(Expr::Alias {
                    expr: Box::new(Expr::Column(src.clone())),
                    name: alias.clone(),
                });
            }
        }
        let restored = LogicalPlan::Project(ProjectNode {
            input: Arc::new(dec_join),
            exprs: restore_exprs,
            schema: agg.schema.clone(),
        });

        if std::env::var("GK_DEBUG").is_ok() {
            eprintln!(
                "[gk] defer fired: key={} topk_key={} dec_tables={:?} renames={:?}",
                key_name, topk_key_name, ordered, renames
            );
        }
        // Re-wrap the original Project chain, Sort, and Limit
        let mut rebuilt = restored;
        for p in projects.iter().rev() {
            rebuilt = LogicalPlan::Project(ProjectNode {
                input: Arc::new(rebuilt),
                exprs: p.exprs.clone(),
                schema: p.schema.clone(),
            });
        }
        let rebuilt = LogicalPlan::Sort(SortNode {
            input: Arc::new(rebuilt),
            order_by: sort.order_by.clone(),
        });
        Some(LogicalPlan::Limit(LimitNode {
            input: Arc::new(rebuilt),
            skip: 0,
            fetch: Some(n),
        }))
    }

    /// Remove Inner joins against bare, unfiltered scans whose unique join
    /// key matches no needed column except possibly the key itself (which is
    /// then remapped to its edge partner). Rebuilds join schemas bottom-up.
    fn prune_row_preserving(
        &self,
        plan: &LogicalPlan,
        needed: &HashSet<String>,
        renames: &mut HashMap<String, String>,
    ) -> LogicalPlan {
        let LogicalPlan::Join(j) = plan else {
            return plan.clone();
        };
        if j.join_type != crate::planner::JoinType::Inner || j.filter.is_some() || j.on.len() != 1 {
            return plan.clone();
        }
        let (l_expr, r_expr) = &j.on[0];
        let (Expr::Column(lc), Expr::Column(rc)) = (l_expr, r_expr) else {
            return plan.clone();
        };

        // Try to drop either side
        for (side, other, side_col, other_col) in
            [(&j.left, &j.right, lc, rc), (&j.right, &j.left, rc, lc)]
        {
            if let LogicalPlan::Scan(scan) = &**side {
                if scan.filter.is_none() {
                    let t = scan.table_name.clone();
                    let key_unique = self
                        .column_table(&side_col.name)
                        .map(|ct| ct == t && self.is_unique_key(&t, &side_col.name))
                        .unwrap_or(false);
                    // Row preservation additionally requires referential
                    // integrity, which footer stats can prove only when the
                    // unique key is DENSE and CONTAINS the other side's
                    // range. TPC-H's o_custkey spans 1.5x the customer key
                    // range (dangling FKs by spec) — dropping that join let
                    // nonexistent customers into Q10's top-20.
                    let key_complete = key_unique
                        && (|| {
                            let far = self
                                .table_stats
                                .get(&t)?
                                .column_stats
                                .get(&side_col.name.to_lowercase())?;
                            let (fmin, fmax) = (far.min_i64?, far.max_i64?);
                            if far.ndv_est? as i64 != fmax - fmin + 1 {
                                return None; // not dense
                            }
                            let near_table = self.column_table(&other_col.name)?;
                            let near = self
                                .table_stats
                                .get(near_table)?
                                .column_stats
                                .get(&other_col.name.to_lowercase())?;
                            let (nmin, nmax) = (near.min_i64?, near.max_i64?);
                            (nmin >= fmin && nmax <= fmax).then_some(())
                        })()
                        .is_some();
                    if key_complete {
                        let side_names: HashSet<String> = scan
                            .schema
                            .fields()
                            .iter()
                            .map(|f| f.name.to_lowercase())
                            .collect();
                        let blocking: Vec<&String> = needed
                            .iter()
                            .filter(|n| {
                                side_names.contains(*n)
                                    && n.as_str() != side_col.name.to_lowercase()
                            })
                            .collect();
                        if blocking.is_empty() {
                            // Drop this side; remap its key to the partner
                            if needed.contains(&side_col.name.to_lowercase()) {
                                renames.insert(
                                    side_col.name.to_lowercase(),
                                    other_col.name.to_lowercase(),
                                );
                            }
                            let mut sub_needed = needed.clone();
                            sub_needed.remove(&side_col.name.to_lowercase());
                            sub_needed.insert(other_col.name.to_lowercase());
                            return self.prune_row_preserving(other, &sub_needed, renames);
                        }
                    }
                }
            }
        }

        // Keep the join; recurse into both sides with join keys added
        let mut sub_needed = needed.clone();
        sub_needed.insert(lc.name.to_lowercase());
        sub_needed.insert(rc.name.to_lowercase());
        let left = self.prune_row_preserving(&j.left, &sub_needed, renames);
        let right = self.prune_row_preserving(&j.right, &sub_needed, renames);
        let schema = left.schema().merge(&right.schema());
        LogicalPlan::Join(crate::planner::JoinNode {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type: j.join_type,
            on: j.on.clone(),
            filter: j.filter.clone(),
            schema,
        })
    }

    fn try_reduce(&self, agg: &AggregateNode) -> Option<LogicalPlan> {
        let dbg = std::env::var("GK_DEBUG").is_ok();
        if agg.group_by.len() < 2 || self.table_stats.is_empty() {
            if dbg {
                eprintln!(
                    "[gk] skip: groups={} stats={}",
                    agg.group_by.len(),
                    self.table_stats.len()
                );
            }
            return None;
        }
        // All group exprs must be plain columns
        let group_cols: Vec<&Column> = agg
            .group_by
            .iter()
            .map(|e| match e {
                Expr::Column(c) => Some(c),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;

        // Join edges + join-type safety (independent of key choice)
        let mut edges = Vec::new();
        if self.collect_edges(&agg.input, &mut edges).is_none() {
            if dbg {
                eprintln!("[gk] skip: collect_edges bailed");
            }
            return None;
        }
        if dbg {
            eprintln!("[gk] edges={:?}", edges);
        }

        // Try every int-typed group column as the key: its FD closure must
        // cover ALL other group columns (e.g. Q18's c_custkey is unique but
        // only determines customer columns; o_orderkey determines all 5).
        // The key itself need NOT be unique in its own table: equal key
        // values still select the same single row of any table reached
        // through an edge FROM THE KEY COLUMN onto a unique far-side key
        // (Q03 groups by non-unique l_orderkey; o_orderdate is FD through
        // l_orderkey = o_orderkey with o_orderkey unique). The key's own
        // table only enters the closure when the key is unique there.
        let key_pos = (0..group_cols.len()).find(|&pos| {
            let c = group_cols[pos];
            let Some(t) = self.column_table(&c.name) else {
                return false;
            };
            if !matches!(
                agg.schema
                    .fields()
                    .iter()
                    .find(|f| f.name == c.name)
                    .map(|f| &f.data_type),
                Some(DataType::Int64) | Some(DataType::Int32) | Some(DataType::Date32)
            ) {
                return false;
            }
            let key_name = c.name.to_lowercase();
            let mut closure: HashSet<String> = HashSet::new();
            if self.is_unique_key(t, &c.name) {
                closure.insert(t.to_string());
            }
            loop {
                let mut grew = false;
                for (a, b) in &edges {
                    for (from, to) in [(a, b), (b, a)] {
                        let (Some(ft), Some(tt)) = (self.column_table(from), self.column_table(to))
                        else {
                            return false;
                        };
                        let from_determined = *from == key_name || closure.contains(ft);
                        if from_determined && !closure.contains(tt) && self.is_unique_key(tt, to) {
                            closure.insert(tt.to_string());
                            grew = true;
                        }
                    }
                }
                if !grew {
                    break;
                }
            }
            group_cols.iter().enumerate().all(|(i, gc)| {
                i == pos
                    || self
                        .column_table(&gc.name)
                        .map(|gt| closure.contains(gt))
                        .unwrap_or(false)
            })
        });
        if dbg {
            eprintln!("[gk] key_pos={:?}", key_pos);
        }
        let key_pos = key_pos?;

        // Build the reduced aggregate: group by key only, decorations as
        // ANY_VALUE. Aggregate output layout is [group fields..., agg fields...]
        // so the new schema is [key, orig aggs..., __fd_i...].
        let n_groups = agg.group_by.len();
        let orig_fields = agg.schema.fields();
        let key_field = orig_fields[key_pos].clone();

        let mut new_aggregates = agg.aggregates.clone();
        let mut new_fields = vec![key_field];
        new_fields.extend_from_slice(&orig_fields[n_groups..]);
        let mut fd_names: HashMap<usize, String> = HashMap::new();
        for (i, c) in group_cols.iter().enumerate() {
            if i == key_pos {
                continue;
            }
            let alias = format!("__fd_{}", i);
            new_aggregates.push(Expr::Alias {
                expr: Box::new(Expr::Aggregate {
                    func: AggregateFunction::AnyValue,
                    args: vec![Expr::Column((*c).clone())],
                    distinct: false,
                }),
                name: alias.clone(),
            });
            new_fields.push(SchemaField {
                name: alias.clone(),
                data_type: orig_fields[i].data_type.clone(),
                nullable: true,
                relation: None,
            });
            fd_names.insert(i, alias);
        }
        let new_agg = LogicalPlan::Aggregate(AggregateNode {
            input: agg.input.clone(),
            group_by: vec![agg.group_by[key_pos].clone()],
            aggregates: new_aggregates,
            schema: PlanSchema::new(new_fields),
        });

        // Project restores the original column order and schema
        let mut project_exprs = Vec::with_capacity(orig_fields.len());
        for (i, f) in orig_fields.iter().enumerate() {
            if i < n_groups {
                if i == key_pos {
                    project_exprs.push(agg.group_by[key_pos].clone());
                } else {
                    project_exprs.push(Expr::Alias {
                        expr: Box::new(Expr::Column(Column {
                            relation: None,
                            name: fd_names[&i].clone(),
                        })),
                        name: f.name.clone(),
                    });
                }
            } else {
                project_exprs.push(Expr::Column(Column {
                    relation: f.relation.clone(),
                    name: f.name.clone(),
                }));
            }
        }
        Some(LogicalPlan::Project(ProjectNode {
            input: Arc::new(new_agg),
            exprs: project_exprs,
            schema: agg.schema.clone(),
        }))
    }
}

/// Find the Scan node for `table` anywhere in the plan.
fn find_scan(plan: &LogicalPlan, table: &str) -> Option<crate::planner::ScanNode> {
    if let LogicalPlan::Scan(s) = plan {
        if s.table_name == table {
            return Some(s.clone());
        }
    }
    for child in plan.children() {
        if let Some(s) = find_scan(child, table) {
            return Some(s);
        }
    }
    None
}

/// Collect lowercase column names referenced by an expression.
fn collect_expr_column_names(e: &Expr, out: &mut HashSet<String>) {
    match e {
        Expr::Column(c) => {
            out.insert(c.name.to_lowercase());
        }
        Expr::Alias { expr, .. } => collect_expr_column_names(expr, out),
        Expr::Aggregate { args, .. } => {
            for a in args {
                collect_expr_column_names(a, out);
            }
        }
        Expr::BinaryExpr { left, right, .. } => {
            collect_expr_column_names(left, out);
            collect_expr_column_names(right, out);
        }
        Expr::UnaryExpr { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expr_column_names(expr, out)
        }
        Expr::ScalarFunc { args, .. } => {
            for a in args {
                collect_expr_column_names(a, out);
            }
        }
        _ => {}
    }
}

/// Rewrite column references according to a lowercase rename map.
fn rename_columns(e: &Expr, renames: &HashMap<String, String>) -> Expr {
    if renames.is_empty() {
        return e.clone();
    }
    match e {
        Expr::Column(c) => match renames.get(&c.name.to_lowercase()) {
            Some(new_name) => Expr::Column(Column {
                relation: None,
                name: new_name.clone(),
            }),
            None => e.clone(),
        },
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(rename_columns(expr, renames)),
            name: name.clone(),
        },
        Expr::Aggregate {
            func,
            args,
            distinct,
        } => Expr::Aggregate {
            func: *func,
            args: args.iter().map(|a| rename_columns(a, renames)).collect(),
            distinct: *distinct,
        },
        Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
            left: Box::new(rename_columns(left, renames)),
            op: *op,
            right: Box::new(rename_columns(right, renames)),
        },
        Expr::UnaryExpr { op, expr } => Expr::UnaryExpr {
            op: *op,
            expr: Box::new(rename_columns(expr, renames)),
        },
        Expr::Cast { expr, data_type } => Expr::Cast {
            expr: Box::new(rename_columns(expr, renames)),
            data_type: data_type.clone(),
        },
        Expr::ScalarFunc { func, args } => Expr::ScalarFunc {
            func: func.clone(),
            args: args.iter().map(|a| rename_columns(a, renames)).collect(),
        },
        _ => e.clone(),
    }
}
