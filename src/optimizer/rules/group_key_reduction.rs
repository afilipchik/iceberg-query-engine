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

    fn try_reduce(&self, agg: &AggregateNode) -> Option<LogicalPlan> {
        if agg.group_by.len() < 2 || self.table_stats.is_empty() {
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
        self.collect_edges(&agg.input, &mut edges)?;

        // Try every unique int-typed group column as the key: the closure
        // must cover ALL other group columns (e.g. Q18's c_custkey is unique
        // but only determines customer columns; o_orderkey determines all 5).
        let key_pos = (0..group_cols.len()).find(|&pos| {
            let c = group_cols[pos];
            let Some(t) = self.column_table(&c.name) else {
                return false;
            };
            if !self.is_unique_key(t, &c.name) {
                return false;
            }
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
            // FD closure over tables starting from this key's table
            let mut closure: HashSet<String> = HashSet::new();
            closure.insert(t.to_string());
            loop {
                let mut grew = false;
                for (a, b) in &edges {
                    for (from, to) in [(a, b), (b, a)] {
                        let (Some(ft), Some(tt)) = (self.column_table(from), self.column_table(to))
                        else {
                            return false;
                        };
                        if closure.contains(ft)
                            && !closure.contains(tt)
                            && self.is_unique_key(tt, to)
                        {
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
        })?;

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
