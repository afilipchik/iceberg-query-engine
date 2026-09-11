//! Uniqueness is an operator property, not an NDV estimate.
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use query_engine::optimizer::{EagerAggregation, GroupKeyReduction, OptimizerRule};
use query_engine::physical::operators::{ColumnStatistics, TableStatistics};
use query_engine::planner::{
    AggregateFunction, AggregateNode, Column, DistinctNode, Expr, JoinNode, JoinType, LogicalPlan,
    PlanSchema, ScanNode, SchemaField,
};
use query_engine::ExecutionContext;
use std::collections::HashMap;
use std::sync::Arc;

fn col(name: &str) -> Expr {
    Expr::Column(Column::new(name))
}
fn scan(name: &str, columns: &[&str]) -> LogicalPlan {
    LogicalPlan::Scan(ScanNode {
        table_name: name.into(),
        schema: PlanSchema::new(
            columns
                .iter()
                .map(|name| SchemaField::new(*name, DataType::Int64))
                .collect(),
        ),
        projection: None,
        filter: None,
    })
}
fn aggregate(
    input: LogicalPlan,
    groups: &[&str],
    value: &str,
    function: AggregateFunction,
) -> LogicalPlan {
    let group_by = groups.iter().map(|name| col(name)).collect::<Vec<_>>();
    let aggregates = vec![Expr::Alias {
        expr: Box::new(Expr::Aggregate {
            func: function,
            args: vec![col(value)],
            distinct: false,
        }),
        name: if groups.contains(&"total") {
            "answer".into()
        } else {
            "total".into()
        },
    }];
    let schema = PlanSchema::new(
        group_by
            .iter()
            .chain(&aggregates)
            .map(|expr| expr.to_field(&input.schema()).unwrap())
            .collect(),
    );
    LogicalPlan::Aggregate(AggregateNode {
        input: Arc::new(input),
        group_by,
        aggregates,
        schema,
    })
}
fn misleading_stats() -> HashMap<String, TableStatistics> {
    HashMap::from([(
        "t".into(),
        TableStatistics {
            row_count: 3,
            total_byte_size: 48,
            column_stats: HashMap::from([
                (
                    "k".into(),
                    ColumnStatistics {
                        min_i64: Some(1),
                        max_i64: Some(3),
                        null_count: Some(0),
                        ndv_est: Some(3),
                        ..Default::default()
                    },
                ),
                ("d".into(), ColumnStatistics::default()),
            ]),
        },
    )])
}
fn group_count(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Aggregate(node) => node.group_by.len(),
        _ => plan
            .children()
            .iter()
            .find_map(|child| {
                let count = group_count(child);
                (count > 0).then_some(count)
            })
            .unwrap_or(0),
    }
}

#[test]
fn estimated_unique_scan_does_not_reduce_groups_or_defer_decorations() {
    let input = aggregate(
        scan("t", &["k", "d"]),
        &["k", "d"],
        "k",
        AggregateFunction::Count,
    );
    let rule = GroupKeyReduction::with_table_statistics(misleading_stats());
    assert_eq!(rule.optimize(&input).unwrap(), input);
    let limited = input.sort(vec![]).limit(0, Some(1));
    assert_eq!(rule.optimize(&limited).unwrap(), limited);
}

#[test]
fn aggregate_key_survives_identity_rename_but_not_dropped_key() {
    let inner = aggregate(scan("t", &["k", "d"]), &["k"], "d", AggregateFunction::Max);
    let projected = inner
        .clone()
        .project(vec![
            Expr::Alias {
                expr: Box::new(col("k")),
                name: "renamed".into(),
            },
            col("total"),
        ])
        .unwrap();
    let outer = aggregate(
        projected,
        &["renamed", "total"],
        "total",
        AggregateFunction::Count,
    );
    let reduced = GroupKeyReduction::new().optimize(&outer).unwrap();
    assert_eq!(group_count(&reduced), 1);
    assert_eq!(reduced.schema(), outer.schema());
    let dropped = inner
        .project(vec![
            col("total"),
            Expr::Alias {
                expr: Box::new(col("total")),
                name: "copy".into(),
            },
        ])
        .unwrap();
    let outer = aggregate(
        dropped,
        &["total", "copy"],
        "copy",
        AggregateFunction::Count,
    );
    assert_eq!(GroupKeyReduction::new().optimize(&outer).unwrap(), outer);
}

#[test]
fn distinct_and_composite_keys_do_not_prove_partial_uniqueness() {
    let distinct = LogicalPlan::Distinct(DistinctNode {
        input: Arc::new(scan("t", &["k", "d"])),
    });
    let outer = aggregate(distinct, &["k", "d"], "k", AggregateFunction::Count);
    assert_eq!(GroupKeyReduction::new().optimize(&outer).unwrap(), outer);
    let inner = aggregate(
        scan("t", &["k", "d"]),
        &["k", "d"],
        "k",
        AggregateFunction::Count,
    );
    let outer = aggregate(inner, &["k", "d", "total"], "k", AggregateFunction::Count);
    assert_eq!(
        group_count(&GroupKeyReduction::new().optimize(&outer).unwrap()),
        2
    );
}

#[test]
fn left_count_requires_proof_on_exact_left_subtree() {
    for structural in [false, true] {
        let left = if structural {
            LogicalPlan::Distinct(DistinctNode {
                input: Arc::new(scan("t", &["k"])),
            })
        } else {
            scan("t", &["k"])
        };
        let right = scan("r", &["rk"]);
        let schema = left.schema().merge(&right.schema());
        let join = LogicalPlan::Join(JoinNode {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type: JoinType::Left,
            on: vec![(col("k"), col("rk"))],
            filter: None,
            schema,
        });
        let plan = aggregate(join, &["k"], "rk", AggregateFunction::Count);
        let result = EagerAggregation::with_table_statistics(misleading_stats())
            .optimize(&plan)
            .unwrap();
        assert_eq!(result != plan, structural);
    }
}

#[tokio::test]
async fn duplicate_sparse_keys_and_dangling_joins_preserve_exact_results() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("duplicate.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("d", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for workers in [1, 2, 4] {
        let mut ctx = ExecutionContext::new().with_parallel_partitions(workers);
        ctx.register_parquet("t", &path).unwrap();
        let right = RecordBatch::try_from_iter(vec![(
            "rk",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        ctx.register_table(
            "r",
            right.schema(),
            vec![right.slice(0, 1), right.slice(1, 2)],
        );
        let result = ctx
            .sql("SELECT k, d, COUNT(*) AS n FROM t GROUP BY k, d ORDER BY k, d")
            .await
            .unwrap();
        let rows = result
            .batches
            .iter()
            .flat_map(|batch| {
                let k = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let d = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let n = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|i| (k.value(i), d.value(i).to_string(), n.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![(1, "a".into(), 1), (1, "b".into(), 1), (3, "c".into(), 1)]
        );
        for (sql, expected) in [
            (
                "SELECT t.k, COUNT(r.rk) FROM t LEFT JOIN r ON t.k=r.rk GROUP BY t.k ORDER BY t.k",
                vec![(1, 2), (3, 1)],
            ),
            (
                "SELECT r.rk, COUNT(*) FROM r JOIN t ON r.rk=t.k GROUP BY r.rk ORDER BY r.rk",
                vec![(1, 2), (3, 1)],
            ),
        ] {
            let result = ctx.sql(sql).await.unwrap();
            let rows = result
                .batches
                .iter()
                .flat_map(|batch| {
                    let k = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let n = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (0..batch.num_rows())
                        .map(|i| (k.value(i), n.value(i)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(rows, expected, "{sql}; workers={workers}");
        }
    }
}

#[tokio::test]
async fn structural_reduction_preserves_null_groups_and_empty_inputs() {
    for workers in [1, 2, 4] {
        let mut ctx = ExecutionContext::new().with_parallel_partitions(workers);
        let batch = RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![Some(1), Some(1), None, None]))
                    as arrow::array::ArrayRef,
            ),
            (
                "v",
                Arc::new(Int64Array::from(vec![Some(4), None, Some(7), None]))
                    as arrow::array::ArrayRef,
            ),
        ])
        .unwrap();
        ctx.register_table(
            "t",
            batch.schema(),
            vec![batch.slice(0, 1), batch.slice(1, 3)],
        );
        for (predicate, expected) in [
            ("1=1", vec![(Some(1), Some(4), 1), (None, Some(7), 1)]),
            ("1=0", vec![]),
        ] {
            let sql = format!("SELECT k, m, COUNT(*) FROM (SELECT k, MAX(v) AS m FROM t WHERE {predicate} GROUP BY k) s GROUP BY k, m ORDER BY k NULLS LAST");
            let result = ctx.sql(&sql).await.unwrap();
            let rows = result
                .batches
                .iter()
                .flat_map(|batch| {
                    use arrow::array::Array;
                    let k = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let m = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let n = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (0..batch.num_rows())
                        .map(|i| {
                            (
                                (!k.is_null(i)).then(|| k.value(i)),
                                (!m.is_null(i)).then(|| m.value(i)),
                                n.value(i),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(rows, expected, "{sql}; workers={workers}");
        }
    }
}

#[test]
fn eager_sum_preserves_exact_arithmetic_and_nullable_additive_terms() {
    use query_engine::planner::BinaryOp;
    for (data_type, additive, should_rewrite) in [
        (DataType::Decimal128(15, 2), false, false),
        (DataType::Int64, false, false),
        (DataType::Float64, true, false),
        // Even non-additive floating products cannot move after SUM:
        // overflowing the partial sum can turn zero products into NaN.
        (DataType::Float64, false, false),
    ] {
        let left = LogicalPlan::Scan(ScanNode {
            table_name: "t".into(),
            schema: PlanSchema::new(vec![
                SchemaField::new("k", DataType::Int64),
                SchemaField::new("rv", data_type.clone()),
            ]),
            projection: None,
            filter: None,
        });
        let right = LogicalPlan::Scan(ScanNode {
            table_name: "s".into(),
            schema: PlanSchema::new(vec![
                SchemaField::new("sk", DataType::Int64),
                SchemaField::new("sv", data_type.clone()),
            ]),
            projection: None,
            filter: None,
        });
        let schema = left.schema().merge(&right.schema());
        let join = LogicalPlan::Join(JoinNode {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type: JoinType::Inner,
            on: vec![(col("k"), col("sk"))],
            filter: None,
            schema,
        });
        let product = Expr::BinaryExpr {
            left: Box::new(col("rv")),
            op: BinaryOp::Multiply,
            right: Box::new(col("sv")),
        };
        let arg = if additive {
            Expr::BinaryExpr {
                left: Box::new(product),
                op: BinaryOp::Add,
                right: Box::new(col("rv")),
            }
        } else {
            product
        };
        let sum = Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![arg],
            distinct: false,
        };
        let schema = PlanSchema::new(vec![
            col("sk").to_field(&join.schema()).unwrap(),
            sum.to_field(&join.schema()).unwrap(),
        ]);
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(join),
            group_by: vec![col("sk")],
            aggregates: vec![sum],
            schema,
        });
        let stats = HashMap::from([(
            "t".into(),
            TableStatistics {
                row_count: 100,
                total_byte_size: 1600,
                column_stats: HashMap::from([
                    (
                        "k".into(),
                        ColumnStatistics {
                            ndv_est: Some(1),
                            null_count: Some(0),
                            ..Default::default()
                        },
                    ),
                    (
                        "rv".into(),
                        ColumnStatistics {
                            null_count: Some(0),
                            ..Default::default()
                        },
                    ),
                ]),
            },
        )]);
        let rewritten = EagerAggregation::with_table_statistics(stats)
            .optimize(&plan)
            .unwrap();
        assert_eq!(
            rewritten != plan,
            should_rewrite,
            "{data_type:?}; additive={additive}"
        );
    }
}

#[tokio::test]
async fn eager_sum_keeps_finite_zero_products_before_an_overflowing_partial_sum() {
    use arrow::array::Float64Array;
    let mut context = ExecutionContext::new();
    let values_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    context.register_table(
        "measurements",
        values_schema.clone(),
        vec![RecordBatch::try_new(
            values_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Float64Array::from(vec![f64::MAX, f64::MAX])),
            ],
        )
        .unwrap()],
    );
    let weights_schema = Arc::new(Schema::new(vec![
        Field::new("lookup_key", DataType::Int64, false),
        Field::new("marker", DataType::Int64, false),
        Field::new("weight", DataType::Float64, false),
    ]));
    context.register_table(
        "weights",
        weights_schema.clone(),
        vec![RecordBatch::try_new(
            weights_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap()],
    );
    let result = context.sql("SELECT marker, SUM(value * weight) AS answer FROM measurements JOIN weights ON key = lookup_key GROUP BY marker").await.unwrap();
    assert_eq!(result.row_count, 1);
    let batch = result
        .batches
        .iter()
        .find(|batch| batch.num_rows() != 0)
        .unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.0
    );
}

#[tokio::test]
async fn left_count_rewrite_executes_with_qualified_internal_name_collision() {
    for managed in [false, true] {
        for distinct in [false, true] {
            for empty in [None, Some("left_source"), Some("right_source")] {
                left_count_binding_case(managed, distinct, empty, false).await;
            }
        }
    }
}

#[tokio::test]
async fn left_count_preaggregation_retains_duplicate_left_multiplicity() {
    for managed in [false, true] {
        for distinct in [false, true] {
            for empty in [None, Some("left_source"), Some("right_source")] {
                left_count_binding_case(managed, distinct, empty, true).await;
            }
        }
    }
}

async fn left_count_binding_case(managed: bool, distinct: bool, empty: Option<&str>, costed: bool) {
    for derived in [false, true] {
        left_count_binding_plan_case(managed, distinct, empty, costed, derived).await;
    }
}

async fn left_count_binding_plan_case(
    managed: bool,
    distinct: bool,
    empty: Option<&str>,
    costed: bool,
    derived: bool,
) {
    use arrow::array::Array;
    use futures::TryStreamExt;
    use query_engine::physical::{MemoryTable, PhysicalPlanner};
    let mut context = ExecutionContext::new();
    let directory = tempfile::tempdir().unwrap();
    let mut planner = if managed {
        let budget = 64 * 1024 * 1024;
        PhysicalPlanner::with_config(
            Arc::new(query_engine::execution::MemoryPool::new(budget)),
            query_engine::ExecutionConfig::new()
                .with_memory_limit(budget)
                .with_spill_path(directory.path().to_path_buf()),
        )
    } else {
        PhysicalPlanner::new()
    };
    for (name, names, columns) in [
        (
            "left_source",
            vec!["__ea_cnt"],
            vec![vec![Some(1), Some(1), Some(2), None]],
        ),
        (
            "right_source",
            vec!["fk", "v"],
            vec![
                vec![Some(1), Some(1), Some(2), None],
                vec![Some(10), None, None, Some(99)],
            ],
        ),
    ] {
        let schema = Arc::new(Schema::new(
            names
                .into_iter()
                .map(|n| Field::new(n, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            columns
                .into_iter()
                .map(|v| Arc::new(Int64Array::from(v)) as arrow::array::ArrayRef)
                .collect(),
        )
        .unwrap();
        let batches = if empty == Some(name) {
            vec![batch.slice(0, 0)]
        } else {
            vec![batch.slice(0, 0), batch.slice(0, 2), batch.slice(2, 2)]
        };
        context.register_table(name, schema.clone(), batches.clone());
        planner.register_table(name, Arc::new(MemoryTable::new(schema, batches)));
    }
    let unique = if distinct { "DISTINCT" } else { "" };
    let (right, key, value) = if derived {
        (
            "(SELECT v AS __ea_cnt_, fk AS match_key FROM right_source WHERE v IS NULL OR v >= 0)",
            "match_key",
            "__ea_cnt_",
        )
    } else {
        ("right_source", "fk", "v")
    };
    let original = context.logical_plan(&format!("SELECT l.__ea_cnt, COUNT(r.{value}) AS n FROM (SELECT {unique} __ea_cnt FROM left_source) AS l LEFT JOIN {right} AS r ON l.__ea_cnt = r.{key} GROUP BY l.__ea_cnt")).unwrap();
    let rule = if costed {
        EagerAggregation::with_table_statistics(HashMap::from([(
            "right_source".into(),
            TableStatistics {
                row_count: 4,
                total_byte_size: 64,
                column_stats: HashMap::from([(
                    "fk".into(),
                    ColumnStatistics {
                        ndv_est: Some(2),
                        ..Default::default()
                    },
                )]),
            },
        )]))
    } else {
        EagerAggregation::new()
    };
    let rewritten = rule.optimize(&original).unwrap();
    assert_eq!(
        rewritten != original,
        distinct || costed,
        "rewrite requires structural uniqueness"
    );
    assert_eq!(rule.optimize(&rewritten).unwrap(), rewritten);
    assert_eq!(rewritten.schema(), original.schema());
    for plan in [&original, &rewritten] {
        let physical = planner.create_physical_plan(plan).unwrap();
        let mut rows = Vec::new();
        for partition in 0..physical.output_partitions() {
            let batches = physical
                .execute(partition)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            for batch in batches {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    assert!(!counts.is_null(i));
                    rows.push(((!keys.is_null(i)).then(|| keys.value(i)), counts.value(i)));
                }
            }
        }
        rows.sort();
        let expected = if empty == Some("left_source") {
            vec![]
        } else {
            let count = if empty == Some("right_source") {
                0
            } else if distinct {
                1
            } else {
                2
            };
            vec![(None, 0), (Some(1), count), (Some(2), 0)]
        };
        assert_eq!(
            rows, expected,
            "managed={managed}, distinct={distinct}, empty={empty:?}"
        );
    }
}

#[test]
fn left_count_cost_estimates_never_erase_final_duplicate_reduction() {
    let left = scan("t", &["k"]);
    let right = scan("r", &["rk"]);
    let schema = left.schema().merge(&right.schema());
    let plan = aggregate(
        LogicalPlan::Join(JoinNode {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type: JoinType::Left,
            on: vec![(col("k"), col("rk"))],
            filter: None,
            schema,
        }),
        &["k"],
        "rk",
        AggregateFunction::Count,
    );
    for ndv in [None, Some(0), Some(1), Some(2), Some(3), Some(4), Some(100)] {
        let mut stats = misleading_stats();
        stats.insert(
            "r".into(),
            TableStatistics {
                row_count: 4,
                total_byte_size: 32,
                column_stats: HashMap::from([(
                    "rk".into(),
                    ColumnStatistics {
                        ndv_est: ndv,
                        ..Default::default()
                    },
                )]),
            },
        );
        let rule = EagerAggregation::with_table_statistics(stats);
        let result = rule.optimize(&plan).unwrap();
        assert_eq!(result != plan, ndv.is_some_and(|n| n <= 2));
        assert!(
            matches!(result, LogicalPlan::Aggregate(_)),
            "statistics cannot remove final duplicate reduction"
        );
        assert_eq!(rule.optimize(&result).unwrap(), result);
    }
}

#[tokio::test]
async fn correlated_aggregate_reduction_preserves_inner_multiplicity() {
    for empty in [None, Some("outer_source"), Some("inner_source")] {
        correlated_reduction_oracle(empty).await;
    }
}

async fn correlated_reduction_oracle(empty: Option<&str>) {
    use arrow::array::Array;
    let mut context = ExecutionContext::new();
    for (name, columns) in [
        (
            "outer_source",
            vec![
                ("k", vec![Some(1), Some(1), Some(2), None, Some(3)]),
                ("flag", vec![Some(1), Some(1), Some(1), Some(1), Some(0)]),
                (
                    "sum_threshold",
                    vec![Some(7), Some(7), Some(6), Some(0), Some(0)],
                ),
                (
                    "count_threshold",
                    vec![Some(2), Some(2), Some(0), Some(0), Some(0)],
                ),
            ],
        ),
        (
            "inner_source",
            vec![
                (
                    "fk",
                    vec![Some(1), Some(1), Some(1), Some(2), None, Some(3)],
                ),
                (
                    "v",
                    vec![Some(2), Some(3), None, Some(7), Some(99), Some(1)],
                ),
            ],
        ),
    ] {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, _)| Field::new(*name, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            columns
                .into_iter()
                .map(|(_, values)| Arc::new(Int64Array::from(values)) as arrow::array::ArrayRef)
                .collect(),
        )
        .unwrap();
        let batches = vec![
            batch.slice(0, 0),
            batch.slice(0, 2),
            batch.slice(2, batch.num_rows() - 2),
        ];
        let batches = if empty == Some(name) {
            vec![batches[0].clone()]
        } else {
            batches
        };
        context.register_table(name, schema, batches);
    }
    for (function, threshold) in [("SUM", "sum_threshold"), ("COUNT", "count_threshold")] {
        for aliased in [false, true] {
            let sql = if aliased {
                format!("SELECT o.k FROM (SELECT * FROM outer_source WHERE flag=1) o WHERE o.{threshold} < (SELECT {function}(i.v) FROM inner_source i WHERE i.fk=o.k)")
            } else {
                format!("SELECT k FROM outer_source WHERE flag=1 AND {threshold} < (SELECT {function}(v) FROM inner_source WHERE fk=k)")
            };
            let plan = context.optimized_plan(&sql).unwrap().to_string();
            if !aliased && empty.is_none() {
                assert!(
                    !plan.contains("scalar subquery"),
                    "must exercise decorrelation: {plan}"
                );
                assert!(
                    plan.contains("SEMI Join"),
                    "must preserve membership semantics: {plan}"
                );
            }
            let result = context.sql(&sql).await.unwrap();
            let mut actual = Vec::new();
            for batch in result.batches {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..keys.len() {
                    assert!(!keys.is_null(i));
                    actual.push(keys.value(i));
                }
            }
            actual.sort();
            let expected = if empty.is_none() { vec![2] } else { vec![] };
            assert_eq!(
                actual, expected,
                "{function}, empty={empty:?}, aliased={aliased}: {plan}"
            );
        }
    }
}

#[tokio::test]
async fn correlated_scalar_aggregate_preserves_empty_result() {
    use arrow::array::Array;
    let mut context = ExecutionContext::new();
    context.register_batch(
        "outer_empty_probe",
        RecordBatch::try_from_iter([
            (
                "k",
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None]))
                    as arrow::array::ArrayRef,
            ),
            (
                "flag",
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])) as arrow::array::ArrayRef,
            ),
            (
                "__scalar_result",
                Arc::new(Int64Array::from(vec![77; 4])) as arrow::array::ArrayRef,
            ),
            (
                "__scalar_present",
                Arc::new(Int64Array::from(vec![88; 4])) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap(),
    );
    context.register_batch(
        "inner_empty_probe",
        RecordBatch::try_from_iter([
            (
                "fk",
                Arc::new(Int64Array::from(vec![1])) as arrow::array::ArrayRef,
            ),
            (
                "v",
                Arc::new(Int64Array::from(vec![5])) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap(),
    );
    let mut results = Vec::new();
    for (expression, empty_value) in [
        ("COUNT(v)", 0),
        ("COUNT(v)+1", 1),
        ("COALESCE(SUM(v),7)", 7),
        ("NULLIF(COUNT(v),1)", 0),
    ] {
        let sql = format!("SELECT k FROM outer_empty_probe WHERE flag=1 AND {empty_value} = (SELECT {expression} FROM inner_empty_probe WHERE fk=k)");
        let plan = context.optimized_plan(&sql).unwrap().to_string();
        assert!(
            !plan.contains("scalar subquery"),
            "must exercise decorrelation: {plan}"
        );
        let result = context.sql(&sql).await.unwrap();
        let mut keys = Vec::new();
        for batch in result.batches {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            keys.extend((0..array.len()).map(|i| (!array.is_null(i)).then(|| array.value(i))));
        }
        keys.sort();
        results.push((expression, keys));
    }
    assert_eq!(
        results,
        vec![
            ("COUNT(v)", vec![None, Some(2)]),
            ("COUNT(v)+1", vec![None, Some(2)]),
            ("COALESCE(SUM(v),7)", vec![None, Some(2)]),
            ("NULLIF(COUNT(v),1)", vec![None, Some(2)])
        ]
    );
    for (expression, suffix, expected, decorrelated) in [
        ("COUNT(DISTINCT v)", "", vec![None, Some(2)], true),
        (
            "COUNT(v)",
            "AND v<0",
            vec![None, Some(1), Some(1), Some(2)],
            true,
        ),
        (
            "COUNT(NULL)",
            "",
            vec![None, Some(1), Some(1), Some(2)],
            true,
        ),
        ("COUNT(v)", "GROUP BY fk", vec![], false),
        ("COUNT(v)", "HAVING COUNT(v)=0", vec![None, Some(2)], false),
        ("COUNT(v)", "LIMIT 0", vec![], false),
    ] {
        let sql=format!("SELECT k FROM outer_empty_probe WHERE flag=1 AND 0=(SELECT {expression} FROM inner_empty_probe WHERE fk=k {suffix})");
        if decorrelated {
            assert!(!context
                .optimized_plan(&sql)
                .unwrap()
                .to_string()
                .contains("scalar subquery"));
        }
        assert_eq!(scalar_probe_keys(&context, &sql).await, expected, "{sql}");
    }
    let collision = "SELECT k,__scalar_result,__scalar_present FROM outer_empty_probe WHERE flag=1 AND 0=(SELECT COUNT(v) FROM inner_empty_probe WHERE fk=k)";
    let result = context.sql(collision).await.unwrap();
    assert_eq!(result.row_count, 2);
    for batch in result.batches {
        for (column, expected) in [(1, 77), (2, 88)] {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(
                values.iter().all(|v| v == Some(expected)),
                "internal alias captured a user field"
            );
        }
    }
    let strict = "CASE WHEN COUNT(v)=0 THEN CAST('bad' AS BIGINT) ELSE COUNT(v) END";
    for (outer_filter, expected) in [
        ("flag=0", vec![]),
        ("flag=1 AND k=1", vec![Some(1), Some(1)]),
    ] {
        let sql=format!("SELECT k FROM outer_empty_probe WHERE {outer_filter} AND 1=(SELECT {strict} FROM inner_empty_probe WHERE fk=k)");
        assert_eq!(scalar_probe_keys(&context, &sql).await, expected, "{sql}");
    }
    let sql=format!("SELECT k FROM outer_empty_probe WHERE flag=1 AND 1=(SELECT {strict} FROM inner_empty_probe WHERE fk=k)");
    assert!(
        context.sql(&sql).await.is_err(),
        "missing groups must evaluate their fallible empty result"
    );
}

async fn scalar_probe_keys(context: &ExecutionContext, sql: &str) -> Vec<Option<i64>> {
    use arrow::array::Array;
    let result = context.sql(sql).await.unwrap();
    let mut keys = Vec::new();
    for batch in result.batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        keys.extend((0..array.len()).map(|i| (!array.is_null(i)).then(|| array.value(i))));
    }
    keys.sort();
    keys
}
