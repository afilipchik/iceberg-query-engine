//! A runtime filter follows proven source values, never a coincidentally equal name.
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use query_engine::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

async fn check(projection: &str, key: &str, increment: i64) {
    let directory = tempfile::tempdir().unwrap();
    let build = vec![Some(1), Some(2), None, Some(2)];
    let probe: Vec<_> = [Some(0), Some(1), None]
        .into_iter()
        .chain((100..10100).map(Some))
        .collect();
    let mut context =
        ExecutionContext::with_config(ExecutionConfig::new().with_memory_limit(64 * 1024 * 1024))
            .with_parallel_partitions(2);
    for (name, values) in [("b", &build), ("p", &probe)] {
        let batch = RecordBatch::try_from_iter([(
            "k",
            Arc::new(Int64Array::from(values.clone())) as ArrayRef,
        )])
        .unwrap();
        let path = directory.path().join(format!("{name}.parquet"));
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), batch.schema(), None)
                .unwrap();
        let split = batch.num_rows() / 2;
        for part in [
            batch.slice(0, split),
            batch.slice(split, batch.num_rows() - split),
        ] {
            writer.write(&part).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        context.register_parquet(name, &path).unwrap();
    }
    let sql = format!(
        "SELECT b.k AS bk, x.{key} AS pk FROM b JOIN (SELECT {projection} FROM p) x ON b.k=x.{key}"
    );
    let result = context.sql(&sql).await.unwrap();
    let mut actual = Vec::new();
    for batch in result.batches {
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!left.is_null(row) && !right.is_null(row));
            actual.push((left.value(row), right.value(row)));
        }
    }
    let mut expected = Vec::new();
    for b in build.into_iter().flatten() {
        for p in probe.iter().flatten() {
            let value = p + increment;
            if b == value {
                expected.push((b, value));
            }
        }
    }
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected, "{sql}");
}
#[tokio::test]
async fn computed_name_collision_cannot_filter_the_uncomputed_source() {
    check("k + 1 AS k", "k", 1).await;
    check("k + 1 AS shifted", "shifted", 1).await;
}
#[tokio::test]
async fn direct_column_aliases_preserve_nulls_and_duplicate_matches() {
    check("k AS renamed", "renamed", 0).await;
    check("k AS k", "k", 0).await;
}

#[test]
fn filter_targets_follow_aliases_ordinals_and_only_the_nonpreserved_probe() {
    use arrow::datatypes::{DataType, Field, Schema};
    use query_engine::physical::{
        HashJoinExec, PhysicalOperator, ProjectExec, StreamingParquetScanExec,
    };
    use query_engine::planner::{Expr, JoinType, ScalarValue};
    let make = |names: [&str; 2]| {
        let schema = Arc::new(Schema::new(Vec::from(
            names.map(|name| Field::new(name, DataType::Int64, true)),
        )));
        Arc::new(
            StreamingParquetScanExec::try_new("source", &[], schema.clone(), None, None, &schema)
                .unwrap(),
        )
    };
    let left = make(["bk", "bv"]);
    let right = make(["pk", "pv"]);
    let cfg = right.runtime_filter_config();
    let alias_schema = Arc::new(Schema::new(vec![Field::new(
        "renamed",
        DataType::Int64,
        true,
    )]));
    let alias = ProjectExec::new(
        right.clone(),
        vec![Expr::column("pv")],
        alias_schema.clone(),
    );
    let (target, ordinal) = alias
        .runtime_filter_target(0)
        .expect("direct alias must remain linked");
    assert!(Arc::ptr_eq(&target, &cfg));
    assert_eq!(ordinal, 1);
    assert!(alias.runtime_filter_target(1).is_none());
    let computed = ProjectExec::new(
        right.clone(),
        vec![Expr::literal(ScalarValue::Int64(1))],
        alias_schema,
    );
    assert!(computed.runtime_filter_target(0).is_none());
    for build_right in [false, true] {
        let mut join = HashJoinExec::new(
            left.clone(),
            right.clone(),
            vec![(Expr::column("bk"), Expr::column("pk"))],
            JoinType::Inner,
        )
        .with_build_right(build_right);
        join.set_retained(Some(if build_right {
            vec![true, false, false, true]
        } else {
            vec![false, true, true, false]
        }));
        let (probe_output, build_output, expected_cfg) = if build_right {
            (0, 1, left.runtime_filter_config())
        } else {
            (1, 0, cfg.clone())
        };
        let (target, ordinal) = join.runtime_filter_target(probe_output).unwrap();
        assert!(Arc::ptr_eq(&target, &expected_cfg));
        assert_eq!(ordinal, 0);
        assert!(join.runtime_filter_target(build_output).is_none());
    }
    for kind in [
        JoinType::Right,
        JoinType::Full,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        let join = HashJoinExec::new(
            left.clone(),
            right.clone(),
            vec![(Expr::column("bk"), Expr::column("pk"))],
            kind,
        );
        for column in 0..join.schema().fields().len() {
            assert!(join.runtime_filter_target(column).is_none());
        }
    }
}

#[tokio::test]
async fn right_built_semi_filters_only_nonpreserved_probe_rows() {
    use futures::TryStreamExt;
    use query_engine::physical::PhysicalOperator;
    fn probe_config(
        plan: &Arc<dyn PhysicalOperator>,
    ) -> Option<query_engine::physical::streaming_parquet_scan::RuntimeFilterConfig> {
        if plan.children().is_empty() {
            let index = plan
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == "p.probe_key")?;
            return plan.runtime_filter_target(index).map(|(cfg, _)| cfg);
        }
        plan.children().iter().find_map(probe_config)
    }
    let directory = tempfile::tempdir().unwrap();
    let probe: Vec<_> = [None, Some(1), Some(1), Some(2), Some(3)]
        .into_iter()
        .chain((100..10100).map(Some))
        .collect();
    for build in [
        vec![None, Some(1), Some(1), Some(2)],
        vec![None, Some(-1)],
        vec![],
    ] {
        for kind in ["semi", "anti", "left"] {
            let mut context = ExecutionContext::with_config(
                ExecutionConfig::new().with_memory_limit(64 * 1024 * 1024),
            );
            for (table, column, values) in [("p", "probe_key", &probe), ("b", "build_key", &build)]
            {
                let batch = RecordBatch::try_from_iter([(
                    column,
                    Arc::new(Int64Array::from(values.clone())) as ArrayRef,
                )])
                .unwrap();
                let path = directory.path().join(format!("{kind}-{table}.parquet"));
                let mut writer = ArrowWriter::try_new(
                    std::fs::File::create(&path).unwrap(),
                    batch.schema(),
                    None,
                )
                .unwrap();
                for start in (0..batch.num_rows()).step_by(256) {
                    writer
                        .write(&batch.slice(start, (batch.num_rows() - start).min(256)))
                        .unwrap();
                    writer.flush().unwrap();
                }
                writer.close().unwrap();
                context.register_parquet(table, &path).unwrap();
            }
            let sql = match kind {
                "semi" => "SELECT probe_key FROM p LEFT SEMI JOIN b ON build_key = probe_key",
                "anti" => "SELECT probe_key FROM p LEFT ANTI JOIN b ON build_key = probe_key",
                _ => "SELECT probe_key FROM p LEFT JOIN b ON probe_key = build_key",
            };
            // Isolate physical join wiring from optimizer-added IS NOT NULL filters,
            // which deliberately route small files through the eager scan policy.
            let mut planner = query_engine::physical::PhysicalPlanner::with_config(
                context.memory_pool().clone(),
                context.config().clone(),
            );
            for name in ["p", "b"] {
                planner.register_table(name, context.table_provider(name).unwrap());
            }
            let plan = planner
                .create_physical_plan(&context.logical_plan(sql).unwrap())
                .unwrap();
            let cfg = probe_config(&plan).expect("probe scan lineage");
            assert_eq!(
                cfg.lock().len(),
                usize::from(kind == "semi"),
                "{kind}: probe filter wiring"
            );
            let mut actual = Vec::new();
            for partition in 0..plan.output_partitions() {
                for batch in plan
                    .execute(partition)
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                {
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    actual.extend(values.iter());
                }
            }
            let mut expected = Vec::new();
            for &key in &probe {
                let matches = build.iter().filter(|&&b| key.is_some() && key == b).count();
                let count = match kind {
                    "semi" => usize::from(matches > 0),
                    "anti" => usize::from(matches == 0),
                    _ => matches.max(1),
                };
                expected.extend(std::iter::repeat_n(key, count));
            }
            actual.sort_unstable();
            expected.sort_unstable();
            assert_eq!(actual, expected, "{kind}");
            if kind == "semi" && !build.is_empty() {
                assert!(
                    cfg.lock()[0].1.lock().is_some(),
                    "build must publish filter"
                );
            }
        }
    }
}
