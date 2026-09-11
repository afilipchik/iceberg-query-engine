//! Physical API boundaries must refuse unsound plans before consuming inputs.
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::TryStreamExt;
use query_engine::error::{QueryError, Result};
use query_engine::execution::VectorSearchMode;
use query_engine::physical::operators::{DelimJoinExec, TableProvider, VectorSearchExec};
use query_engine::physical::vector::{VectorMetric, VectorQuery};
use query_engine::physical::{PhysicalOperator, RecordBatchStream};
use query_engine::planner::{Expr, JoinType, SchemaField};
use query_engine::ExecutionConfig;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct FixedScan {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    calls: AtomicUsize,
}

impl FixedScan {
    fn new(partitions: Vec<Vec<i64>>) -> Arc<Self> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let partitions = partitions
            .into_iter()
            .map(|values| {
                vec![
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                        .unwrap(),
                ]
            })
            .collect();
        Arc::new(Self {
            schema,
            partitions,
            calls: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl PhysicalOperator for FixedScan {
    fn name(&self) -> &str {
        "FixedScan"
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        self.partitions.len()
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        query_engine::physical::check_partition(self, partition)?;
        Ok(Box::pin(futures::stream::iter(
            self.partitions[partition].clone().into_iter().map(Ok),
        )))
    }
}

async fn assert_delim_refuses(left: Arc<FixedScan>, right: Arc<FixedScan>, kind: JoinType) {
    let join = DelimJoinExec::new(
        left.clone(),
        right.clone(),
        kind,
        vec![Expr::column("id")],
        vec![(Expr::column("id"), Expr::column("id"))],
        left.schema(),
    );
    let error = match join.execute(0).await {
        Err(error) => error,
        Ok(_) => panic!("uncertified DelimJoin must refuse execution"),
    };
    assert!(
        matches!(&error, QueryError::NotImplemented(message) if message.contains("exact dependent-join contract")),
        "{error}"
    );
    assert_eq!(left.calls.load(Ordering::SeqCst), 0);
    assert_eq!(right.calls.load(Ordering::SeqCst), 0);
    assert!(join.delim_state().get_distinct_values().is_none());
}

#[tokio::test]
async fn delim_join_refuses_before_omitting_outer_partitions() {
    assert_delim_refuses(
        FixedScan::new(vec![vec![1], vec![2]]),
        FixedScan::new(vec![vec![1, 2]]),
        JoinType::Semi,
    )
    .await;
}

#[tokio::test]
async fn delim_join_refuses_before_omitting_inner_partitions() {
    assert_delim_refuses(
        FixedScan::new(vec![vec![1, 2]]),
        FixedScan::new(vec![vec![1], vec![2]]),
        JoinType::Semi,
    )
    .await;
}

#[tokio::test]
async fn delim_join_single_partition_is_also_quarantined_for_every_supported_kind() {
    for kind in [
        JoinType::Semi,
        JoinType::Anti,
        JoinType::Mark,
        JoinType::Single,
    ] {
        assert_delim_refuses(
            FixedScan::new(vec![vec![1]]),
            FixedScan::new(vec![vec![1]]),
            kind,
        )
        .await;
    }
}

#[derive(Debug)]
struct CountingIndexProvider {
    schema: SchemaRef,
    calls: AtomicUsize,
}
impl TableProvider for CountingIndexProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn scan(&self, _projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        panic!("unexpected scan")
    }
    fn scan_knn(
        &self,
        _projection: Option<&[usize]>,
        _query: &VectorQuery,
    ) -> Result<Option<Vec<RecordBatch>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(vec![]))
    }
}

fn vector_operator(
    fallback: Arc<FixedScan>,
    provider: Option<Arc<dyn TableProvider>>,
    mode: VectorSearchMode,
) -> VectorSearchExec {
    let config = ExecutionConfig {
        vector_search_mode: mode,
        ..Default::default()
    };
    VectorSearchExec::new(
        fallback.clone(),
        provider,
        vec![0],
        vec![SchemaField::new("id", DataType::Int64)],
        "embedding".into(),
        vec![0.0],
        2,
        0,
        VectorMetric::L2,
        None,
        fallback.schema(),
        config,
    )
}

async fn assert_vector_refuses(mode: VectorSearchMode) {
    for partitions in [vec![], vec![vec![1], vec![2]]] {
        let fallback = FixedScan::new(partitions);
        let provider = Arc::new(CountingIndexProvider {
            schema: fallback.schema(),
            calls: AtomicUsize::new(0),
        });
        let operator = vector_operator(fallback.clone(), Some(provider.clone()), mode);
        let error = match operator.execute(0).await {
            Err(error) => error,
            Ok(_) => panic!("invalid global fallback must fail"),
        };
        assert!(
            matches!(&error, QueryError::Execution(message) if message.contains("one global output partition")),
            "{error}"
        );
        assert_eq!(fallback.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn vector_search_rejects_non_global_exact_fallback_before_consuming_it() {
    assert_vector_refuses(VectorSearchMode::Exact).await;
}

#[tokio::test]
async fn vector_search_rejects_non_global_fallback_before_index_provider_work() {
    assert_vector_refuses(VectorSearchMode::Indexed).await;
}

#[tokio::test]
async fn vector_search_preserves_valid_single_partition_fallback() {
    for mode in [VectorSearchMode::Exact, VectorSearchMode::Indexed] {
        let fallback = FixedScan::new(vec![vec![1, 2]]);
        let operator = vector_operator(fallback.clone(), None, mode);
        let batches: Vec<RecordBatch> = operator
            .execute(0)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(fallback.calls.load(Ordering::SeqCst), 1);
    }
}
