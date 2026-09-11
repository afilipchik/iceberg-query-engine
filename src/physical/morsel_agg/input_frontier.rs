//! Owned partition pulls. Explicit copied-output bounds or admitted buffer
//! ownership permit CPU parallelism; unknown sources retain serial polling.
use crate::{
    execution::{MemoryReservation, SharedMemoryPool},
    physical::{
        operators::spillable::{own_input_batch, owned_input_batch_charge},
        PhysicalOperator, RecordBatchStream,
    },
    QueryError, Result,
};
use arrow::record_batch::RecordBatch;
use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, SelectAll},
    FutureExt, StreamExt, TryStreamExt,
};
use std::{collections::VecDeque, sync::Arc};

#[cfg(test)]
mod tests;

fn error(message: impl std::fmt::Display) -> QueryError {
    QueryError::Execution(format!("aggregate input frontier: {message}"))
}

pub(super) struct InputBatch {
    pub batch: RecordBatch,
    // A parallel slot remains charged through consumer processing. The next
    // pull for this slot starts only on the consumer's next next() call.
    admission: InputAdmission,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

enum InputAdmission {
    ConsumerRequired,
    Copied { _lease: Arc<MemoryReservation> },
    Buffers,
}
impl InputBatch {
    pub(super) fn is_admitted(&self) -> bool {
        !matches!(self.admission, InputAdmission::ConsumerRequired)
    }
}

enum PreparedStreams {
    Copied(std::vec::IntoIter<RecordBatchStream>),
    Admitted(crate::execution::reserved_vec::ReservedIntoIter<RecordBatchStream>),
}
impl Iterator for PreparedStreams {
    type Item = RecordBatchStream;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Copied(streams) => streams.next(),
            Self::Admitted(streams) => streams.next(),
        }
    }
}

enum Job {
    Open(Arc<dyn PhysicalOperator>, usize),
    Stream(RecordBatchStream),
}

struct Pull {
    stream: RecordBatchStream,
    batch: Option<InputBatch>,
}

pub(super) struct InputFrontier {
    opening: FuturesUnordered<BoxFuture<'static, Result<RecordBatchStream>>>,
    serial: SelectAll<RecordBatchStream>,
    pending: VecDeque<Job>,
    recycle: Option<RecordBatchStream>,
    tasks: tokio::task::JoinSet<Result<Pull>>,
    metadata: Arc<MemoryReservation>,
    envelope: Option<Arc<MemoryReservation>>,
    bound: usize,
    slots: usize,
    admitted_buffers: bool,
    demand: Arc<tokio::sync::Semaphore>,
}

impl InputFrontier {
    pub async fn new(input: Arc<dyn PhysicalOperator>, pool: &SharedMemoryPool) -> Result<Self> {
        let partitions = input.output_partitions();
        if partitions == 0 {
            return Err(error("input declares no partitions"));
        }
        let admitted =
            std::panic::AssertUnwindSafe(input.prepare_admitted_queue_input(pool.clone()))
                .catch_unwind()
                .await
                .map_err(super::live_spill::panic_error)??;
        let admitted_buffers = admitted.is_some();
        if let Some(prepared) = &admitted {
            if !prepared.pool.is_within(pool) || prepared.streams.as_slice().len() != partitions {
                return Err(error("admitted input pool/partition contract violated"));
            }
        }
        // Conservative task/pending-job bookkeeping allowance. Decoder and
        // buffer owners have their own leases; original plan residency is separate.
        let metadata = Arc::new(
            pool.allocate(
                partitions
                    .checked_mul(if admitted_buffers { 16384 } else { 1024 })
                    .and_then(|n| n.checked_add(512))
                    .ok_or_else(|| error("metadata extent overflow"))?,
            )?,
        );
        // Preparation owns initialized streams, even if its bound is unknown.
        // Never reconstruct those streams by calling execute again.
        let prepared = if admitted_buffers {
            None
        } else {
            std::panic::AssertUnwindSafe(input.prepare_queue_input())
                .catch_unwind()
                .await
                .map_err(super::live_spill::panic_error)??
        };
        let (bound, prepared) = if let Some(prepared) = admitted {
            (
                None,
                Some(PreparedStreams::Admitted(
                    prepared.streams.into_owned_iter(),
                )),
            )
        } else {
            match prepared {
                Some(prepared) => {
                    if prepared.streams.len() != partitions {
                        return Err(error("prepared partition count mismatch"));
                    }
                    (
                        prepared.output.max_bytes(),
                        Some(PreparedStreams::Copied(prepared.streams.into_iter())),
                    )
                }
                None => (
                    input
                        .pool_independent_queue_copy_bound()
                        .and_then(|b| b.max_bytes()),
                    None,
                ),
            }
        };
        // Copied prefetch competes with downstream expression/state/spill work.
        // Keep a bounded working window instead of admitting queues up to the
        // last byte. On small pools this is half the remaining space; cap the
        // window at 1 MiB so large copied frames can still overlap when they
        // leave ample downstream headroom. This is a scheduling heuristic, not
        // a certified consumer minimum. Hard query admission remains unchanged.
        // Admitted buffers keep their producer-owned progress-credit contract.
        let downstream_window = (pool.available() / 2).min(1024 * 1024);
        let copied_queue_budget = pool.available().saturating_sub(downstream_window);
        let admission = bound.and_then(|bytes| {
            (2..=partitions.min(rayon::current_num_threads()))
                .rev()
                .find_map(|slots| {
                    let envelope = bytes.checked_mul(slots)?;
                    if envelope > copied_queue_budget {
                        return None;
                    }
                    pool.try_allocate(envelope)
                        .map(|lease| (slots, Arc::new(lease)))
                })
        });
        let (slots, envelope) = if admitted_buffers {
            (partitions.min(rayon::current_num_threads().max(1)), None)
        } else {
            admission.map_or((1, None), |(slots, lease)| (slots, Some(lease)))
        };
        let mut frontier = Self {
            opening: FuturesUnordered::new(),
            serial: SelectAll::new(),
            pending: VecDeque::new(),
            recycle: None,
            tasks: tokio::task::JoinSet::new(),
            metadata,
            envelope,
            bound: bound.unwrap_or(0),
            slots,
            admitted_buffers,
            demand: Arc::new(tokio::sync::Semaphore::new(slots)),
        };
        if slots > 1 {
            frontier
                .pending
                .try_reserve_exact(partitions)
                .map_err(error)?;
            if let Some(streams) = prepared {
                frontier
                    .pending
                    .extend(streams.into_iter().map(Job::Stream));
            } else {
                frontier
                    .pending
                    .extend((0..partitions).map(|p| Job::Open(input.clone(), p)));
            }
        } else if let Some(streams) = prepared {
            frontier.serial.extend(streams);
        } else {
            for partition in 0..partitions {
                let input = input.clone();
                frontier.opening.push(
                    async move {
                        std::panic::AssertUnwindSafe(input.execute(partition))
                            .catch_unwind()
                            .await
                            .map_err(super::live_spill::panic_error)?
                    }
                    .boxed(),
                );
            }
        }
        if std::env::var_os("QE_AGG_PROF").is_some() {
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr().lock(),
                "[aggregate-frontier] {}",
                serde_json::json!({"operator":input.name(), "partitions":partitions,
                    "slots":slots, "copy_bound":bound, "admitted_buffers":admitted_buffers,
                    "envelope_bytes":frontier.envelope.as_ref().map(|g| g.size())})
            );
        }
        Ok(frontier)
    }

    fn spawn(&mut self, job: Job) {
        let envelope = self.envelope.clone();
        let admitted_buffers = self.admitted_buffers;
        let metadata = self.metadata.clone();
        let bound = self.bound;
        let demand = self.demand.clone();
        self.tasks.spawn(async move {
            let _metadata = metadata;
            let permit = demand.acquire_owned().await.map_err(error)?;
            let mut stream = match job {
                Job::Open(input, partition) => input.execute(partition).await?,
                Job::Stream(stream) => stream,
            };
            let batch = match stream.try_next().await? {
                Some(batch) => {
                    let (batch, admission) = if admitted_buffers {
                        (batch, InputAdmission::Buffers)
                    } else {
                        let bytes = owned_input_batch_charge(&batch)?;
                        if bytes > bound {
                            return Err(error(format!(
                                "copy bound violated: actual {bytes}, bound {bound}"
                            )));
                        }
                        let lease =
                            envelope.ok_or_else(|| error("parallel copy lacks admission"))?;
                        (
                            own_input_batch(batch)?,
                            InputAdmission::Copied { _lease: lease },
                        )
                    };
                    Some(InputBatch {
                        batch,
                        admission,
                        _permit: Some(permit),
                    })
                }
                None => None,
            };
            Ok(Pull { stream, batch })
        });
    }

    pub async fn next(&mut self) -> Result<Option<InputBatch>> {
        if self.slots == 1 {
            loop {
                let mut opening_error = None;
                loop {
                    match self.opening.next().now_or_never() {
                        Some(Some(Ok(stream))) => self.serial.push(stream),
                        Some(Some(Err(error))) => {
                            opening_error.get_or_insert(error);
                        }
                        _ => break,
                    }
                }
                if let Some(error) = opening_error {
                    return Err(error);
                }
                tokio::select! {
                    result=self.opening.next(), if !self.opening.is_empty()=>{
                        if let Some(result)=result {self.serial.push(result?);}
                    }
                    result=std::panic::AssertUnwindSafe(self.serial.next()).catch_unwind(), if !self.serial.is_empty()=>{
                        if let Some(result)=result.map_err(super::live_spill::panic_error)? {
                            return Ok(Some(InputBatch { batch: result?, admission: if self.admitted_buffers { InputAdmission::Buffers } else { InputAdmission::ConsumerRequired }, _permit: None }));
                        }
                    }
                    else=>return Ok(None),
                }
            }
        }
        if let Some(stream) = self.recycle.take() {
            self.spawn(Job::Stream(stream));
        }
        loop {
            while self.tasks.len() < self.slots {
                let Some(job) = self.pending.pop_front() else {
                    break;
                };
                self.spawn(job);
            }
            let Some(result) = self.tasks.join_next().await else {
                return Ok(None);
            };
            let Pull { stream, batch } = result.map_err(error)??;
            if let Some(batch) = batch {
                self.recycle = Some(stream);
                return Ok(Some(batch));
            }
        }
    }

    pub async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
        self.recycle.take();
        self.pending.clear();
        self.serial.clear();
        self.opening.clear();
        self.envelope.take();
    }
}
