use crate::{
    consensus::{
        services::{
            ConsensusServices, DbBlockDepthManager, DbDagTraversalManager, DbGhostdagManager, DbParentsManager, DbPruningPointManager,
            DbWindowManager,
        },
        storage::ConsensusStorage,
    },
    errors::{BlockProcessResult, RuleError},
    model::{
        services::reachability::MTReachabilityService,
        stores::{
            block_window_cache::{BlockWindowCacheStore, BlockWindowHeap},
            daa::DbDaaStore,
            depth::DbDepthStore,
            ghostdag::{DbGhostdagStore, GhostdagData, GhostdagStoreReader},
            headers::DbHeadersStore,
            headers_selected_tip::{DbHeadersSelectedTipStore, HeadersSelectedTipStoreReader},
            pruning::{DbPruningStore, PruningPointInfo, PruningStoreReader},
            reachability::{DbReachabilityStore, StagingReachabilityStore},
            relations::{DbRelationsStore, RelationsStoreReader},
            statuses::{DbStatusesStore, StatusesStore, StatusesStoreBatchExtensions, StatusesStoreReader},
            DB,
        },
    },
    params::Params,
    pipeline::deps_manager::{BlockProcessingMessage, BlockTask, BlockTaskDependencyManager, TaskId},
    processes::{ghostdag::ordering::SortableBlock, reachability::inquirer as reachability, relations::RelationsStoreExtensions, window::WindowManager},
};
use crossbeam_channel::{Receiver, Sender};
use itertools::Itertools;
use kaspa_consensus_core::{
    blockhash::{BlockHashes, ORIGIN},
    blockstatus::BlockStatus::{self, StatusHeaderOnly, StatusInvalid},
    config::genesis::GenesisBlock,
    header::Header,
    BlockHashSet, BlockLevel,
};
use kaspa_consensusmanager::SessionLock;
use kaspa_core::info;
use kaspa_database::prelude::{StoreResultEmptyTuple, StoreResultExtensions};
use kaspa_hashes::Hash;
use kaspa_utils::vec::VecExtensions;
use parking_lot::RwLock;
use rayon::ThreadPool;
use rocksdb::WriteBatch;
use std::{os::unix, sync::{atomic::{AtomicU64, Ordering}, Arc}, time::{Duration, Instant, UNIX_EPOCH}};
use std::time::SystemTime;

use super::super::ProcessingCounters;


pub struct HeaderBenching {
    pub check_blue_score_bench: AtomicU64,
    pub check_blue_work_bench: AtomicU64,
    pub check_median_timestamp_bench: AtomicU64,
    pub check_merge_size_limit_bench: AtomicU64,
    pub check_bounded_merge_depth_bench: AtomicU64,
    pub check_pruning_point_bench: AtomicU64,
    pub check_indirect_parents_bench: AtomicU64,
    pub check_difficulty_and_daa_score_bench: AtomicU64,
    pub check_pruning_violation_bench: AtomicU64,
    pub check_header_version_bench: AtomicU64,
    pub check_block_timestamp_in_isolation_bench: AtomicU64,
    pub check_parents_limit_bench: AtomicU64,
    pub check_parents_not_origin_bench: AtomicU64,
    pub check_parents_exist_bench: AtomicU64,
    pub check_parents_incest_bench: AtomicU64,
    pub check_pow_and_calc_block_level_bench: AtomicU64,

}

impl HeaderBenching {
    pub fn new() -> Self {
        Self {
            check_blue_score_bench: AtomicU64::new(0),
            check_blue_work_bench: AtomicU64::new(0),
            check_median_timestamp_bench: AtomicU64::new(0),
            check_merge_size_limit_bench: AtomicU64::new(0),
            check_bounded_merge_depth_bench: AtomicU64::new(0),
            check_pruning_point_bench: AtomicU64::new(0),
            check_indirect_parents_bench: AtomicU64::new(0),
            check_difficulty_and_daa_score_bench: AtomicU64::new(0),
            check_pruning_violation_bench: AtomicU64::new(0),
            check_header_version_bench: AtomicU64::new(0),
            check_block_timestamp_in_isolation_bench: AtomicU64::new(0),
            check_parents_limit_bench: AtomicU64::new(0),
            check_parents_not_origin_bench: AtomicU64::new(0),
            check_parents_exist_bench: AtomicU64::new(0),
            check_parents_incest_bench: AtomicU64::new(0),
            check_pow_and_calc_block_level_bench: AtomicU64::new(0),
            }
    }

    fn log(&self) {
        let check_blue_work_bench = self.check_blue_work_bench.load(Ordering::SeqCst);
        let check_blue_score_bench = self.check_blue_score_bench.load(Ordering::SeqCst);
        let check_median_timestamp_bench = self.check_median_timestamp_bench.load(Ordering::SeqCst);
        let check_merge_size_limit_bench = self.check_merge_size_limit_bench.load(Ordering::SeqCst);
        let check_bounded_merge_depth_bench = self.check_bounded_merge_depth_bench.load(Ordering::SeqCst);
        let check_pruning_point_bench = self.check_pruning_point_bench.load(Ordering::SeqCst);
        let check_indirect_parents_bench = self.check_indirect_parents_bench.load(Ordering::SeqCst);
        let check_difficulty_and_daa_score_bench = self.check_difficulty_and_daa_score_bench.load(Ordering::SeqCst);
        let check_pruning_violation_bench = self.check_pruning_violation_bench.load(Ordering::SeqCst);
        let check_header_version_bench = self.check_header_version_bench.load(Ordering::SeqCst);
        let check_block_timestamp_in_isolation_bench = self.check_block_timestamp_in_isolation_bench.load(Ordering::SeqCst);
        let check_parents_limit_bench = self.check_parents_limit_bench.load(Ordering::SeqCst);
        let check_parents_not_origin_bench = self.check_parents_not_origin_bench.load(Ordering::SeqCst);
        let check_parents_exist_bench = self.check_parents_exist_bench.load(Ordering::SeqCst);
        let check_parents_incest_bench = self.check_parents_incest_bench.load(Ordering::SeqCst);
        let check_pow_and_calc_block_level_bench = self.check_pow_and_calc_block_level_bench.load(Ordering::SeqCst);
        let total = check_blue_work_bench + check_blue_score_bench + check_median_timestamp_bench + check_merge_size_limit_bench + check_pruning_point_bench + check_indirect_parents_bench + check_difficulty_and_daa_score_bench + check_pruning_violation_bench + check_header_version_bench + check_block_timestamp_in_isolation_bench + check_parents_limit_bench + check_parents_not_origin_bench + check_parents_exist_bench + check_parents_incest_bench + check_pow_and_calc_block_level_bench;
        let check_blue_work_percentile = check_blue_work_bench as f64 / total as f64 * 100.0;
        let check_blue_score_percentile = check_blue_score_bench as f64 / total as f64 * 100.0;
        let check_median_timestamp_percentile = check_median_timestamp_bench as f64 / total as f64 * 100.0;
        let check_merge_size_limit_percentile = check_merge_size_limit_bench as f64 / total as f64 * 100.0;
        let check_pruning_point_percentile = check_pruning_point_bench as f64 / total as f64 * 100.0;
        let check_indirect_parents_percentile = check_indirect_parents_bench as f64 / total as f64 * 100.0;
        let check_difficulty_and_daa_score_percentile = check_difficulty_and_daa_score_bench as f64 / total as f64 * 100.0;
        let check_pruning_violation_percentile = check_pruning_violation_bench as f64 / total as f64 * 100.0;
        let check_header_version_percentile = check_header_version_bench as f64 / total as f64 * 100.0;
        let check_block_timestamp_in_isolation_percentile = check_block_timestamp_in_isolation_bench as f64 / total as f64 * 100.0;
        let check_parents_limit_percentile = check_parents_limit_bench as f64 / total as f64 * 100.0;
        let check_parents_not_origin_percentile = check_parents_not_origin_bench as f64 / total as f64 * 100.0;
        let check_parents_exist_percentile = check_parents_exist_bench as f64 / total as f64 * 100.0;
        let check_parents_incest_percentile = check_parents_incest_bench as f64 / total as f64 * 100.0;
        let check_pow_and_calc_block_level_percentile = check_pow_and_calc_block_level_bench as f64 / total as f64 * 100.0;
        info!(
            "HeaderBenching:\n\
            check_blue_work_bench: {:?}  ({:.2}%)\n\
            check_blue_score_bench: {:?} ({:.2}%)\n\
            check_median_timestamp_bench: {:?} ({:.2}%)\n\
            check_merge_size_limit_bench: {:?} ({:.2}%)\n\
            check_pruning_point_bench: {:?} ({:.2}%)\n\
            check_indirect_parents_bench: {:?} ({:.2}%)\n\
            check_difficulty_and_daa_score_bench: {:?} ({:.2}%)\n\
            check_pruning_violation_bench: {:?} ({:.2}%)\n\
            check_header_version_bench: {:?} ({:.2}%)\n\
            check_block_timestamp_in_isolation_bench: {:?} ({:.2}%)\n\
            check_parents_limit_bench: {:?} ({:.2}%)\n\
            check_parents_not_origin_bench: {:?} ({:.2}%)\n\
            check_parents_exist_bench: {:?} ({:.2}%)\n\
            check_parents_incest_bench: {:?} ({:.2}%)\n\
            check_pow_and_calc_block_level_bench: {:?} ({:.2}%)\n\
            Total: {:?}",
            Duration::from_millis(check_blue_work_bench.try_into().unwrap()), check_blue_work_percentile,
            Duration::from_millis(check_blue_score_bench.try_into().unwrap()), check_blue_score_percentile,
            Duration::from_millis(check_median_timestamp_bench.try_into().unwrap()), check_median_timestamp_percentile,
            Duration::from_millis(check_merge_size_limit_bench.try_into().unwrap()), check_merge_size_limit_percentile,
            Duration::from_millis(check_pruning_point_bench.try_into().unwrap()), check_pruning_point_percentile,
            Duration::from_millis(check_indirect_parents_bench.try_into().unwrap()), check_indirect_parents_percentile,
            Duration::from_millis(check_difficulty_and_daa_score_bench.try_into().unwrap()), check_difficulty_and_daa_score_percentile,
            Duration::from_millis(check_pruning_violation_bench.try_into().unwrap()), check_pruning_violation_percentile,
            Duration::from_millis(check_header_version_bench.try_into().unwrap()), check_header_version_percentile,
            Duration::from_millis(check_block_timestamp_in_isolation_bench.try_into().unwrap()), check_block_timestamp_in_isolation_percentile,
            Duration::from_millis(check_parents_limit_bench.try_into().unwrap()), check_parents_limit_percentile,
            Duration::from_millis(check_parents_not_origin_bench.try_into().unwrap()), check_parents_not_origin_percentile,
            Duration::from_millis(check_parents_exist_bench.try_into().unwrap()), check_parents_exist_percentile,
            Duration::from_millis(check_parents_incest_bench.try_into().unwrap()), check_parents_incest_percentile,
            Duration::from_millis(check_pow_and_calc_block_level_bench.try_into().unwrap()), check_pow_and_calc_block_level_percentile,
            Duration::from_millis(total.try_into().unwrap())
        );
    }
}

pub struct HeaderProcessingContext {
    pub hash: Hash,
    pub header: Arc<Header>,
    pub pruning_info: PruningPointInfo,
    pub block_level: BlockLevel,
    pub known_parents: Vec<BlockHashes>,

    // Staging data
    pub ghostdag_data: Option<Vec<Arc<GhostdagData>>>,
    pub block_window_for_difficulty: Option<Arc<BlockWindowHeap>>,
    pub block_window_for_past_median_time: Option<Arc<BlockWindowHeap>>,
    pub mergeset_non_daa: Option<BlockHashSet>,
    pub merge_depth_root: Option<Hash>,
    pub finality_point: Option<Hash>,
}

impl HeaderProcessingContext {
    pub fn new(
        hash: Hash,
        header: Arc<Header>,
        block_level: BlockLevel,
        pruning_info: PruningPointInfo,
        known_parents: Vec<BlockHashes>,
    ) -> Self {
        Self {
            hash,
            header,
            block_level,
            pruning_info,
            known_parents,
            ghostdag_data: None,
            block_window_for_difficulty: None,
            mergeset_non_daa: None,
            block_window_for_past_median_time: None,
            merge_depth_root: None,
            finality_point: None,
        }
    }

    /// Returns the direct parents of this header after removal of unknown parents
    pub fn direct_known_parents(&self) -> &[Hash] {
        &self.known_parents[0]
    }

    /// Returns the pruning point at the time this header began processing
    pub fn pruning_point(&self) -> Hash {
        self.pruning_info.pruning_point
    }

    /// Returns the primary (level 0) GHOSTDAG data of this header.
    /// NOTE: is expected to be called only after GHOSTDAG computation was pushed into the context
    pub fn ghostdag_data(&self) -> &Arc<GhostdagData> {
        &self.ghostdag_data.as_ref().unwrap()[0]
    }
}

pub struct HeaderProcessor {
    // Channels
    receiver: Receiver<BlockProcessingMessage>,
    body_sender: Sender<BlockProcessingMessage>,

    // Thread pool
    pub(super) thread_pool: Arc<ThreadPool>,

    // Config
    pub(super) genesis: GenesisBlock,
    pub(super) timestamp_deviation_tolerance: u64,
    pub(super) target_time_per_block: u64,
    pub(super) max_block_parents: u8,
    pub(super) mergeset_size_limit: u64,
    pub(super) skip_proof_of_work: bool,
    pub(super) max_block_level: BlockLevel,

    // DB
    db: Arc<DB>,

    // Stores
    pub(super) relations_stores: Arc<RwLock<Vec<DbRelationsStore>>>,
    pub(super) reachability_store: Arc<RwLock<DbReachabilityStore>>,
    pub(super) reachability_relations_store: Arc<RwLock<DbRelationsStore>>,
    pub(super) ghostdag_stores: Arc<Vec<Arc<DbGhostdagStore>>>,
    pub(super) statuses_store: Arc<RwLock<DbStatusesStore>>,
    pub(super) pruning_point_store: Arc<RwLock<DbPruningStore>>,
    pub(super) block_window_cache_for_difficulty: Arc<BlockWindowCacheStore>,
    pub(super) block_window_cache_for_past_median_time: Arc<BlockWindowCacheStore>,
    pub(super) daa_excluded_store: Arc<DbDaaStore>,
    pub(super) headers_store: Arc<DbHeadersStore>,
    pub(super) headers_selected_tip_store: Arc<RwLock<DbHeadersSelectedTipStore>>,
    pub(super) depth_store: Arc<DbDepthStore>,

    // Managers and services
    pub(super) ghostdag_managers: Arc<Vec<DbGhostdagManager>>,
    pub(super) dag_traversal_manager: DbDagTraversalManager,
    pub(super) window_manager: DbWindowManager,
    pub(super) depth_manager: DbBlockDepthManager,
    pub(super) reachability_service: MTReachabilityService<DbReachabilityStore>,
    pub(super) pruning_point_manager: DbPruningPointManager,
    pub(super) parents_manager: DbParentsManager,

    // Benchmarks
    pub(super) benching: HeaderBenching,

    // Pruning lock
    pruning_lock: SessionLock,

    // Dependency manager
    task_manager: BlockTaskDependencyManager,

    // Counters
    counters: Arc<ProcessingCounters>,

    last_log_instance: RwLock<Instant>,
}

impl HeaderProcessor {
    pub fn new(
        receiver: Receiver<BlockProcessingMessage>,
        body_sender: Sender<BlockProcessingMessage>,
        thread_pool: Arc<ThreadPool>,
        params: &Params,
        db: Arc<DB>,
        storage: &Arc<ConsensusStorage>,
        services: &Arc<ConsensusServices>,
        pruning_lock: SessionLock,
        counters: Arc<ProcessingCounters>,
    ) -> Self {
        Self {
            receiver,
            body_sender,
            thread_pool,
            genesis: params.genesis.clone(),
            db,

            relations_stores: storage.relations_stores.clone(),
            reachability_store: storage.reachability_store.clone(),
            reachability_relations_store: storage.reachability_relations_store.clone(),
            ghostdag_stores: storage.ghostdag_stores.clone(),
            statuses_store: storage.statuses_store.clone(),
            pruning_point_store: storage.pruning_point_store.clone(),
            daa_excluded_store: storage.daa_excluded_store.clone(),
            headers_store: storage.headers_store.clone(),
            depth_store: storage.depth_store.clone(),
            headers_selected_tip_store: storage.headers_selected_tip_store.clone(),
            block_window_cache_for_difficulty: storage.block_window_cache_for_difficulty.clone(),
            block_window_cache_for_past_median_time: storage.block_window_cache_for_past_median_time.clone(),

            ghostdag_managers: services.ghostdag_managers.clone(),
            dag_traversal_manager: services.dag_traversal_manager.clone(),
            window_manager: services.window_manager.clone(),
            reachability_service: services.reachability_service.clone(),
            depth_manager: services.depth_manager.clone(),
            pruning_point_manager: services.pruning_point_manager.clone(),
            parents_manager: services.parents_manager.clone(),

            task_manager: BlockTaskDependencyManager::new(),
            pruning_lock,
            counters,
            benching: HeaderBenching::new(),

            // TODO (HF): make sure to also pass `new_timestamp_deviation_tolerance` and use according to HF activation score
            timestamp_deviation_tolerance: params.timestamp_deviation_tolerance(0),
            target_time_per_block: params.target_time_per_block,
            max_block_parents: params.max_block_parents,
            mergeset_size_limit: params.mergeset_size_limit,
            skip_proof_of_work: params.skip_proof_of_work,
            max_block_level: params.max_block_level,
            last_log_instance: RwLock::new(Instant::now()),
        }
    }

    pub fn worker(self: &Arc<HeaderProcessor>) {
        while let Ok(msg) = self.receiver.recv() {
            match msg {
                BlockProcessingMessage::Exit => {
                    break;
                }
                BlockProcessingMessage::Process(task, block_result_transmitter, virtual_state_result_transmitter) => {
                    if let Some(task_id) = self.task_manager.register(task, block_result_transmitter, virtual_state_result_transmitter)
                    {
                        let processor = self.clone();
                        self.thread_pool.spawn(move || {
                            processor.queue_block(task_id);
                        });
                    }
                }
            };
        }

        // Wait until all workers are idle before exiting
        self.task_manager.wait_for_idle();

        // Pass the exit signal on to the following processor
        self.body_sender.send(BlockProcessingMessage::Exit).unwrap();
    }

    fn queue_block(self: &Arc<HeaderProcessor>, task_id: TaskId) {
        if let Some(task) = self.task_manager.try_begin(task_id) {
            let res = self.process_header(&task);

            let dependent_tasks = self.task_manager.end(
                task,
                |task,
                 block_result_transmitter: tokio::sync::oneshot::Sender<Result<BlockStatus, RuleError>>,
                 virtual_state_result_transmitter| {
                    if res.is_err() || task.block().is_header_only() {
                        // We don't care if receivers were dropped
                        let _ = block_result_transmitter.send(res.clone());
                        let _ = virtual_state_result_transmitter.send(res.clone());
                    } else {
                        self.body_sender
                            .send(BlockProcessingMessage::Process(task, block_result_transmitter, virtual_state_result_transmitter))
                            .unwrap();
                    }
                },
            );

            for dep in dependent_tasks {
                let processor = self.clone();
                self.thread_pool.spawn(move || processor.queue_block(dep));
            }
        }
    }

    fn process_header(&self, task: &BlockTask) -> BlockProcessResult<BlockStatus> {
        let _prune_guard = self.pruning_lock.blocking_read();
        let header = &task.block().header;
        let status_option = self.statuses_store.read().get(header.hash).unwrap_option();

        match status_option {
            Some(StatusInvalid) => return Err(RuleError::KnownInvalid),
            Some(status) => return Ok(status),
            None => {}
        }

        // Validate the header depending on task type
        match task {
            BlockTask::Ordinary { .. } => {
                let ctx = self.validate_header(header)?;
                self.commit_header(ctx, header);
            }
            BlockTask::Trusted { .. } => {
                let ctx = self.validate_trusted_header(header)?;
                self.commit_trusted_header(ctx, header);
            }
        }

        // Report counters
        self.counters.header_counts.fetch_add(1, Ordering::Relaxed);
        self.counters.dep_counts.fetch_add(header.direct_parents().len() as u64, Ordering::Relaxed);

        Ok(StatusHeaderOnly)
    }

    /// Runs full ordinary header validation
    fn validate_header(&self, header: &Arc<Header>) -> BlockProcessResult<HeaderProcessingContext> {
        let block_level = self.validate_header_in_isolation(header)?;
        self.validate_parent_relations(header)?;
        let mut ctx = self.build_processing_context(header, block_level);
        self.ghostdag(&mut ctx);
        self.pre_pow_validation(&mut ctx, header)?;
        if let Err(e) = self.post_pow_validation(&mut ctx, header) {
            self.statuses_store.write().set(ctx.hash, StatusInvalid).unwrap();
            return Err(e);
        }
        Ok(ctx)
    }

    // Runs partial header validation for trusted blocks (currently validates only header-in-isolation and computes GHOSTDAG).
    fn validate_trusted_header(&self, header: &Arc<Header>) -> BlockProcessResult<HeaderProcessingContext> {
        // TODO: For now we skip most validations for trusted blocks, but in the future we should
        // employ some validations to avoid spam etc.
        let block_level = self.validate_header_in_isolation(header)?;
        let mut ctx = self.build_processing_context(header, block_level);
        self.ghostdag(&mut ctx);
        Ok(ctx)
    }

    fn build_processing_context(&self, header: &Arc<Header>, block_level: u8) -> HeaderProcessingContext {
        HeaderProcessingContext::new(
            header.hash,
            header.clone(),
            block_level,
            self.pruning_point_store.read().get().unwrap(),
            self.collect_known_parents(header, block_level),
        )
    }

    /// Collects the known parents for all block levels
    fn collect_known_parents(&self, header: &Header, block_level: BlockLevel) -> Vec<Arc<Vec<Hash>>> {
        let relations_read = self.relations_stores.read();
        (0..=block_level)
            .map(|level| {
                Arc::new(
                    self.parents_manager
                        .parents_at_level(header, level)
                        .iter()
                        .copied()
                        .filter(|parent| relations_read[level as usize].has(*parent).unwrap())
                        .collect_vec()
                        // This kicks-in only for trusted blocks or for level > 0. If an ordinary block is 
                        // missing direct parents it will fail validation.
                        .push_if_empty(ORIGIN),
                )
            })
            .collect_vec()
    }

    /// Runs the GHOSTDAG algorithm for all block levels and writes the data into the context (if hasn't run already)
    fn ghostdag(&self, ctx: &mut HeaderProcessingContext) {
        let ghostdag_data = (0..=ctx.block_level as usize)
            .map(|level| {
                self.ghostdag_stores[level]
                    .get_data(ctx.hash)
                    .unwrap_option()
                    .unwrap_or_else(|| Arc::new(self.ghostdag_managers[level].ghostdag(&ctx.known_parents[level])))
            })
            .collect_vec();

        self.counters.mergeset_counts.fetch_add(ghostdag_data[0].mergeset_size() as u64, Ordering::Relaxed);
        ctx.ghostdag_data = Some(ghostdag_data);
    }

    fn commit_header(&self, ctx: HeaderProcessingContext, header: &Header) {
        if self.last_log_instance.read().elapsed() > Duration::from_secs(10) {
            self.window_manager.maybe_log_perf();
            *self.last_log_instance.write() = Instant::now();
        }

        let ghostdag_data = ctx.ghostdag_data.as_ref().unwrap();
        let pp = ctx.pruning_point();

        // Create a DB batch writer
        let mut batch = WriteBatch::default();

        //
        // Append-only stores: these require no lock and hence done first in order to reduce locking time
        //

        for (level, datum) in ghostdag_data.iter().enumerate() {
            self.ghostdag_stores[level].insert_batch(&mut batch, ctx.hash, datum).unwrap();
        }
        if let Some(window) = ctx.block_window_for_difficulty {
            self.block_window_cache_for_difficulty.insert(ctx.hash, window);
        }
        if let Some(window) = ctx.block_window_for_past_median_time {
            self.block_window_cache_for_past_median_time.insert(ctx.hash, window);
        }

        self.daa_excluded_store.insert_batch(&mut batch, ctx.hash, Arc::new(ctx.mergeset_non_daa.unwrap())).unwrap();
        self.headers_store.insert_batch(&mut batch, ctx.hash, ctx.header, ctx.block_level).unwrap();
        self.depth_store.insert_batch(&mut batch, ctx.hash, ctx.merge_depth_root.unwrap(), ctx.finality_point.unwrap()).unwrap();

        //
        // Reachability and header chain stores
        //

        // Create staging reachability store. We use an upgradable read here to avoid concurrent
        // staging reachability operations. PERF: we assume that reachability processing time << header processing
        // time, and thus serializing this part will do no harm. However this should be benchmarked. The
        // alternative is to create a separate ReachabilityProcessor and to manage things more tightly.
        let mut staging = StagingReachabilityStore::new(self.reachability_store.upgradable_read());
        let selected_parent = ghostdag_data[0].selected_parent;
        let mut reachability_mergeset = ghostdag_data[0].unordered_mergeset_without_selected_parent();
        reachability::add_block(&mut staging, ctx.hash, selected_parent, &mut reachability_mergeset).unwrap();

        // Non-append only stores need to use write locks.
        // Note we need to keep the lock write guards until the batch is written.
        let mut hst_write = self.headers_selected_tip_store.write();
        let prev_hst = hst_write.get().unwrap();
        if SortableBlock::new(ctx.hash, header.blue_work) > prev_hst
            && reachability::is_chain_ancestor_of(&staging, pp, ctx.hash).unwrap()
        {
            // Hint reachability about the new tip.
            // TODO: identify a disqualified hst and make sure to use sink instead
            reachability::hint_virtual_selected_parent(&mut staging, ctx.hash).unwrap();
            hst_write.set_batch(&mut batch, SortableBlock::new(ctx.hash, header.blue_work)).unwrap();
        }

        //
        // Relations and statuses
        //

        let reachability_parents = ctx.known_parents[0].clone();

        let mut relations_write = self.relations_stores.write();
        ctx.known_parents.into_iter().enumerate().for_each(|(level, parents_by_level)| {
            relations_write[level].insert_batch(&mut batch, header.hash, parents_by_level).unwrap();
        });

        // Write reachability relations. These relations are only needed during header pruning
        let mut reachability_relations_write = self.reachability_relations_store.write();
        reachability_relations_write.insert_batch(&mut batch, ctx.hash, reachability_parents).unwrap();

        let statuses_write = self.statuses_store.set_batch(&mut batch, ctx.hash, StatusHeaderOnly).unwrap();

        // Write reachability data. Only at this brief moment the reachability store is locked for reads.
        // We take special care for this since reachability read queries are used throughout the system frequently.
        // Note we hold the lock until the batch is written
        let reachability_write = staging.commit(&mut batch).unwrap();

        // Flush the batch to the DB
        self.db.write(batch).unwrap();

        // Calling the drops explicitly after the batch is written in order to avoid possible errors.
        drop(reachability_write);
        drop(statuses_write);
        drop(reachability_relations_write);
        drop(relations_write);
        drop(hst_write);
    }

    fn commit_trusted_header(&self, ctx: HeaderProcessingContext, _header: &Header) {
        let ghostdag_data = ctx.ghostdag_data.as_ref().unwrap();

        // Create a DB batch writer
        let mut batch = WriteBatch::default();

        for (level, datum) in ghostdag_data.iter().enumerate() {
            // This data might have been already written when applying the pruning proof.
            self.ghostdag_stores[level].insert_batch(&mut batch, ctx.hash, datum).unwrap_or_exists();
        }

        let mut relations_write = self.relations_stores.write();
        ctx.known_parents.into_iter().enumerate().for_each(|(level, parents_by_level)| {
            // This data might have been already written when applying the pruning proof.
            relations_write[level].insert_batch(&mut batch, ctx.hash, parents_by_level).unwrap_or_exists();
        });

        let statuses_write = self.statuses_store.set_batch(&mut batch, ctx.hash, StatusHeaderOnly).unwrap();

        // Flush the batch to the DB
        self.db.write(batch).unwrap();

        // Calling the drops explicitly after the batch is written in order to avoid possible errors.
        drop(statuses_write);
        drop(relations_write);
    }

    pub fn process_genesis(&self) {
        // Init headers selected tip and selected chain stores
        let mut batch = WriteBatch::default();
        let mut hst_write = self.headers_selected_tip_store.write();
        hst_write.set_batch(&mut batch, SortableBlock::new(self.genesis.hash, 0.into())).unwrap();
        self.db.write(batch).unwrap();
        drop(hst_write);

        // Write the genesis header
        let mut genesis_header: Header = (&self.genesis).into();
        // Force the provided genesis hash. Important for some tests which manually modify the genesis hash.
        // Note that for official nets (mainnet, testnet etc) they are guaranteed to be equal as enforced by a test in genesis.rs
        genesis_header.hash = self.genesis.hash;
        let genesis_header = Arc::new(genesis_header);

        let mut ctx = HeaderProcessingContext::new(
            self.genesis.hash,
            genesis_header.clone(),
            self.max_block_level,
            PruningPointInfo::from_genesis(self.genesis.hash),
            (0..=self.max_block_level).map(|_| BlockHashes::new(vec![ORIGIN])).collect(),
        );
        ctx.ghostdag_data =
            Some(self.ghostdag_managers.iter().map(|manager_by_level| Arc::new(manager_by_level.genesis_ghostdag_data())).collect());
        ctx.mergeset_non_daa = Some(Default::default());
        ctx.merge_depth_root = Some(ORIGIN);
        ctx.finality_point = Some(ORIGIN);

        self.commit_header(ctx, &genesis_header);
    }

    pub fn init(&self) {
        if self.relations_stores.read()[0].has(ORIGIN).unwrap() {
            return;
        }

        let mut batch = WriteBatch::default();
        let mut relations_write = self.relations_stores.write();
        (0..=self.max_block_level)
            .for_each(|level| relations_write[level as usize].insert_batch(&mut batch, ORIGIN, BlockHashes::new(vec![])).unwrap());
        let mut hst_write = self.headers_selected_tip_store.write();
        hst_write.set_batch(&mut batch, SortableBlock::new(ORIGIN, 0.into())).unwrap();
        self.db.write(batch).unwrap();
        drop(hst_write);
        drop(relations_write);
    }
}
