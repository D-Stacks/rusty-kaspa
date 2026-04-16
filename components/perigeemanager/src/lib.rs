use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt::Display,
    net::SocketAddr,
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};

use itertools::Itertools;
use kaspa_consensus_core::{BlockHashSet, Hash, HashMapCustomHasher};
use kaspa_core::{debug, info};
use kaspa_p2p_lib::{Peer, PeerKey, Router};
use parking_lot::Mutex;
use rand::{Rng, seq::IteratorRandom, thread_rng};

// Tolerance for the number of blocks verified in a round to trigger evaluation.
// For example, at 0.175, if we expect to see 200 blocks verified in a round, but fewer or more than
// 175 or 225 (respectively) are verified, we skip the leverage evaluation for this round.
// The reasoning is that network conditions are not considered stable enough to make a good decision,
// and we would rather skip and wait for the next round.
// Note that exploration can still happen even if this threshold is not met.
// This ensures that we continue to explore in case network conditions are the fault of the connect peers, not network-wide.
const BLOCKS_VERIFIED_FAULT_TOLERANCE: f64 = 0.175;
const IDENT: &str = "PerigeeManager";

// The fraction of blocks at a given rank level that must be covered by selected peers
// before advancing to the next rank level during leverage selection.
const RANK_COVERAGE_THRESHOLD: f64 = 1.0;

/// Holds a rank-based score for a peer.
/// `rank_counts[i]` = number of blocks where this peer achieved rank `i+1`.
/// Comparison is lexicographic on rank_counts (more rank-1 wins = better, then rank-2, etc.).
#[derive(Debug, Clone)]
pub struct RankScore {
    rank_counts: Vec<usize>,
}

impl RankScore {
    const EMPTY: RankScore = RankScore { rank_counts: Vec::new() };

    fn new(rank_counts: Vec<usize>) -> Self {
        RankScore { rank_counts }
    }
}

impl PartialEq for RankScore {
    fn eq(&self, other: &Self) -> bool {
        self.rank_counts == other.rank_counts
    }
}

impl Eq for RankScore {}

impl PartialOrd for RankScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher counts at lower rank indices = better.
        // Compare lexicographically in *reverse* (descending) so more rank-1 wins → Greater (i.e., "better").
        // We want: peer with more rank-1 wins to be "less" in ordering (selected first),
        // so we reverse: other's counts compared to self's counts.
        let max_len = self.rank_counts.len().max(other.rank_counts.len());
        for i in 0..max_len {
            let s = if i < self.rank_counts.len() { self.rank_counts[i] } else { 0 };
            let o = if i < other.rank_counts.len() { other.rank_counts[i] } else { 0 };
            // More wins at this rank = better = should sort first (Less)
            match o.cmp(&s) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }
        Ordering::Equal
    }
}

/// Configuration for the perigee manager.
#[derive(Debug, Clone)]
pub struct PerigeeConfig {
    pub perigee_outbound_target: usize,
    pub leverage_target: usize,
    pub exploration_target: usize,
    pub round_frequency: usize,
    pub round_duration: Duration,
    pub expected_blocks_per_round: u64,
    pub statistics: bool,
    pub persistence: bool,
}

impl PerigeeConfig {
    pub fn new(
        perigee_outbound_target: usize,
        leverage_target: usize,
        exploration_target: usize,
        round_duration: usize,
        connection_manager_tick_duration: Duration,
        statistics: bool,
        persistence: bool,
        bps: u64,
    ) -> Self {
        let expected_blocks_per_round = bps * round_duration as u64;
        let round_duration = Duration::from_secs(round_duration as u64);
        Self {
            perigee_outbound_target,
            leverage_target,
            exploration_target,
            round_frequency: round_duration.as_secs() as usize / connection_manager_tick_duration.as_secs() as usize,
            round_duration,
            expected_blocks_per_round,
            statistics,
            persistence,
        }
    }
}

impl Display for PerigeeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Perigee outbound target: {}, Leverage target: {}, Exploration target: {}, Round duration: {:2} secs, Expected blocks per round: {}, Statistics: {}, Persistence: {}",
            self.perigee_outbound_target,
            self.leverage_target,
            self.exploration_target,
            self.round_duration.as_secs(),
            self.expected_blocks_per_round,
            self.statistics,
            self.persistence
        )
    }
}

#[derive(Debug)]

/// Manages peer selection and scoring.
pub struct PerigeeManager {
    verified_blocks: BlockHashSet,  // holds blocks that are consensus verified.
    to_ignore_blocks: BlockHashSet, // holds blocks that should be ignored for peer scoring (in the case of FTR blocks this is useful),
    first_seen: HashMap<Hash, Instant>,
    last_round_leveraged_peers: Vec<PeerKey>,
    round_start: Instant,
    round_counter: u64,
    config: PerigeeConfig,
    is_ibd_running: Arc<AtomicBool>,
}

impl PerigeeManager {
    pub fn new(config: PerigeeConfig, is_ibd_running: Arc<AtomicBool>) -> Mutex<Self> {
        Mutex::new(Self {
            verified_blocks: BlockHashSet::new(),
            to_ignore_blocks: BlockHashSet::new(),
            first_seen: HashMap::new(),
            last_round_leveraged_peers: Vec::new(),
            round_start: Instant::now(),
            round_counter: 0,
            config,
            is_ibd_running,
        })
    }

    pub fn ignore_perigee_timestamp(&mut self, hash: Hash) {
        // Marks a block as to be ignored for peer scoring.
        // This is useful for blocks that we do not want to factor into our peer evaluation, such as FTR blocks.
        self.to_ignore_blocks.insert(hash);
    }

    pub fn insert_perigee_timestamp(&mut self, router: &Arc<Router>, hash: Hash, timestamp: Instant, verify: bool) {
        if self.to_ignore_blocks.contains(&hash) {
            // This block is marked to be ignored for peer scoring, so we do not insert a perigee timestamp for it.
            return;
        };
        // Inserts and updates the perigee timestamp for the given router
        // and into the local state.
        if router.is_perigee() || (self.config.statistics && router.is_random_graph()) {
            router.add_perigee_timestamp(hash, timestamp);
        }
        if verify {
            self.verify_block(hash);
        }
        self.maybe_insert_first_seen(hash, timestamp);
    }

    pub fn set_initial_persistent_peers(&mut self, peer_keys: Vec<PeerKey>) {
        debug!("PerigeeManager: Setting initial persistent perigee peers for first round");
        self.last_round_leveraged_peers = peer_keys
    }

    pub fn is_first_round(&self) -> bool {
        self.round_counter == 0
    }

    pub fn trim_peers(&mut self, peers_by_address: Arc<HashMap<SocketAddr, Peer>>) -> Vec<PeerKey> {
        // Contains logic to trim excess perigee peers beyond the configured target
        // without executing a full evaluation.

        debug!("PerigeeManager: Trimming excess peers from perigee");
        let perigee_peers = peers_by_address.values().filter(|p| p.is_perigee()).cloned().collect::<Vec<Peer>>();
        let to_remove_amount = perigee_peers.len().saturating_sub(self.config.perigee_outbound_target);
        let excused_peers = self.get_excused_peers(&perigee_peers);

        perigee_peers
            .iter()
            // Ensure we do not remove leveraged or excused peers
            .filter(|r| !self.last_round_leveraged_peers.contains(&r.key()) && !excused_peers.contains(&r.key()))
            .map(|r| r.key())
            .choose_multiple(&mut thread_rng(), to_remove_amount)
            .iter()
            // In cases where we do not have enough non-excused/non-leveraged peers to remove,
            // we fill the remaining slots with excused peers.
            // Note: We do not expect to ever need to chain with last round's leveraged peers.
            .chain(excused_peers.iter())
            .take(to_remove_amount)
            .cloned()
            .collect()
    }

    pub fn evaluate_round(&mut self, peer_by_address: &HashMap<SocketAddr, Peer>) -> (Vec<PeerKey>, HashSet<PeerKey>, bool) {
        self.round_counter += 1;
        debug!("[{}]: evaluating round: {}", IDENT, self.round_counter);

        let (mut peer_table, perigee_peers) = self.build_table(peer_by_address);

        let is_ibd_running = self.is_ibd_running();

        // First, we excuse all peers with insufficient data this round
        self.excuse(&mut peer_table, &perigee_peers);

        // This excludes peers that have been excused, as well as those that have not provided any data this round.
        let amount_of_contributing_perigee_peers = peer_table.len();
        // In contrast, this is the total number of perigee peers registered in the hub.
        let amount_of_perigee_peers = perigee_peers.len();
        debug!(
            "[{}]: amount_of_perigee_peers: {}, amount_of_contributing_perigee_peers: {}",
            IDENT, amount_of_perigee_peers, amount_of_contributing_perigee_peers
        );
        // For should_leverage, we are conservative and require that we have enough contributing peers for sufficient data.
        let should_leverage = self.should_leverage(is_ibd_running, amount_of_contributing_perigee_peers);
        // For should_explore, we are more aggressive and only require that we have enough total perigee peers.
        // As insufficient data may be malicious behavior by some peers, we prefer to continue churning peers.
        let should_explore = self.should_explore(is_ibd_running, amount_of_perigee_peers);

        let mut has_leveraged_changed = false;

        if !should_leverage && !should_explore {
            // In this case we skip leveraging and exploration this round.
            // We maintain the last round leveraged peers as-is.
            debug!("[{}]: skipping leveraging and exploration this round", IDENT);
            return (self.last_round_leveraged_peers.clone(), HashSet::new(), has_leveraged_changed);
        }

        // i.e. the peers that we mark as "to leverage" this round.
        let selected_peers = if should_leverage {
            let selected_peers = self.leverage(&mut peer_table);
            debug!(
                "[{}]: Selected peers for leveraging this round: {:?}",
                IDENT,
                selected_peers.iter().map(|pk| pk.to_string()).collect_vec()
            );
            // We consider rank changes as well as peer changes here,
            if self.last_round_leveraged_peers != selected_peers {
                // Leveraged peers has changed
                debug!("[{}]: Leveraged peers have changed this round", IDENT);
                has_leveraged_changed = true;
                // Update last round's leveraged peers to the newly selected peers
                self.last_round_leveraged_peers = selected_peers.clone();
            }
            // Return the newly selected peers
            selected_peers
        } else {
            debug!("[{}]: skipping leveraging this round", IDENT);
            // Remove all previously leveraged peers from the peer table to avoid eviction
            for pk in self.last_round_leveraged_peers.iter() {
                peer_table.remove(pk);
            }
            // Return the previous set
            self.last_round_leveraged_peers.clone()
        };

        // i.e. the peers that we mark as "to evict" this round.
        let deselected_peers = if should_explore {
            debug!("[{}]: exploring peers this round", IDENT);
            self.explore(&mut peer_table, amount_of_perigee_peers)
        } else {
            debug!("[{}]: skipping exploration this round", IDENT);
            HashSet::new()
        };

        (selected_peers, deselected_peers, has_leveraged_changed)
    }

    fn leverage(&self, peer_table: &mut HashMap<PeerKey, Vec<u64>>) -> Vec<PeerKey> {
        // Rank-based greedy leverage algorithm.
        // For each block, peers are ranked by delay. Selection proceeds rank-by-rank:
        // at each rank level, pick the peer with the most wins at that rank.
        // Once selected peers collectively cover ≥95% of blocks at the current rank, advance to the next rank.
        // Fill remaining slots randomly if ranks are exhausted.

        // Sanity check
        assert!(peer_table.len() >= self.config.leverage_target, "Potentially entering an endless loop");

        let num_blocks = peer_table.values().next().map(|v| v.len()).unwrap_or(0);
        let num_peers = peer_table.len();

        // Build the rank table from delays
        let rank_table = Self::build_rank_table(peer_table);

        // Find the maximum rank (= num_peers in the worst case)
        let max_rank = num_peers;

        let mut selected_peers: Vec<PeerKey> = Vec::with_capacity(self.config.leverage_target);
        let mut candidates: HashSet<PeerKey> = peer_table.keys().copied().collect();
        let mut current_rank: usize = 1;

        // Track which blocks are "covered" at the current rank level by selected peers
        let coverage_target = (num_blocks as f64 * RANK_COVERAGE_THRESHOLD).ceil() as usize;

        // For coverage tracking: blocks where at least one selected peer has current_rank
        let mut covered_blocks: HashSet<usize> = HashSet::new();

        while selected_peers.len() < self.config.leverage_target && current_rank <= max_rank {
            debug!(
                "[{}]: Leverage rank {} — selected {}/{}, candidates remaining: {}",
                IDENT,
                current_rank,
                selected_peers.len(),
                self.config.leverage_target,
                candidates.len()
            );

            if candidates.is_empty() {
                break;
            }

            let (best_peer, best_count) = Self::get_top_ranked_peer(&rank_table, &candidates, current_rank);

            if best_count == 0 || best_peer.is_none() {
                // No candidate has any blocks at this rank; advance to next rank
                debug!("[{}]: No candidates with rank {} blocks, advancing to rank {}", IDENT, current_rank, current_rank + 1);
                current_rank += 1;
                covered_blocks.clear();
                continue;
            }

            let peer = best_peer.unwrap();
            selected_peers.push(peer);
            candidates.remove(&peer);
            peer_table.remove(&peer);

            // Update coverage: mark blocks where this peer has current_rank
            if let Some(ranks) = rank_table.get(&peer) {
                for (block_idx, &r) in ranks.iter().enumerate() {
                    if r == current_rank {
                        covered_blocks.insert(block_idx);
                    }
                }
            }

            debug!(
                "[{}]: Selected peer {:?} with {} rank-{} blocks. Coverage: {}/{}",
                IDENT,
                peer,
                best_count,
                current_rank,
                covered_blocks.len(),
                num_blocks
            );

            // Check if we've covered enough blocks at this rank level
            if covered_blocks.len() >= coverage_target {
                debug!(
                    "[{}]: Rank {} coverage reached ({:.1}%), advancing to rank {}",
                    IDENT,
                    current_rank,
                    covered_blocks.len() as f64 / num_blocks as f64 * 100.0,
                    current_rank + 1
                );
                current_rank += 1;
                covered_blocks.clear();
            }
        }

        if selected_peers.len() < self.config.leverage_target {
            // Fill remaining slots randomly from remaining candidates
            let to_choose = self.config.leverage_target - selected_peers.len();
            debug!("[{}]: Leveraging did not reach intended target, randomly selecting {} remaining peers", IDENT, to_choose);
            let random_keys: Vec<PeerKey> =
                peer_table.keys().choose_multiple(&mut thread_rng(), to_choose).into_iter().copied().collect();
            for pk in random_keys {
                selected_peers.push(pk);
                peer_table.remove(&pk);
            }
        }

        selected_peers
    }

    fn excuse(&self, peer_table: &mut HashMap<PeerKey, Vec<u64>>, perigee_peers: &[Peer]) {
        // Removes excused peers from the peer table so they are not considered for eviction.
        for k in self.get_excused_peers(perigee_peers) {
            peer_table.remove(&k);
        }
    }

    fn explore(&self, peer_table: &mut HashMap<PeerKey, Vec<u64>>, amount_of_active_perigee: usize) -> HashSet<PeerKey> {
        // This is conceptually simple: we randomly choose peers to evict from the passed peer table.
        // It is expected that other logic, such as leveraging and excusing peers, has already been applied to the peer table.
        let to_remove_target = std::cmp::min(
            self.config.exploration_target,
            amount_of_active_perigee.saturating_sub(self.config.perigee_outbound_target - self.config.exploration_target),
        );

        peer_table.keys().choose_multiple(&mut thread_rng(), to_remove_target).into_iter().cloned().collect()
    }

    pub fn start_new_round(&mut self) {
        // Clears state and starts a new round timer
        self.clear();
        self.round_start = Instant::now();
    }

    pub fn config(&self) -> PerigeeConfig {
        self.config.clone()
    }

    fn maybe_insert_first_seen(&mut self, hash: Hash, timestamp: Instant) {
        // Inserts the first-seen timestamp for a block if it is earlier than the existing one
        // or if it does not exist yet.
        match self.first_seen.entry(hash) {
            Entry::Occupied(mut o) => {
                let current = o.get_mut();
                if timestamp.lt(current) {
                    *current = timestamp;
                }
            }
            Entry::Vacant(v) => {
                v.insert(timestamp);
            }
        }
    }

    fn verify_block(&mut self, hash: Hash) {
        // Marks a block as verified for this round.
        // I.e., this block will be considered in the current round's evaluation.
        self.verified_blocks.insert(hash);
    }

    fn clear(&mut self) {
        // Resets state for a new round
        debug!("[{}]: Clearing state for new round", IDENT);
        self.verified_blocks.clear();
        self.first_seen.clear();
        self.to_ignore_blocks.clear();
    }

    fn get_excused_peers(&self, perigee_peers: &[Peer]) -> Vec<PeerKey> {
        // Define excused peers as those that joined perigee after the round started.
        // They should not be penalized for not having enough data in this round.
        // We also sort them by connection time to give more trimming security to the longest connected peers first,
        // This allows them more time to complete a full round.
        perigee_peers
            .iter()
            .sorted_by_key(|p| p.connection_started())
            .filter(|p| p.connection_started() > self.round_start)
            .map(|p| p.key())
            .collect()
    }

    fn build_rank_table(peer_table: &HashMap<PeerKey, Vec<u64>>) -> HashMap<PeerKey, Vec<usize>> {
        // Builds a per-block rank table from the delay table.
        // For each block (column), peers are ranked 1..N by delay (ascending).
        // Ties share the same rank (standard competition ranking: 1,1,3 not 1,1,2).
        // Peers with u64::MAX delay get worst rank for that block.

        if peer_table.is_empty() {
            return HashMap::new();
        }

        let peer_keys: Vec<PeerKey> = peer_table.keys().copied().collect();
        let num_blocks = peer_table.values().next().map(|v| v.len()).unwrap_or(0);
        let num_peers = peer_keys.len();

        // Initialize rank table
        let mut rank_table: HashMap<PeerKey, Vec<usize>> = peer_keys.iter().map(|pk| (*pk, Vec::with_capacity(num_blocks))).collect();

        #[allow(clippy::needless_range_loop)] // j indexes into per-peer delay vectors across multiple peers
        for j in 0..num_blocks {
            // Collect (peer_key, delay) for this block
            let mut block_delays: Vec<(PeerKey, u64)> = peer_keys.iter().map(|pk| (*pk, peer_table[pk][j])).collect();

            // Sort by delay ascending
            block_delays.sort_by_key(|&(_, delay)| delay);

            // Assign competition ranks
            let mut ranks: HashMap<PeerKey, usize> = HashMap::with_capacity(num_peers);
            let mut rank = 1;
            let mut i = 0;
            while i < block_delays.len() {
                let current_delay = block_delays[i].1;
                let group_start = i;
                // Find all peers with the same delay (tie group)
                while i < block_delays.len() && block_delays[i].1 == current_delay {
                    ranks.insert(block_delays[i].0, rank);
                    i += 1;
                }
                // Next distinct delay gets rank = group_start + group_size + 1
                rank = group_start + (i - group_start) + 1;
            }

            // Push ranks into the rank table
            for pk in &peer_keys {
                rank_table.get_mut(pk).unwrap().push(ranks[pk]);
            }
        }

        rank_table
    }

    fn score_peer_from_ranks(ranks: &[usize], max_rank: usize) -> RankScore {
        // Counts occurrences of each rank value, producing the rank-count distribution.
        if ranks.is_empty() {
            return RankScore::EMPTY;
        }
        let mut counts = vec![0usize; max_rank];
        for &r in ranks {
            if r >= 1 && r <= max_rank {
                counts[r - 1] += 1;
            }
        }
        RankScore::new(counts)
    }

    fn get_top_ranked_peer(
        rank_table: &HashMap<PeerKey, Vec<usize>>,
        candidates: &HashSet<PeerKey>,
        current_rank: usize,
    ) -> (Option<PeerKey>, usize) {
        // Finds the candidate peer with the most blocks at `current_rank`.
        // Returns (best_peer, best_count). Random tie-breaking among equals.
        let mut best_peer: Option<PeerKey> = None;
        let mut best_count: usize = 0;
        let mut tied_count: u32 = 0;

        for pk in candidates.iter() {
            if let Some(ranks) = rank_table.get(pk) {
                let count = ranks.iter().filter(|&&r| r == current_rank).count();
                if count > best_count {
                    best_count = count;
                    best_peer = Some(*pk);
                    tied_count = 1;
                } else if count == best_count && count > 0 {
                    tied_count += 1;
                    if thread_rng().gen_ratio(1, tied_count) {
                        best_peer = Some(*pk);
                    }
                }
            }
        }

        debug!("[{}]: Top ranked peer at rank {} is {:?} with {} blocks", IDENT, current_rank, best_peer, best_count,);
        (best_peer, best_count)
    }

    fn should_leverage(&self, is_ibd_running: bool, amount_of_contributing_perigee_peers: usize) -> bool {
        // Conditions that need to be met to trigger leveraging:

        // 1. IBD is not running
        !is_ibd_running &&
        // 2. Sufficient blocks have been verified this round
        self.block_threshold_reached() &&
        // 3. We have enough contributing perigee peers to choose from
        amount_of_contributing_perigee_peers >= self.config.leverage_target
    }

    fn should_explore(&self, is_ibd_running: bool, amount_of_perigee_peers: usize) -> bool {
        // Conditions that should trigger exploration:

        // 1. IBD is not running
        !is_ibd_running &&
        // 2. We are within bounds to evict at least one peer - else we prefer to wait on more peers joining perigee first.
        amount_of_perigee_peers > (self.config.perigee_outbound_target - self.config.exploration_target)
    }

    fn block_threshold_reached(&self) -> bool {
        // Checks whether the amount of verified blocks this round is within the expected bounds to consider leveraging.
        // If this is not the case, the node is likely experiencing network issues, and we rather skip leveraging this round.
        let verified_count = self.verified_blocks.len() + self.to_ignore_blocks.len();
        let expected_count = self.config.expected_blocks_per_round;
        let lower_bound = (expected_count as f64 * (1.0 - BLOCKS_VERIFIED_FAULT_TOLERANCE)) as usize;
        let upper_bound = (expected_count as f64 * (1.0 + BLOCKS_VERIFIED_FAULT_TOLERANCE)) as usize;
        debug!(
            "[{}]: block_threshold_reached: verified_count={}, expected_count={}, lower_bound={}, upper_bound={}",
            IDENT, verified_count, expected_count, lower_bound, upper_bound
        );
        verified_count >= lower_bound && verified_count <= upper_bound
    }

    fn is_ibd_running(&self) -> bool {
        self.is_ibd_running.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn iterate_verified_first_seen(&self) -> impl Iterator<Item = (&Hash, &Instant)> {
        // Iterates over first_seen entries that correspond to verified blocks only.
        self.first_seen.iter().filter(move |(hash, _)| self.verified_blocks.contains(hash))
    }

    fn build_table(&self, peer_by_address: &HashMap<SocketAddr, Peer>) -> (HashMap<PeerKey, Vec<u64>>, Vec<Peer>) {
        // Builds the peer delay table for all perigee peers.
        debug!("[{}]: Building peer table", IDENT);
        let mut peer_table: HashMap<PeerKey, Vec<u64>> = HashMap::new();

        // Pre-fetch perigee timestamps for all perigee peers.
        // Calling the .perigee_timestamps() method in the loop would become expensive.
        let mut perigee_timestamps = HashMap::new();
        let mut perigee_peers = Vec::new();
        for p in peer_by_address.values() {
            if p.is_perigee() {
                perigee_timestamps.insert(p.key(), p.perigee_timestamps());
                perigee_peers.push(p.clone());
            }
        }

        for (hash, first_ts) in self.iterate_verified_first_seen() {
            for (peer_key, peer_timestamps) in perigee_timestamps.iter_mut() {
                let mut timestamps = peer_timestamps.as_ref().clone();
                match timestamps.entry(*hash) {
                    Entry::Occupied(o) => {
                        let delay = o.get().duration_since(*first_ts).as_millis() as u64;
                        peer_table.entry(*peer_key).or_default().push(delay);
                    }
                    Entry::Vacant(_) => {
                        // Peer did not report this block this round; assign max delay
                        peer_table.entry(*peer_key).or_default().push(u64::MAX);
                    }
                }
            }
        }
        (peer_table, perigee_peers)
    }

    pub fn log_statistics(&self, peer_by_address: &HashMap<SocketAddr, Peer>) {
        let (perigee_ts, rg_ts): (Vec<_>, Vec<_>) =
            peer_by_address.values().filter(|p| p.is_perigee() || p.is_random_graph()).partition_map(|p| {
                if p.is_perigee() {
                    itertools::Either::Left((p.key(), p.perigee_timestamps()))
                } else {
                    itertools::Either::Right((p.key(), p.perigee_timestamps()))
                }
            });

        let (mut p_delays, mut rg_delays, mut p_wins, mut rg_wins, mut ties) = (vec![], vec![], 0usize, 0usize, 0usize);

        for (hash, ts) in self.iterate_verified_first_seen() {
            let p_d = perigee_ts.iter().filter_map(|(_, hm)| hm.get(hash).map(|t| t.duration_since(*ts).as_millis() as u64)).min();
            let rg_d = rg_ts.iter().filter_map(|(_, hm)| hm.get(hash).map(|t| t.duration_since(*ts).as_millis() as u64)).min();
            match (p_d, rg_d) {
                (Some(p), Some(rg)) => {
                    p_delays.push(p);
                    rg_delays.push(rg);
                    match p.cmp(&rg) {
                        Ordering::Less => p_wins += 1,
                        Ordering::Greater => rg_wins += 1,
                        Ordering::Equal => ties += 1,
                    }
                }
                (Some(p), None) => {
                    p_delays.push(p);
                    p_wins += 1;
                }
                (None, Some(rg)) => {
                    rg_delays.push(rg);
                    rg_wins += 1;
                }
                _ => {}
            }
        }

        if p_delays.is_empty() && rg_delays.is_empty() {
            debug!("PerigeeManager Statistics: No data available for this round");
            return;
        }

        let stats = |d: &mut [u64]| -> (usize, f64, u64, u64, u64, u64, u64, u64) {
            if d.is_empty() {
                return (0, 0.0, 0, 0, 0, 0, 0, 0);
            }
            d.sort_unstable();
            let n = d.len();
            let pct = |p: f64| d[((n as f64 * p) as usize).min(n - 1)];
            (n, d.iter().sum::<u64>() as f64 / n as f64, d[n / 2], d[0], d[n - 1], pct(0.90), pct(0.95), pct(0.99))
        };

        let (pc, pm, pmed, pmin, pmax, p90, p95, p99) = stats(&mut p_delays);
        let (rc, rm, rmed, rmin, rmax, r90, r95, r99) = stats(&mut rg_delays);
        let total = p_wins + rg_wins + ties;
        let pct = |p, t| if t == 0 { 0.0 } else { p as f64 / t as f64 * 100.0 };
        let imp = |p: f64, r: f64| if r == 0.0 { 0.0 } else { (r - p) / r * 100.0 };

        // Build rank distribution for leveraged peers
        let (mut peer_table, _) = self.build_table(peer_by_address);
        let rank_table = Self::build_rank_table(&peer_table);
        let num_peers = peer_table.len();
        let mut rank_summary = String::new();
        for pk in self.last_round_leveraged_peers.iter() {
            if let Some(ranks) = rank_table.get(pk) {
                let score = Self::score_peer_from_ranks(ranks, num_peers);
                let top3: Vec<String> =
                    score.rank_counts.iter().take(3).enumerate().map(|(i, c)| format!("R{}={}", i + 1, c)).collect();
                rank_summary.push_str(&format!("\n        {:?}: {}", pk, top3.join(", ")));
            }
        }
        // Clean up peer_table to avoid unused warning
        peer_table.clear();

        info!(
            "[{}]\n\
     ════════════════════════════════════════════════════════════════════════════ \n\
                           PERIGEE STATISTICS - Round {:4}                     \n\
     ════════════════════════════════════════════════════════════════════════════ \n\
      Config: Out={:<2} Leverage={:<2} Explore={:<2} Duration={:<5}s                   \n\
      Peers:  Perigee={:<2} ({:<5} blks) | Random={:<2} ({:<5} blks)                 \n\
      Blocks: Verified={:<5} | Seen={:<5}                                           \n\
     ════════════════════════════════════════════════════════════════════════════ \n\
      BLOCK DELIVERY RACE                                                         \n\
        Perigee Wins:       {:5} ({:5.1}%)                                         \n\
        Random Graph Wins:  {:5} ({:5.1}%)                                         \n\
        Ties:               {:5} ({:5.1}%)                                         \n\
     ════════════════════════════════════════════════════════════════════════════ \n\
      DELAY STATISTICS (ms)        │  Perigee  │ Random Graph │ Improvement      \n\
     ─────────────────────────────┼───────────┼──────────────┼────────────────── \n\
      Count                        │ {:9} │ {:12} │                 \n\
      Mean                         │ {:9.2} │ {:12.2} │ {:7.2} ({:5.1}%) \n\
      Median                       │ {:9} │ {:12} │ {:7} ({:5.1}%) \n\
      Min                          │ {:9} │ {:12} │                 \n\
      Max                          │ {:9} │ {:12} │                 \n\
      P90                          │ {:9} │ {:12} │ {:7} ({:5.1}%) \n\
      P95                          │ {:9} │ {:12} │                 \n\
      P99                          │ {:9} │ {:12} │                 \n\
     ════════════════════════════════════════════════════════════════════════════ \n\
      LEVERAGED PEER RANKS (top 3){}                                              \n\
     ════════════════════════════════════════════════════════════════════════════ ",
            IDENT,
            self.round_counter,
            self.config.perigee_outbound_target,
            self.config.leverage_target,
            self.config.exploration_target,
            self.config.round_duration.as_secs(),
            perigee_ts.len(),
            perigee_ts.iter().map(|(_, hm)| hm.len()).sum::<usize>(),
            rg_ts.len(),
            rg_ts.iter().map(|(_, hm)| hm.len()).sum::<usize>(),
            self.verified_blocks.len(),
            self.first_seen.len(),
            p_wins,
            pct(p_wins, total),
            rg_wins,
            pct(rg_wins, total),
            ties,
            pct(ties, total),
            pc,
            rc,
            pm,
            rm,
            rm - pm,
            imp(pm, rm),
            pmed,
            rmed,
            rmed as i64 - pmed as i64,
            imp(pmed as f64, rmed as f64),
            pmin,
            rmin,
            pmax,
            rmax,
            p90,
            r90,
            r90 as i64 - p90 as i64,
            imp(p90 as f64, r90 as f64),
            p95,
            r95,
            p99,
            r99,
            rank_summary,
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use kaspa_consensus_core::config::params::TESTNET_PARAMS;
    use kaspa_hashes::Hash;
    use kaspa_p2p_lib::PeerOutboundType;
    use kaspa_p2p_lib::test_utils::RouterTestExt;
    use kaspa_utils::networking::PeerId;

    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;
    use uuid::Uuid;

    /// Generates a unique Router wit incremental IPv4 SocketAddr and PeerId for testing purposes.
    fn generate_unique_router(time_connected: Instant) -> std::sync::Arc<kaspa_p2p_lib::Router> {
        static ROUTER_COUNTER: AtomicU64 = AtomicU64::new(1);

        let id = ROUTER_COUNTER.fetch_add(1, Ordering::Relaxed);
        let ip_seed = id;
        let octet1 = ((ip_seed >> 24) & 0xFF) as u8;
        let octet2 = ((ip_seed >> 16) & 0xFF) as u8;
        let octet3 = ((ip_seed >> 8) & 0xFF) as u8;
        let octet4 = (ip_seed & 0xFF) as u8;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(octet1, octet2, octet3, octet4)), TESTNET_PARAMS.default_p2p_port());
        let peer_id = PeerId::new(Uuid::from_u128(id as u128));
        RouterTestExt::test_new(peer_id, addr, Some(PeerOutboundType::Perigee), time_connected)
    }

    // Helper to generate a default PerigeeConfig for testing purposes
    fn generate_config() -> PerigeeConfig {
        PerigeeConfig::new(8, 4, 2, 30, std::time::Duration::from_secs(30), true, true, TESTNET_PARAMS.bps())
    }

    // Helper to generate a globally unique block hash
    fn generate_unique_block_hash() -> Hash {
        static HASH_COUNTER: AtomicU64 = AtomicU64::new(1);

        Hash::from_u64_word(HASH_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    #[test]
    fn test_insertions() {
        let routers = (0..2).map(|_| generate_unique_router(Instant::now())).collect::<Vec<_>>();
        let manager = PerigeeManager::new(generate_config(), Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let now: Vec<_> = (0..4).map(|_| Instant::now()).collect();
        let block_hashes: Vec<_> = (0..4).map(|_| generate_unique_block_hash()).collect();
        for (i, (now, block_hash)) in now.iter().zip(block_hashes.iter()).enumerate() {
            manager.lock().insert_perigee_timestamp(&routers[(i + 1) % 2].clone(), *block_hash, *now, (i + 1) % 2 == 0);
        }

        let manager = manager.lock();
        // Check first_seen and router timestamps
        for (i, (now, block_hash)) in now.iter().zip(block_hashes.iter()).enumerate() {
            let ts = manager.first_seen.get(block_hash).unwrap();
            assert_eq!(ts, now);
            // Only the router that received the block should have it, and timestamp should match
            let idx = (i + 1) % 2;
            if idx == 0 {
                assert!(manager.verified_blocks.contains(block_hash), "Block should be verified for even indices");
            } else {
                assert!(!manager.verified_blocks.contains(block_hash), "Block should not be verified for odd indices");
            }
            let perigee_timestamps = &routers[idx].perigee_timestamps();
            let router_ts = perigee_timestamps.get(block_hash).unwrap();
            assert_eq!(router_ts, now, "Router's perigee_timestamps should match inserted timestamp");
            assert_eq!(router_ts, ts, "Router's perigee_timestamps should match manager's first_seen");
            // The other router should NOT have this block_hash
            let other_perigee_timestamps = &routers[1 - idx].perigee_timestamps();
            assert!(!other_perigee_timestamps.contains_key(block_hash), "Other router should not have this block hash");
        }

        // Check lengths
        assert_eq!(manager.first_seen.len(), block_hashes.len(), "first_seen should have all inserted blocks");
        assert_eq!(manager.verified_blocks.len(), block_hashes.len().div_ceil(2), "verified_blocks should have half the blocks");
        for router in &routers {
            let perigee_timestamps = router.perigee_timestamps();
            assert_eq!(perigee_timestamps.len(), block_hashes.len() / 2, "Each router should have half the block hashes");
        }
    }

    #[test]
    fn test_trim_peers() {
        // Set-up environment
        let config = generate_config();
        let manager = PerigeeManager::new(config.clone(), Arc::new(std::sync::atomic::AtomicBool::new(false)));
        let leverage_target = config.leverage_target;
        let perigee_outbound_target = config.perigee_outbound_target;
        let excused_count = perigee_outbound_target + 1 - leverage_target;
        // Set up so that all non-leveraged, non-excused peers are needed to fill the outbound target, so only one excused peer can be trimmed
        let total_peers = leverage_target + excused_count;

        // Create leveraged peers (should not be trimmed)
        let now = Instant::now() - std::time::Duration::from_secs(3600);
        let mut routers = Vec::new();
        for _ in 0..leverage_target {
            routers.push(generate_unique_router(now));
        }

        // Create excused peers, joined after round start, should be excused and and only trimmed as a last resort (ordered by connection time)
        let mut excused_routers = Vec::new();
        for i in 0..excused_count {
            let t = now + std::time::Duration::from_secs(10 + i as u64);
            excused_routers.push(generate_unique_router(t));
        }

        // Build all peers
        let mut peers = HashMap::new();
        for router in routers.iter().chain(excused_routers.iter()) {
            let peer = Peer::from((&**router, true));
            peers.insert(router.key(), peer.clone());
        }

        // Build peer_by_addr
        let mut peer_by_addr = HashMap::new();
        for peer in peers.values() {
            peer_by_addr.insert(peer.net_address(), peer.clone());
        }

        // Set leveraged and excused peers in manager
        manager.lock().set_initial_persistent_peers(routers.iter().map(|r| r.key()).collect());
        manager.lock().round_start = now; // Set round start to 'now' for excused logic

        // Call trim_peers
        let to_remove = manager.lock().trim_peers(Arc::new(peer_by_addr));

        // Assert correct number trimmed
        let expected_trim = total_peers - perigee_outbound_target;
        assert_eq!(to_remove.len(), expected_trim, "Should trim down to perigee_outbound_target");

        // Assert no leveraged peer is trimmed
        let leveraged_keys: Vec<_> = routers.iter().map(|r| r.key()).collect();
        for k in &to_remove {
            assert!(!leveraged_keys.contains(k), "Leveraged peer should not be trimmed");
        }

        // Assert that exactly one excused peer is trimmed (evicted), and the rest are not
        let excused_keys: Vec<_> = excused_routers.iter().map(|r| r.key()).collect();
        let excused_trimmed: Vec<_> = excused_keys.iter().filter(|k| to_remove.contains(k)).collect();
        assert_eq!(excused_trimmed.len(), 1, "Exactly one excused peer should be trimmed as a last resort");
        // The rest of the excused peers should not be trimmed
        let excused_not_trimmed: Vec<_> = excused_keys.iter().filter(|k| !to_remove.contains(k)).collect();
        assert_eq!(excused_not_trimmed.len(), excused_count - 1, "All but one excused peer should remain");
        // Check excused ordering by connection time (still valid for remaining excused)
        let mut excused_peers: Vec<_> = excused_routers.iter().map(|r| peers.get(&r.key()).unwrap()).collect();
        excused_peers.sort_by_key(|p| p.connection_started());
        for w in excused_peers.windows(2) {
            assert!(w[0].connection_started() <= w[1].connection_started(), "Excused peers should be ordered by connection time");
        }
    }

    #[test]
    fn test_rank_table_and_scoring() {
        // Test build_rank_table with known delays and ties
        // 3 peers, 4 blocks:
        //   Block 0: peer A=10, peer B=20, peer C=10  → ranks: A=1, C=1, B=3
        //   Block 1: peer A=50, peer B=30, peer C=40  → ranks: B=1, C=2, A=3
        //   Block 2: peer A=5,  peer B=5,  peer C=5   → ranks: all=1 (three-way tie)
        //   Block 3: peer A=MAX,peer B=10, peer C=20  → ranks: B=1, C=2, A=3 (MAX = worst)
        use kaspa_utils::networking::IpAddress;

        let pk_a = PeerKey::new(PeerId::new(Uuid::from_u128(100)), IpAddress::from(Ipv4Addr::new(10, 0, 0, 1)), 16111);
        let pk_b = PeerKey::new(PeerId::new(Uuid::from_u128(101)), IpAddress::from(Ipv4Addr::new(10, 0, 0, 2)), 16111);
        let pk_c = PeerKey::new(PeerId::new(Uuid::from_u128(102)), IpAddress::from(Ipv4Addr::new(10, 0, 0, 3)), 16111);

        let mut peer_table: HashMap<PeerKey, Vec<u64>> = HashMap::new();
        peer_table.insert(pk_a, vec![10, 50, 5, u64::MAX]);
        peer_table.insert(pk_b, vec![20, 30, 5, 10]);
        peer_table.insert(pk_c, vec![10, 40, 5, 20]);

        let rank_table = PerigeeManager::build_rank_table(&peer_table);

        // Verify ranks for peer A: [1, 3, 1, 3]
        assert_eq!(rank_table[&pk_a], vec![1, 3, 1, 3]);
        // Verify ranks for peer B: [3, 1, 1, 1]
        assert_eq!(rank_table[&pk_b], vec![3, 1, 1, 1]);
        // Verify ranks for peer C: [1, 2, 1, 2]
        assert_eq!(rank_table[&pk_c], vec![1, 2, 1, 2]);

        // Test scoring from ranks (max_rank = 3)
        let score_a = PerigeeManager::score_peer_from_ranks(&rank_table[&pk_a], 3);
        let score_b = PerigeeManager::score_peer_from_ranks(&rank_table[&pk_b], 3);
        let score_c = PerigeeManager::score_peer_from_ranks(&rank_table[&pk_c], 3);

        // A: rank1=2, rank2=0, rank3=2
        assert_eq!(score_a.rank_counts, vec![2, 0, 2]);
        // B: rank1=3, rank2=0, rank3=1
        assert_eq!(score_b.rank_counts, vec![3, 0, 1]);
        // C: rank1=2, rank2=2, rank3=0
        assert_eq!(score_c.rank_counts, vec![2, 2, 0]);

        // Comparison: B best (3 rank-1 wins), then C (2 rank-1 but 2 rank-2), then A (2 rank-1 but 2 rank-3)
        assert!(score_b < score_c, "B should be better than C (more rank-1 wins)");
        assert!(score_c < score_a, "C should be better than A (more rank-2 wins)");
    }

    #[test]
    fn test_rank_score_ordering() {
        // More rank-1 wins = better (Less in ordering)
        let s1 = RankScore::new(vec![10, 5, 2]);
        let s2 = RankScore::new(vec![8, 7, 2]);
        assert!(s1 < s2, "More rank-1 wins should be better");

        // Equal rank-1, more rank-2 = better
        let s3 = RankScore::new(vec![10, 5, 2]);
        let s4 = RankScore::new(vec![10, 3, 4]);
        assert!(s3 < s4, "Equal rank-1, more rank-2 should be better");

        // Empty = worst
        let empty = RankScore::EMPTY;
        let some = RankScore::new(vec![1, 0, 0]);
        assert!(some < empty, "Any score should be better than empty");
    }

    #[test]
    fn test_perigee_round_leverage_and_eviction() {
        run_round(false);
    }

    #[test]
    fn test_perigee_round_skips_while_ibd_running() {
        run_round(true);
    }

    fn run_round(ibd_running: bool) {
        kaspa_core::log::try_init_logger("debug");

        // Set up environment
        let is_ibd_running = Arc::new(std::sync::atomic::AtomicBool::new(ibd_running));
        let mut config = generate_config();
        let now = Instant::now() - std::time::Duration::from_secs(3600);
        let peer_count = config.perigee_outbound_target;
        let blocks_per_router = 300;
        config.expected_blocks_per_round = blocks_per_router as u64;
        let manager = PerigeeManager::new(config, is_ibd_running);
        let mut routers = Vec::new();
        for _ in 0..peer_count {
            let router = generate_unique_router(now);
            routers.push(router);
        }

        // Insert blocks using a deterministic delay pattern:
        // Peers 0..leverage_target get distinct low delays (each peer gets a unique rank per block)
        // Peers leverage_target..peer_count get very high delays (always worst ranks)
        let leverage_target = manager.lock().config.leverage_target;
        for block_idx in 0..blocks_per_router {
            let block_hash = generate_unique_block_hash();
            let base_ts = now + std::time::Duration::from_millis((block_idx as u64) * 10_000);
            for (i, router) in routers.iter().enumerate() {
                let ts = if i < leverage_target {
                    // Each peer gets a unique delay bucket: peer 0 → 0-9ms, peer 1 → 10-19ms, etc.
                    let bucket_start = (i as u64) * 10;
                    let delay = bucket_start + (block_idx as u64 % 10);
                    base_ts + std::time::Duration::from_millis(delay)
                } else {
                    base_ts + std::time::Duration::from_millis(100_000 + (i as u64) * 10)
                };
                manager.lock().insert_perigee_timestamp(router, block_hash, ts, true);
            }
        }

        assert!(manager.lock().verified_blocks.len() == blocks_per_router);

        // Build peers and peer_by_addr after all timestamps are inserted
        let mut peers = HashMap::new();
        for router in &routers {
            let peer = Peer::from((&**router, true));
            peers.insert(router.key(), peer.clone());
        }
        let mut peer_by_addr = HashMap::new();
        for peer in peers.values() {
            peer_by_addr.insert(peer.net_address(), peer.clone());
        }

        // Execute a perigee round
        let (leveraged, evicted, has_leveraged_changed) = manager.lock().evaluate_round(&peer_by_addr);
        debug!("Leveraged peers: {:?}", leveraged);
        debug!("Evicted peers: {:?}", evicted);

        // Perform assertions:
        if ibd_running {
            // While IBD is running, no leveraging or eviction should occur
            assert!(!has_leveraged_changed, "Leveraging should be skipped while IBD is running");
            assert!(leveraged.is_empty(), "No peers should be leveraged while IBD is running");
            assert!(evicted.is_empty(), "No peers should be evicted while IBD is running");
            return;
        };
        assert!(has_leveraged_changed, "Leveraging should not be skipped in this test");
        assert_eq!(
            leveraged,
            routers.iter().take(leverage_target).map(|r| r.key()).collect::<Vec<PeerKey>>(),
            "Leverage set should match actual deterministic selection (order and membership)"
        );
        // No leveraged peer should be evicted
        assert!(leveraged.iter().all(|p| !evicted.contains(p)), "No leveraged peer should be evicted");
        assert_eq!(evicted.len(), manager.lock().config.exploration_target);

        // Reset round.
        manager.lock().start_new_round();
        // Ensure state is cleared.
        assert!(manager.lock().verified_blocks.is_empty(), "Verified blocks should be cleared after starting new round");
        assert!(manager.lock().first_seen.is_empty(), "First seen timestamps should be cleared after starting new round");
    }

    #[test]
    fn test_variable_mining_share() {
        // Scenario: Peer 0 only reports 30% of blocks but is rank-1 on all of them (fastest).
        // Peer 1 reports 100% of blocks but is always rank-2 (second fastest).
        // Peers 2-7 report 100% of blocks with very high delays.
        // Expectation: Peer 0 should still be selected (rank-1 on its blocks), along with peer 1.
        kaspa_core::log::try_init_logger("debug");

        let is_ibd_running = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut config = generate_config(); // 8 peers, leverage=4
        let now = Instant::now() - std::time::Duration::from_secs(3600);
        let peer_count = config.perigee_outbound_target;
        let blocks_per_router = 300;
        config.expected_blocks_per_round = blocks_per_router as u64;
        let manager = PerigeeManager::new(config, is_ibd_running);
        let mut routers = Vec::new();
        for _ in 0..peer_count {
            routers.push(generate_unique_router(now));
        }

        let leverage_target = manager.lock().config.leverage_target;

        for block_idx in 0..blocks_per_router {
            let block_hash = generate_unique_block_hash();
            let base_ts = now + std::time::Duration::from_millis((block_idx as u64) * 10_000);

            for (i, router) in routers.iter().enumerate() {
                if i == 0 {
                    // Peer 0: only reports 30% of blocks, but fastest (delay=1ms) when it does
                    if block_idx < (blocks_per_router * 30 / 100) {
                        let ts = base_ts + std::time::Duration::from_millis(1);
                        manager.lock().insert_perigee_timestamp(router, block_hash, ts, true);
                    } else {
                        // Peer 0 does NOT report this block — it will get u64::MAX → worst rank
                        // But still need to insert for other peers to have data, so insert via other peers only
                    }
                } else if i == 1 {
                    // Peer 1: reports all blocks, second fastest (delay=5ms)
                    let ts = base_ts + std::time::Duration::from_millis(5);
                    manager.lock().insert_perigee_timestamp(router, block_hash, ts, true);
                } else {
                    // Peers 2-7: all blocks, very slow (delay=100,000ms+)
                    let ts = base_ts + std::time::Duration::from_millis(100_000 + (i as u64) * 10);
                    manager.lock().insert_perigee_timestamp(router, block_hash, ts, true);
                }
            }
        }

        // Build peers
        let mut peer_by_addr = HashMap::new();
        for router in &routers {
            let peer = Peer::from((&**router, true));
            peer_by_addr.insert(peer.net_address(), peer);
        }

        let (leveraged, evicted, has_leveraged_changed) = manager.lock().evaluate_round(&peer_by_addr);
        debug!("Variable share test - Leveraged: {:?}", leveraged);
        debug!("Variable share test - Evicted: {:?}", evicted);

        assert!(has_leveraged_changed, "Should have leveraged");
        assert_eq!(leveraged.len(), leverage_target);

        // Peer 0 (30% miner) and Peer 1 (100% coverage, rank-2) should both be selected
        let peer0_key = routers[0].key();
        let peer1_key = routers[1].key();
        assert!(leveraged.contains(&peer0_key), "Peer 0 (30% coverage, rank-1 on reported blocks) should be leveraged");
        assert!(leveraged.contains(&peer1_key), "Peer 1 (100% coverage, rank-2) should be leveraged");

        // Peer 0 should be selected first (it has rank-1 on 30% of blocks, no one else is consistently rank-1)
        // Actually peer 1 has rank-1 on 70% of blocks where peer 0 didn't report (peer 1 is fastest there)
        // So at rank-1 level: peer 1 has ~210 rank-1 blocks, peer 0 has ~90 rank-1 blocks
        // Peer 1 should be selected first at rank-1, then peer 0 should still be selected
        // (either still at rank-1 if 95% not yet covered, or at rank-2)
        assert!(
            leveraged.iter().position(|p| p == &peer1_key).unwrap() < leveraged.iter().position(|p| p == &peer0_key).unwrap(),
            "Peer 1 (more rank-1 blocks) should be selected before peer 0"
        );

        // No leveraged peer should be evicted
        assert!(leveraged.iter().all(|p| !evicted.contains(p)), "No leveraged peer should be evicted");
    }
}
