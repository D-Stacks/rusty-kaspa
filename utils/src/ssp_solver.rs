use indexmap::{set::MutableValues, IndexMap, IndexSet};
use itertools::{Combinations, Itertools};
use kaspa_core::{assert, debug, info, trace};
use rand::{
    rngs::SmallRng, seq::{index::{self, sample, sample_weighted}, IteratorRandom, SliceRandom}, Rng, SeedableRng
};
use triggered::Trigger;
use uuid::timestamp::context;
use std::{borrow::BorrowMut, cmp::min, collections::{HashSet, VecDeque}, mem::{replace, swap}, ops::AddAssign, thread::{self, Thread}, time::{Duration, Instant}};
use crate::{as_slice::{AsMutSlice, AsSlice}, iter, triggers::DuplexTrigger, vec::VecExtensions};

#[derive(Debug, Clone)]
struct SubsetSum {
    pub subset: IndexSet<usize>,
    pub sum: u64,
}

impl SubsetSum {
    fn update(&mut self, old_index: usize, new_index: usize, new_val: u64) {
        self.subset.swap_remove(&old_index);
        self.subset.insert(new_index);
        self.sum = new_val;
    }    

    fn contains(&self, index: usize) -> bool {
        self.subset.contains(&index)
    }

    fn push(&mut self, index: usize, val: u64) {
        self.subset.insert(index);
        self.sum = val;
    }
}

#[derive(Debug, Clone)]
struct Context<'a> {   
    input: &'a [u64],
    target: u64,
    subset_solution_size: usize,
    filtered_input: Vec<u64>,
    initial_subset_sum: SubsetSum,
}


impl<'a> Context<'a> {
    /// Builds the context for the subset sum problem
    /// This function pre-processes the input data, and runs in `O(n)` time for the worst case. 
    /// it is required for `approximate_subset_sum` to be called.
    /// abstracting this allows for parallelization of `approximate_subset_sum`, in theory.
    /// Without re-procesing the input data for each call.
    fn build_context(input: &'a [u64], target: u64) -> Self {
        let mut subset_solution_size = 0;
        let mut filtered_input = vec![];
        let mut best_subset_sum = SubsetSum { subset: IndexSet::new(), sum: 0 };    

        for (i, val) in input.iter().copied().enumerate() {
            if subset_solution_size == 0 { // we haven't found the smallest possible subset yet
                best_subset_sum.push(i, best_subset_sum.sum + val);
                if best_subset_sum.sum >= target {
                    subset_solution_size = i + 1;
                };
            } else {

                if best_subset_sum.sum - input[*best_subset_sum.subset.last().unwrap()] + input[i] < target {
                    // all below this "current_val" & "i" cannot be part of the `best_subset_sum`, while being in the smallest subset. 

                    // We expect the last added index to be the largest index in the subset.
                    debug_assert_eq!(best_subset_sum.subset.iter().max().unwrap(), best_subset_sum.subset.last().unwrap());
                    
                    let last = best_subset_sum.subset.pop().unwrap();
                    best_subset_sum.push(filtered_input.len() - 1, best_subset_sum.sum - input[last] + filtered_input.last().unwrap());
                    // we may break here, and end the loop, i.e. filter out the rest of the input.
                    break;
                };
    
                // this is a continues duplicate, larger then the expected solution size, so we can skip it.
                // note: since we iterated past the subset_solution_size, `i - subset_solution_size` should never be out of bounds.
                if input[i - subset_solution_size] == input[i] {
                    continue;
                };
            };
            filtered_input.push(val);
        };

        Self {
            input,
            target,
            subset_solution_size,
            filtered_input, 
            initial_subset_sum: if subset_solution_size > 0 { best_subset_sum } else { SubsetSum { subset: IndexSet::new(), sum: 0 } }
        }
    }

    /// check if the context is solvable
    fn is_solvable(&self) -> bool {
        self.subset_solution_size > 0
    }
}

/// try and minimize the distance to the target sum, 
/// by replacing the value at the `subset_index` with a value that is as close to the target sum as possible, while being above it.
/// This is done via binary searching.
fn minimize_to_target(ctx: &Context, solution: &mut SubsetSum, filtered_input_index: usize) -> Option<(usize, u64)> {

    let removed_val = ctx.filtered_input[filtered_input_index];
    let target_val = ctx.target - (solution.sum - removed_val); // make sure we point to the minimal value, in the worst case. 
    
    
    // two things to consider here:
    // if we point above below the target val, we will be under the target sum.
    // if we point to a val above or the same as the removed val, the change will result in a larger or same diff to the target sum then before. 
    
    // below this index, are all values that are larger then the target value.
    let pp = ctx.filtered_input.partition_point(|probe| target_val <= *probe);

    if pp == 0 {
        // we only have values to choose from that are less then the target value, so we may break with None. 
        return None;
    };

    for ((i, val)) in ctx.filtered_input[..pp]
    .iter()
    .rev()
    .enumerate()
    .take_while(|(_, val)| target_val <= **val && **val < removed_val) // we are only interested in values reducing dist to target without exceeding it. 
    {

        if !solution.subset.contains(&(pp - i - 1)) {
            // we are not already pointing to the target val, so we may break with it.
            debug_assert!(ctx.filtered_input[pp - i - 1] == *val);
            return Some((pp - i - 1, solution.sum -removed_val + val));
        };
    };
    // we have no range to choose from, so we may break with None.
    None
    
    }


fn generate_random_subset_sum(ctx: &Context, rng: &mut impl Rng) -> SubsetSum {
    let subset = sample(rng, ctx.filtered_input.len(), ctx.subset_solution_size).into_iter().collect::<IndexSet<usize>>();
    let sum = subset.iter().map(|ref_index| ctx.filtered_input[*ref_index]).sum();
    SubsetSum {subset, sum}
}

fn get_input_indexes_from_subset_sum(ctx: &Context, subset_sum: &SubsetSum) -> Vec<usize> {
    let mut input_indexes = Vec::with_capacity(subset_sum.subset.len()); 
    for filtered_index in subset_sum.subset.iter() {
        let mut o_index = ctx.input.partition_point( |probe| probe > &ctx.filtered_input[*filtered_index]);
        //assert!(ctx.input[o_index] == ctx.filtered_input[*filtered_index], "expected the input value to be equal to the filtered input value, but found input value: {}, filtered input value: {}", ctx.input[o_index], ctx.filtered_input[*filtered_index]);
        while input_indexes.contains(&o_index) {
            o_index += 1;
        };
        //assert!(ctx.input[o_index] == ctx.filtered_input[*filtered_index], "expected the input value to be equal to the filtered input value, but found input value: {}, filtered input value: {}", ctx.input[o_index], ctx.filtered_input[*filtered_index]);
        input_indexes.push(o_index);
    };
    input_indexes.sort_unstable();
    input_indexes
}

fn is_better_subset_sum(ctx: &Context, lhs: &SubsetSum, rhs: &SubsetSum) -> bool {
    //assert!(lhs.sum >= ctx.target, "expected the lhs sum to be above the target sum, but found lhs sum: {}, target sum: {}", lhs.sum, ctx.target);
    //assert!(rhs.sum >= ctx.target, "expected the rhs sum to be above the target sum, but found rhs sum: {}, target sum: {}", rhs.sum, ctx.target);
    lhs.sum < rhs.sum
}

fn check_exact_subset_sum(ctx: &Context, subset_sum: &SubsetSum) -> bool {
    subset_sum.sum == ctx.target
}

fn is_subset_sum_above_target(ctx: &Context, subset_sum: &SubsetSum) -> bool {
    subset_sum.sum >= ctx.target
}

fn find_global_subset_sum(ctx: &Context, subset_sum: &mut SubsetSum, rng: &mut impl Rng) {

    // No need to search if we are already above the target sum.
    if is_subset_sum_above_target(ctx, subset_sum) {
        return;
    }

    loop {
        let iter = subset_sum.subset.clone().into_iter();
        for to_remove_index in iter {
                if is_subset_sum_above_target(ctx, &subset_sum) {
                    return;
                }
                if to_remove_index == 0 {
                    continue;
                }
                let to_add_index = rng.gen_range(0..to_remove_index);
                if subset_sum.contains(to_add_index) {
                    continue;
                }
                let to_remove_val = ctx.filtered_input[to_remove_index];
                let to_add_val = ctx.filtered_input[to_add_index];
                // we expect to climb 
                debug_assert!(to_remove_val < to_add_val);
                subset_sum.update(to_remove_index, to_add_index, subset_sum.sum - to_remove_val + to_add_val);
            };
        }
}

fn optimize_subset_sum(ctx: &Context, subset_sum: &mut SubsetSum, rng: &mut impl Rng) {

    // We expect to only optimize on solutions >= target sum.
    debug_assert!(subset_sum.sum >= ctx.target, "expected the solution sum to be above the target sum, but found subset_sum sum: {}, target sum: {}", subset_sum.sum, ctx.target);

    let mut mini_misses = 0; 

    loop {
        let iter = subset_sum.subset.clone().into_iter();
        for i in iter {
            
                // check if we should continue the search, or not. 
                if mini_misses == ctx.subset_solution_size  {
                    debug_assert!(minimize_to_target(&ctx, subset_sum, i).is_none(), "expected this to already be tried in a former iteration");
                    return;
                }

                debug_assert!(subset_sum.sum >= ctx.target, "expected the solution sum to be above the target sum, but found subset_sum sum: {}, target sum: {}", subset_sum.sum, ctx.target);
                if let Some((new_index, new_sum)) = minimize_to_target(&ctx, subset_sum, i) {
                    // We expect minimization to happen on Some value. 
                    debug_assert!(new_sum < subset_sum.sum);
                    debug_assert!(!subset_sum.contains(new_index));
                    
                    subset_sum.update(i, new_index, new_sum);

                    if check_exact_subset_sum(&ctx, &subset_sum) {
                        return;
                    } 
                    mini_misses = 0;
                } else {
                    // we did not find a better subset sum, increment the miss counter.
                    mini_misses += 1;
                }; 
            };
        }
}

/// To use this function build an appropriate context with `Context::build_context` first, and then call this function.
///
/// This handles a special case of the subset sum problem, whereby
/// 1) the input is sorted in descending order.
/// 2) We are only interested in being at, or as close to the target sum as possible, while being above it.
/// 3) We are only interested in the smallest subset that is at or above the target sum.
/// 4) We don't care about finding multiple subset sum solutions, only the best one.
/// 
/// The returned [`Some`] consists of a tuple, with:
/// 1) the indexes of the input (as passed into the supplied [`Context`]), that are part of the best found subset, 
/// 2) the sum of the subset.
/// 
/// If None is returned, the problem is not solvable i.e. there is no subset that is larger then the target sum..
fn approximate_subset_sum_with_context<'a>(ctx: Context, iterations: usize, rng: &mut impl Rng) -> Option<(Vec<usize>, u64)> {
    
    debug_assert_subset_sum_consistency(&ctx, &ctx.initial_subset_sum, ctx.is_solvable());
    
    if !ctx.is_solvable() {
        debug!("SSP: not solvable");
        return None;
    };

    if ctx.subset_solution_size == ctx.filtered_input.len() {
        debug!("SSP: only possible solution found {},", ctx.initial_subset_sum.sum);
        return Some((get_input_indexes_from_subset_sum(&ctx, &ctx.initial_subset_sum), ctx.initial_subset_sum.sum));
    } 

    if ctx.initial_subset_sum.sum == ctx.target {
        debug!("SSP: exact solution found: {},", ctx.initial_subset_sum.sum);
        return Some((get_input_indexes_from_subset_sum(&ctx, &ctx.initial_subset_sum), ctx.initial_subset_sum.sum));
    };
    //info!("subset solution size: {}", ctx.subset_solution_size);
    let mut best_subset_sum = ctx.initial_subset_sum.clone();
    let mut exchange_count = 0;

    let mut i = 0;
    
    while i < iterations {

        i += 1;

        let mut current_subset_sum = generate_random_subset_sum(&ctx, rng);
        debug_assert_subset_sum_consistency(&ctx, &current_subset_sum, false);

        find_global_subset_sum(&ctx, &mut current_subset_sum, rng);
        debug_assert_subset_sum_consistency(&ctx, &current_subset_sum, true);
        
        optimize_subset_sum(&ctx, &mut current_subset_sum, rng);
        debug_assert_subset_sum_consistency(&ctx, &current_subset_sum, true);

        if is_better_subset_sum(&ctx, &current_subset_sum, &best_subset_sum) {
            debug!("SSP: Found better solution: {}, global searches: {}, exchange count {}", best_subset_sum.sum, i, exchange_count);
            best_subset_sum = current_subset_sum;
            exchange_count += 1;
            if check_exact_subset_sum(&ctx, &best_subset_sum) {
                debug!("SSP: exact solution found: {},", best_subset_sum.sum);
                break;
            };
        };
    };

    debug!("SSP: Finished search: best_subset_sum: {}, global searches: {}, exchange count {}", best_subset_sum.sum, i, exchange_count);
    Some((get_input_indexes_from_subset_sum(&ctx, &best_subset_sum), best_subset_sum.sum))
}


#[cfg(debug_assertions)]
fn debug_assert_subset_sum_consistency(ctx: &Context, subset_sum: &SubsetSum, check_above_target: bool) {
    debug_assert_eq!(subset_sum.sum, subset_sum.subset.iter().map(|i| ctx.filtered_input[*i]).sum::<u64>(), "Sum is not consistant");  // Check the sum is consistent with the subset
    debug_assert_eq!(subset_sum.subset.iter().dedup().count(), ctx.subset_solution_size, "Subset has duplicate indices"); // Check we have no duplicates
    debug_assert_eq!(subset_sum.subset.len(), ctx.subset_solution_size, "Subset len is not consistant"); // Check the subset size is correct
    if check_above_target {
        debug_assert!(subset_sum.sum >= ctx.target, "Subset is under target - not consistant"); // Check the sum is above the target, we expect this at most points in the search.
    }
}

pub fn approximate_subset_sum<'a> (input: &'a [u64], target: u64, iterations: usize) -> Option<(Vec<usize>, u64)> {
    let ctx = Context::build_context(input, target);
    approximate_subset_sum_with_context(ctx, iterations, &mut SmallRng::from_entropy())
} 


/*
pub fn approximate_subset_sum_par<'a>(input: &'a [u64], target: u64, stopper: Stopper, num_of_threads: usize) {
    let trigger = Trigger::new();
    let threads = (0..num_of_threads).map(|_| {
        thread::spawn(move || {
            let ctx = Context::build_context(input, target);
            approximate_subset_sum_with_context(ctx, iterations, &mut SmallRng::from_entropy(), None)
        })
    }).collect::<Vec<_>>();

}
*/

#[cfg(test)]
pub mod test {

    use std::u64;

    use itertools::interleave;
    use kaspa_core::info;
    use rand::{
        distributions::{self, Standard},
        prelude::Distribution,
    };

    use super::*;

    // This is the maximum space we define for the subset sum problem. 
    // it corrosponds to MAX_SOMPI in practice, but used here for generic testing.
    const MAX_SPACE: u64 = 29_000_000_000_000_000; 
    const ITERATIONS: usize = 50;

    #[test]
    fn test_approximate_subset_sum_uniform() {
        kaspa_core::log::try_init_logger("debug, kaspa_utils=debug");
        let test_name: &str = "UNIFORM";
    

        let input_amounts = [
            10, 
            100, 
            1_000, 
            100_000, 
            1_000_000
            ];
        let approximate_k = [
            2,
            8,
            32,
            128,
            512,
            ];
        for input_amount in input_amounts.iter() {
            for k in approximate_k.iter() {
                if k > input_amount {
                    continue;
                }
                let space = MAX_SPACE / input_amount;
                let target = space * k ;
                let distribution = rand_distr::Uniform::new(0, space);
                test_approximate_subset_sum(test_name, space, *input_amount as usize, target, ITERATIONS, distribution.sample_iter(rand::thread_rng()));
            }
        }
    }

    #[test]
    fn test_approximate_subset_sum_gauss_mid() {
        kaspa_core::log::try_init_logger("debug, kaspa_utils=debug");
        let test_name: &str = "GAUSS_MID";

        let input_amounts = [
            10, 
            100, 
            1_000, 
            100_000, 
            1_000_000
            ];
        let approximate_k = [
            2,
            8,
            32,
            128,
            512,
            ];
        for input_amount in input_amounts.iter() {
            for k in approximate_k.iter() {
                if k > input_amount {
                    continue;
                }
                let space = MAX_SPACE / input_amount;
                let std_dev = space as f64 / 2.0;
                let mean = space as f64 / 2.0;
                let target = space as u64 * k * 2;
                let distribution =  rand_distr::Normal::new(mean, std_dev).unwrap();
                test_approximate_subset_sum(test_name, space as u64, *input_amount as usize, target, ITERATIONS, distribution.sample_iter(rand::thread_rng()).map(|val| val as u64));
            }
        }
    }

    #[test]
    fn test_approximate_subset_sum_gauss_low() {
        kaspa_core::log::try_init_logger("debug, kaspa_utils=debug");
        let test_name: &str = "GAUSS_LOW";

        let input_amounts = [
            10, 
            100, 
            1_000, 
            100_000, 
            1_000_000
            ];
        let approximate_k = [
            2,
            8,
            32,
            128,
            512,
            ];
        for input_amount in input_amounts.iter() {
            for k in approximate_k.iter() {
                if k > input_amount {
                    continue;
                }
                let space = MAX_SPACE / input_amount;
                let mean = space as f64 / 9.0;
                let std_dev = mean * 9.0;
                let target = space as u64 * k * 4;
                let distribution =  rand_distr::Normal::new(mean, std_dev).unwrap();
                test_approximate_subset_sum(test_name, space as u64, *input_amount as usize, target, ITERATIONS, distribution.sample_iter(rand::thread_rng()).map(|val| val as u64));
            }
        }
    }

    #[test]
    fn test_approximate_subset_sum_gauss_high() {
        kaspa_core::log::try_init_logger("debug, kaspa_utils=debug");
        let test_name: &str = "GAUSS_HIGH";

        let input_amounts = [
            10, 
            100, 
            1_000, 
            100_000, 
            1_000_000
            ];
        let approximate_k = [
            2,
            8,
            32,
            128,
            512,
            ];
        for input_amount in input_amounts.iter() {
            for k in approximate_k.iter() {
                if k > input_amount {
                    continue;
                }
                let space = MAX_SPACE / input_amount;
                let std_dev = space as f64 / 9.0;
                let mean = space as f64 - space as f64 / 9.0;
                let target = space as u64 * k;
                let distribution =  rand_distr::Normal::new(mean, std_dev).unwrap();
                test_approximate_subset_sum(test_name, space as u64, *input_amount as usize, target, ITERATIONS, distribution.sample_iter(rand::thread_rng()).map(|val| val as u64));
            }
        }
    }

    #[test]
    fn test_approximate_subset_sum_bimodal() {
        kaspa_core::log::try_init_logger("debug, kaspa_utils=debug");
        let test_name: &str = "BIMODAL";

        let input_amounts = [
            10, 
            100, 
            1_000, 
            100_000, 
            1_000_000
            ];
        let approximate_k = [
            2,
            8,
            32,
            128,
            512,
            ];
        for input_amount in input_amounts.iter() {
            for k in approximate_k.iter() {
                if k > input_amount {
                    continue;
                }

                let space = (MAX_SPACE / input_amount) as f64;
                let mean_low = space as f64 / 9.0;
                let std_dev_low = mean_low * 9.0;
                let mean_high = space as f64 - mean_low;
                let std_dev_high = space - mean_high;

                let target = space as u64 * k * 4;
                let distribution =  rand_distr::Normal::new(mean_low, std_dev_low).unwrap();
                let distribution2 =  rand_distr::Normal::new(mean_high, std_dev_high).unwrap();
                let sample = distribution.sample_iter(rand::thread_rng()).map(|val| val as u64).interleave(distribution2.sample_iter(rand::thread_rng()).map(|val| val as u64));
                test_approximate_subset_sum(test_name, space as u64, *input_amount as usize, target, ITERATIONS, sample);
            }
        }

    }

    fn test_approximate_subset_sum(test_name: &str, space: u64, input_amount: usize, target: u64, iterations: usize, sample: impl Iterator<Item = u64>) 
    {
        let mut inputs = Vec::<u64>::with_capacity(input_amount);
        for input in sample.take(input_amount) {
            //info!("input: {}", input);
            inputs.push(input);
        }
        inputs.sort_by(|a, b| b.cmp(a));
        assert!(inputs[0] >= inputs[inputs.len() -1], "expected inputs to be sorted in desc order");
        //info!("inputs: {:?}", inputs);
        // give info on spae, density, target, iterations, and distibution:
        if let Some(result) = approximate_subset_sum(&inputs, target, iterations) {
            let sum = result.0.iter().map(|i| inputs[*i]).sum::<u64>();
            assert_eq!(sum, result.1, "expected the sum of the input data to be equal");
            assert!(sum >= target);
            info!("Result for test with space: {}, input_amount: {}, target: {}, iterations: {} test_name: {}-  sum: {}, k: {} \n", space, input_amount, target, iterations, test_name, sum, result.0.len());
            info!("res: {:?}", result.0);
        } else {
            info!("Result for test with space: {}, input_amount: {}, target: {}, iterations: {}, test_name: {} - None \n", space, input_amount, target, iterations, test_name);
        }
    }
}
