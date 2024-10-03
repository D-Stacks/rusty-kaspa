use itertools::{Combinations, Itertools};
use kaspa_core::{assert, debug, info, trace};
use rand::{
    rngs::SmallRng, seq::{index::{self, sample, sample_weighted}, IteratorRandom, SliceRandom}, Rng, SeedableRng
};
use uuid::timestamp::context;
use std::{borrow::BorrowMut, collections::{HashSet, VecDeque}, cmp::min, mem::{replace, swap}, ops::AddAssign, thread::{self, Thread}, time::Duration};
use crate::{as_slice::{AsMutSlice, AsSlice}, iter, vec::VecExtensions};

/*
/// finds the natural breaks in the data, using the jenks natural breaks optimization algorithm.
/// This is used to cluster the data, and abstract the subset sum problem to a smaller space.
fn jenks_natural_breaks(values: &[u64], nb_class: u32) -> Vec<usize> {
    /// Many thanks to jenks, and https://gist.github.com/mthh/20c599e47e275c8afa04e7c4dde7fe8f for this. 

    // Lets fake a 2D array (square shape matrix) with a 1D vector:
    struct Matrix<T>{
        values: Vec<T>,
        dim: usize
    }

    // ... and implement getter and setter methods using 'unsafe' functions :
    impl<T: PartialEq + Clone> Matrix<T> {
        pub fn new(init_value: T, dim: usize) -> Matrix<T> {
            Matrix { values: vec![init_value; dim * dim], dim: dim }
        }

        #[inline(always)]
        pub fn get(&self, ix: (usize, usize)) -> &T {
            unsafe { self.values.get_unchecked(ix.0 * self.dim + ix.1) }
        }

        #[inline(always)]
        pub fn set(&mut self, ix: (usize, usize), value: T) {
            let mut v = unsafe { self.values.get_unchecked_mut(ix.0 * self.dim + ix.1) };
            *v = value;
        }
    }


    let k = nb_class as usize;
    let nb_elem: usize = values.len();
    let mut v1 = Matrix::new(1, nb_elem);
    let mut v2 = Matrix::new(f64::MAX, nb_elem);

    let (mut v, mut val, mut s1, mut s2, mut w, mut i3, mut i4):
            (f64, f64, f64, f64, f64, usize, usize);

    for l in (2..(nb_elem + 1)) {
        s1 = 0.0; s2 = 0.0; w = 0.0;
        for m in (1..(l+1)) {
            i3 = l - m + 1;
            val = unsafe { *values.get_unchecked(i3-1) } as f64;
            s2 += val * val as f64;
            s1 += val as f64;
            w += 1.0;
            v = s2 - (s1 * s1) / w;
            i4 = i3 - 1;
            if i4 != 0 {
                for j in (2..k+1).rev() {
                    let _v = v + v2.get((i4-1, j-2));
                    if *v2.get((l-1, j-1)) >= _v {
                        v2.set((l-1, j-1), _v);
                        v1.set((l-1, j-1), i3);
                    }
                }
            }
            v1.set((l-1, 0), 1);
            v2.set((l-1, 0), v);
        }
    }
    let mut kclass = vec![0; k as usize];
    let mut k = nb_elem as u32;
    let mut j = nb_class;
    while j > 1 {
        k = *v1.get(((k-1) as usize, (j-1) as usize)) as u32 - 1;
        kclass[(j - 2) as usize] = k;
        j -= 1;
    }
    let mut breaks = Vec::with_capacity(nb_class as usize);
    breaks.push(0);
    for i in 1..nb_class {
        breaks.push((kclass[(i - 1) as usize]) as usize);
    }
    breaks.push(nb_elem - 1);
    breaks
}


struct JenkBin {
    pub start: usize,
    pub end: usize,
    pub sum: u64
}

impl JenkBin {
    pub fn new(start: usize, end: usize, sum: u64) -> Self {
        Self { start, end, sum }
    }

    fn len(&self) -> usize {
        self.end - self.start
    }
}

fn solve_subset_sum_exaustive(
    ctx: &Context
) -> Vec<Solution> {
    let mut solutions = ctx.jenk_bins.iter().enumerate().combinations(ctx.subset_solution_size).filter_map(|c| {
        let max_sum = c.iter().map(|bin| ctx.filtered_input[bin[1].start]).sum();
        if max_sum < ctx.target {
            return None; // we can't reach the target sum with this combination.
        };
        let sum = c.iter().map(|bin| bin[1].sum).sum();
        let subset = c.iter().flat_map(|bin| bin[0]).collect();
        Some(Solution { subset, sum })
    }).collect();
    solutions.sort_unstable_by(|a, b| b.abs_diff(ctx.target).cmp(&a.abs_diff(ctx.target)));
    solutions
};
*/

#[derive(Debug, Clone)]
struct Solution {
    pub subset: Vec<usize>,
    pub sum: u64,
}

#[derive(Debug, Clone)]
struct Context<'a> {   
    input: &'a [u64], // original input data is only required for the final solution
    target: u64,

    // processed data
    subset_solution_size: usize,
    filtered_input: Vec<u64>,
    //jenk_bins: Vec<JenkBin>,
    //jenk_solutions: Vec<Solution>,
    initial_solution: Solution,
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
        let mut best_solution = Solution { subset: Vec::new(), sum: 0 };    

        for (i, val) in input.iter().copied().enumerate() {
            if subset_solution_size == 0 { // we haven't found the smallest possible subset yet
                best_solution.subset.push(i);
                best_solution.sum += val;
                if best_solution.sum >= target {
                    subset_solution_size = i + 1;
                };
            } else {

                if best_solution.sum - input[*best_solution.subset.last().unwrap()] + input[i] < target {
                    // all below this "current_val" & "i" cannot be part of the solution, while being in the smallest subset. 
                    best_solution.sum = best_solution.sum - input[best_solution.subset.pop().unwrap()] + filtered_input.last().unwrap();
                    best_solution.subset.push(filtered_input.len() - 1);
                    // we may break here, and end the loop, i.e. filter out the rest of the input.
                    break;
                };
    
                // since we iterated past the subset_solution_size, 
                // `i - subset_solution_size` shouldn't ever be out of bounds.
                if input[i - subset_solution_size] == input[i] {
                    // this is a continues duplicate, larger then the expected solution size, so we can skip it.
                    continue;
                };
            };
            filtered_input.push(val);
        };

        /*
        let jenk_breaks = jenks_natural_breaks(&filtered_input, subset_solution_size + 1);

        for (start, end) in jenk_breaks.windows(2) {
            let sum = filtered_input[start..end].iter().sum();
            jenk_solutions.push(JenkBin { start, end, sum });
        };
        */
        let s = Self {
            input,
            target,
            subset_solution_size,
            filtered_input, 
            initial_solution: best_solution,
            //clusters,
        };

        let test_solution = s.initial_solution.clone();
        //assert!(test_solution.subset.len() == s.subset_solution_size);
        //assert_eq!(get_input_indexes_from_solution(&s, &test_solution).iter().map(|i| input[*i]).sum::<u64>(), test_solution.sum.clone(), "expected the sum of the test solution to be equal to the sum of the input data.");

        
        s
    }

    /// check if the context is solvable
    fn is_solvable(&self) -> bool {
        self.subset_solution_size > 0
    }
}

/// try and minimize the distance to the target sum, 
/// by replacing the value at the `subset_index` with a value that is as close to the target sum as possible, while being above it.
/// This is done via binary searching.
fn minimize_to_target(ctx: &Context, solution: &mut Solution, subset_index: usize) -> Option<(usize, u64)> {

    let removed_val = ctx.filtered_input[solution.subset[subset_index]];
    let target_val = std::cmp::min(ctx.target - (solution.sum - removed_val), *ctx.filtered_input.last().unwrap()); // make sure we point to the minimal value, in the worst case. 

    let (new_index, new_val) = 'index_search: {
        // below this index, are all values that are smaller then the target value.
        let pp = ctx.filtered_input.partition_point(|s| *s > target_val).saturating_sub(1);
        //assert!(pp < ctx.filtered_input.len());

        // below this solution_pp are all indexes, that point to larger target values.
        // above this solution_pp are all indexes, that point to smaller target values, including the target value, potentially.
        let solution_pp = solution.subset.partition_point(|i| *i < pp);
        //assert!(solution.subset[..solution_pp].iter().all(|i| ctx.filtered_input[*i] > ctx.filtered_input[pp]));
        //assert!(solution.subset[solution_pp..].iter().all(|i| ctx.filtered_input[*i] <= ctx.filtered_input[pp]));
        //info!("solution_pp: {}, pp: {}, target_val: {}, ctx.filtered_input[pp]: {}, ctx.filtered_input[solution.subset[solution_pp]]: {}", solution_pp, pp, target_val, ctx.filtered_input[pp], ctx.filtered_input[solution.subset[solution_pp]]);
        if solution_pp == solution.subset.len() { // all values indexed are larger than the target value.
            //assert!(!solution.subset.contains(&pp)); // fix this
            break 'index_search (pp, ctx.filtered_input[pp]);
        } else if solution.subset[solution_pp] != pp { // we are not pointing to the target val, so we may break with it.
            //assert!(!solution.subset.contains(&pp)); // fix this
            // we are not already pointing to the target val, so we may break with it.
            break 'index_search (pp, ctx.filtered_input[pp]);
        } else if  solution.subset[solution_pp] == pp && solution_pp == subset_index { // we are pointing to the target val, but will replace it anyway. 
            //assert!(solution.subset.contains(&pp)); // fix this
            break 'index_search (pp, ctx.filtered_input[pp]);
        }else { 
            return None;

            /*
            let left_index = 'left_search: {
                for (i, w) in  solution.subset[..solution_pp].windows(2).rev().enumerate() {
                assert!(w[0] < w[1]);
                if w[1] != w[0] + 1 {
                    break 'left_search Some(w[1] - 1);
                };
            }
            solution.subset.first().unwrap().checked_sub(1)
            };
            
            let right_index = 'right_search: {
                for (i, w) in solution.subset[solution_pp..].windows(2).enumerate() {
                assert!(w[0] < w[1]);
                if w[0] + 1 != w[1] {
                    break 'right_search Some(w[0] + 1);
                };
                };
                let right_index = solution.subset.last().unwrap().clone(); 
                if right_index + 1 == ctx.filtered_input.len() { 
                    None 
                } else { 
                    Some(*right_index + 1)
                }
            };

            break 'index_search match (left_index, right_index) {
                (Some(left), Some(right)) => {
                    //assert!(!solution.subset.contains(&left));
                    //assert!(!solution.subset.contains(&right));
                    assert!(left < solution.subset[solution_pp], "expected left to be larger than solution_pp, but found left: {}, solution_pp: {}, subset_index: {}", left, solution_pp, subset_index);
                    assert!(right >= solution.subset[solution_pp], "expected right to be smaller than solution_pp, but found right: {}, solution_pp: {}, subset_index: {}", right, solution_pp, subset_index);
                    if ctx.filtered_input[right].abs_diff(target_val) < target_val.abs_diff(ctx.filtered_input[left]) {
                        (right, ctx.filtered_input[right])
                    } else {
                        (left, ctx.filtered_input[left])
                    }
                },
                (Some(left), None) => {
                    //assert!(!solution.subset.contains(&left));
                    (left, ctx.filtered_input[left])
                },
                (None, Some(right)) => {
                    //assert!(!solution.subset.contains(&right));
                    (right, ctx.filtered_input[right])
                },
                _ => return None,
                }
                */
            };
        };

            
    let new_sum = solution.sum - removed_val + new_val;
    
    // we check if we are closer to the target sum;
    if (solution.sum >= ctx.target // IF former sum IS ABOVE target
        && new_sum >= ctx.target  // AND new sum IS ABOVE target
        && new_sum <= solution.sum) // AND new sum IS BELOW the former sum
        || (solution.sum < ctx.target // OR IF former sum IS BELOW target
        && new_sum > solution.sum)  // AND new sum IS ABOVE former sum. 
        {
            Some((new_index, new_sum))
        } else {
            None
        }
}

fn generate_random_solution(ctx: &Context, rng: &mut impl Rng) -> Solution {
    let subset = sample_weighted(rng, ctx.filtered_input.len(), |val| ctx.filtered_input[val] as f64 / ctx.target as f64, ctx.subset_solution_size).unwrap().into_vec();
    let sum = subset.iter().map(|i| ctx.filtered_input[*i]).sum();
    Solution {subset, sum}
}

fn get_input_indexes_from_solution(ctx: &Context, solution: &Solution) -> Vec<usize> {
    let current_sum = solution.subset.iter().map(|i| ctx.filtered_input[*i]).sum::<u64>();
    let mut input_indexes = Vec::with_capacity(solution.subset.len()); 
    for filtered_index in solution.subset.iter() {
        let mut o_index = ctx.input.partition_point( |probe| probe > &ctx.filtered_input[*filtered_index]);
        while input_indexes.contains(&o_index) {
            o_index += 1;
        };
        assert!(ctx.input[o_index] == ctx.filtered_input[*filtered_index]);
        input_indexes.push(o_index);
    };
    assert!(input_indexes.iter().map(|i| ctx.input[*i]).sum::<u64>() == solution.sum);
    let input_sum = input_indexes.iter().map(|i| ctx.input[*i]).sum::<u64>();
    assert!(input_sum == solution.sum);
    input_indexes
}

/// To use this function build an appropriate context with `Context::build_context` first, and then call this function.
///
/// This handles a special case of the subset sum problem, whereby
/// 1) the input is sorted in descending order.
/// 2) We are only interested in being at, or as close to the target sum as possible, while being above it.
/// 3) We are only interested in the smallest subset that is at or above the target sum.
/// 4) We don't care about finding multiple solutions, only the best one.
/// 
/// The returned [`Some`] consists of a tuple, with:
/// 1) the indexes of the input (as passed into the supplied [`Context`]), that are part of the best found subset, 
/// 2) the sum of the subset.
/// 
/// If None is returned, the problem is not solvable i.e. there is no subset that is larger then the target sum..
fn approximate_subset_sum_with_context<'a>(ctx: Context, iterations: usize, rng: &mut impl Rng) -> Option<(Vec<usize>, u64)> {
    
    if !ctx.is_solvable() {
        debug!("SSP: not solvable");
        return None;
    };

    if ctx.subset_solution_size == ctx.filtered_input.len() {
        debug!("SSP: only possible solution found {},", ctx.initial_solution.sum);
        return Some((get_input_indexes_from_solution(&ctx, &ctx.initial_solution), ctx.initial_solution.sum));
    } 

    if ctx.initial_solution.sum == ctx.target {
        debug!("SSP: exact solution found: {},", ctx.initial_solution.sum);
        return Some((get_input_indexes_from_solution(&ctx, &ctx.initial_solution), ctx.initial_solution.sum));
    };

    //info!("subset solution size: {}", ctx.subset_solution_size);
    let mut iterations = iterations; // we need to be able to mutate this value.
    let mut best_solution = ctx.initial_solution.clone();

    let mut hits = 0;
    let mut misses = 0;

    let mut global_searches = 0;
    let mut local_searches = 0;

    let mut cache_hits = 0;
    

    let mut already_searched = HashSet::<Vec<usize>>::new();

    // global search
    'global: loop {

        let mut current_solution = generate_random_solution(&ctx, rng);


        // climb randomly to a new solution. over or equal to the target sum.
        while current_solution.sum < ctx.target {
            for i in 0..ctx.subset_solution_size {
                let to_add_index = rng.gen_range(0..ctx.filtered_input[..current_solution.subset[i]].len());
                let to_remove_index = current_solution.subset[i];
                if to_remove_index == 0 {
                    continue;
                }
                if current_solution.subset.binary_search(&to_add_index).is_ok() {
                    continue;
                };
                current_solution.subset[i] = to_add_index;
                current_solution.sum -= ctx.filtered_input[to_remove_index];
                current_solution.sum += ctx.filtered_input[to_add_index];
                current_solution.subset.sort();
                iterations -= 1;
                if iterations == 0 {
                    if current_solution.sum >= ctx.target { 
                        if best_solution.sum < ctx.target {
                            debug!("SSP: found better solution: {}, local searches {}, global searches: {}", current_solution.sum, local_searches, global_searches);
                            best_solution = current_solution;
                        } else if current_solution.sum < best_solution.sum {
                            debug!("SSP found better solution: {}, local searches {}, global searches: {}", current_solution.sum, local_searches, global_searches);
                            best_solution = current_solution;
                        };
                    };
                    break 'global;
                };

                if current_solution.sum >= ctx.target {
                    break;
                }
            };

        current_solution.subset.sort();
    
        // local search 
        'local: {
        //info!("local_searches: {}", local_searches);
        // how many times, we have failed to minimize the distance to the target sum, consecutively.
        let mut mini_misses = 0; 

        loop {
            //info!("mini_misses: {}", mini_misses);

            // individual index search
            let mut sample = (0..ctx.subset_solution_size).collect::<Vec<usize>>();
            sample.shuffle(rng);

            for i in sample {
            
                // check if we should continue the search, or not. 
                if iterations == 0 || mini_misses == ctx.subset_solution_size + 1 {
                    break 'local;
                }

                if let Some((new_index, new_sum)) = minimize_to_target(&ctx, &mut current_solution, i) {
                    
                    current_solution.subset[i] = new_index;
                    current_solution.subset.sort();                    
                    current_solution.sum = new_sum;

                    // bunch of sanity checks. 
                    //assert!(!((1..current_solution.subset.len()).any(|i| current_solution.subset[i] == current_solution.subset[i - 1])));
                    //assert!(!(current_solution.subset.iter().any(|i| *i >= ctx.filtered_input.len())));
                    //assert!(current_solution.subset.len() == ctx.subset_solution_size);

                   // check if we found an exact solution, and if yes, return early.
                   if current_solution.sum == ctx.target {
                        debug!("SSP: found exact solution: {}, local searches {}, global searches: {}", current_solution.sum, local_searches, global_searches);
                        return Some((get_input_indexes_from_solution(&ctx, &current_solution), ctx.target));
                   };

                   if already_searched.contains(&current_solution.subset) {
                        cache_hits += 1;
                        iterations -= 1;
                       // we have already searched this solution, so we may break early.
                       break 'local;
                   } else {
                       already_searched.insert(current_solution.subset.clone());
                   };

                   // reset miss counter, as we found a better solution.
                   mini_misses = 0;
                   hits += 1;
                } else {
                    // we did not find a better solution, increment the miss counter.
                    mini_misses += 1;
                    misses += 1;
                }; 
                
                iterations -= 1;
                local_searches += 1;
            };
        };
        };

        if current_solution.sum >= ctx.target { 
                if best_solution.sum < ctx.target {
                    debug!("SSP: found better solution: {}, local searches {}, global searches: {}", current_solution.sum, local_searches, global_searches);
                    best_solution = current_solution;
                } else if current_solution.sum < best_solution.sum {
                    debug!("SSP found better solution: {}, local searches {}, global searches: {}", current_solution.sum, local_searches, global_searches);
                    best_solution = current_solution;
                };
        };

        global_searches += 1;

        if iterations == 0 {
            break 'global;
        };
    };

    debug!("SSP: best solution: {}, local searches {}, global searches: {}", best_solution.sum, local_searches, global_searches);
    Some((get_input_indexes_from_solution(&ctx, &best_solution), best_solution.sum))
}


pub fn appropriate_subset_sum<'a> (input: &'a [u64], target: u64, iterations: usize) -> Option<(Vec<usize>, u64)> {
    let ctx = Context::build_context(input, target);
    approximate_subset_sum_with_context(ctx, iterations, &mut SmallRng::from_entropy())
} 

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
    const ITERATIONS: usize = 100_000;

    #[test]
    fn test_approximate_subset_sum_uniform() {
        kaspa_core::log::try_init_logger("debug, kaspa_utils=debug");
        let test_name: &str = "UNIFORM";
    

        let input_amounts = [10, 100, 1_000, 100_000, 1_000_000];
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

        let input_amounts = [10, 100, 1_000, 100_000, 1_000_000];
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

        let input_amounts = [10, 100, 1_000, 100_000, 1_000_000];
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

        let input_amounts = [10, 100, 1_000, 100_000, 1_000_000];
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

        let input_amounts = [10, 100, 1_000, 100_000, 1_000_000];
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
        assert!(inputs[0] >= inputs[inputs.len() -1], "expected inputs to be sorted");
        //info!("inputs: {:?}", inputs);
        // give info on spae, density, target, iterations, and distibution:
        if let Some(result) = appropriate_subset_sum(&inputs, target, iterations) {
            let sum = result.0.iter().map(|i| inputs[*i]).sum::<u64>();
            assert_eq!(sum, result.1, "expected the sum of the input data to be equal");
            assert!(sum >= target);
            info!("Result for test with space: {}, input_amount: {}, target: {}, iterations: {} test_name: {}-  sum: {}, k: {} \n", space, input_amount, target, iterations, test_name, sum, result.0.len());
        } else {
            info!("Result for test with space: {}, input_amount: {}, target: {}, iterations: {}, test_name: {} - None \n", space, input_amount, target, iterations, test_name);
        }
    }
}
