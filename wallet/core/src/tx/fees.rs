//!
//! Primitives for declaring transaction fees.
//!

use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

use crate::{imports::local::storage, result::Result};
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use kaspa_utils::vec::VecExtensions;
use serde::{Deserialize, Serialize};
use sorted_insert::{SortedInsertBinary, SortedInsertBinaryByKey, SortedInsertBy};
use thiserror::Error;

use super::{payment, MassCalculator};

#[derive(Error, Debug, Clone)]
pub enum InputSelectionError {
    #[error("Operation failed due to arithmetic overflow")]
    ArthmeticOverflow,
}

pub struct InputSelectionContext {
    pub total_input_amount: Option<u64>,
    pub selected_total_amount: u64,
    pub sorted_input_amounts: Vec<u64>,
    pub selected_indexes: HashSet<usize>,
    pub payment_amount: u64,
    pub target_fee: u64,
    pub tx_mass_limit: u64,
    pub input_compute_mass: u64,
    pub payment_compute_mass: u64,
    pub change_compute_mass: u64,
    pub base_transaction_mass: u64,
    pub change_amount: u64,
    pub storage_mass_parameter: u64,
}

impl InputSelectionContext {
    pub fn new(
        total_input_amount: Option<u64>,
        sorted_input_amounts: Vec<u64>,
        payment_amount: u64,
        target_fee: u64,
        tx_mass_limit: u64,
        input_compute_mass: u64,
        payment_compute_mass: u64,
        change_compute_mass: u64,
        base_transaction_mass: u64,
        change_amount: u64,
        storage_mass_parameter: u64,
    ) -> Self {
        Self {
            total_input_amount,
            sorted_input_amounts,
            selected_indexes: HashSet::new(),
            selected_total_amount: 0,
            payment_amount,
            target_fee,
            tx_mass_limit,
            input_compute_mass,
            payment_compute_mass,
            change_compute_mass,
            base_transaction_mass,
            change_amount,
            storage_mass_parameter,
        }
    }
    /*

    // all operations are checked for overflow, and return None if overflow occurs.
    // this is because we are in the wallet, and to play it safe, we don't want overflow exploits to be possible.
    fn mean_ins(&self) -> InputSelectionResult<u64> {
        self.selected_total_amount.checked_div(self.selected_indexes.len() as u64).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn arithmetic_ins(&self) -> InputSelectionResult<u64> {
        (self.selected_indexes.len() as u64).checked_mul(self.storage_mass_parameter.checked_div(self.mean_ins()?).ok_or(InputSelectionError::ArthmeticOverflow)?).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn selected_amounts(&self) -> &[u64] {
        let selected_amounts = Vec::with_capacity(self.selected_indexes.len());
        self.selected_indexes.iter().for_each(|index| selected_amounts.push(self.sorted_input_amounts[*index].copied()));
        &selected_amounts
    }

    fn change_harmonic(&self) -> InputSelectionResult<u64> {
        self.storage_mass_parameter.checked_div(self.change_amount).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn payment_harmonic(&self) -> InputSelectionResult<u64> {
        self.storage_mass_parameter.checked_div(self.payment_amount).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    pub fn harmonic_outs_without_change(&self) -> InputSelectionResult<u64> {
        self.payment_harmonic()
    }

    pub fn harmonic_outs(&self) -> InputSelectionResult<u64> {
        self.payment_harmonic()?.checked_add(self.change_harmonic()?).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn total_compute_mass_without_change(&self, num_of_ins: u64) -> InputSelectionResult<u64> {
        num_of_ins
            .checked_mul(self.input_compute_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.payment_compute_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.base_transaction_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn total_compute_fees_without_change(&self, num_of_ins: u64) -> InputSelectionResult<u64> {
        self.total_compute_mass_without_change(num_of_ins)?.checked_mul(self.target_fee).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn total_compute_mass(&self, num_of_ins: u64) -> InputSelectionResult<u64> {
            num_of_ins
            .checked_mul(self.input_compute_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.payment_compute_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.base_transaction_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.change_compute_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn total_compute_fees(&self, num_of_ins: u64) -> InputSelectionResult<u64> {
        self.total_compute_mass(num_of_ins)?.checked_mul(self.target_fee).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn total_compute_mass(&self, num_of_ins) -> InputSelectionResult<u64> {
        (self.selected_indexes.len() as u64)
            .checked_mul(self.input_compute_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.payment_compute_mass)?
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.base_transaction_mass)?
            .ok_or(InputSelectionError::ArthmeticOverflow)?
            .checked_add(self.change_compute_mass)
            .ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn total_compute_fees(&self, num_of_ins: u64) -> InputSelectionResult<u64> {
        self.total_compute_mass(num_of_ins)?.checked_mul(self.target_fee).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn change_compute_fee(&self) -> InputSelectionResult<u64> {
        self.change_compute_mass.checked_mul(self.target_fee).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn input_compute_fee(&self) -> InputSelectionResult<u64> {
        self.input_compute_mass.checked_mul(self.target_fee).ok_or(InputSelectionError::ArthmeticOverflow)
    }

    fn is_storage_relaxed_without_change(&self, num_of_ins: u64) -> bool {
        num_of_ins >= 1
    }

    fn is_storage_relaxed(&self, num_of_ins: u64) -> bool {
        num_of_ins >= 2
    }

    */

    /*
    fn find_closest_above_or_below_target_mean_subset(&self, target_mean: u64, num_of_inputs: u64) {
    // this can be restated as find_closest_above_or_below_target_sum_subset(target_mean * num_of_inputs, num_of_inputs);
    self.find_closest_above_or_below_target_sum_subset(target_mean.checked_mul(num_of_inputs)?, num_of_inputs);
    }

    fn search_optimal_solution(&self, num_of_ins: u64) -> InputSelectionResult<Option<(Vec<usize>, i64)>> {
        // first we find solution with change.
        if self.is_storage_relaxed(num_of_ins) {
            // optimal solution is by using the lowest inputs which can cover fees and payment.
            let payment_cost = self.payment_amount.checked_add(self.total_fees(num_of_ins)?).ok_or(InputSelectionError::ArthmeticOverflow)?;

        } else {

        }

        // compare to solution without change.

        let indexes = Vec::<usize>::with_capacity(amount);
        let arthmetic_mean_offset = 0i64;

        // search for min. consecutive window of inputs that incur the least storage mass.
        for (min_index, window) in self.sorted_input_amounts.as_slice().windows(amount).enumerate() {
            let sum = window.iter().sum::<u64>();
            let mean = sum.checked_div(amount)?;
            let arthmetic_mean = amount.checked_mul(self.storage_mass_parameter.checked_div(mean)?)?;
            let offset = self.harmonic_outs_without_change()? - arthmetic_mean;
            if offset <= 0 {
                arthmetic_mean_offset = offset;
                break;
            } else if arthmetic_mean_offset < offset {
                arthmetic_mean_offset = offset;
            }
        };

        if arthmetic_mean_offset.is_positive() {
            // we cannot get an offset <= 0
            // in this case choosing the highest inputs is the best option, and should return the lowest offset.
            return Some((self.sorted_input_amounts.len() - amount as usize)..self.sorted_input_amounts.len()).collect(), (arthmetic_mean_offset);
        }
        // we cannot get an offset <= 0

        // search to see if we can find a positive offset closer to the min arthmetic mean.


        Some((0, 0))
    }

    fn should_gift_change(&self) -> bool {
        self.change_amount <= self.change_compute_fee() + std::cmp::min(self.input_compute_fee()?, self.change_storage_mass()?)
    }

    fn change_storage_mass(&self) -> Option<u64> {
        self.calculate_storage_mass_without_change()?.checked_sub(self.calculate_storage_mass()?)
    }

    pub fn calculate_storage_mass_without_change(&self) -> Option<u64> {
        Some(self.harmonic_outs_without_change()?.saturating_sub(self.arithmetic_ins()?))
    }

    pub fn calculate_storage_mass(&self) -> Option<u64> {
        Some(self.harmonic_outs()?.saturating_sub(self.arithmetic_ins()?))
    }
    */
}

/*
fn select_inputs(
    // amounts
    sorted_input_amounts: &[u64],
    total_input_amount: Option<u64>,
    payment_amount: u64,

    // mass
    input_compute_mass: u64,
    payment_compute_mass: u64,
    change_compute_mass: u64,
    base_transaction_mass: u64,
    tx_mass_limt: u64,

    // fees
    target_fee: u64) -> Option<(Vec<usize>, bool)> // None, if no solution exits, else Some((indexes, should_generate_change_output))

{
    // 0) define helper functions and initial values.self.calulate_storage_mass()
    let mut selected_amount = 0u64;
    let mut selected_indexes = vec![];
    let mut to_query_selected_inputs = 1usize;
    let mut check_for_storage_mass_solution = true;

    let should_gift_change = |selected_change: u64, target_fee: u64, change_compute_mass: u64| {
        // if equal we prefer gifting, as it will reduce ETA, with no cost.
        selected_change <= (change_compute_mass * target_fee)
    };

    let calc_change = |num_of_inputs:usize, selected_input_amount: u64, payment_amount: u64, payment_compute_mass: u64, base_transaction_mass: u64, input_compute_mass: u64, target_fee: u64| {
        selected_input_amount - (payment_amount + (payment_compute_mass + base_transaction_mass + input_compute_mass) * target_fee)
    };

    let total_fees = |num_of_inputs:usize, payment_compute_mass: u64, base_transaction_mass: u64, input_compute_mass: u64, target_fee: u64| {
        ((input_compute_mass * (num_of_inputs as u64)) + payment_compute_mass + base_transaction_mass) * target_fee
    };

    // 1) check if, and where, solution exists
    let min_inputs_to_pay = match total_input_amount {
        Some(total_input_amount) => {
            // check if the total input amount is enough to cover the payment and fees
            if total_input_amount < (payment_amount + (payment_compute_mass + base_transaction_mass + input_compute_mass) * target_fee) {
                return None;
            }
        }
        None => {
                // TODO: if total_input_amount is not known in advance
                // consider doing a pass of `input_amounts.iter().sum() - (input.len() * input_compute_mass + payment_mass) * target_fee)`
                // to check if a solution exists.
                // although this may be a bit expensive in and off itself, doing an exhaustive search for a solution that doesn't exist is even more expensive.
                for i in 1..sorted_input_amounts.len() {
                    lower_bound = sorted_input_amounts.binary_search();
                    sorted_input_amounts[lower_bound]
                }
        }
    };


    // 2) check for only solution condition
    if sorted_input_amounts.len() == 1 && sorted_input_amounts[0] >= total_fees(to_query_selected_inputs,  payment_compute_mass, base_transaction_mass, input_compute_mass, target_fee) {
        // only solution exists
        selected_amount += sorted_input_amounts[0];
        selected_indexes.push(0);
        let change = calc_change(selected_indexes.len(), selected_amount, payment_amount, payment_compute_mass, base_transaction_mass, input_compute_mass, target_fee);
        (selected_indexes, should_gift_change(change, target_fee, change_compute_mass))
    } else {
        return None
    };

    // 3) check for optimal solution condition
    if let Ok(index) = sorted_input_amounts.binary_search(&(payment_amount + total_fees(to_query_selected_inputs, payment_compute_mass, base_transaction_mass, input_compute_mass, target_fee))) {
        return Some((vec![index], false)) // Yay, found optimal solution! - no change in this case exists.
    };

    // 4) loop, keep adding and analyzing inputs until we reach solution

    let x = 1; // start with x = 1 input.
    let gift_change = 1; // 2 outputs, payment and change.
    loop {

        // 4.1) find lowest x inputs >= payment and fees.

        // 4.1.1) if change is <= cost of extra output, we gift change.
        // whereby: cost of extra output = min(change_in storage mass fee, input compute mass * target_fee) + output compute_fee.

        // 4.1.2) if change in storage mass is cheaper then adding another input,
        // we do x inputs and 2 outputs (with change).

        // 4.1.3) if adding another input is cheaper then paying then storage mass, we continue loop.
        // --> reloop with x = x + 1


        // 4.1.2) if change is  cost of extra output + storage mass fee for extra output.
        // 4.1.3)
        // 4.1.3) find input that minimizes storage mass.
        // 4.2) check cost of gifting change.
        // 4.2.1) if cost of gifting change is cheaper then adding another output, we gift change.
        // --> solution found.
        // 4.3) check savings on storage mass by adding another input.
        // 4.3.1) if adding another input, is cheaper then storage mass, we continue loop with another input.
        // -> reloop with x = x + 1
        // 4.3.2) if adding another input is more expensive then storage mass
        // -> solution found.

        // let optimal_input = input that lowers the storage mass the most,



        // 4.2) check cost of x inputs and 2 output

        // 4.3) check cost of x inputs and 2 outputs with storage.
        if check_for_storage_mass_solution {
            // TODO: maybe remove this check,
            // if we know that adding another input is always cheaper then paying storage mass.
            // at least for standard transaction scenarios.

            // 4.2) we choose the highest ones to minimize storage mass:
            selected_indexes.clear();
            selected_amount = 0;
            for index in ((sorted_input_amounts.len() - to_query_selected_inputs)..sorted_input_amounts.len()) {
                selected_indexes.push(index);
                selected_amount += sorted_input_amounts[index]
            };

            let harmonic_outs = payment_amount;
            let sum_ins = selected_amount / to_query_selected_inputs as u64;
            let mean_ins = selected_amount / (selected_indexes.len() as u64);
            let arithmetic_ins = ins_len.saturating_mul(self.storage_mass_parameter / mean_ins);

            let min_storage_fees =  harmonic_outs.saturating_sub(arithmetic_ins);

            if min_storage_fees < input_compute_mass * target_fee {
                // paying storage is cheaper then adding an input.
            };
        }

        // take min of the 3 options.
    }

    Some((Default::default(), Default::default())) // placeholder, to keep rust analyzer happy.

}
*/
/*let harmonic_outs = outputs
    .iter()
    .map(|out| self.storage_mass_parameter / out.value)
    .try_fold(0u64, |total, current| total.checked_add(current))?; // C·|O|/H(O)

// Total supply is bounded, so a sum of existing UTXO entries cannot overflow (nor can it be zero)
let sum_ins = inputs.iter().map(|entry| entry.amount()).sum::<u64>(); // |I|·A(I)
let ins_len = inputs.len() as u64;
let mean_ins = sum_ins / ins_len;

// Inner fraction must be with C and over the mean value, in order to maximize precision.
// We can saturate the overall expression at u64::MAX since we lower-bound the subtraction below by zero anyway
let arithmetic_ins = ins_len.saturating_mul(self.storage_mass_parameter / mean_ins); // C·|I|/A(I)

Some(harmonic_outs.saturating_sub(arithmetic_ins)) // max( 0 , C·( |O|/H(O) - |I|/A(I) ) ) */

/// Transaction fees.  Fees are comprised of 2 values:
///
/// `relay` fees - mandatory fees that are required to relay the transaction
/// `priority` fees - optional fees applied to the final outgoing transaction
/// in addition to `relay` fees.
///
/// Fees can be:
/// - `SenderPaysAll` - (standard) fees are added to outgoing transaction value
/// - `ReceiverPaysAll` - all transaction fees are paid by the receiver.
///
/// NOTE: If priority fees are `0`, fee variants can be used control
/// who pays the `network` fees.
///
/// NOTE: `ReceiverPays` variants can fail during the aggregation process
/// if there are not enough funds to cover the final transaction.
/// There are 2 solutions to this problem:
///
/// 1. Use estimation to check that the funds are sufficient.
/// 2. Check balance and ensure that there is a sufficient amount of funds.
///
#[derive(Clone, Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum Fees {
    /// Fee management disabled (sweep transactions, pays all fees)
    None,
    /// all fees are added to the final transaction value
    SenderPays(u64),
    /// all fees are subtracted from the final transaction value
    ReceiverPays(u64),
}

impl Fees {
    pub fn is_none(&self) -> bool {
        matches!(self, Fees::None)
    }

    pub fn sender_pays(&self) -> bool {
        matches!(self, Fees::SenderPays(_))
    }

    pub fn receiver_pays(&self) -> bool {
        matches!(self, Fees::ReceiverPays(_))
    }

    pub fn additional(&self) -> u64 {
        match self {
            Fees::SenderPays(fee) => *fee,
            _ => 0,
        }
    }
}

/// This trait converts supplied positive `i64` value as `Exclude` fees
/// and negative `i64` value as `Include` fees. I.e. `Fees::from(-100)` will
/// result in priority fees that are included in the transaction value.
impl From<i64> for Fees {
    fn from(fee: i64) -> Self {
        if fee < 0 {
            Fees::ReceiverPays(fee.unsigned_abs())
        } else {
            Fees::SenderPays(fee as u64)
        }
    }
}

impl From<u64> for Fees {
    fn from(fee: u64) -> Self {
        Fees::SenderPays(fee)
    }
}

impl TryFrom<&str> for Fees {
    type Error = crate::error::Error;
    fn try_from(fee: &str) -> Result<Self> {
        if fee.is_empty() {
            Ok(Fees::None)
        } else {
            let fee = crate::utils::try_kaspa_str_to_sompi_i64(fee)?.unwrap_or(0);
            Ok(Fees::from(fee))
        }
    }
}

impl TryFrom<String> for Fees {
    type Error = crate::error::Error;
    fn try_from(fee: String) -> Result<Self> {
        Self::try_from(fee.as_str())
    }
}

pub struct Solution {
    pub selected_indexes: Vec<usize>,
    pub input_amount: u64,
    pub change_amount: u64,
    pub compute_fees: u64,
    pub should_generate_change: bool,
    pub storage_mass: u64,
    pub is_storage_relaxed: bool,
}

impl Solution {
    pub fn new(
        selected_indexes: Vec<usize>,
        input_amount: u64,
        change_amount: u64,
        compute_fees: u64,
        should_generate_change: bool,
        storage_mass: u64,
        is_storage_relaxed: bool,
    ) -> Self {
        Self { selected_indexes, input_amount, change_amount, compute_fees, should_generate_change, storage_mass, is_storage_relaxed }
    }

    pub fn cost(&self) -> u64 {
        if self.should_generate_change {
            self.change_amount + self.fees()
        } else {
            self.fees()
        }
    }

    pub fn fees(&self) -> u64 {
        self.compute_fees + self.storage_mass
    }
}

// Finds a solution to a variation of the subset sum problem, where the sum of the subset is expected to minimize dist to the target,
// while being greater or equal to the target.
// k is the number of elements in the subset, and t is the target treshold.
// problem is NP-complete, and there is no known polynomial time solution.
// on the plus side it is well studied, and there are some optimizations that can be made.
// consider: https://en.wikipedia.org/wiki/Subset_sum_problem#Fully-polynomial_time_approximation_scheme
// some available bounds in our case are: known k, and having a sorted array.
// on the downside, if storage mass is not relaxed, we need to find the treshold where it becomes relaxed, as the target
// which in and off itself is probably a subset sum problem 😅 .
fn find_closest_subset(arr: &[u64], k: usize, t: u64) -> (Vec<u64>, u64) {
    // below is a depth first search solution to the problem.
    let mut best_subset = Vec::new();
    let mut best_sum = u64::MAX;

    // Helper function for depth first search.
    fn dfs(arr: &[u64], k: usize, t: u64, start: usize, current: &mut Vec<u64>, best_subset: &mut Vec<u64>, best_sum: &mut u64) {
        if current.len() == k {
            let current_sum: u64 = current.iter().sum();
            if current_sum >= t && (current_sum < *best_sum || *best_sum == u64::MAX) {
                *best_sum = current_sum;
                *best_subset = current.clone();
            }
            return;
        }

        for i in start..arr.len() {
            current.push(arr[i]);
            dfs(arr, k, t, i + 1, current, best_subset, best_sum);
            current.pop();
        }
    }

    let mut current = Vec::new();
    dfs(arr, k, t, 0, &mut current, &mut best_subset, &mut best_sum);

    (best_subset, best_sum - t)
}

// given a solution finds the lowest input amounts that can cover the payment amount, and fees, without incurring more fees.
// Generally Choosing the lowest inputs in such a way is the most optimal solution, as it minimizes storage cost waste, and gifted change, depending on the solution.
fn minimize_to_lowest_inputs(sorted_input_amounts: &[u64], solution: &mut Solution) {
    let payment_amount = solution.input_amount - solution.fees() - solution.change_amount; // this should reconstruct payment amount

    // below does not handle the case where solution.is_storage_relaxed is false.
    // more consideration is needed to handle this case, as it adds further analysis to find the optimal inputs.
    // TODO: consider returning indices..
    let (best_subset, best_sum) =
        find_closest_subset(sorted_input_amounts, solution.selected_indexes.len(), solution.input_amount - solution.fees());

    solution.selected_indexes =
        best_subset.iter().map(|amount| sorted_input_amounts.iter().position(|&x| x == *amount).unwrap()).collect();
    solution.input_amount = best_sum;
    solution.change_amount = solution.input_amount - payment_amount - solution.fees();
}

/// finds the most optimal selection of inputs to cover the payment amount, and fees with least amount of cost.
/// This algorithm is assumes constant input compute mass, and x inputs : 2 outputs (payment and change).
/// Note: solution may consider gifting change, if it is the most optimal solution. cost here, is defined as the sum of gifted change and fees.
fn find_most_optimal_solution(
    sorted_input_amounts: &[u64],
    storage_mass_parameter: u64,
    target_fee: u64,
    payment_amount: u64,
    payment_output_mass: u64,
    base_tx_mass: u64,
    compute_mass_per_input: u64,
    change_output_mass: u64,
    tx_mass_limit: u64,
) -> Option<Solution> {
    // (indexes, is_above_target_sum)
    let mut current_sum = 0;
    let base_target_amount = payment_amount + (payment_output_mass + base_tx_mass) * target_fee;
    let base_mass = payment_output_mass + base_tx_mass;
    let mut gifted_solution = None;
    let mut change_solution = None;

    // first we make a pass at finding the number of inputs required to cover both
    // 1) the solution with change, and 2) the solution without change.
    // traverse by highest input amounts, increase selection until we breach target_amount treshold.
    for k in (1..=sorted_input_amounts.len()) {
        current_sum += sorted_input_amounts[k - 1];
        // storage should always be relaxed without change.
        let mut target_amount_without_change = base_target_amount + ((k as u64 + 1) * target_fee); // compute fees
        let mut target_amount_with_change = base_target_amount + change_output_mass * target_fee; // compute fees
        let mass_without_change = base_mass + (k as u64 + 1) * compute_mass_per_input;
        let mass_with_change = base_mass + change_output_mass + k as u64 * compute_mass_per_input;

        if mass_without_change < tx_mass_limit {
            if gifted_solution.is_none() // we haven't found solution for k_without_change yet. 
            && current_sum >= target_amount_without_change
            // AND: we can cover the fees and payment.
            {
                gifted_solution = Some(Solution::new(
                    (0..k).collect(),
                    current_sum,
                    current_sum - target_amount_with_change,
                    target_amount_with_change - payment_amount,
                    false,
                    0,
                    true,
                ));
            };
        };

        if mass_with_change <= tx_mass_limit {
            if k == 1 {
                // handle storage mass

                let storage_mass_with_change = {
                    // here we use the upper-bound on storage_mass, which should indicate the least amount of storage mass we can pay.
                    // this is because:
                    // 1) we decrease harmonic outs, via change produced by the highest inputs
                    // 2) we increase arthmetic mean, via the sum of the highest inputs.
                    let mean_ins = current_sum / (k as u64 + 1);
                    let arthmetic_mean = (k as u64 + 1) * storage_mass_parameter / mean_ins;
                    let harmonic_outs = storage_mass_parameter / payment_amount
                        + (storage_mass_parameter / (current_sum - payment_amount - target_amount_with_change));
                    harmonic_outs.saturating_sub(arthmetic_mean)
                };

                if change_solution.is_none()  // IF: we haven't found a solution for k_with_change yet.
                && { // AND
                    k < sorted_input_amounts.len() // we cannot add another input.
                    || storage_mass_with_change < compute_mass_per_input * target_fee // OR: paying storage is cheaper then adding another input (to relax storage).
                }
                && current_sum >= target_amount_with_change
                {
                    // AND: we can cover the fees and payment.
                    change_solution = Some(Solution::new(
                        (0..k).collect(),
                        current_sum,
                        current_sum - target_amount_with_change - storage_mass_with_change,
                        target_amount_with_change - payment_amount,
                        true,
                        storage_mass_with_change,
                        false,
                    ));
                };
            } else {
                // relaxed storage mass case

                if change_solution.is_none()  // we haven't found a solution for k_with_change yet.
                && current_sum >= target_amount_with_change
                // AND: we can cover the fees and payment.
                {
                    change_solution = Some(Solution::new(
                        (0..k).collect(),
                        current_sum,
                        current_sum - target_amount_with_change,
                        target_amount_with_change - payment_amount,
                        true,
                        0,
                        true,
                    ));
                };
            };
        }

        if change_solution.is_some() && gifted_solution.is_some() {
            // found a solution for both with and without change, and may exit this loop.
            break;
        };

        if tx_mass_limit > mass_with_change &&   // we can skip this check as checking without change is enough, here for completeness.
           tx_mass_limit > mass_without_change
        {
            return None; // no solution exists which doesn't adhere to the tx_mass_limit.
        };
    }

    if let Some(mut gifted_solution) = gifted_solution {
        if let Some(mut change_solution) = change_solution {
            // both solutions exist, and we must compare them.
            // TODO: explorer short-cuts here, especially if both have the same number of inputs.
            // probably prefferable to do `minimize_to_lowest_inputs` alongside each other for this case.`
            minimize_to_lowest_inputs(sorted_input_amounts, &mut gifted_solution);
            minimize_to_lowest_inputs(sorted_input_amounts, &mut change_solution);
            if gifted_solution.cost() <= change_solution.cost() {
                // we prefer gifting if equal, as it will reduce ETA, with no extra cost.
                Some(gifted_solution)
            } else {
                Some(change_solution)
            }
        } else {
            // only gifted solution exists, and is the most optimal solution.
            minimize_to_lowest_inputs(sorted_input_amounts, &mut gifted_solution);
            Some(gifted_solution)
        }
        // only gifted solution exists, and is the most optimal solution.
    } else if let Some(change_solution) = change_solution {
        // a change solution exists, but no gifted solution exists.
        // this is unexpected, as we should always be able to gift change.
        panic!("unexpected state: change solution exists, but no gifted solution exists.");
    } else {
        None // no solution exists.
    }
}

#[cfg(test)]
pub mod test {

    use super::*;

    #[test]
    fn test_find_closest_solution() {
        let target_sum = 100;
        let sorted_input_amounts = vec![78, 55, 23, 10, 8, 3, 2, 1];
        let target_size = 3;
        let (vals, above_target_amount) =
            find_closest_subset(&sorted_input_amounts, target_size, target_sum);
        let indexes = vals.iter().map(|val| sorted_input_amounts.iter().position(|&x| x == *val).unwrap()).collect::<Vec<usize>>();
        assert_eq!(vals.iter().sum::<u64>(), target_sum + above_target_amount);
        assert_eq!(indexes, vec![0, 2, 7]);
        assert_eq!(above_target_amount, 2);
        println!("num_of_inputs: {:?}, indexes:{:?}, above_target_amount {:?}", indexes.len(), indexes, above_target_amount);
    }
}
