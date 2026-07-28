//! Correctness checks that sit beside the timing measurements.
//!
//! The rest of the tool answers "who delivered this first". These modules answer
//! "was what they delivered real" — a separate question, deliberately kept off
//! the hot path and out of the racing logic.

pub mod onchain_signatures;
