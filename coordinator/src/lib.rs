//! Node-facing mining: the coordinator, its workers and the round loop.
//! No network operation happens at construction.
#![forbid(unsafe_code)]
pub mod config;
pub mod coordinator;
pub mod core;
pub mod http;
pub mod run;
pub mod worker;
pub use config::*;
pub use coordinator::*;
pub use core::*;
pub use http::*;
pub use run::*;
pub use worker::*;
