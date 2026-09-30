// ABOUTME: Exposes Codex native session and protocol discovery operations.
// ABOUTME: Keeps transport internals behind the provider adapter boundary.
//! Native Codex app-server adapter.

mod adapter;
mod jsonrpc;
mod mapping;
mod process;
mod schema;

#[cfg(unix)]
pub use process::interactive_endpoint;

pub use adapter::{
    CodexAdapter, CodexConfig, InteractiveThreadIdentity, InteractiveThreadSnapshot,
    MAX_INTERACTIVE_THREAD_SNAPSHOT,
};
pub use schema::{
    CodexInstallation, detect_installation, generate_schema_cache, generate_schema_cache_under,
};
