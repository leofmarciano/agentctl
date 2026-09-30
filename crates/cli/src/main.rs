// ABOUTME: Dispatches agentctl commands and reports bounded CLI failures.
// ABOUTME: Preserves native exit codes and blocking hook failure semantics.
mod app;
mod args;
#[cfg(unix)]
mod codex_launch;
mod config;
mod doctor;
mod native;
mod native_args;
mod native_hooks;
mod operations;
mod paths;
mod runtime;

use agentctl_telemetry::{PayloadGuard, PayloadLimits, RedactionConfig, Redactor, neutralize_ansi};
use anyhow::Result;
use args::Cli;
use clap::Parser;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        if let Some(native) = error.downcast_ref::<native::NativeExitError>() {
            std::process::exit(native.code);
        }
        let message = Redactor::new(&RedactionConfig::with_secret_defaults())
            .map(|redactor| PayloadGuard::new(PayloadLimits::default(), redactor))
            .ok()
            .and_then(|guard| guard.process_text(&format!("{error:#}")).ok())
            .unwrap_or_else(|| neutralize_ansi(&format!("{error:#}")));
        eprintln!("error: {message}");
        std::process::exit(if is_internal_claude_hook_invocation() {
            // Claude Code treats hook exit 2 as a blocking failure. This also
            // covers failures which occur before the hook dispatcher can emit
            // its structured `continue:false` response (config/DB/open errors).
            2
        } else {
            1
        });
    }
}

fn is_internal_claude_hook_invocation() -> bool {
    let arguments = std::env::args_os().collect::<Vec<_>>();
    arguments
        .windows(2)
        .any(|pair| pair[0] == "hook" && pair[1] == "claude")
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    app::dispatch(cli).await
}
