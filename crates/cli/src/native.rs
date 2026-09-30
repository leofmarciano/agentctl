// ABOUTME: Launches native provider CLIs with inherited terminal IO.
// ABOUTME: Journals child creation and bounds process-tree cleanup.
use std::{
    ffi::OsString,
    path::Path,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use agentctl_core::ProviderKind;
use agentctl_provider_claude::HANDOFF_POLICY;
use agentctl_workspace::{
    ProcessGroupId, ProcessTree, configure_tokio_process_group, process_root_exists,
};
#[cfg(unix)]
use agentctl_workspace::{kill_process_group, process_group_exists, terminate_process_group};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use tokio::process::{Child, Command};

const NATIVE_TERMINATION_GRACE: Duration = Duration::from_secs(3);
const NATIVE_KILL_WAIT: Duration = Duration::from_secs(3);

/// Marker attached to errors raised after the operating system has created the
/// native provider process. Callers use it to keep the launch journal open for
/// reconciliation instead of misclassifying a post-spawn failure as a safe
/// pre-spawn failure.
#[derive(Clone, Copy, Debug)]
struct NativeProcessWasSpawned;

impl std::fmt::Display for NativeProcessWasSpawned {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("native provider process was spawned; reconciliation is required")
    }
}

/// Returns true when `error` was produced after the native process existed.
///
/// This remains true even when process-tree containment, PID journaling,
/// terminal handoff, waiting, restoration, or cleanup subsequently failed.
#[must_use]
pub(crate) fn error_happened_after_spawn(error: &anyhow::Error) -> bool {
    error.downcast_ref::<NativeProcessWasSpawned>().is_some()
}

/// Returned only after the native process has exited and its canonical capture
/// has completed. `main` propagates this code without adding wrapper output.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeExitError {
    pub code: i32,
}

impl std::fmt::Display for NativeExitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "native provider exited with status {}",
            self.code
        )
    }
}

impl std::error::Error for NativeExitError {}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct NativeCliSpawn {
    pub provider: ProviderKind,
    pub native_session_id: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct NativeCliExit {
    pub provider: ProviderKind,
    pub native_session_id: String,
    /// The provider's ordinary process exit code, when it exited normally.
    pub code: Option<i32>,
    /// The signal that killed the provider on Unix, when applicable.
    pub signal: Option<i32>,
    /// SIGTERM or SIGHUP received by agentctl while the provider was active.
    pub parent_signal: Option<i32>,
    pub success: bool,
}

impl NativeCliExit {
    /// Returns the nonzero code the caller should propagate after it finishes
    /// capturing the native transcript. The module deliberately does not call
    /// `process::exit`, because doing so would skip that post-exit capture.
    #[must_use]
    pub(crate) fn propagated_exit_code(&self) -> Option<i32> {
        if self.success {
            return None;
        }
        self.parent_signal
            .map(signal_exit_code)
            .or(self.code)
            .or_else(|| self.signal.map(signal_exit_code))
            .or(Some(1))
    }
}

/// Launches Codex with inherited native stdio and invokes `on_spawn`
/// synchronously as soon as the child PID is available, before waiting for it.
pub(crate) async fn launch_codex_with_spawn<F>(
    binary: &str,
    workspace: &Path,
    native_session_id: &str,
    native_args: &[OsString],
    endpoint: Option<&Path>,
    on_spawn: F,
) -> Result<NativeCliExit>
where
    F: FnOnce(&NativeCliSpawn) -> Result<()>,
{
    crate::native_args::validate(&ProviderKind::Codex, native_args)?;
    let signals = NativeSignalHandlers::install()?;
    let mut command = Command::new(binary);
    command
        .arg("resume")
        .arg(native_session_id)
        .arg("--cd")
        .arg(workspace)
        .args(native_args)
        .current_dir(workspace)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if let Some(endpoint) = endpoint {
        command
            .arg("--remote")
            .arg(format!("unix://{}", endpoint.display()));
    }
    configure_tokio_process_group(&mut command);
    let child = command
        .spawn()
        .with_context(|| format!("failed to open native Codex CLI using {binary}"))?;
    finish_native_child(
        ProviderKind::Codex,
        native_session_id,
        child,
        signals,
        on_spawn,
    )
    .await
    .context(NativeProcessWasSpawned)
}

/// Launches Claude Code with inherited native stdio and invokes `on_spawn`
/// synchronously as soon as the child PID is available, before waiting for it.
pub(crate) async fn launch_claude_with_spawn<F>(
    binary: &str,
    workspace: &Path,
    native_session_id: &str,
    resume: bool,
    settings: &Path,
    native_args: &[OsString],
    on_spawn: F,
) -> Result<NativeCliExit>
where
    F: FnOnce(&NativeCliSpawn) -> Result<()>,
{
    crate::native_args::validate(&ProviderKind::Claude, native_args)?;
    let signals = NativeSignalHandlers::install()?;
    let mut command = Command::new(binary);
    if resume {
        command.arg("--resume").arg(native_session_id);
    } else {
        command.arg("--session-id").arg(native_session_id);
    }
    command
        .arg("--settings")
        .arg(settings)
        // This policy remains wrapper-owned. Native arguments that could
        // replace settings, session identity, or the interactive lifecycle are
        // rejected by the fail-closed native argument validator before spawn.
        .arg("--append-system-prompt")
        .arg(HANDOFF_POLICY)
        .args(native_args)
        .current_dir(workspace)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    configure_tokio_process_group(&mut command);
    let child = command
        .spawn()
        .with_context(|| format!("failed to open native Claude Code CLI using {binary}"))?;
    finish_native_child(
        ProviderKind::Claude,
        native_session_id,
        child,
        signals,
        on_spawn,
    )
    .await
    .context(NativeProcessWasSpawned)
}

async fn finish_native_child<F>(
    provider: ProviderKind,
    native_session_id: &str,
    child: Child,
    signals: NativeSignalHandlers,
    on_spawn: F,
) -> Result<NativeCliExit>
where
    F: FnOnce(&NativeCliSpawn) -> Result<()>,
{
    finish_native_child_with_terminal(
        provider,
        native_session_id,
        child,
        signals,
        on_spawn,
        NativeTerminalForeground::give_to,
    )
    .await
}

async fn finish_native_child_with_terminal<F, G>(
    provider: ProviderKind,
    native_session_id: &str,
    mut child: Child,
    signals: NativeSignalHandlers,
    on_spawn: F,
    give_terminal: G,
) -> Result<NativeCliExit>
where
    F: FnOnce(&NativeCliSpawn) -> Result<()>,
    G: FnOnce(ProcessGroupId) -> Result<NativeTerminalForeground>,
{
    let pid = child
        .id()
        .context("native provider exited before its child PID could be recorded")?;
    // On Windows this attaches the child to the owned Job Object before any
    // user callback runs. Keeping containment first prevents a fast child from
    // escaping while still minimizing the spawn-to-journal interval.
    let process_tree = ProcessTree::attach(&child)
        .context("failed to contain the native provider process tree")?;
    let spawned = NativeCliSpawn {
        provider: provider.clone(),
        native_session_id: native_session_id.to_owned(),
        pid,
    };
    // The durable PID receipt must exist before the provider can receive the
    // foreground terminal. A wrapper crash before this callback completes is
    // intentionally left as an ambiguous Started/no-PID journal entry.
    if let Err(error) = on_spawn(&spawned) {
        let cleanup = abort_native_child(&mut child, &process_tree, None).await;
        return match cleanup {
            Ok(()) => Err(error.context("failed recording the native provider child PID")),
            Err(cleanup) => Err(error.context(format!(
                "failed recording the native provider child PID; child cleanup also failed: {cleanup:#}"
            ))),
        };
    }
    let terminal = match give_terminal(process_tree.id()) {
        Ok(terminal) => terminal,
        Err(error) => {
            let cleanup = abort_native_child(&mut child, &process_tree, None).await;
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "terminal handoff failed; child cleanup also failed: {cleanup:#}"
                ))),
            };
        }
    };

    // The provider owns the terminal foreground while it runs, preserving its
    // native keyboard handling. Agentctl retakes the terminal before capture.
    let waited = wait_preserving_native_signals(&mut child, &process_tree, signals).await;
    let restored = terminal.restore();
    // Cleanup must run even when waiting or terminal restoration fails. The
    // workspace lease cannot be released while an owned descendant may still
    // be executing provider work.
    let cleanup = cleanup_native_descendants(&process_tree).await;
    let waited = match waited {
        Ok(waited) => waited,
        Err(error) => {
            let mut error = error;
            if let Err(restore) = restored {
                error = error.context(format!("terminal restoration also failed: {restore:#}"));
            }
            if let Err(cleanup) = cleanup {
                error = error.context(format!("descendant cleanup also failed: {cleanup:#}"));
            }
            return Err(error);
        }
    };
    if let Err(error) = restored {
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => {
                Err(error.context(format!("descendant cleanup also failed: {cleanup:#}")))
            }
        };
    }
    cleanup?;
    Ok(exit_report(
        provider,
        native_session_id,
        waited.status,
        waited.parent_signal,
    ))
}

async fn abort_native_child(
    child: &mut Child,
    process_tree: &ProcessTree,
    terminal: Option<NativeTerminalForeground>,
) -> Result<()> {
    let terminated = terminate_native_child(child, process_tree).await;
    let restored = terminal.map_or(Ok(()), NativeTerminalForeground::restore);
    let descendants = cleanup_native_descendants(process_tree).await;
    let mut failures = Vec::new();
    if let Err(error) = terminated {
        failures.push(format!("root termination failed: {error:#}"));
    }
    if let Err(error) = restored {
        failures.push(format!("terminal restoration failed: {error:#}"));
    }
    if let Err(error) = descendants {
        failures.push(format!("descendant cleanup failed: {error:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("{}", failures.join("; "))
    }
}

struct NativeWaitResult {
    status: ExitStatus,
    parent_signal: Option<i32>,
}

#[cfg(unix)]
struct NativeSignalHandlers {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl NativeSignalHandlers {
    fn install() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())
                .context("failed to install native CLI SIGINT handler")?,
            terminate: signal(SignalKind::terminate())
                .context("failed to install native CLI SIGTERM handler")?,
            hangup: signal(SignalKind::hangup())
                .context("failed to install native CLI SIGHUP handler")?,
        })
    }
}

#[cfg(not(unix))]
struct NativeSignalHandlers;

#[cfg(not(unix))]
impl NativeSignalHandlers {
    fn install() -> Result<Self> {
        Ok(Self)
    }
}

#[cfg(unix)]
async fn wait_preserving_native_signals(
    child: &mut Child,
    process_tree: &ProcessTree,
    mut signals: NativeSignalHandlers,
) -> Result<NativeWaitResult> {
    use nix::sys::signal::Signal;

    loop {
        tokio::select! {
            biased;
            status = child.wait() => {
                return Ok(NativeWaitResult {
                    status: status.context("failed waiting for native provider CLI")?,
                    parent_signal: None,
                });
            }
            received = signals.terminate.recv() => {
                received.context("native CLI SIGTERM listener closed unexpectedly")?;
                return Ok(NativeWaitResult {
                    status: terminate_native_child(child, process_tree).await?,
                    parent_signal: Some(Signal::SIGTERM as i32),
                });
            }
            received = signals.hangup.recv() => {
                received.context("native CLI SIGHUP listener closed unexpectedly")?;
                return Ok(NativeWaitResult {
                    status: terminate_native_child(child, process_tree).await?,
                    parent_signal: Some(Signal::SIGHUP as i32),
                });
            }
            received = signals.interrupt.recv() => {
                received.context("native CLI SIGINT listener closed unexpectedly")?;
                // The foreground provider receives the terminal signal itself.
                // Agentctl deliberately stays alive for post-exit capture.
            }
        }
    }
}

#[cfg(not(unix))]
async fn wait_preserving_native_signals(
    child: &mut Child,
    _process_tree: &ProcessTree,
    _signals: NativeSignalHandlers,
) -> Result<NativeWaitResult> {
    loop {
        tokio::select! {
            biased;
            status = child.wait() => {
                return Ok(NativeWaitResult {
                    status: status.context("failed waiting for native provider CLI")?,
                    parent_signal: None,
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for Ctrl-C while native provider was active")?;
                // The foreground provider receives the console event itself.
            }
        }
    }
}

#[cfg(unix)]
async fn terminate_native_child(
    child: &mut Child,
    process_tree: &ProcessTree,
) -> Result<ExitStatus> {
    terminate_process_group(process_tree.id())
        .context("failed to terminate native provider process group")?;
    if let Ok(status) = tokio::time::timeout(NATIVE_TERMINATION_GRACE, child.wait()).await {
        status.context("failed waiting for terminated native provider CLI")
    } else {
        kill_process_group(process_tree.id())
            .context("failed to kill unresponsive native provider process group")?;
        tokio::time::timeout(NATIVE_KILL_WAIT, child.wait())
            .await
            .context("timed out reaping killed native provider CLI")?
            .context("failed reaping killed native provider CLI")
    }
}

#[cfg(not(unix))]
async fn terminate_native_child(
    child: &mut Child,
    process_tree: &ProcessTree,
) -> Result<ExitStatus> {
    process_tree
        .terminate()
        .context("failed to terminate native provider process tree")?;
    tokio::time::timeout(NATIVE_KILL_WAIT, child.wait())
        .await
        .context("timed out reaping terminated native provider CLI")?
        .context("failed reaping terminated native provider CLI")
}

#[cfg(unix)]
async fn cleanup_native_descendants(process_tree: &ProcessTree) -> Result<()> {
    if !process_group_exists(process_tree.id())? {
        return Ok(());
    }
    terminate_process_group(process_tree.id())?;
    let deadline = tokio::time::Instant::now() + NATIVE_TERMINATION_GRACE;
    while process_group_exists(process_tree.id())? && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if process_group_exists(process_tree.id())? {
        kill_process_group(process_tree.id())?;
        let deadline = tokio::time::Instant::now() + NATIVE_KILL_WAIT;
        while process_group_exists(process_tree.id())? && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    if process_group_exists(process_tree.id())? {
        bail!("native provider descendants remained alive after process-group cleanup");
    }
    Ok(())
}

#[cfg(not(unix))]
async fn cleanup_native_descendants(process_tree: &ProcessTree) -> Result<()> {
    // On Windows, terminating the owned Job Object after the root exits also
    // terminates any background descendants. The object stays owned until this
    // function returns.
    #[cfg(windows)]
    process_tree.terminate()?;
    #[cfg(not(windows))]
    let _ = process_tree;
    Ok(())
}

/// Checks the journaled native process identity after a wrapper crash.
pub(crate) fn native_process_is_running(pid: u32) -> Result<bool> {
    process_root_exists(ProcessGroupId::from_child_id(pid)).map_err(Into::into)
}

#[cfg(unix)]
struct NativeTerminalForeground {
    parent_group: Option<nix::unistd::Pid>,
}

#[cfg(unix)]
impl NativeTerminalForeground {
    fn give_to(group: ProcessGroupId) -> Result<Self> {
        use std::io::IsTerminal;

        if !std::io::stdin().is_terminal() {
            return Ok(Self { parent_group: None });
        }
        let group = i32::try_from(group.get()).context("native process group exceeds pid_t")?;
        let parent_group = nix::unistd::getpgrp();
        nix::unistd::tcsetpgrp(std::io::stdin(), nix::unistd::Pid::from_raw(group))
            .context("failed to give the terminal to the native provider")?;
        Ok(Self {
            parent_group: Some(parent_group),
        })
    }

    fn restore(self) -> Result<()> {
        use nix::sys::signal::{SigSet, SigmaskHow, Signal, pthread_sigmask};

        let Some(parent_group) = self.parent_group else {
            return Ok(());
        };
        let mut blocked = SigSet::empty();
        blocked.add(Signal::SIGTTOU);
        let mut previous = SigSet::empty();
        pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&blocked), Some(&mut previous))
            .context("failed to block SIGTTOU while restoring the terminal")?;
        let restored = nix::unistd::tcsetpgrp(std::io::stdin(), parent_group)
            .context("failed to restore agentctl as the terminal foreground process");
        let mask = pthread_sigmask(SigmaskHow::SIG_SETMASK, Some(&previous), None)
            .context("failed to restore the terminal signal mask");
        restored.and(mask)
    }
}

#[cfg(not(unix))]
struct NativeTerminalForeground;

#[cfg(not(unix))]
impl NativeTerminalForeground {
    fn give_to(_group: ProcessGroupId) -> Result<Self> {
        Ok(Self)
    }

    fn restore(self) -> Result<()> {
        Ok(())
    }
}

fn exit_report(
    provider: ProviderKind,
    native_session_id: &str,
    status: ExitStatus,
    parent_signal: Option<i32>,
) -> NativeCliExit {
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    };
    #[cfg(not(unix))]
    let signal = None;
    NativeCliExit {
        provider,
        native_session_id: native_session_id.to_owned(),
        code: status.code(),
        signal,
        parent_signal,
        success: parent_signal.is_none() && status.success(),
    }
}

fn signal_exit_code(signal: i32) -> i32 {
    128_i32.saturating_add(signal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_exit_reports_a_propagatable_code() {
        let report = NativeCliExit {
            provider: ProviderKind::Codex,
            native_session_id: "native".to_owned(),
            code: Some(37),
            signal: None,
            parent_signal: None,
            success: false,
        };
        assert_eq!(report.propagated_exit_code(), Some(37));
        let interrupted = NativeCliExit {
            code: None,
            signal: Some(15),
            ..report
        };
        assert_eq!(interrupted.propagated_exit_code(), Some(143));
    }

    #[cfg(unix)]
    mod unix {
        use std::{
            fs,
            os::unix::fs::PermissionsExt,
            sync::{
                Arc,
                atomic::{AtomicU32, Ordering},
            },
        };

        use tempfile::TempDir;

        use super::*;

        fn fake_native_script(body: &str) -> (TempDir, std::path::PathBuf) {
            let directory = tempfile::tempdir().expect("tempdir");
            let script = directory.path().join("fake-native");
            fs::write(&script, format!("#!/bin/sh\n{body}\n")).expect("write fake native CLI");
            let mut permissions = fs::metadata(&script)
                .expect("script metadata")
                .permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(&script, permissions).expect("make fake native CLI executable");
            (directory, script)
        }

        #[tokio::test]
        async fn spawn_callback_observes_child_before_wait() {
            let (directory, script) = fake_native_script("exit 0");
            let observed = Arc::new(AtomicU32::new(0));
            let callback_observed = observed.clone();
            let report = launch_codex_with_spawn(
                script.to_str().expect("UTF-8 script"),
                directory.path(),
                "native-session",
                &[],
                None,
                move |spawn| {
                    callback_observed.store(spawn.pid, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .expect("launch fake native CLI");
            assert!(report.success);
            assert_ne!(observed.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn spawn_callback_runs_before_terminal_handoff() {
            let (_directory, script) = fake_native_script("sleep 0.1");
            let mut command = Command::new(script);
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            configure_tokio_process_group(&mut command);
            let child = command.spawn().expect("spawn fake native CLI");
            let order = Arc::new(AtomicU32::new(0));
            let callback_order = order.clone();
            let terminal_order = order.clone();
            let report = finish_native_child_with_terminal(
                ProviderKind::Codex,
                "native-session",
                child,
                NativeSignalHandlers::install().expect("install signals"),
                move |_| {
                    assert_eq!(callback_order.swap(1, Ordering::SeqCst), 0);
                    Ok(())
                },
                move |_| {
                    assert_eq!(terminal_order.swap(2, Ordering::SeqCst), 1);
                    Ok(NativeTerminalForeground { parent_group: None })
                },
            )
            .await
            .expect("finish fake native CLI");
            assert!(report.success);
            assert_eq!(order.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn callback_failure_is_classified_as_post_spawn() {
            let (directory, script) = fake_native_script("sleep 30");
            let error = launch_codex_with_spawn(
                script.to_str().expect("UTF-8 script"),
                directory.path(),
                "native-session",
                &[],
                None,
                |_| bail!("simulated journal failure"),
            )
            .await
            .expect_err("callback must fail");
            assert!(error_happened_after_spawn(&error));
            assert!(format!("{error:#}").contains("simulated journal failure"));
        }

        #[tokio::test]
        async fn spawn_failure_is_not_classified_as_post_spawn() {
            let directory = tempfile::tempdir().expect("tempdir");
            let error = launch_codex_with_spawn(
                "/definitely/not/an/agentctl/provider",
                directory.path(),
                "native-session",
                &[],
                None,
                |_| Ok(()),
            )
            .await
            .expect_err("spawn must fail");
            assert!(!error_happened_after_spawn(&error));
        }

        #[tokio::test]
        async fn native_nonzero_exit_is_preserved() {
            let (directory, script) = fake_native_script("exit 37");
            let report = launch_codex_with_spawn(
                script.to_str().expect("UTF-8 script"),
                directory.path(),
                "native-session",
                &[],
                None,
                |_| Ok(()),
            )
            .await
            .expect("launch fake native CLI");
            assert!(!report.success);
            assert_eq!(report.code, Some(37));
            assert_eq!(report.propagated_exit_code(), Some(37));
        }

        #[tokio::test]
        async fn termination_is_bounded_and_reaps_child() {
            let (_directory, script) =
                fake_native_script("trap 'exit 42' TERM\nwhile :; do sleep 1; done");
            let mut command = Command::new(script);
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            configure_tokio_process_group(&mut command);
            let mut child = command.spawn().expect("spawn fake native CLI");
            let process_tree = ProcessTree::attach(&child).expect("attach process tree");
            let status = tokio::time::timeout(
                NATIVE_TERMINATION_GRACE + NATIVE_KILL_WAIT + Duration::from_secs(1),
                terminate_native_child(&mut child, &process_tree),
            )
            .await
            .expect("termination exceeded its bound")
            .expect("terminate fake native CLI");
            assert!(!status.success());
        }
    }
}
