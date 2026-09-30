// ABOUTME: Coordinates native provider launches, handoffs, and canonical history capture.
// ABOUTME: Enforces workspace and session integrity across CLI operations.
//! Application composition root. This is the only layer that knows concrete
//! storage, provider protocol, native process, and workspace types.

use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use agentctl_core::{
    AgentProvider, AuthMode, CanonicalEvent, EventId, EventVisibility, NativeSession,
    ProviderError, ProviderKind, SessionContext, SessionStatus, TurnId, UnifiedSession,
    UnifiedSessionId,
};
use agentctl_plugin_protocol::{ClientOptions, PluginClient};
use agentctl_provider_claude::{
    ClaudeAdapter, ClaudeConfig, HANDOFF_POLICY, ShouldQuerySupport, UserPromptSubmitHookSupport,
};
use agentctl_provider_codex::{CodexAdapter, InteractiveThreadSnapshot};
use agentctl_storage::{
    AgentctlStore, NativeLaunchRecord, NativeLaunchState, ProtocolCapabilityRecord,
    WorkspaceSnapshotRecord, canonical_content_hash,
};
use agentctl_telemetry::{LoggingConfig, init_logging};
use agentctl_transcript::CompactionPolicy;
use agentctl_workspace::{
    GitSnapshot, WorkspaceIdentity, WorkspaceLease, capture_git_snapshot, git_diff_summary,
};
use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde::Serialize;

use crate::{
    args::{
        Cli, Command, HookCommand, NativeProviderChoice, PluginCommand, ProviderChoice,
        WorkspaceCommand,
    },
    config::Config,
    doctor, native,
    operations::{self, ExportOptions},
    paths::AgentctlPaths,
    runtime::Runtime,
};

const MAX_NATIVE_PROVIDER_ATTEMPTS: usize = 2;

#[allow(clippy::too_many_lines)]
pub(crate) async fn dispatch(cli: Cli) -> Result<()> {
    // Reject unsafe forwarding before resolving paths, opening/migrating the
    // database, applying retention, creating a session, probing providers, or
    // reconciling an earlier native launch.
    preflight_native_command(cli.command.as_ref())?;
    let paths = AgentctlPaths::resolve(cli.home.clone())?;
    let _ = init_logging(&LoggingConfig::default());
    let cwd = std::env::current_dir()?;
    let config = Config::load(&paths, &cwd)?;
    let store = operations::open_store(&paths)?;
    let internal_hook = matches!(&cli.command, Some(Command::Hook(_)));
    if !internal_hook && let Some(retention_days) = config.retention_days {
        let report = operations::enforce_retention(&store, retention_days, Utc::now())?;
        if !report.deleted_sessions.is_empty() {
            tracing::info!(
                deleted_sessions = report.deleted_sessions.len(),
                retention_days,
                "applied local session retention policy"
            );
        }
    }

    match cli.command {
        None => {
            let session = match operations::resolve_session(&store, None, &cwd) {
                Ok(session) => session,
                Err(_) => create_session(&store, &config, &cwd, None, None)?,
            };
            let config = Config::load(&paths, &session.workspace_path)?;
            run_native_session(store, paths, config, session, None, Vec::new(), cli.json).await
        }
        Some(Command::Open(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            let config = Config::load(&paths, &session.workspace_path)?;
            run_native_session(
                store,
                paths,
                config,
                session,
                args.provider.map(native_provider_choice),
                args.native_args,
                cli.json,
            )
            .await
        }
        Some(Command::Switch(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            let config = Config::load(&paths, &session.workspace_path)?;
            run_native_session(
                store,
                paths,
                config,
                session,
                Some(native_provider_choice(args.provider)),
                args.native_args,
                cli.json,
            )
            .await
        }
        Some(Command::New(args)) => {
            let config = Config::load(&paths, &args.workspace)?;
            let manual = provider_choice(args.provider);
            let session = create_session(
                &store,
                &config,
                &args.workspace,
                args.name.as_deref(),
                manual.clone(),
            )?;
            if args.no_launch {
                print_value(&session, cli.json)
            } else {
                run_native_session(
                    store,
                    paths,
                    config,
                    session,
                    manual,
                    args.native_args,
                    cli.json,
                )
                .await
            }
        }
        Some(Command::Resume(args)) => {
            let session = operations::resolve_session(&store, Some(&args.session), &cwd)?;
            let config = Config::load(&paths, &session.workspace_path)?;
            run_native_session(
                store,
                paths,
                config,
                session,
                args.provider.map(native_provider_choice),
                args.native_args,
                cli.json,
            )
            .await
        }
        Some(Command::List) => print_value(&operations::list_sessions(&store)?, cli.json),
        Some(Command::Status(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            print_value(&operations::session_status(&store, session)?, cli.json)
        }
        Some(Command::Metrics(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            print_value(&operations::session_metrics(&store, session)?, cli.json)
        }
        Some(Command::History(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            print_value(
                &operations::session_history(&store, session.id, args.raw, args.limit)?,
                cli.json,
            )
        }
        Some(Command::Export(args)) => {
            let session = operations::resolve_session(&store, Some(&args.session), &cwd)?;
            print_value(
                &operations::export_session(
                    &store,
                    session,
                    &args.output,
                    ExportOptions {
                        include_blobs: args.include_blobs,
                        redact: args.redact,
                        include_internal: args.include_internal,
                    },
                )?,
                cli.json,
            )
        }
        Some(Command::Import(args)) => {
            print_value(&operations::import_session(&store, &args.input)?, cli.json)
        }
        Some(Command::Attach(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            let config = Config::load(&paths, &session.workspace_path)?;
            let provider = native_provider_choice(args.provider);
            print_value(
                &attach_native_session(
                    &paths,
                    &config,
                    &store,
                    &session,
                    provider,
                    &args.native_session_id,
                    args.activate,
                )
                .await?,
                cli.json,
            )
        }
        Some(Command::ImportNative(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            let config = Config::load(&paths, &session.workspace_path)?;
            print_value(
                &import_native_session(
                    &paths,
                    &config,
                    &store,
                    &session,
                    ProviderKind::Codex,
                    &args.native_session_id,
                    args.activate,
                )
                .await?,
                cli.json,
            )
        }
        Some(Command::Delete(args)) => {
            let session = operations::resolve_session(&store, Some(&args.session), &cwd)?;
            let _mutation_guard =
                operations::guard_session_mutation(&paths, &store, &session, "session deletion")?;
            print_value(&operations::delete_session(&store, &session)?, cli.json)
        }
        Some(Command::Compact(args)) => {
            let session = operations::resolve_session(&store, Some(&args.session), &cwd)?;
            let _mutation_guard =
                operations::guard_session_mutation(&paths, &store, &session, "session compaction")?;
            print_value(
                &operations::compact_session(&store, session.id, CompactionPolicy::default())?,
                cli.json,
            )
        }
        Some(Command::Sync(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            let config = Config::load(&paths, &session.workspace_path)?;
            let _mutation_guard = operations::guard_session_mutation(
                &paths,
                &store,
                &session,
                "projection synchronization",
            )?;
            let runtime = build_runtime(store, paths, config, session).await?;
            let result = runtime.sync_all().await;
            let value = finish_runtime(&runtime, result).await?;
            print_value(&value, cli.json)
        }
        Some(Command::Fork(args)) => {
            let session = operations::resolve_session(&store, Some(&args.session), &cwd)?;
            let _mutation_guard =
                operations::guard_session_mutation(&paths, &store, &session, "session fork")?;
            print_value(
                &operations::fork_session(
                    &store,
                    &session,
                    args.name.as_deref(),
                    args.through_seq,
                )?,
                cli.json,
            )
        }
        Some(Command::Repair(args)) => {
            let report = operations::repair_local_state(
                &paths,
                &store,
                args.rebuild_projections,
                args.abandon_native_launch,
            )?;
            print_value(&report, cli.json)?;
            if !report.healthy() {
                bail!("repair completed but local state still has integrity problems");
            }
            Ok(())
        }
        Some(Command::Doctor(args)) => {
            run_doctor(
                &paths,
                &config,
                &store,
                &cwd,
                args.live,
                args.report.as_deref(),
                cli.json,
            )
            .await
        }
        Some(Command::Provider(args)) => {
            let session = operations::resolve_session(&store, args.session.as_deref(), &cwd)?;
            let config = Config::load(&paths, &session.workspace_path)?;
            let _mutation_guard = operations::guard_session_mutation(
                &paths,
                &store,
                &session,
                "routing policy change",
            )?;
            let provider = provider_choice(args.provider);
            let policy = if provider.is_some() {
                "manual"
            } else {
                &config.routing.policy
            };
            store.update_session_routing(
                session.id,
                provider.as_ref(),
                policy,
                SessionStatus::Active,
                Utc::now(),
            )?;
            print_value(
                &serde_json::json!({
                    "session": session.id,
                    "provider": provider,
                    "routing_policy": policy,
                }),
                cli.json,
            )
        }
        Some(Command::Workspace(args)) => match args.command {
            WorkspaceCommand::List => {
                let entries = operations::list_sessions(&store)?;
                let mut workspaces = entries
                    .into_iter()
                    .map(|entry| entry.session.workspace_path)
                    .collect::<Vec<_>>();
                workspaces.sort();
                workspaces.dedup();
                print_value(&workspaces, cli.json)
            }
            WorkspaceCommand::Use { path } => {
                let session =
                    operations::resolve_session(&store, Some(&path.to_string_lossy()), &cwd)?;
                print_value(&session, cli.json)
            }
        },
        Some(Command::Plugin(args)) => match args.command {
            PluginCommand::List => print_value(&operations::list_plugins(&paths)?, cli.json),
            PluginCommand::Install { manifest } => {
                print_value(&operations::install_plugin(&paths, &manifest)?, cli.json)
            }
            PluginCommand::Remove { name } => {
                print_value(&operations::remove_plugin(&paths, &name)?, cli.json)
            }
            PluginCommand::Doctor { name } => print_value(
                &operations::doctor_plugins(&paths, name.as_deref())?,
                cli.json,
            ),
        },
        Some(Command::Hook(args)) => match args.command {
            HookCommand::Claude(args) => {
                crate::native_hooks::handle_claude_hook(&paths, &config, &store, args).await
            }
        },
    }
}

fn create_session(
    store: &AgentctlStore,
    config: &Config,
    workspace: &Path,
    name: Option<&str>,
    active_provider: Option<ProviderKind>,
) -> Result<UnifiedSession> {
    let identity = WorkspaceIdentity::discover(workspace)?;
    let id = UnifiedSessionId::new();
    let now = Utc::now();
    let default_name = identity
        .execution_root()
        .file_name()
        .and_then(|name| name.to_str())
        .map_or_else(
            || format!("session-{}", &id.to_string()[..8]),
            ToOwned::to_owned,
        );
    let session = UnifiedSession {
        id,
        name: name
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map_or(default_name, ToOwned::to_owned),
        workspace_path: identity.execution_root().to_path_buf(),
        workspace_fingerprint: identity.fingerprint,
        active_provider,
        routing_policy: config.routing.policy.clone(),
        auth_mode: AuthMode::NativeLocal,
        status: SessionStatus::Active,
        parent_session_id: None,
        created_at: now,
        updated_at: now,
        schema_version: 1,
    };
    store.create_session(&session)?;
    Ok(session)
}

#[allow(clippy::too_many_lines)]
async fn run_native_session(
    store: AgentctlStore,
    paths: AgentctlPaths,
    config: Config,
    session: UnifiedSession,
    requested_provider: Option<ProviderKind>,
    native_args: Vec<std::ffi::OsString>,
    json: bool,
) -> Result<()> {
    // This must remain the first operation in the shared native path. Callers
    // preflight before their own mutations too, but this boundary prevents a
    // future caller from acquiring a lease, reconciling a dead launch, probing
    // health, or preparing a projection for invalid forwarded arguments.
    crate::native_args::preflight(requested_provider.as_ref(), &native_args)?;
    let explicit_provider = requested_provider.is_some();
    let identity = WorkspaceIdentity::discover(&session.workspace_path)?;
    let _lease =
        WorkspaceLease::acquire_for_identity(&paths.locks, &identity, session.id, TurnId::new())
            .context("another native agentctl session already owns this worktree")?;
    let recovered = reconcile_open_native_launch(
        &store,
        &paths,
        &config,
        &session,
        &identity,
        requested_provider.as_ref(),
    )
    .await?;
    let mut session = session;
    let mut requested_provider = requested_provider;
    if requested_provider.is_none()
        && let Some(recovered) = &recovered
        && !recovered.clean_exit
        && recovered.continuation.is_none()
    {
        // An implicit reopen cannot silently change providers unless the
        // crash was first converted into a durable continuation capsule.
        requested_provider = Some(recovered.provider.clone());
    }
    let mut reports = Vec::new();
    for attempt in 0..MAX_NATIVE_PROVIDER_ATTEMPTS {
        let provider =
            select_native_provider(&paths, &config, &store, &session, requested_provider.take())
                .await?;
        ensure!(
            matches!(provider, ProviderKind::Claude | ProviderKind::Codex),
            "provider {provider} does not expose a supported native interactive CLI"
        );
        // Validate at the shared provider-selection boundary, before either
        // adapter can create a native session, project context, update
        // routing, journal a launch, write hook settings, or capture a
        // workspace snapshot. Provider launchers repeat the same validation as
        // defense in depth, but must never be its first execution point.
        crate::native_args::validate(&provider, &native_args)?;
        let report = match run_selected_native_provider(
            &store,
            &paths,
            &config,
            &session,
            &identity,
            &native_args,
            &provider,
            explicit_provider,
        )
        .await
        {
            Ok(report) => report,
            Err(error) => {
                if !native_failover_attempt_allowed(
                    attempt,
                    explicit_provider,
                    !native_args.is_empty(),
                ) {
                    return Err(error);
                }
                let fallback = alternate_native_provider(&provider);
                let recovery = match reconcile_open_native_launch(
                    &store,
                    &paths,
                    &config,
                    &session,
                    &identity,
                    Some(&fallback),
                )
                .await
                {
                    Ok(Some(recovery)) if recovery.continuation.is_some() => recovery,
                    Ok(Some(_)) => {
                        return Err(error.context(
                            "native process failed, but no durable cross-provider continuation was created",
                        ));
                    }
                    Ok(None) => {
                        return Err(error.context(
                            "native process failed before a recoverable launch was journaled",
                        ));
                    }
                    Err(recovery) => {
                        return Err(error.context(format!(
                            "safe native failover reconciliation was refused: {recovery:#}"
                        )));
                    }
                };
                let health = probe_native_candidate(&paths, &config, &store, &session, &fallback)
                    .await
                    .with_context(|| {
                        format!(
                            "{provider} crash was preserved as a continuation, but {fallback} is unavailable"
                        )
                    })?;
                ensure!(
                    health.status.available(),
                    "{} crash was preserved as a continuation, but {fallback} is {:?}",
                    provider,
                    health.status
                );
                let continuation = recovery.continuation.as_ref().expect("checked above");
                eprintln!(
                    "agentctl: {provider} ended unexpectedly after {:?} side effects; opening native {fallback} for continuation (no prompt was replayed)",
                    continuation.side_effect_state
                );
                reports.push(serde_json::json!({
                    "provider": provider,
                    "native_crash": true,
                    "recovered_launch_id": recovery.launch_id,
                    "continuation_event_id": continuation.event_id,
                    "side_effect_state": continuation.side_effect_state,
                    "fallback_provider": fallback,
                    "prompt_replayed": false,
                }));
                session = store
                    .get_session(session.id)?
                    .context("canonical session disappeared before native crash failover")?;
                requested_provider = Some(fallback);
                continue;
            }
        };
        let exit_code = native_report_exit_code(&report);
        let failover =
            native_failover_attempt_allowed(attempt, explicit_provider, !native_args.is_empty())
                && should_failover_after_native(
                    &store,
                    &provider,
                    config.routing.failure_window_seconds,
                )?;
        reports.push(report);
        if failover {
            session = store
                .get_session(session.id)?
                .context("canonical session disappeared before native failover")?;
            continue;
        }
        if json {
            if reports.len() == 1 {
                print_value(&reports[0], true)?;
            } else {
                print_value(&serde_json::json!({"native_failover": reports}), true)?;
            }
        }
        if let Some(code) = exit_code {
            return Err(native::NativeExitError { code }.into());
        }
        return Ok(());
    }
    unreachable!("native failover loop is bounded to two attempts")
}

fn preflight_native_command(command: Option<&Command>) -> Result<()> {
    match command {
        Some(Command::Open(args)) => crate::native_args::preflight(
            args.provider.map(native_provider_choice).as_ref(),
            &args.native_args,
        ),
        Some(Command::Switch(args)) => crate::native_args::preflight(
            Some(&native_provider_choice(args.provider)),
            &args.native_args,
        ),
        Some(Command::New(args)) => crate::native_args::preflight(
            provider_choice(args.provider).as_ref(),
            &args.native_args,
        ),
        Some(Command::Resume(args)) => crate::native_args::preflight(
            args.provider.map(native_provider_choice).as_ref(),
            &args.native_args,
        ),
        None
        | Some(
            Command::List
            | Command::Status(_)
            | Command::Metrics(_)
            | Command::Doctor(_)
            | Command::History(_)
            | Command::Export(_)
            | Command::Import(_)
            | Command::Attach(_)
            | Command::ImportNative(_)
            | Command::Delete(_)
            | Command::Repair(_)
            | Command::Compact(_)
            | Command::Sync(_)
            | Command::Fork(_)
            | Command::Provider(_)
            | Command::Workspace(_)
            | Command::Plugin(_)
            | Command::Hook(_),
        ) => Ok(()),
    }
}

fn alternate_native_provider(provider: &ProviderKind) -> ProviderKind {
    match provider {
        ProviderKind::Claude => ProviderKind::Codex,
        ProviderKind::Codex => ProviderKind::Claude,
        ProviderKind::Plugin(_) => unreachable!("plugins do not expose a native CLI"),
    }
}

fn native_failover_attempt_allowed(
    attempt: usize,
    explicit_provider: bool,
    has_forwarded_native_args: bool,
) -> bool {
    !explicit_provider
        && !has_forwarded_native_args
        && attempt.saturating_add(1) < MAX_NATIVE_PROVIDER_ATTEMPTS
}

#[allow(clippy::too_many_arguments)]
async fn run_selected_native_provider(
    store: &AgentctlStore,
    paths: &AgentctlPaths,
    config: &Config,
    session: &UnifiedSession,
    identity: &WorkspaceIdentity,
    native_args: &[std::ffi::OsString],
    provider: &ProviderKind,
    explicit_provider: bool,
) -> Result<serde_json::Value> {
    let before = capture_git_snapshot(identity.execution_root())
        .context("failed to capture workspace state before native launch")?;
    persist_workspace_snapshot(
        store,
        config,
        session,
        provider,
        "before_native",
        &before,
        None,
    )?;
    let result = match provider {
        ProviderKind::Codex => {
            run_native_codex(
                store,
                paths,
                config,
                session,
                identity,
                native_args,
                explicit_provider,
            )
            .await
        }
        ProviderKind::Claude => {
            crate::native_hooks::run_native_claude(
                store,
                paths,
                config,
                session,
                identity,
                native_args,
                explicit_provider,
            )
            .await
        }
        ProviderKind::Plugin(_) => bail!("plugins do not expose a native interactive CLI"),
    };
    let after = capture_git_snapshot(identity.execution_root())
        .context("failed to capture workspace state after native launch");
    let snapshot = after.and_then(|after| {
        persist_workspace_snapshot(
            store,
            config,
            session,
            provider,
            "after_native",
            &after,
            Some(&before),
        )
    });
    match (result, snapshot) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(snapshot)) => Err(snapshot),
        (Err(error), Err(snapshot)) => {
            Err(error.context(format!("workspace capture also failed: {snapshot:#}")))
        }
    }
}

fn persist_workspace_snapshot(
    store: &AgentctlStore,
    config: &Config,
    session: &UnifiedSession,
    provider: &ProviderKind,
    phase: &str,
    snapshot: &GitSnapshot,
    previous: Option<&GitSnapshot>,
) -> Result<()> {
    let guard = config.payload_guard()?;
    let snapshot_json = guard.process_json(&serde_json::to_value(snapshot)?)?;
    store.record_workspace_snapshot(&WorkspaceSnapshotRecord {
        id: EventId::new(),
        session_id: session.id,
        turn_id: None,
        phase: phase.to_owned(),
        fingerprint: session.workspace_fingerprint.clone(),
        snapshot: snapshot_json.clone(),
        diff_digest: Some(snapshot.diff_digest.clone()),
        created_at: Utc::now(),
    })?;
    if previous.is_none_or(|previous| !snapshot.changed_since(previous)) {
        return Ok(());
    }
    let diff_summary = git_diff_summary(&session.workspace_path, 64 * 1024).ok();
    let payload = guard.process_json(&serde_json::json!({
        "type": "workspace_snapshot",
        "phase": phase,
        "provider": provider,
        "snapshot": snapshot_json,
        "before_diff_digest": previous.map(|snapshot| snapshot.diff_digest.as_str()),
        "diff_summary": diff_summary,
    }))?;
    let event = CanonicalEvent {
        schema_version: 1,
        session_id: session.id,
        seq: 0,
        event_id: EventId::new(),
        turn_id: None,
        origin_provider: Some(provider.clone()),
        kind: "workspace_snapshot".to_owned(),
        visibility: EventVisibility::Projection,
        content_hash: canonical_content_hash(
            "workspace_snapshot",
            EventVisibility::Projection,
            &payload,
        )?,
        payload,
        raw_event_id: None,
        created_at: Utc::now(),
    };
    store.append_event_allocating_seq(event, None)?;
    Ok(())
}

fn should_failover_after_native(
    store: &AgentctlStore,
    provider: &ProviderKind,
    failure_window_seconds: u64,
) -> Result<bool> {
    Ok(store.latest_health(provider)?.is_some_and(|health| {
        matches!(
            health.status,
            agentctl_core::ProviderStatus::Exhausted { .. }
                | agentctl_core::ProviderStatus::Overloaded
                | agentctl_core::ProviderStatus::AuthError
                | agentctl_core::ProviderStatus::Offline
        ) && persisted_health_blocks_auto(&health, failure_window_seconds)
    }))
}

fn native_report_exit_code(report: &serde_json::Value) -> Option<i32> {
    let exit = report.get("native_exit")?;
    if exit.get("success").and_then(serde_json::Value::as_bool) == Some(true) {
        return None;
    }
    let explicit = |field: &str| {
        exit.get(field)
            .and_then(serde_json::Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
    };
    explicit("parent_signal")
        .map(|signal| 128_i32.saturating_add(signal))
        .or_else(|| explicit("code"))
        .or_else(|| explicit("signal").map(|signal| 128_i32.saturating_add(signal)))
        .or(Some(1))
}

#[derive(Clone, Debug)]
struct ReconciledNativeLaunch {
    launch_id: uuid::Uuid,
    provider: ProviderKind,
    clean_exit: bool,
    continuation: Option<operations::NativeFailoverContinuation>,
}

async fn reconcile_open_native_launch(
    store: &AgentctlStore,
    paths: &AgentctlPaths,
    config: &Config,
    session: &UnifiedSession,
    identity: &WorkspaceIdentity,
    requested_provider: Option<&ProviderKind>,
) -> Result<Option<ReconciledNativeLaunch>> {
    let Some(launch) = store.open_native_launch_for_workspace(&identity.lease_key)? else {
        return Ok(None);
    };
    let pid = journaled_native_pid(&launch)?;
    let running = native::native_process_is_running(pid).with_context(|| {
        format!(
            "cannot verify journaled native {} launch {} (PID {pid})",
            launch.provider, launch.id
        )
    })?;
    ensure!(
        !running,
        "native {} launch {} is still running as process group {pid}; return to that terminal before opening another provider",
        launch.provider,
        launch.id
    );

    ensure!(
        launch.session_id == session.id,
        "worktree has an unfinished native launch owned by canonical session {}; resume that session first",
        launch.session_id
    );
    ensure_post_crash_workspace_snapshot(store, config, session, identity, &launch)?;
    let recovery = match &launch.provider {
        ProviderKind::Codex => {
            reconcile_codex_launch(
                store,
                paths,
                config,
                session,
                identity,
                requested_provider,
                &launch,
            )
            .await?
        }
        ProviderKind::Claude => {
            reconcile_claude_launch(store, config, session, requested_provider, &launch)?
        }
        ProviderKind::Plugin(_) => bail!("plugins cannot own provider-native interactive launches"),
    };
    Ok(Some(recovery))
}

fn ensure_post_crash_workspace_snapshot(
    store: &AgentctlStore,
    config: &Config,
    session: &UnifiedSession,
    identity: &WorkspaceIdentity,
    launch: &NativeLaunchRecord,
) -> Result<()> {
    let snapshots = operations::native_launch_workspace_snapshots(store, session, launch)?;
    if snapshots.after.is_some() {
        return Ok(());
    }
    let after = capture_git_snapshot(identity.execution_root())
        .context("failed to capture workspace state while reconciling dead native process")?;
    persist_workspace_snapshot(
        store,
        config,
        session,
        &launch.provider,
        "after_native_recovery",
        &after,
        Some(&snapshots.before),
    )
}

fn journaled_native_pid(launch: &NativeLaunchRecord) -> Result<u32> {
    launch.child_pid.with_context(|| {
        format!(
            "native {} launch {} is {:?} without a journaled PID; a spawn-time wrapper crash cannot be ruled out, so automatic reconciliation and abandonment remain blocked",
            launch.provider, launch.id, launch.state
        )
    })
}

async fn reconcile_codex_launch(
    store: &AgentctlStore,
    paths: &AgentctlPaths,
    config: &Config,
    session: &UnifiedSession,
    identity: &WorkspaceIdentity,
    requested_provider: Option<&ProviderKind>,
    launch: &NativeLaunchRecord,
) -> Result<ReconciledNativeLaunch> {
    let provider = store
        .provider_session(session.id, &ProviderKind::Codex)?
        .context("unfinished Codex launch has no provider-session binding")?;
    ensure!(
        provider.native_session_id == launch.native_session_id,
        "unfinished Codex launch no longer matches its provider-session binding"
    );
    let native_session = NativeSession {
        id: provider.id,
        provider: provider.provider,
        native_session_id: provider.native_session_id,
        native_version: provider.native_version,
        capabilities: provider.capabilities,
    };
    let baseline: InteractiveThreadSnapshot = serde_json::from_value(
        launch
            .metadata
            .get("codex_thread_baseline")
            .cloned()
            .context("unfinished Codex launch has no thread/list baseline")?,
    )
    .context("unfinished Codex launch has an invalid thread/list baseline")?;
    let current = snapshot_codex_threads(paths, config, identity).await?;
    if let Err(error) = validate_codex_launch_continuity(
        &baseline,
        &current,
        &launch.native_session_id,
        &launch.metadata,
    ) {
        mark_codex_launch_uncertain(store, config, launch, &error)?;
        return Err(error);
    }
    capture_codex_native_history(store, paths, config, session, &native_session)
        .await
        .context("failed to reconcile the unfinished native Codex transcript")?;
    let switching_provider =
        requested_provider.is_some_and(|provider| provider != &launch.provider);
    let continuation = switching_provider
        .then(|| {
            operations::persist_native_failover_continuation(
                store,
                &config.payload_guard()?,
                session,
                launch,
            )
        })
        .transpose()?;
    store.update_native_launch(
        launch.id,
        NativeLaunchState::Captured,
        launch.exit_code,
        None,
        Utc::now(),
    )?;
    Ok(ReconciledNativeLaunch {
        launch_id: launch.id,
        provider: launch.provider.clone(),
        clean_exit: false,
        continuation,
    })
}

fn mark_codex_launch_uncertain(
    store: &AgentctlStore,
    config: &Config,
    launch: &NativeLaunchRecord,
    error: &anyhow::Error,
) -> Result<()> {
    let safe = config
        .payload_guard()?
        .process_text(&error.to_string())
        .unwrap_or_else(|_| "native Codex thread identity diverged".to_owned());
    store.update_native_launch(
        launch.id,
        NativeLaunchState::Uncertain,
        launch.exit_code,
        Some(&safe),
        Utc::now(),
    )?;
    Ok(())
}

fn reconcile_claude_launch(
    store: &AgentctlStore,
    config: &Config,
    session: &UnifiedSession,
    requested_provider: Option<&ProviderKind>,
    launch: &NativeLaunchRecord,
) -> Result<ReconciledNativeLaunch> {
    if launch.state == NativeLaunchState::CaptureReady {
        store
            .update_native_launch(
                launch.id,
                NativeLaunchState::Captured,
                launch.exit_code,
                None,
                Utc::now(),
            )
            .map_err(anyhow::Error::from)?;
        return Ok(ReconciledNativeLaunch {
            launch_id: launch.id,
            provider: launch.provider.clone(),
            clean_exit: true,
            continuation: None,
        });
    }
    let switching_provider =
        requested_provider.is_some_and(|provider| provider != &launch.provider);
    let reopening_same_provider = requested_provider == Some(&ProviderKind::Claude)
        || (requested_provider.is_none()
            && session.active_provider.as_ref() == Some(&ProviderKind::Claude));
    ensure!(
        reopening_same_provider || switching_provider,
        "the previous Claude launch ended without a confirmed SessionEnd hook; explicitly reopen Claude or switch to Codex through a durable continuation"
    );
    ensure!(
        launch.child_pid.is_some(),
        "the previous Claude wrapper crashed before its child PID was journaled; process ownership is uncertain and cannot be resumed automatically"
    );
    ensure_claude_handoff_is_replay_safe(store, launch)?;
    operations::terminalize_native_crash_turns(store, &config.payload_guard()?, session, launch)?;
    crate::native_hooks::reconcile_claude_materialization(store, session.id)?;
    let continuation = switching_provider
        .then(|| {
            operations::persist_native_failover_continuation(
                store,
                &config.payload_guard()?,
                session,
                launch,
            )
        })
        .transpose()?;
    store.update_native_launch(
        launch.id,
        NativeLaunchState::Failed,
        launch.exit_code,
        Some(if switching_provider {
            "superseded by a cross-provider native continuation"
        } else {
            "superseded by a same-provider crash-resume launch"
        }),
        Utc::now(),
    )?;
    Ok(ReconciledNativeLaunch {
        launch_id: launch.id,
        provider: launch.provider.clone(),
        clean_exit: false,
        continuation,
    })
}

fn ensure_claude_handoff_is_replay_safe(
    store: &AgentctlStore,
    launch: &NativeLaunchRecord,
) -> Result<()> {
    if let Some(handoff) = store.native_handoff(launch.id)? {
        ensure!(
            !matches!(
                handoff.state,
                agentctl_storage::NativeHandoffState::Delivering
                    | agentctl_storage::NativeHandoffState::Uncertain
            ),
            "the previous Claude handoff may already have been delivered; refusing to duplicate historical context"
        );
    }
    Ok(())
}

async fn select_native_provider(
    paths: &AgentctlPaths,
    config: &Config,
    store: &AgentctlStore,
    session: &UnifiedSession,
    requested: Option<ProviderKind>,
) -> Result<ProviderKind> {
    if let Some(requested) = requested {
        return Ok(requested);
    }
    if session.routing_policy == "manual" {
        return session
            .active_provider
            .clone()
            .filter(|provider| matches!(provider, ProviderKind::Claude | ProviderKind::Codex))
            .context("manual routing requires an active Claude or Codex provider");
    }
    let mut candidates = Vec::new();
    if let Some(active) = &session.active_provider
        && matches!(active, ProviderKind::Claude | ProviderKind::Codex)
    {
        candidates.push(active.clone());
    }
    let preferred = match session.routing_policy.as_str() {
        "codex-first" => ProviderKind::Codex,
        _ => ProviderKind::Claude,
    };
    for candidate in [preferred, ProviderKind::Claude, ProviderKind::Codex] {
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
    let mut failures = Vec::new();
    let mut healthy = Vec::new();
    let latest = store.next_seq(session.id)?.saturating_sub(1);
    for candidate in candidates {
        if let Some(persisted) = store.latest_health(&candidate)?
            && persisted_health_blocks_auto(&persisted, config.routing.failure_window_seconds)
        {
            failures.push(format!("{candidate}: {:?}", persisted.status));
            continue;
        }
        match probe_native_candidate(paths, config, store, session, &candidate).await {
            Ok(health) if health.status.available() => {
                let lag = store
                    .provider_session(session.id, &candidate)?
                    .map_or(latest, |record| {
                        latest.saturating_sub(record.last_synced_seq)
                    });
                let failure_window =
                    i64::try_from(config.routing.failure_window_seconds).unwrap_or(i64::MAX);
                let recent_failures = store
                    .recent_failure_count(
                        session.id,
                        &candidate,
                        Utc::now() - chrono::TimeDelta::seconds(failure_window),
                    )?
                    .min(u32::try_from(config.routing.max_recent_failures).unwrap_or(u32::MAX));
                healthy.push((
                    candidate.clone(),
                    native_provider_score(
                        &session.routing_policy,
                        session.active_provider.as_ref(),
                        &candidate,
                        &health,
                        lag,
                        recent_failures,
                    ),
                ));
            }
            Ok(health) => failures.push(format!("{candidate}: {:?}", health.status)),
            Err(error) => failures.push(format!("{candidate}: {error:#}")),
        }
    }
    if let Some(selected) = choose_scored_native_provider(
        &session.routing_policy,
        session.active_provider.as_ref(),
        config.routing.switch_threshold,
        &healthy,
    ) {
        return Ok(selected);
    }
    bail!(
        "no native provider is currently available ({})",
        failures.join("; ")
    )
}

fn choose_scored_native_provider(
    policy: &str,
    active: Option<&ProviderKind>,
    switch_threshold: f64,
    candidates: &[(ProviderKind, f64)],
) -> Option<ProviderKind> {
    let mut ranked = candidates.to_vec();
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    if policy == "sticky-balanced"
        && let Some(active) = active
        && let Some((_, active_score)) = ranked.iter().find(|(provider, _)| provider == active)
        && ranked
            .first()
            .is_some_and(|(_, best_score)| best_score - active_score <= switch_threshold)
    {
        return Some(active.clone());
    }
    ranked.first().map(|(provider, _)| provider.clone())
}

async fn probe_native_candidate(
    paths: &AgentctlPaths,
    config: &Config,
    store: &AgentctlStore,
    session: &UnifiedSession,
    candidate: &ProviderKind,
) -> Result<agentctl_core::ProviderHealth> {
    let provider = load_native_provider(paths, config, candidate).await?;
    let probe = tokio::time::timeout(std::time::Duration::from_secs(20), provider.probe()).await;
    let shutdown = provider.shutdown().await;
    let health = match (probe, shutdown) {
        (Ok(Ok(health)), Ok(())) => health,
        (Ok(Err(error)), Ok(())) => return Err(error.into()),
        (Err(_), Ok(())) => bail!("health probe timed out"),
        (Ok(Ok(_)), Err(error)) => bail!("probe teardown failed: {error}"),
        (Ok(Err(error)), Err(shutdown)) => {
            return Err(anyhow::Error::new(error)
                .context(format!("probe teardown also failed: {shutdown}")));
        }
        (Err(_), Err(shutdown)) => bail!("health probe timed out; teardown failed: {shutdown}"),
    };
    let health = config
        .payload_guard()?
        .process_json(&serde_json::to_value(health)?)?;
    let health: agentctl_core::ProviderHealth = serde_json::from_value(health)?;
    store.record_health(Some(session.id), &health)?;
    Ok(health)
}

fn persisted_health_blocks_auto(
    health: &agentctl_core::ProviderHealth,
    failure_window_seconds: u64,
) -> bool {
    match health.status {
        agentctl_core::ProviderStatus::Exhausted { resets_at } => resets_at.map_or_else(
            || health_within_failure_window(health, failure_window_seconds),
            |reset| reset > Utc::now(),
        ),
        agentctl_core::ProviderStatus::AuthError | agentctl_core::ProviderStatus::Incompatible => {
            true
        }
        agentctl_core::ProviderStatus::Overloaded | agentctl_core::ProviderStatus::Offline => {
            health_within_failure_window(health, failure_window_seconds)
        }
        agentctl_core::ProviderStatus::Unknown
        | agentctl_core::ProviderStatus::Ready
        | agentctl_core::ProviderStatus::Warning => false,
    }
}

fn health_within_failure_window(
    health: &agentctl_core::ProviderHealth,
    failure_window_seconds: u64,
) -> bool {
    let seconds = i64::try_from(failure_window_seconds).unwrap_or(i64::MAX);
    health
        .checked_at
        .checked_add_signed(chrono::TimeDelta::seconds(seconds))
        .is_none_or(|until| until > Utc::now())
}

fn native_provider_score(
    policy: &str,
    active: Option<&ProviderKind>,
    provider: &ProviderKind,
    health: &agentctl_core::ProviderHealth,
    sync_lag: u64,
    recent_failures: u32,
) -> f64 {
    let availability = match health.status {
        agentctl_core::ProviderStatus::Ready => 1.0,
        agentctl_core::ProviderStatus::Warning => 0.45,
        _ => 0.0,
    };
    let quota_headroom = health
        .rate_limit
        .as_ref()
        .and_then(|limit| limit.utilization)
        .map_or(0.25, |used| (1.0 - used.clamp(0.0, 1.0)) * 0.75);
    let bounded_lag = u32::try_from(sync_lag.min(50)).unwrap_or(50);
    let sync_penalty = f64::from(bounded_lag) / 100.0;
    let failure_penalty = f64::from(recent_failures.min(10)) * 0.12;
    let policy_bonus = match (policy, provider) {
        ("claude-first", ProviderKind::Claude) | ("codex-first", ProviderKind::Codex) => 2.0,
        ("sticky-balanced", candidate) if active == Some(candidate) => 0.35,
        _ => 0.0,
    };
    availability + quota_headroom + policy_bonus - sync_penalty - failure_penalty
}

async fn run_native_codex(
    store: &AgentctlStore,
    paths: &AgentctlPaths,
    config: &Config,
    session: &UnifiedSession,
    identity: &WorkspaceIdentity,
    native_args: &[std::ffi::OsString],
    explicit_provider: bool,
) -> Result<serde_json::Value> {
    let runtime = build_runtime(
        store.clone(),
        paths.clone(),
        config.clone(),
        session.clone(),
    )
    .await?;
    let prepared = runtime
        .prepare_native_projection(&ProviderKind::Codex)
        .await;
    let native_session = finish_runtime(&runtime, prepared).await?;
    let thread_baseline = snapshot_codex_threads(paths, config, identity).await?;
    store.update_session_routing(
        session.id,
        Some(&ProviderKind::Codex),
        if explicit_provider {
            "manual"
        } else {
            &session.routing_policy
        },
        SessionStatus::Active,
        Utc::now(),
    )?;

    let (launch_id, native_exit) = launch_journaled_codex(
        store,
        config,
        session,
        identity,
        &native_session,
        &thread_baseline,
        native_args,
    )
    .await?;
    let finalized = async {
        let thread_after = snapshot_codex_threads(paths, config, identity).await?;
        let launch = store
            .native_launch(launch_id)?
            .context("native Codex launch disappeared")?;
        validate_codex_launch_continuity(
            &thread_baseline,
            &thread_after,
            &native_session.native_session_id,
            &launch.metadata,
        )?;

        // Read persisted native turns through the official app-server without
        // acquiring the thread's writer, then capture its terminal delta.
        let capture =
            capture_codex_native_history(store, paths, config, session, &native_session).await?;
        store.update_native_launch(
            launch_id,
            NativeLaunchState::Captured,
            native_exit.propagated_exit_code(),
            None,
            Utc::now(),
        )?;
        Ok::<_, anyhow::Error>(serde_json::json!({
            "mode": "native_cli",
            "session_id": session.id,
            "launch_id": launch_id,
            "native_exit": native_exit,
            "capture": capture,
        }))
    }
    .await;
    match finalized {
        Ok(report) => Ok(report),
        Err(error) => {
            mark_native_launch_after_error(
                store,
                config,
                launch_id,
                &error,
                true,
                native_exit.propagated_exit_code(),
            )?;
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn launch_journaled_codex(
    store: &AgentctlStore,
    config: &Config,
    session: &UnifiedSession,
    identity: &WorkspaceIdentity,
    native_session: &NativeSession,
    thread_baseline: &InteractiveThreadSnapshot,
    native_args: &[std::ffi::OsString],
) -> Result<(uuid::Uuid, native::NativeCliExit)> {
    let launch_id = uuid::Uuid::now_v7();
    let started_at = Utc::now();
    store.start_native_launch(&NativeLaunchRecord {
        id: launch_id,
        session_id: session.id,
        provider: ProviderKind::Codex,
        native_session_id: native_session.native_session_id.clone(),
        workspace_lease_key: identity.lease_key.clone(),
        child_pid: None,
        state: NativeLaunchState::Started,
        exit_code: None,
        error: None,
        metadata: serde_json::json!({"codex_thread_baseline": thread_baseline}),
        started_at,
        updated_at: started_at,
    })?;
    #[cfg(unix)]
    let relay = match crate::codex_launch::Relay::prepare(
        Path::new(&config.providers.codex_binary),
        store.clone(),
        launch_id,
    )
    .await
    {
        Ok(relay) => relay,
        Err(error) => {
            mark_native_launch_after_error(store, config, launch_id, &error, false, None)?;
            return Err(error);
        }
    };
    #[cfg(unix)]
    let endpoint = relay.as_ref().map(crate::codex_launch::Relay::endpoint);
    #[cfg(not(unix))]
    let endpoint: Option<PathBuf> = None;
    let launched = native::launch_codex_with_spawn(
        &config.providers.codex_binary,
        identity.execution_root(),
        &native_session.native_session_id,
        native_args,
        endpoint.as_deref(),
        |spawn| {
            store
                .record_native_launch_pid(launch_id, spawn.pid)
                .map_err(Into::into)
        },
    )
    .await;
    #[cfg(unix)]
    let launched = match (
        launched,
        async {
            match relay {
                Some(relay) => relay.finish().await,
                None => Ok(()),
            }
        }
        .await,
    ) {
        (Ok(exit), Ok(())) => Ok(exit),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(teardown)) => {
            Err(error.context(format!("native Codex relay also failed: {teardown}")))
        }
    };
    let native_exit = match launched {
        Ok(native_exit) => native_exit,
        Err(error) => {
            mark_native_launch_after_error(
                store,
                config,
                launch_id,
                &error,
                native::error_happened_after_spawn(&error),
                None,
            )?;
            return Err(error);
        }
    };
    if let Err(error) = store.update_native_launch(
        launch_id,
        NativeLaunchState::Exited,
        native_exit.propagated_exit_code(),
        None,
        Utc::now(),
    ) {
        let error = anyhow::Error::new(error).context("failed journaling native Codex exit");
        mark_native_launch_after_error(
            store,
            config,
            launch_id,
            &error,
            true,
            native_exit.propagated_exit_code(),
        )?;
        return Err(error);
    }
    Ok((launch_id, native_exit))
}

fn mark_native_launch_after_error(
    store: &AgentctlStore,
    config: &Config,
    launch_id: uuid::Uuid,
    error: &anyhow::Error,
    happened_after_spawn: bool,
    exit_code: Option<i32>,
) -> Result<()> {
    let launch = store
        .native_launch(launch_id)?
        .with_context(|| format!("native launch journal {launch_id} disappeared"))?;
    let target = if happened_after_spawn || launch.child_pid.is_some() {
        NativeLaunchState::Uncertain
    } else {
        NativeLaunchState::Failed
    };
    if launch.state == target
        || launch.state == NativeLaunchState::Uncertain
        || matches!(
            launch.state,
            NativeLaunchState::Captured | NativeLaunchState::Failed
        )
    {
        return Ok(());
    }
    let sanitized = config
        .payload_guard()
        .ok()
        .and_then(|guard| guard.process_text(&format!("{error:#}")).ok())
        .unwrap_or_else(|| "native provider launch failed; inspect debug logs".to_owned());
    store.update_native_launch(
        launch_id,
        target,
        exit_code.or(launch.exit_code),
        Some(&sanitized),
        Utc::now(),
    )?;
    Ok(())
}

async fn snapshot_codex_threads(
    paths: &AgentctlPaths,
    config: &Config,
    identity: &WorkspaceIdentity,
) -> Result<InteractiveThreadSnapshot> {
    let adapter = CodexAdapter::new(
        &config.providers.codex_binary,
        Some(paths.codex_protocol_root()),
    );
    let snapshot = adapter
        .snapshot_interactive_threads(identity.execution_root())
        .await;
    let shutdown = adapter.shutdown().await;
    match (snapshot, shutdown) {
        (Ok(snapshot), Ok(())) => Ok(snapshot),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error.into()),
        (Err(error), Err(shutdown)) => Err(anyhow::Error::new(error)
            .context(format!("Codex snapshot teardown also failed: {shutdown}"))),
    }
}

fn validate_codex_launch_continuity(
    before: &InteractiveThreadSnapshot,
    after: &InteractiveThreadSnapshot,
    mapped_thread_id: &str,
    metadata: &serde_json::Value,
) -> Result<()> {
    #[cfg(unix)]
    if let Some(evidence) = metadata.get("codex_evidence") {
        ensure!(before.cwd == after.cwd, "Codex launch workspace changed");
        return serde_json::from_value::<crate::codex_launch::Evidence>(evidence.clone())
            .context("invalid native Codex thread-selection evidence")?
            .validate(mapped_thread_id);
    }
    #[cfg(not(unix))]
    let _ = metadata;
    validate_codex_thread_continuity(before, after, mapped_thread_id)
}

fn validate_codex_thread_continuity(
    before: &InteractiveThreadSnapshot,
    after: &InteractiveThreadSnapshot,
    mapped_thread_id: &str,
) -> Result<()> {
    ensure!(
        before.cwd == after.cwd,
        "Codex thread snapshot workspace changed during the native launch"
    );
    let known = before
        .threads
        .iter()
        .map(|thread| (thread.thread_id.as_str(), thread))
        .collect::<BTreeMap<_, _>>();
    let unexpected = after
        .threads
        .iter()
        .filter(|thread| {
            thread.thread_id != mapped_thread_id
                && known
                    .get(thread.thread_id.as_str())
                    .is_none_or(|previous| **previous != **thread)
        })
        .map(|thread| thread.thread_id.clone())
        .collect::<Vec<_>>();
    ensure!(
        unexpected.is_empty(),
        "Codex created or switched to additional native thread(s) during the mapped launch: {}. Resume the original mapped thread and repair before switching providers",
        unexpected.join(", ")
    );
    Ok(())
}

async fn capture_codex_native_history(
    store: &AgentctlStore,
    paths: &AgentctlPaths,
    config: &Config,
    session: &UnifiedSession,
    native_session: &NativeSession,
) -> Result<operations::NativeImportReport> {
    let provider = load_native_provider(paths, config, &ProviderKind::Codex).await?;
    let captured = async {
        let transcript = provider.read_native_history(native_session).await?;
        operations::persist_native_capture(
            store,
            &config.payload_guard()?,
            session,
            native_session,
            &transcript,
        )
    }
    .await;
    match provider.probe().await {
        Ok(health) => {
            let health = config
                .payload_guard()?
                .process_json(&serde_json::to_value(health)?)?;
            let health: agentctl_core::ProviderHealth = serde_json::from_value(health)?;
            store.record_health(Some(session.id), &health)?;
        }
        Err(error) => tracing::warn!(%error, "post-capture Codex health probe failed"),
    }
    let shutdown = provider.shutdown().await;
    match (captured, shutdown) {
        (Ok(capture), Ok(())) => Ok(capture),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => {
            bail!("native Codex history was captured but app-server shutdown failed: {error}")
        }
        (Err(error), Err(shutdown)) => {
            Err(error.context(format!("Codex app-server shutdown also failed: {shutdown}")))
        }
    }
}

async fn finish_runtime<T>(runtime: &Runtime, result: Result<T>) -> Result<T> {
    let shutdown = runtime.shutdown().await;
    match (result, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(error)) | (Err(error), Ok(())) => Err(error),
        (Err(error), Err(shutdown)) => {
            Err(error.context(format!("provider teardown also failed: {shutdown:#}")))
        }
    }
}

async fn build_runtime(
    store: AgentctlStore,
    paths: AgentctlPaths,
    config: Config,
    session: UnifiedSession,
) -> Result<Runtime> {
    let providers = load_providers(&paths, &config).await;
    let runtime = Runtime::new(store, &config, session, providers)?;
    let recovered = runtime.recover_crashed_turns()?;
    if !recovered.is_empty() {
        tracing::warn!(
            turns = ?recovered,
            "recovered non-terminal turns from a previous agentctl process"
        );
    }
    Ok(runtime)
}

async fn load_providers(
    paths: &AgentctlPaths,
    config: &Config,
) -> BTreeMap<ProviderKind, Arc<dyn AgentProvider>> {
    let mut providers: BTreeMap<ProviderKind, Arc<dyn AgentProvider>> = BTreeMap::new();
    for kind in [ProviderKind::Codex, ProviderKind::Claude] {
        if let Ok(provider) = load_native_provider(paths, config, &kind).await {
            providers.insert(kind, provider);
        }
    }
    if let Ok(installed) = operations::list_plugins(paths) {
        for plugin in installed {
            match PluginClient::connect(&plugin.manifest, ClientOptions::default()).await {
                Ok(client) => {
                    providers.insert(
                        ProviderKind::Plugin(plugin.manifest.name.clone()),
                        Arc::new(client),
                    );
                }
                Err(error) => tracing::warn!(
                    plugin = %plugin.manifest.name,
                    %error,
                    "provider plugin is unavailable"
                ),
            }
        }
    }
    providers
}

async fn load_native_provider(
    paths: &AgentctlPaths,
    config: &Config,
    kind: &ProviderKind,
) -> Result<Arc<dyn AgentProvider>> {
    match kind {
        ProviderKind::Codex => Ok(Arc::new(CodexAdapter::for_interactive_sessions(
            &config.providers.codex_binary,
            Some(paths.codex_protocol_root()),
        ))),
        ProviderKind::Claude => Ok(Arc::new(ClaudeAdapter::from_config(ClaudeConfig {
            binary: PathBuf::from(&config.providers.claude_binary),
            runtime_root: Some(paths.home.join("runtime/claude")),
            append_system_prompt_file: Some(ensure_claude_policy_file(paths)?),
            should_query: load_claude_should_query_support(paths, &config.providers.claude_binary)
                .await,
            user_prompt_submit_hook: load_claude_hook_support(
                paths,
                &config.providers.claude_binary,
            )
            .await,
            ..ClaudeConfig::default()
        }))),
        ProviderKind::Plugin(name) => bail!("{name} is not a built-in native provider"),
    }
}

async fn attach_native_session(
    paths: &AgentctlPaths,
    config: &Config,
    store: &AgentctlStore,
    session: &UnifiedSession,
    provider_kind: ProviderKind,
    native_session_id: &str,
    activate: bool,
) -> Result<operations::NativeAttachmentReport> {
    if let Some(existing) = store.provider_session(session.id, &provider_kind)? {
        ensure_same_native_session(
            &existing.native_session_id,
            native_session_id,
            &provider_kind,
        )?;
    }
    let identity = WorkspaceIdentity::discover(&session.workspace_path)?;
    let _lease =
        WorkspaceLease::acquire_for_identity(&paths.locks, &identity, session.id, TurnId::new())
            .context("another agentctl operation already owns this worktree")?;
    operations::ensure_session_mutation_idle(
        store,
        session,
        &identity,
        "native session attachment",
    )?;
    let context = SessionContext {
        unified_session_id: session.id,
        workspace_root: identity.execution_root().to_path_buf(),
        workspace_fingerprint: identity.fingerprint,
        auth_mode: session.auth_mode,
    };
    let provider = load_native_provider(paths, config, &provider_kind).await?;
    let native = match provider
        .attach_existing_session(&context, native_session_id)
        .await
    {
        Ok(native) => native,
        Err(error) => {
            let _ = provider.shutdown().await;
            return Err(error.into());
        }
    };
    let report = operations::persist_native_attachment(store, session, &native, activate);
    let shutdown = provider.shutdown().await;
    let report = report?;
    shutdown?;
    Ok(report)
}

async fn import_native_session(
    paths: &AgentctlPaths,
    config: &Config,
    store: &AgentctlStore,
    session: &UnifiedSession,
    provider_kind: ProviderKind,
    native_session_id: &str,
    activate: bool,
) -> Result<operations::NativeImportReport> {
    ensure!(
        provider_kind == ProviderKind::Codex,
        "Claude does not expose a public transcript-read API; use `agentctl attach claude ...` for semantic continuation"
    );
    if let Some(existing) = store.provider_session(session.id, &provider_kind)? {
        ensure_same_native_session(
            &existing.native_session_id,
            native_session_id,
            &provider_kind,
        )?;
    }
    let identity = WorkspaceIdentity::discover(&session.workspace_path)?;
    let _lease =
        WorkspaceLease::acquire_for_identity(&paths.locks, &identity, session.id, TurnId::new())
            .context("another agentctl operation already owns this worktree")?;
    operations::ensure_session_mutation_idle(
        store,
        session,
        &identity,
        "native transcript import",
    )?;
    let context = SessionContext {
        unified_session_id: session.id,
        workspace_root: identity.execution_root().to_path_buf(),
        workspace_fingerprint: identity.fingerprint,
        auth_mode: session.auth_mode,
    };
    let provider = load_native_provider(paths, config, &provider_kind).await?;
    let result = async {
        let native = provider
            .attach_existing_session(&context, native_session_id)
            .await?;
        let transcript = provider.read_native_history(&native).await?;
        operations::persist_native_import(
            store,
            &config.payload_guard()?,
            session,
            &native,
            &transcript,
            activate,
        )
    }
    .await;
    let shutdown = provider.shutdown().await;
    finish_native_import(result, shutdown)
}

fn finish_native_import(
    result: Result<operations::NativeImportReport>,
    shutdown: std::result::Result<(), ProviderError>,
) -> Result<operations::NativeImportReport> {
    match (result, shutdown) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), Ok(())) => Err(error),
        (Ok(mut report), Err(error)) => {
            report.warning = Some(format!(
                "native import committed successfully, but provider shutdown failed: {error}"
            ));
            Ok(report)
        }
        (Err(error), Err(shutdown)) => Err(error.context(format!(
            "provider shutdown also failed after native import: {shutdown}"
        ))),
    }
}

fn ensure_same_native_session(
    existing: &str,
    requested: &str,
    provider: &ProviderKind,
) -> Result<()> {
    if existing == requested {
        Ok(())
    } else {
        bail!(
            "{provider} already has native session {existing}; refusing to replace it because sync receipts belong to that projection"
        )
    }
}

#[allow(clippy::too_many_lines)]
async fn run_doctor(
    paths: &AgentctlPaths,
    config: &Config,
    store: &AgentctlStore,
    cwd: &Path,
    live: bool,
    report_path: Option<&Path>,
    json: bool,
) -> Result<()> {
    let mut report = doctor::inspect(paths, config, live).await;
    if live {
        report
            .checks
            .retain(|check| check.name != "live protocol turns");
        let temporary = tempfile::tempdir()?;
        let codex = CodexAdapter::new(
            &config.providers.codex_binary,
            Some(paths.codex_protocol_root()),
        );
        let claude = ClaudeAdapter::from_config(ClaudeConfig {
            binary: PathBuf::from(&config.providers.claude_binary),
            runtime_root: Some(paths.home.join("runtime/doctor-claude")),
            append_system_prompt_file: Some(ensure_claude_policy_file(paths)?),
            ..ClaudeConfig::default()
        });
        report
            .checks
            .push(doctor::run_live_turn(&codex, temporary.path()).await);
        report
            .checks
            .push(doctor::run_live_turn(&claude, temporary.path()).await);
        let native_version = claude.probe().await.ok().and_then(|health| health.version);
        let should_query = claude.probe_should_query(temporary.path(), true).await;
        let (supported, evidence, check) = match should_query {
            Ok(probe) => {
                let evidence = serde_json::json!({
                    "session_id": probe.session_id,
                    "synthetic_triggered_turn": probe.synthetic_triggered_turn,
                    "nonce_observed": probe.nonce_observed,
                    "persisted_after_resume": probe.persisted_after_resume,
                    "result_messages": probe.result_messages,
                });
                let check = doctor::CompatibilityCheck {
                    provider: Some("claude".to_owned()),
                    name: "shouldQuery:false behavioral probe".to_owned(),
                    status: if probe.supported {
                        doctor::CheckStatus::Passed
                    } else {
                        doctor::CheckStatus::Warning
                    },
                    optional: true,
                    detail: if probe.supported {
                        "synthetic context persisted across resume without an extra model turn"
                            .to_owned()
                    } else {
                        "unsupported; a separately validated hook or next-prompt capsule is required"
                            .to_owned()
                    },
                };
                (probe.supported, evidence, check)
            }
            Err(error) => (
                false,
                serde_json::json!({"error": error.to_string()}),
                doctor::CompatibilityCheck {
                    provider: Some("claude".to_owned()),
                    name: "shouldQuery:false behavioral probe".to_owned(),
                    status: doctor::CheckStatus::Warning,
                    optional: true,
                    detail: format!(
                        "probe failed; a separately validated hook or next-prompt capsule is required: {error}"
                    ),
                },
            ),
        };
        if let Some(version) = native_version.as_ref() {
            store.record_protocol_capability(&ProtocolCapabilityRecord {
                provider: ProviderKind::Claude,
                native_version: version.clone(),
                capability: "should_query_false".to_owned(),
                supported,
                evidence,
                probed_at: Utc::now(),
            })?;
        }
        report.checks.push(check);

        let hook = claude
            .probe_user_prompt_submit_hook(temporary.path(), true)
            .await;
        let (hook_supported, hook_evidence, hook_check) = match hook {
            Ok(probe) => {
                let evidence = serde_json::json!({
                    "session_id": probe.session_id,
                    "nonce_observed": probe.nonce_observed,
                    "persisted_after_resume": probe.persisted_after_resume,
                    "result_messages": probe.result_messages,
                });
                let check = doctor::CompatibilityCheck {
                    provider: Some("claude".to_owned()),
                    name: "UserPromptSubmit behavioral probe".to_owned(),
                    status: if probe.supported {
                        doctor::CheckStatus::Passed
                    } else {
                        doctor::CheckStatus::Warning
                    },
                    optional: true,
                    detail: if probe.supported {
                        "additionalContext reached the model and persisted across resume".to_owned()
                    } else {
                        "unsupported; agentctl will fail closed to the next-prompt capsule"
                            .to_owned()
                    },
                };
                (probe.supported, evidence, check)
            }
            Err(error) => (
                false,
                serde_json::json!({"error": error.to_string()}),
                doctor::CompatibilityCheck {
                    provider: Some("claude".to_owned()),
                    name: "UserPromptSubmit behavioral probe".to_owned(),
                    status: doctor::CheckStatus::Warning,
                    optional: true,
                    detail: format!(
                        "probe failed; agentctl will fail closed to the next-prompt capsule: {error}"
                    ),
                },
            ),
        };
        if let Some(version) = native_version {
            store.record_protocol_capability(&ProtocolCapabilityRecord {
                provider: ProviderKind::Claude,
                native_version: version,
                capability: "user_prompt_submit_hook".to_owned(),
                supported: hook_supported,
                evidence: hook_evidence,
                probed_at: Utc::now(),
            })?;
        }
        report.checks.push(hook_check);
        let (codex_shutdown, claude_shutdown) = tokio::join!(codex.shutdown(), claude.shutdown());
        for (provider, result) in [
            ("codex", codex_shutdown.map_err(|error| error.to_string())),
            ("claude", claude_shutdown.map_err(|error| error.to_string())),
        ] {
            report.checks.push(doctor::CompatibilityCheck {
                provider: Some(provider.to_owned()),
                name: "live probe process teardown".to_owned(),
                status: if result.is_ok() {
                    doctor::CheckStatus::Passed
                } else {
                    doctor::CheckStatus::Failed
                },
                optional: false,
                detail: result.map_or_else(
                    |error| format!("provider teardown failed: {error}"),
                    |()| "provider process group stopped".to_owned(),
                ),
            });
        }
    } else {
        let cached = load_claude_should_query_support(paths, &config.providers.claude_binary).await;
        if cached != ShouldQuerySupport::Unknown {
            report.checks.push(doctor::CompatibilityCheck {
                provider: Some("claude".to_owned()),
                name: "shouldQuery:false behavioral probe".to_owned(),
                status: if cached == ShouldQuerySupport::Supported {
                    doctor::CheckStatus::Passed
                } else {
                    doctor::CheckStatus::Warning
                },
                optional: true,
                detail: "cached behavioral result for the currently installed Claude version"
                    .to_owned(),
            });
        }
        let cached_hook = load_claude_hook_support(paths, &config.providers.claude_binary).await;
        if cached_hook != UserPromptSubmitHookSupport::Unknown {
            report.checks.push(doctor::CompatibilityCheck {
                provider: Some("claude".to_owned()),
                name: "UserPromptSubmit behavioral probe".to_owned(),
                status: if cached_hook == UserPromptSubmitHookSupport::Supported {
                    doctor::CheckStatus::Passed
                } else {
                    doctor::CheckStatus::Warning
                },
                optional: true,
                detail: "cached behavioral result for the currently installed Claude version"
                    .to_owned(),
            });
        }
    }
    report = serde_json::from_value(
        config
            .payload_guard()?
            .process_json(&serde_json::to_value(&report)?)?,
    )
    .context("sanitized compatibility report became invalid")?;
    let report_path = report_path.map_or_else(
        || paths.home.join("compatibility-report.json"),
        Path::to_path_buf,
    );
    doctor::write_report(&report, &report_path).await?;
    let capabilities_path = paths.home.join("protocol-capabilities.json");
    let capabilities = serde_json::json!({
        "generated_at": report.generated_at,
        "checks": report.checks,
    });
    tokio::fs::write(
        &capabilities_path,
        serde_json::to_vec_pretty(&capabilities)?,
    )
    .await?;
    set_private_file(&capabilities_path)?;
    let output = serde_json::json!({
        "compatible": report.compatible(),
        "report": report_path,
        "capabilities": capabilities_path,
        "workspace": cwd,
        "checks": report.checks,
    });
    print_value(&output, json)?;
    if !report.compatible() {
        bail!("one or more required compatibility checks failed");
    }
    Ok(())
}

async fn load_claude_should_query_support(
    paths: &AgentctlPaths,
    binary: &str,
) -> ShouldQuerySupport {
    match load_claude_behavioral_check(paths, binary, "shouldQuery:false behavioral probe").await {
        Some(true) => ShouldQuerySupport::Supported,
        Some(false) => ShouldQuerySupport::Unsupported,
        None => ShouldQuerySupport::Unknown,
    }
}

async fn load_claude_hook_support(
    paths: &AgentctlPaths,
    binary: &str,
) -> UserPromptSubmitHookSupport {
    match load_claude_behavioral_check(paths, binary, "UserPromptSubmit behavioral probe").await {
        Some(true) => UserPromptSubmitHookSupport::Supported,
        Some(false) => UserPromptSubmitHookSupport::Unsupported,
        None => UserPromptSubmitHookSupport::Unknown,
    }
}

async fn load_claude_behavioral_check(
    paths: &AgentctlPaths,
    binary: &str,
    check_name: &str,
) -> Option<bool> {
    let path = paths.home.join("protocol-capabilities.json");
    let Ok(bytes) = std::fs::read(path) else {
        return None;
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return None;
    };
    let checks = value.get("checks").and_then(serde_json::Value::as_array)?;
    let recorded_version = checks
        .iter()
        .find(|check| {
            check.get("provider").and_then(serde_json::Value::as_str) == Some("claude")
                && check.get("name").and_then(serde_json::Value::as_str) == Some("binary")
        })
        .and_then(|check| check.get("detail"))
        .and_then(serde_json::Value::as_str);
    let current_version = tokio::process::Command::new(binary)
        .arg("--version")
        .output()
        .await
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok());
    if recorded_version
        .is_none_or(|recorded| current_version.as_deref().map(str::trim) != Some(recorded))
    {
        return None;
    }
    let supported = checks
        .iter()
        .find(|check| {
            check.get("provider").and_then(serde_json::Value::as_str) == Some("claude")
                && check.get("name").and_then(serde_json::Value::as_str) == Some(check_name)
        })
        .and_then(|check| check.get("status"))
        .and_then(serde_json::Value::as_str);
    match supported {
        Some("passed") => Some(true),
        Some("warning" | "failed") => Some(false),
        _ => None,
    }
}

fn provider_choice(choice: ProviderChoice) -> Option<ProviderKind> {
    match choice {
        ProviderChoice::Auto => None,
        ProviderChoice::Claude => Some(ProviderKind::Claude),
        ProviderChoice::Codex => Some(ProviderKind::Codex),
    }
}

fn native_provider_choice(choice: NativeProviderChoice) -> ProviderKind {
    match choice {
        NativeProviderChoice::Claude => ProviderKind::Claude,
        NativeProviderChoice::Codex => ProviderKind::Codex,
    }
}

fn ensure_claude_policy_file(paths: &AgentctlPaths) -> Result<PathBuf> {
    let directory = paths.protocols.join("claude");
    if directory.exists()
        && std::fs::symlink_metadata(&directory)?
            .file_type()
            .is_symlink()
    {
        bail!("refusing symlinked Claude protocol directory");
    }
    std::fs::create_dir_all(&directory)?;
    set_private_directory(&directory)?;
    let path = directory.join("handoff-policy-v1.txt");
    if path.exists() && std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
        bail!("refusing symlinked Claude handoff policy file");
    }
    if std::fs::read_to_string(&path).ok().as_deref() != Some(HANDOFF_POLICY) {
        let mut options = std::fs::OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path)?;
        file.write_all(HANDOFF_POLICY.as_bytes())?;
        file.sync_all()?;
    }
    set_private_file(&path)?;
    Ok(path)
}

fn print_value(value: &impl Serialize, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(value)?);
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod routing_tests {
    use std::{collections::BTreeMap, ffi::OsString, process::Command as StdCommand};

    use agentctl_core::{ProviderSessionId, ProviderStatus};
    use agentctl_storage::{ProviderSessionRecord, TurnRecord};
    use clap::Parser;

    use super::*;

    fn provider_health(
        provider: ProviderKind,
        status: agentctl_core::ProviderStatus,
        checked_at: chrono::DateTime<Utc>,
        utilization: Option<f64>,
    ) -> agentctl_core::ProviderHealth {
        agentctl_core::ProviderHealth {
            provider,
            status,
            version: None,
            capabilities: BTreeMap::new(),
            usage: None,
            rate_limit: utilization.map(|utilization| agentctl_core::RateLimitSnapshot {
                utilization: Some(utilization),
                window_seconds: None,
                resets_at: None,
                source: "test".to_owned(),
            }),
            checked_at,
            message: None,
        }
    }

    #[tokio::test]
    #[ignore = "requires a locally installed Codex CLI"]
    async fn installed_codex_capture_reads_history_without_acquiring_its_writer() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = Config::default();
        let session = create_session(
            &store,
            &config,
            workspace.path(),
            Some("capture-live-probe"),
            Some(ProviderKind::Codex),
        )
        .unwrap();
        let identity = WorkspaceIdentity::discover(workspace.path()).unwrap();
        let context = SessionContext {
            unified_session_id: session.id,
            workspace_root: identity.execution_root().to_path_buf(),
            workspace_fingerprint: identity.fingerprint.clone(),
            auth_mode: session.auth_mode,
        };
        let owner = CodexAdapter::new("codex", None);
        let native = owner.ensure_session(&context).await.unwrap();
        let now = Utc::now();
        store
            .upsert_provider_session(&ProviderSessionRecord {
                id: native.id,
                unified_session_id: session.id,
                provider: native.provider.clone(),
                native_session_id: native.native_session_id.clone(),
                native_version: native.native_version.clone(),
                last_synced_seq: 0,
                status: ProviderStatus::Ready,
                reset_at: None,
                capabilities: native.capabilities.clone(),
                metadata: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            })
            .unwrap();

        let captured =
            capture_codex_native_history(&store, &paths, &config, &session, &native).await;
        owner.shutdown().await.unwrap();
        captured.unwrap();
        assert!(
            store
                .list_events(session.id, 0, usize::MAX)
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires a running native Codex daemon"]
    async fn installed_codex_relay_persists_only_its_client_selections() {
        use futures::{SinkExt, StreamExt};
        use tokio_tungstenite::{client_async, tungstenite::Message};
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let config = Config::default();
        let session = create_session(
            &store,
            &config,
            directory.path(),
            Some("relay-live-probe"),
            Some(ProviderKind::Codex),
        )
        .unwrap();
        let owner = CodexAdapter::for_interactive_sessions("codex", None);
        let native = owner
            .ensure_session(&SessionContext {
                unified_session_id: session.id,
                workspace_root: session.workspace_path.clone(),
                workspace_fingerprint: session.workspace_fingerprint.clone(),
                auth_mode: session.auth_mode,
            })
            .await
            .unwrap();
        let id = uuid::Uuid::now_v7();
        store
            .start_native_launch(&NativeLaunchRecord {
                id,
                session_id: session.id,
                provider: ProviderKind::Codex,
                native_session_id: native.native_session_id.clone(),
                workspace_lease_key: "relay-live-probe".to_owned(),
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({"preserved":true}),
                started_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .unwrap();
        let relay = crate::codex_launch::Relay::prepare(Path::new("codex"), store.clone(), id)
            .await
            .unwrap()
            .unwrap();
        let (mut client, _) = client_async(
            "ws://localhost/",
            tokio::net::UnixStream::connect(relay.endpoint())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        for (index, (method, params)) in [
            ("initialize", serde_json::json!({"clientInfo":{"name":"agentctl-relay-live-probe","version":"0.1.0"},"capabilities":{"experimentalApi":true}})),
            ("thread/resume", serde_json::json!({"threadId":native.native_session_id})),
            ("thread/fork", serde_json::json!({"threadId":native.native_session_id})),
        ].into_iter().enumerate() {
            client.send(Message::text(serde_json::json!({"id":index,"method":method,"params":params}).to_string())).await.unwrap();
            loop {
                let frame = tokio::time::timeout(std::time::Duration::from_secs(30),client.next()).await.unwrap().unwrap().unwrap();
                if let Message::Text(text) = frame {
                    let response: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if response["id"] == index {
                        assert!(response.get("error").is_none(), "{response}");
                        break;
                    }
                }
            }
            if index == 0 {
                client.send(Message::text(serde_json::json!({"method":"initialized"}).to_string())).await.unwrap();
            }
        }
        let pending: crate::codex_launch::Evidence = serde_json::from_value(
            store.native_launch(id).unwrap().unwrap().metadata["codex_evidence"].clone(),
        )
        .unwrap();
        assert!(!pending.complete);
        assert_eq!(pending.selected_threads.len(), 2);
        client.close(None).await.unwrap();
        relay.finish().await.unwrap();
        owner.shutdown().await.unwrap();
        let reopened = operations::open_store(&paths).unwrap();
        let launch = reopened.native_launch(id).unwrap().unwrap();
        assert_eq!(launch.metadata["preserved"], true);
        let evidence: crate::codex_launch::Evidence =
            serde_json::from_value(launch.metadata["codex_evidence"].clone()).unwrap();
        assert!(evidence.complete);
        assert_eq!(evidence.pending_requests, 0);
        assert!(evidence.validate(&native.native_session_id).is_err());
    }

    #[test]
    fn stale_transient_health_does_not_trigger_post_native_failover() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let health = provider_health(
            ProviderKind::Claude,
            agentctl_core::ProviderStatus::Overloaded,
            Utc::now() - chrono::TimeDelta::minutes(10),
            None,
        );
        store.record_health(None, &health).unwrap();

        assert!(!should_failover_after_native(&store, &ProviderKind::Claude, 300).unwrap());
    }

    #[test]
    fn persisted_health_blocks_only_for_the_status_specific_lifetime() {
        let now = Utc::now();
        let recent = now - chrono::TimeDelta::seconds(30);
        let stale = now - chrono::TimeDelta::minutes(10);
        let status =
            |status, checked_at| provider_health(ProviderKind::Claude, status, checked_at, None);

        assert!(!persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::Ready, recent),
            300
        ));
        assert!(!persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::Warning, recent),
            300
        ));
        assert!(persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::Overloaded, recent),
            300
        ));
        assert!(!persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::Overloaded, stale),
            300
        ));
        assert!(persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::Offline, recent),
            300
        ));
        assert!(!persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::Offline, stale),
            300
        ));
        assert!(persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::AuthError, stale),
            300
        ));
        assert!(persisted_health_blocks_auto(
            &status(agentctl_core::ProviderStatus::Incompatible, stale),
            300
        ));
        assert!(persisted_health_blocks_auto(
            &status(
                agentctl_core::ProviderStatus::Exhausted {
                    resets_at: Some(now + chrono::TimeDelta::minutes(10)),
                },
                stale,
            ),
            300
        ));
        assert!(!persisted_health_blocks_auto(
            &status(
                agentctl_core::ProviderStatus::Exhausted {
                    resets_at: Some(now - chrono::TimeDelta::minutes(1)),
                },
                recent,
            ),
            300
        ));
        assert!(persisted_health_blocks_auto(
            &status(
                agentctl_core::ProviderStatus::Exhausted { resets_at: None },
                recent,
            ),
            300
        ));
        assert!(!persisted_health_blocks_auto(
            &status(
                agentctl_core::ProviderStatus::Exhausted { resets_at: None },
                stale,
            ),
            300
        ));
    }

    #[test]
    fn scored_selection_honors_the_sticky_switch_threshold() {
        let candidates = vec![(ProviderKind::Claude, 1.80), (ProviderKind::Codex, 2.00)];

        assert_eq!(
            choose_scored_native_provider(
                "balanced",
                Some(&ProviderKind::Claude),
                0.25,
                &candidates
            ),
            Some(ProviderKind::Codex)
        );
        assert_eq!(
            choose_scored_native_provider(
                "sticky-balanced",
                Some(&ProviderKind::Claude),
                0.25,
                &candidates,
            ),
            Some(ProviderKind::Claude)
        );
        assert_eq!(
            choose_scored_native_provider(
                "sticky-balanced",
                Some(&ProviderKind::Claude),
                0.10,
                &candidates,
            ),
            Some(ProviderKind::Codex)
        );
        assert_eq!(
            choose_scored_native_provider("sticky-balanced", None, 0.25, &candidates),
            Some(ProviderKind::Codex)
        );
    }

    #[test]
    fn native_score_rewards_health_headroom_locality_and_policy_stickiness() {
        let ready_low_usage = provider_health(
            ProviderKind::Claude,
            agentctl_core::ProviderStatus::Ready,
            Utc::now(),
            Some(0.10),
        );
        let ready_high_usage = provider_health(
            ProviderKind::Claude,
            agentctl_core::ProviderStatus::Ready,
            Utc::now(),
            Some(0.90),
        );
        let warning = provider_health(
            ProviderKind::Claude,
            agentctl_core::ProviderStatus::Warning,
            Utc::now(),
            Some(0.10),
        );

        let baseline = native_provider_score(
            "balanced",
            None,
            &ProviderKind::Claude,
            &ready_low_usage,
            0,
            0,
        );
        assert!(
            baseline
                > native_provider_score(
                    "balanced",
                    None,
                    &ProviderKind::Claude,
                    &ready_high_usage,
                    0,
                    0,
                )
        );
        assert!(
            baseline
                > native_provider_score(
                    "balanced",
                    None,
                    &ProviderKind::Claude,
                    &ready_low_usage,
                    50,
                    0,
                )
        );
        assert!(
            baseline
                > native_provider_score("balanced", None, &ProviderKind::Claude, &warning, 0, 0,)
        );
        assert!(
            native_provider_score(
                "claude-first",
                None,
                &ProviderKind::Claude,
                &ready_low_usage,
                0,
                0,
            ) > baseline
        );
        assert!(
            native_provider_score(
                "sticky-balanced",
                Some(&ProviderKind::Claude),
                &ProviderKind::Claude,
                &ready_low_usage,
                0,
                0,
            ) > baseline
        );
        assert!(
            baseline
                > native_provider_score(
                    "balanced",
                    None,
                    &ProviderKind::Claude,
                    &ready_low_usage,
                    0,
                    3,
                )
        );
        assert!(
            native_provider_score(
                "balanced",
                None,
                &ProviderKind::Claude,
                &ready_low_usage,
                u64::MAX,
                u32::MAX,
            )
            .is_finite()
        );
    }

    #[test]
    fn native_failover_is_limited_to_one_automatic_provider_change() {
        assert!(native_failover_attempt_allowed(0, false, false));
        assert!(!native_failover_attempt_allowed(1, false, false));
        assert!(!native_failover_attempt_allowed(usize::MAX, false, false));
        assert!(!native_failover_attempt_allowed(0, true, false));
        assert!(!native_failover_attempt_allowed(0, false, true));
    }

    #[tokio::test]
    async fn invalid_new_native_args_fail_before_home_or_session_creation() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();

        for provider in ["claude", "codex"] {
            let home = directory.path().join(format!("home-{provider}"));
            let cli = Cli::try_parse_from(vec![
                OsString::from("agentctl"),
                OsString::from("--home"),
                home.clone().into_os_string(),
                OsString::from("new"),
                OsString::from("--workspace"),
                workspace.clone().into_os_string(),
                OsString::from("--provider"),
                OsString::from(provider),
                OsString::from("--"),
                OsString::from("--not-an-agentctl-safe-option"),
            ])
            .unwrap();

            let error = dispatch(cli).await.unwrap_err().to_string();

            assert!(error.contains("safe forwarding allowlist"));
            assert!(
                !home.exists(),
                "invalid new {provider} arguments must not even initialize agentctl state"
            );
        }
    }

    #[tokio::test]
    async fn automatic_provider_specific_args_fail_before_any_state_initialization() {
        let directory = tempfile::tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();

        for (name, option) in [
            ("claude-only", "--ax-screen-reader"),
            ("codex-only", "--no-alt-screen"),
        ] {
            let home = directory.path().join(format!("home-{name}"));
            let cli = Cli::try_parse_from(vec![
                OsString::from("agentctl"),
                OsString::from("--home"),
                home.clone().into_os_string(),
                OsString::from("new"),
                OsString::from("--workspace"),
                workspace.clone().into_os_string(),
                OsString::from("--"),
                OsString::from(option),
            ])
            .unwrap();

            let error = dispatch(cli).await.unwrap_err().to_string();

            assert!(error.contains("automatic provider selection"));
            assert!(error.contains("select a provider explicitly"));
            assert!(
                !home.exists(),
                "provider-specific automatic arguments must fail before opening local state"
            );
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn assert_invalid_native_args_do_not_prepare_provider(provider: ProviderKind) {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut config = Config::default();
        config.providers.claude_binary = "binary-must-not-run".to_owned();
        config.providers.codex_binary = "binary-must-not-run".to_owned();
        let session = create_session(
            &store,
            &config,
            &workspace,
            Some("invalid-native-args"),
            None,
        )
        .unwrap();
        let identity = WorkspaceIdentity::discover(&workspace).unwrap();
        let before = capture_git_snapshot(&workspace).unwrap();
        let started_at = Utc::now();
        store
            .record_workspace_snapshot(&WorkspaceSnapshotRecord {
                id: EventId::new(),
                session_id: session.id,
                turn_id: None,
                phase: "before_native".to_owned(),
                fingerprint: session.workspace_fingerprint.clone(),
                snapshot: serde_json::to_value(&before).unwrap(),
                diff_digest: Some(before.diff_digest.clone()),
                created_at: started_at - chrono::TimeDelta::milliseconds(1),
            })
            .unwrap();
        let launch_id = uuid::Uuid::now_v7();
        store
            .start_native_launch(&NativeLaunchRecord {
                id: launch_id,
                session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id: uuid::Uuid::new_v4().to_string(),
                workspace_lease_key: identity.lease_key,
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({}),
                started_at,
                updated_at: started_at,
            })
            .unwrap();
        let mut command = if cfg!(windows) {
            let mut command = StdCommand::new("cmd");
            command.args(["/C", "exit", "0"]);
            command
        } else {
            let mut command = StdCommand::new("sh");
            command.args(["-c", "exit 0"]);
            command
        };
        let mut dead_child = command.spawn().unwrap();
        let dead_pid = dead_child.id();
        assert!(dead_child.wait().unwrap().success());
        store.record_native_launch_pid(launch_id, dead_pid).unwrap();
        store
            .update_native_launch(
                launch_id,
                NativeLaunchState::Uncertain,
                Some(1),
                Some("simulated dead native launch"),
                Utc::now(),
            )
            .unwrap();

        let session_before = serde_json::to_value(store.get_session(session.id).unwrap()).unwrap();
        let launch_before = serde_json::to_value(store.native_launch(launch_id).unwrap()).unwrap();
        let events_before =
            serde_json::to_value(store.list_events(session.id, 0, usize::MAX).unwrap()).unwrap();
        let providers_before =
            serde_json::to_value(store.list_provider_sessions(session.id).unwrap()).unwrap();
        let snapshots_before =
            serde_json::to_value(store.list_workspace_snapshots(session.id, None).unwrap())
                .unwrap();

        let error = run_native_session(
            store.clone(),
            paths.clone(),
            config,
            session.clone(),
            Some(provider),
            vec![std::ffi::OsString::from("--not-an-agentctl-safe-option")],
            false,
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("safe forwarding allowlist"));
        assert_eq!(
            serde_json::to_value(store.get_session(session.id).unwrap()).unwrap(),
            session_before
        );
        assert_eq!(
            serde_json::to_value(store.native_launch(launch_id).unwrap()).unwrap(),
            launch_before,
            "invalid arguments must not reconcile the dead native launch"
        );
        assert_eq!(
            serde_json::to_value(store.list_events(session.id, 0, usize::MAX).unwrap()).unwrap(),
            events_before
        );
        assert_eq!(
            serde_json::to_value(store.list_provider_sessions(session.id).unwrap()).unwrap(),
            providers_before
        );
        assert_eq!(
            serde_json::to_value(store.list_workspace_snapshots(session.id, None).unwrap())
                .unwrap(),
            snapshots_before,
            "invalid arguments must not capture a post-crash workspace snapshot"
        );
        assert!(
            store
                .latest_health(&ProviderKind::Claude)
                .unwrap()
                .is_none()
        );
        assert!(store.latest_health(&ProviderKind::Codex).unwrap().is_none());
        assert!(
            !paths.home.join("native-runtime").exists(),
            "Claude hook settings must not be prepared"
        );
        assert!(
            !paths.codex_protocol_root().exists(),
            "Codex schema/thread preparation must not begin"
        );
    }

    #[tokio::test]
    async fn invalid_claude_open_args_do_not_reconcile_a_dead_native_launch() {
        assert_invalid_native_args_do_not_prepare_provider(ProviderKind::Claude).await;
    }

    #[tokio::test]
    async fn invalid_codex_switch_args_do_not_reconcile_a_dead_native_launch() {
        assert_invalid_native_args_do_not_prepare_provider(ProviderKind::Codex).await;
    }

    #[test]
    fn claude_crash_recovery_does_not_materialize_an_empty_native_session() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = Config::default();
        let session =
            create_session(&store, &config, workspace.path(), Some("empty-crash"), None).unwrap();
        let identity = WorkspaceIdentity::discover(workspace.path()).unwrap();
        let now = Utc::now();
        let native_session_id = uuid::Uuid::new_v4().to_string();
        store
            .upsert_provider_session(&ProviderSessionRecord {
                id: ProviderSessionId::new(),
                unified_session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id: native_session_id.clone(),
                native_version: Some("2.1.139".to_owned()),
                last_synced_seq: 0,
                status: ProviderStatus::Unknown,
                reset_at: None,
                capabilities: BTreeMap::new(),
                metadata: serde_json::json!({
                    "mode": "native_interactive",
                    "native_started": true,
                }),
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        let launch_id = uuid::Uuid::now_v7();
        store
            .start_native_launch(&NativeLaunchRecord {
                id: launch_id,
                session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id,
                workspace_lease_key: identity.lease_key,
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({}),
                started_at: now,
                updated_at: now,
            })
            .unwrap();
        store.record_native_launch_pid(launch_id, 424_242).unwrap();
        store
            .update_native_launch(
                launch_id,
                NativeLaunchState::Exited,
                Some(1),
                None,
                Utc::now(),
            )
            .unwrap();
        let launch = store.native_launch(launch_id).unwrap().unwrap();

        reconcile_claude_launch(
            &store,
            &config,
            &session,
            Some(&ProviderKind::Claude),
            &launch,
        )
        .unwrap();

        let provider = store
            .provider_session(session.id, &ProviderKind::Claude)
            .unwrap()
            .unwrap();
        assert_eq!(provider.metadata["native_materialized"], false);
        assert!(provider.metadata.get("native_started").is_none());
        assert_eq!(
            store.native_launch(launch.id).unwrap().unwrap().state,
            NativeLaunchState::Failed
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn claude_crash_switches_to_codex_only_as_a_non_replay_continuation() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = Config::default();
        let session =
            create_session(&store, &config, workspace.path(), Some("cross-crash"), None).unwrap();
        let identity = WorkspaceIdentity::discover(workspace.path()).unwrap();
        let before_at = Utc::now();
        let snapshot = GitSnapshot {
            root: session.workspace_path.clone(),
            head: Some("deadbeef".to_owned()),
            branch: Some("main".to_owned()),
            changed_paths: Vec::new(),
            dirty: false,
            diff_digest: "sha256:stable".to_owned(),
            coverage_complete: true,
            captured_at: before_at,
        };
        store
            .record_workspace_snapshot(&WorkspaceSnapshotRecord {
                id: EventId::new(),
                session_id: session.id,
                turn_id: None,
                phase: "before_native".to_owned(),
                fingerprint: session.workspace_fingerprint.clone(),
                snapshot: serde_json::to_value(&snapshot).unwrap(),
                diff_digest: Some(snapshot.diff_digest.clone()),
                created_at: before_at,
            })
            .unwrap();
        let launch_id = uuid::Uuid::now_v7();
        let started_at = before_at + chrono::TimeDelta::milliseconds(1);
        let native_session_id = uuid::Uuid::new_v4().to_string();
        let provider_id = ProviderSessionId::new();
        store
            .upsert_provider_session(&ProviderSessionRecord {
                id: provider_id,
                unified_session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id: native_session_id.clone(),
                native_version: Some("2.1.139".to_owned()),
                last_synced_seq: 0,
                status: ProviderStatus::Ready,
                reset_at: None,
                capabilities: BTreeMap::new(),
                metadata: serde_json::json!({"native_materialized": true}),
                created_at: before_at,
                updated_at: before_at,
            })
            .unwrap();
        store
            .start_native_launch(&NativeLaunchRecord {
                id: launch_id,
                session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id,
                workspace_lease_key: identity.lease_key,
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({}),
                started_at,
                updated_at: started_at,
            })
            .unwrap();
        store.record_native_launch_pid(launch_id, 424_242).unwrap();
        let turn_id = TurnId::new();
        let turn_at = started_at + chrono::TimeDelta::milliseconds(1);
        store
            .create_turn(&TurnRecord {
                id: turn_id,
                session_id: session.id,
                provider: Some(ProviderKind::Claude),
                prompt_seq: 1,
                status: agentctl_core::TurnStatus::Running,
                side_effect_state: agentctl_core::SideEffectState::Confirmed,
                native_turn_id: Some("turn-crashed".to_owned()),
                continuation: false,
                started_at: Some(turn_at),
                completed_at: None,
                created_at: turn_at,
                updated_at: turn_at,
            })
            .unwrap();
        store
            .record_workspace_snapshot(&WorkspaceSnapshotRecord {
                id: EventId::new(),
                session_id: session.id,
                turn_id: None,
                phase: "after_native_recovery".to_owned(),
                fingerprint: session.workspace_fingerprint.clone(),
                snapshot: serde_json::to_value(&snapshot).unwrap(),
                diff_digest: Some(snapshot.diff_digest.clone()),
                created_at: turn_at + chrono::TimeDelta::milliseconds(1),
            })
            .unwrap();
        store
            .update_native_launch(
                launch_id,
                NativeLaunchState::Exited,
                Some(1),
                None,
                Utc::now(),
            )
            .unwrap();
        let launch = store.native_launch(launch_id).unwrap().unwrap();

        let recovery = reconcile_claude_launch(
            &store,
            &config,
            &session,
            Some(&ProviderKind::Codex),
            &launch,
        )
        .unwrap();

        assert!(!recovery.clean_exit);
        let continuation = recovery.continuation.unwrap();
        assert_eq!(
            continuation.side_effect_state,
            agentctl_core::SideEffectState::Confirmed
        );
        assert_eq!(continuation.turn_id, Some(turn_id));
        let turn = store.get_turn(turn_id).unwrap().unwrap();
        assert_eq!(turn.status, agentctl_core::TurnStatus::Failed);
        assert_eq!(
            turn.side_effect_state,
            agentctl_core::SideEffectState::Confirmed
        );
        assert!(
            crate::native_hooks::active_claude_turn(&store, session.id)
                .unwrap()
                .is_none(),
            "returning to Claude must accept a fresh user prompt"
        );
        let events = store.list_events(session.id, 0, usize::MAX).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == "native_context_marker")
                .count(),
            1
        );
        assert!(events.iter().all(|event| event.kind != "user_prompt"));
        assert_eq!(
            store.native_launch(launch_id).unwrap().unwrap().state,
            NativeLaunchState::Failed
        );
    }

    #[test]
    fn claude_crash_with_ambiguous_handoff_stays_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = Config::default();
        let session = create_session(
            &store,
            &config,
            workspace.path(),
            Some("handoff-crash"),
            None,
        )
        .unwrap();
        let identity = WorkspaceIdentity::discover(workspace.path()).unwrap();
        let now = Utc::now();
        let native_session_id = uuid::Uuid::new_v4().to_string();
        let provider_id = ProviderSessionId::new();
        store
            .upsert_provider_session(&ProviderSessionRecord {
                id: provider_id,
                unified_session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id: native_session_id.clone(),
                native_version: None,
                last_synced_seq: 0,
                status: ProviderStatus::Ready,
                reset_at: None,
                capabilities: BTreeMap::new(),
                metadata: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        let launch_id = uuid::Uuid::now_v7();
        store
            .start_native_launch(&NativeLaunchRecord {
                id: launch_id,
                session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id: native_session_id.clone(),
                workspace_lease_key: identity.lease_key,
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({}),
                started_at: now,
                updated_at: now,
            })
            .unwrap();
        store
            .stage_native_handoff(&agentctl_storage::NativeHandoffRecord {
                launch_id,
                provider_session_id: provider_id,
                session_id: session.id,
                native_session_id,
                through_seq: 0,
                capsule: "<agent-handoff/>".to_owned(),
                content_digest: "sha256:test".to_owned(),
                state: agentctl_storage::NativeHandoffState::Staged,
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        store.begin_native_handoff_delivery(launch_id).unwrap();
        let launch = store.native_launch(launch_id).unwrap().unwrap();

        let error = ensure_claude_handoff_is_replay_safe(&store, &launch)
            .unwrap_err()
            .to_string();

        assert!(error.contains("may already have been delivered"));
        assert_eq!(
            store.native_launch(launch_id).unwrap().unwrap().state,
            NativeLaunchState::Started
        );
        assert!(
            store
                .list_events(session.id, 0, usize::MAX)
                .unwrap()
                .iter()
                .all(|event| event.kind != "native_context_marker")
        );
    }

    #[test]
    fn post_spawn_codex_error_without_pid_receipt_stays_uncertain() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = Config::default();
        let session =
            create_session(&store, &config, workspace.path(), Some("journal"), None).unwrap();
        let identity = WorkspaceIdentity::discover(workspace.path()).unwrap();
        let launch_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        store
            .start_native_launch(&NativeLaunchRecord {
                id: launch_id,
                session_id: session.id,
                provider: ProviderKind::Codex,
                native_session_id: "thread-test".to_owned(),
                workspace_lease_key: identity.lease_key,
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({}),
                started_at: now,
                updated_at: now,
            })
            .unwrap();

        let error = anyhow::anyhow!("simulated post-spawn containment failure");
        mark_native_launch_after_error(&store, &config, launch_id, &error, true, None).unwrap();

        let launch = store.native_launch(launch_id).unwrap().unwrap();
        assert_eq!(launch.state, NativeLaunchState::Uncertain);
        assert!(launch.child_pid.is_none());
        assert!(journaled_native_pid(&launch).is_err());
        assert!(
            store
                .open_native_launches(session.id)
                .unwrap()
                .iter()
                .any(|open| open.id == launch_id)
        );
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{os::unix::fs::PermissionsExt, process::Command};

    use std::os::unix::process::CommandExt;

    #[tokio::test]
    async fn live_native_process_group_blocks_cross_provider_reconciliation() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let store = operations::open_store(&paths).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let config = Config::default();
        let session =
            create_session(&store, &config, workspace.path(), Some("live-launch"), None).unwrap();
        let identity = WorkspaceIdentity::discover(workspace.path()).unwrap();
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 30"]).process_group(0);
        let mut child = command.spawn().unwrap();
        let pid = child.id();
        let launch_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        store
            .start_native_launch(&NativeLaunchRecord {
                id: launch_id,
                session_id: session.id,
                provider: ProviderKind::Claude,
                native_session_id: uuid::Uuid::new_v4().to_string(),
                workspace_lease_key: identity.lease_key.clone(),
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({}),
                started_at: now,
                updated_at: now,
            })
            .unwrap();
        store.record_native_launch_pid(launch_id, pid).unwrap();

        let error = reconcile_open_native_launch(
            &store,
            &paths,
            &config,
            &session,
            &identity,
            Some(&ProviderKind::Codex),
        )
        .await
        .unwrap_err()
        .to_string();

        agentctl_workspace::kill_process_group(agentctl_workspace::ProcessGroupId::from_child_id(
            pid,
        ))
        .unwrap();
        let _ = child.wait();
        assert!(error.contains("still running as process group"));
        assert_eq!(
            store.native_launch(launch_id).unwrap().unwrap().state,
            NativeLaunchState::Started
        );
        assert!(
            store
                .list_events(session.id, 0, usize::MAX)
                .unwrap()
                .iter()
                .all(|event| event.kind != "native_context_marker")
        );
    }

    #[test]
    fn committed_native_import_reports_shutdown_failure_as_warning() {
        let session_id = UnifiedSessionId::new();
        let report = operations::NativeImportReport {
            session_id,
            provider: ProviderKind::Codex,
            native_session_id: "thr_test".to_owned(),
            imported_turns: 1,
            imported_events: 3,
            skipped_existing_events: 0,
            activated: false,
            canonical_history_imported: true,
            validation: "official_thread_read",
            warning: None,
        };
        let result = finish_native_import(
            Ok(report),
            Err(ProviderError::Process("shutdown failed".to_owned())),
        )
        .unwrap();
        assert_eq!(result.session_id, session_id);
        assert!(
            result
                .warning
                .as_deref()
                .is_some_and(|warning| warning.contains("committed successfully"))
        );
    }

    #[tokio::test]
    async fn claude_behavioral_capabilities_are_version_bound_and_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let binary = directory.path().join("claude-fake");
        std::fs::write(&binary, "#!/bin/sh\necho '1.0.0 (Claude Code)'\n").unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(
            paths.home.join("protocol-capabilities.json"),
            serde_json::to_vec(&serde_json::json!({
                "checks": [
                    {"provider":"claude","name":"binary","status":"passed","detail":"1.0.0 (Claude Code)"},
                    {"provider":"claude","name":"shouldQuery:false behavioral probe","status":"passed","detail":"ok"},
                    {"provider":"claude","name":"UserPromptSubmit behavioral probe","status":"passed","detail":"ok"}
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            load_claude_should_query_support(&paths, binary.to_str().unwrap()).await,
            ShouldQuerySupport::Supported
        );
        assert_eq!(
            load_claude_hook_support(&paths, binary.to_str().unwrap()).await,
            UserPromptSubmitHookSupport::Supported
        );
        let capabilities_path = paths.home.join("protocol-capabilities.json");
        let mut capabilities: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&capabilities_path).unwrap()).unwrap();
        let hook_check = capabilities["checks"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|check| check["name"] == "UserPromptSubmit behavioral probe")
            .unwrap();
        hook_check["status"] = serde_json::json!("warning");
        std::fs::write(
            &capabilities_path,
            serde_json::to_vec(&capabilities).unwrap(),
        )
        .unwrap();
        assert_eq!(
            load_claude_hook_support(&paths, binary.to_str().unwrap()).await,
            UserPromptSubmitHookSupport::Unsupported
        );
        std::fs::write(&binary, "#!/bin/sh\necho '2.0.0 (Claude Code)'\n").unwrap();
        assert_eq!(
            load_claude_should_query_support(&paths, binary.to_str().unwrap()).await,
            ShouldQuerySupport::Unknown
        );
        assert_eq!(
            load_claude_hook_support(&paths, binary.to_str().unwrap()).await,
            UserPromptSubmitHookSupport::Unknown
        );
    }

    #[test]
    fn claude_handoff_policy_is_stable_private_and_contains_no_credentials() {
        let directory = tempfile::tempdir().unwrap();
        let paths = AgentctlPaths::resolve(Some(directory.path().join("home"))).unwrap();
        let policy = ensure_claude_policy_file(&paths).unwrap();
        assert_eq!(std::fs::read_to_string(&policy).unwrap(), HANDOFF_POLICY);
        assert_eq!(
            std::fs::metadata(&policy).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let normalized = HANDOFF_POLICY.to_ascii_lowercase();
        for secret_marker in ["bearer ", "api_key", "password=", "token="] {
            assert!(!normalized.contains(secret_marker));
        }
    }

    fn thread(
        thread_id: &str,
        session_id: &str,
        updated_at: i64,
    ) -> agentctl_provider_codex::InteractiveThreadIdentity {
        agentctl_provider_codex::InteractiveThreadIdentity {
            thread_id: thread_id.to_owned(),
            session_id: session_id.to_owned(),
            forked_from_id: None,
            updated_at,
        }
    }

    #[test]
    fn codex_thread_continuity_allows_only_the_mapped_thread_to_change() {
        let cwd = PathBuf::from("/tmp/agentctl-worktree");
        let before = InteractiveThreadSnapshot {
            cwd: cwd.clone(),
            threads: vec![thread("mapped", "tree-a", 1), thread("other", "tree-b", 1)],
        };
        let after = InteractiveThreadSnapshot {
            cwd,
            threads: vec![thread("mapped", "tree-a", 2), thread("other", "tree-b", 1)],
        };
        validate_codex_thread_continuity(&before, &after, "mapped").unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn codex_thread_continuity_allows_timestamp_updates_in_existing_siblings() {
        let cwd = PathBuf::from("/tmp/agentctl-worktree");
        let before = InteractiveThreadSnapshot {
            cwd: cwd.clone(),
            threads: vec![thread("mapped", "tree-a", 1), thread("other", "tree-b", 1)],
        };
        let after = InteractiveThreadSnapshot {
            cwd,
            threads: vec![thread("mapped", "tree-a", 2), thread("other", "tree-b", 2)],
        };
        validate_codex_launch_continuity(&before, &after, "mapped", &serde_json::json!({
            "codex_evidence": {"selected_threads": ["mapped"], "pending_requests": 0, "complete": true, "error": null}
        })).unwrap();
    }

    #[test]
    fn codex_thread_continuity_rejects_new_or_switched_existing_threads() {
        let cwd = PathBuf::from("/tmp/agentctl-worktree");
        let before = InteractiveThreadSnapshot {
            cwd: cwd.clone(),
            threads: vec![thread("mapped", "tree-a", 1), thread("other", "tree-b", 1)],
        };
        let switched = InteractiveThreadSnapshot {
            cwd: cwd.clone(),
            threads: vec![thread("mapped", "tree-a", 2), thread("other", "tree-b", 2)],
        };
        assert!(validate_codex_thread_continuity(&before, &switched, "mapped").is_err());

        let created = InteractiveThreadSnapshot {
            cwd,
            threads: vec![thread("mapped", "tree-a", 2), thread("new", "tree-c", 2)],
        };
        assert!(validate_codex_thread_continuity(&before, &created, "mapped").is_err());
    }
}
