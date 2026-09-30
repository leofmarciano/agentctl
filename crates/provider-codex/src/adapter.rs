// ABOUTME: Connects Codex native sessions to canonical events and provider operations.
// ABOUTME: Reads native history and manages the official app-server protocol.
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use agentctl_core::{
    AgentEvent, AgentProvider, ApprovalAction, ApprovalDecision, ApprovalId, ApprovalRequest,
    NativeAttachment, NativeEffectStatus, NativeSession, NativeTranscript, NativeTranscriptItem,
    NativeTranscriptTurn, ProviderError, ProviderEventStream, ProviderHealth, ProviderKind,
    ProviderSessionId, ProviderStatus, RiskLevel, SessionContext, SyncBatch, SyncReceipt,
    TurnExecutionMode, TurnRequest, TurnStatus, UnifiedSessionId,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    jsonrpc::{CodexRpcClient, RpcInbound},
    mapping::{map_notification, parse_rate_limit},
    process::spawn_app_server,
    schema::{
        CodexInstallation, detect_installation, generate_schema_cache, generate_schema_cache_under,
    },
};

#[derive(Clone, Debug)]
pub struct CodexConfig {
    pub binary: PathBuf,
    /// Root containing one generated schema directory per installed Codex version.
    pub schema_cache_root: Option<PathBuf>,
    pub channel_capacity: usize,
    pub request_timeout: Duration,
    /// Shares the native CLI service when it is running.
    pub interactive: bool,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            binary: PathBuf::from("codex"),
            schema_cache_root: None,
            channel_capacity: 256,
            request_timeout: Duration::from_secs(30),
            interactive: false,
        }
    }
}

const THREAD_SNAPSHOT_PAGE_SIZE: u32 = 100;
const MAX_THREAD_SNAPSHOT_PAGES: usize = 64;
pub const MAX_INTERACTIVE_THREAD_SNAPSHOT: usize = 4096;
const NEW_THREAD_MATERIALIZATION_MARKER: &str =
    r#"<agentctl-session-marker version="1" purpose="rollout-materialization" />"#;

/// Minimal, non-transcript metadata used to detect native `/new`, `/fork`, or
/// thread switching while the Codex TUI owns the terminal.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[allow(clippy::struct_field_names)]
pub struct InteractiveThreadIdentity {
    pub thread_id: String,
    pub session_id: String,
    pub forked_from_id: Option<String>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InteractiveThreadSnapshot {
    pub cwd: PathBuf,
    pub threads: Vec<InteractiveThreadIdentity>,
}

#[derive(Debug, Default)]
struct ThreadSnapshotAccumulator {
    pages: usize,
    seen_cursors: HashSet<String>,
    seen_threads: HashSet<String>,
    threads: Vec<InteractiveThreadIdentity>,
}

#[derive(Clone, Debug)]
struct PendingApproval {
    client: Option<Arc<CodexRpcClient>>,
    rpc_id: Value,
    method: String,
    params: Value,
    native_turn_id: String,
}

/// Provider adapter backed by one multiplexed `codex app-server` process.
#[derive(Debug)]
pub struct CodexAdapter {
    config: CodexConfig,
    connection: Mutex<Option<Arc<CodexRpcClient>>>,
    installation: RwLock<Option<CodexInstallation>>,
    sessions: RwLock<HashMap<UnifiedSessionId, String>>,
    pending_approvals: Arc<Mutex<HashMap<ApprovalId, PendingApproval>>>,
    active_turns: Arc<Mutex<HashMap<String, String>>>,
    applied_syncs: Mutex<HashSet<(String, u64, u32)>>,
}

impl CodexAdapter {
    /// Creates an adapter for a concrete binary and optional versioned schema-cache root.
    pub fn new(binary: impl Into<PathBuf>, schema_cache_root: Option<PathBuf>) -> Self {
        Self::from_config(CodexConfig {
            binary: binary.into(),
            schema_cache_root,
            ..CodexConfig::default()
        })
    }

    /// Connects interactive sessions through the same native service as the Codex CLI.
    pub fn for_interactive_sessions(
        binary: impl Into<PathBuf>,
        schema_cache_root: Option<PathBuf>,
    ) -> Self {
        Self::from_config(CodexConfig {
            binary: binary.into(),
            schema_cache_root,
            interactive: true,
            ..CodexConfig::default()
        })
    }

    pub fn from_config(config: CodexConfig) -> Self {
        Self {
            config,
            connection: Mutex::new(None),
            installation: RwLock::new(None),
            sessions: RwLock::new(HashMap::new()),
            pending_approvals: Arc::new(Mutex::new(HashMap::new())),
            active_turns: Arc::new(Mutex::new(HashMap::new())),
            applied_syncs: Mutex::new(HashSet::new()),
        }
    }

    /// Registers a persisted thread so the next `ensure_session` uses `thread/resume`.
    pub async fn attach_native_session(
        &self,
        unified_session_id: UnifiedSessionId,
        native_session_id: impl Into<String>,
    ) {
        self.sessions
            .write()
            .await
            .insert(unified_session_id, native_session_id.into());
    }

    /// Lists every interactive thread associated with one exact workspace cwd.
    /// Pagination is exhaustive but bounded so a corrupt or adversarial cursor
    /// stream cannot keep the local wrapper alive indefinitely.
    pub async fn snapshot_interactive_threads(
        &self,
        cwd: &Path,
    ) -> Result<InteractiveThreadSnapshot, ProviderError> {
        let cwd = std::fs::canonicalize(cwd).map_err(|error| {
            ProviderError::Incompatible(format!(
                "cannot snapshot Codex threads for {}: {error}",
                cwd.display()
            ))
        })?;
        let cwd_text = cwd.to_str().ok_or_else(|| {
            ProviderError::Incompatible(
                "Codex thread/list requires a UTF-8 workspace path".to_owned(),
            )
        })?;
        self.require_installed_client_method("thread/list").await?;
        let client = self.client().await?;
        let mut cursor: Option<String> = None;
        let mut accumulator = ThreadSnapshotAccumulator::default();
        loop {
            let response = client
                .request(
                    "thread/list",
                    json!({
                        "archived": false,
                        "cursor": cursor,
                        "cwd": cwd_text,
                        "limit": THREAD_SNAPSHOT_PAGE_SIZE,
                        "sourceKinds": ["cli", "vscode", "appServer", "unknown"],
                        "sortDirection": "asc",
                        "sortKey": "created_at",
                        "useStateDbOnly": false,
                    }),
                )
                .await?;
            cursor = accumulator.accept_page(&response.result, &cwd)?;
            if cursor.is_none() {
                break;
            }
        }
        Ok(accumulator.finish(cwd))
    }

    /// Checks only binary presence/version. It never starts app-server or a model turn.
    pub async fn probe_static(&self) -> Result<ProviderHealth, ProviderError> {
        let installation = self.installation().await?;
        Ok(ProviderHealth {
            provider: ProviderKind::Codex,
            status: ProviderStatus::Ready,
            version: Some(installation.version),
            capabilities: codex_capabilities(),
            usage: None,
            rate_limit: None,
            checked_at: Utc::now(),
            message: Some("binary detected; live app-server protocol not checked".to_owned()),
        })
    }

    /// Starts app-server and checks account/rate-limit endpoints without invoking a model.
    pub async fn probe_live(&self) -> Result<ProviderHealth, ProviderError> {
        let installation = self.installation().await?;
        let client = self.client().await?;
        let account = client.request("account/read", Value::Null).await?;
        let rate_limits = client
            .request("account/rateLimits/read", Value::Null)
            .await?;
        let rate_limit = parse_rate_limit(
            rate_limits
                .result
                .get("rateLimits")
                .unwrap_or(&rate_limits.result),
            "codex_account",
        );
        let unauthenticated = account.result.get("account").is_some_and(Value::is_null);
        let status = if unauthenticated {
            ProviderStatus::AuthError
        } else if rate_limit
            .as_ref()
            .and_then(|limit| limit.utilization)
            .is_some_and(|utilization| utilization >= 1.0)
        {
            ProviderStatus::Exhausted {
                resets_at: rate_limit.as_ref().and_then(|limit| limit.resets_at),
            }
        } else if rate_limit
            .as_ref()
            .and_then(|limit| limit.utilization)
            .is_some_and(|utilization| utilization >= 0.9)
        {
            ProviderStatus::Warning
        } else {
            ProviderStatus::Ready
        };
        Ok(ProviderHealth {
            provider: ProviderKind::Codex,
            status,
            version: Some(installation.version),
            capabilities: codex_capabilities(),
            usage: None,
            rate_limit,
            checked_at: Utc::now(),
            message: unauthenticated.then(|| "Codex has no authenticated account".to_owned()),
        })
    }

    /// Returns an existing schema directory or generates one for the installed version.
    async fn prepare_schema(&self) -> Result<PathBuf, ProviderError> {
        let installation = self.installation().await?;
        if let Some(root) = &self.config.schema_cache_root {
            return generate_schema_cache_under(&installation, root).await;
        }
        generate_schema_cache(&installation).await
    }

    async fn installation(&self) -> Result<CodexInstallation, ProviderError> {
        if let Some(installation) = self.installation.read().await.clone() {
            return Ok(installation);
        }
        let installation = detect_installation(&self.config.binary).await?;
        *self.installation.write().await = Some(installation.clone());
        Ok(installation)
    }

    async fn client(&self) -> Result<Arc<CodexRpcClient>, ProviderError> {
        let mut connection = self.connection.lock().await;
        if let Some(client) = connection.as_ref()
            && !client.is_closed()
        {
            return Ok(Arc::clone(client));
        }
        #[cfg(unix)]
        let socket = if self.config.interactive {
            crate::process::interactive_endpoint(&self.config.binary).await?
        } else {
            None
        };
        #[cfg(unix)]
        if let Some(socket) = socket {
            let client = CodexRpcClient::connect(
                &socket,
                self.config.channel_capacity,
                self.config.request_timeout,
            )
            .await?;
            *connection = Some(Arc::clone(&client));
            return Ok(client);
        }
        let client = spawn_app_server(
            &self.config.binary,
            self.config.channel_capacity,
            self.config.request_timeout,
        )
        .await?;
        *connection = Some(Arc::clone(&client));
        Ok(client)
    }

    async fn ensure_thread_loaded(
        &self,
        client: &CodexRpcClient,
        session: &NativeSession,
        cwd: Option<&std::path::Path>,
    ) -> Result<(), ProviderError> {
        if client.is_thread_loaded(&session.native_session_id).await {
            return Ok(());
        }
        let response = client
            .request(
                "thread/resume",
                json!({
                    "threadId": session.native_session_id,
                    "cwd": cwd
                }),
            )
            .await?;
        let resumed = resumed_thread_id(&session.native_session_id, &response.result)?;
        client.mark_thread_loaded(&resumed).await;
        Ok(())
    }

    async fn require_installed_client_method(&self, method: &str) -> Result<(), ProviderError> {
        let schema_dir = self.prepare_schema().await?;
        let schema_path = schema_dir.join("ClientRequest.json");
        let schema = tokio::fs::read_to_string(&schema_path)
            .await
            .map_err(|error| {
                ProviderError::Incompatible(format!(
                    "could not read installed-version schema {}: {error}",
                    schema_path.display()
                ))
            })?;
        let schema: Value = serde_json::from_str(&schema).map_err(|error| {
            ProviderError::Incompatible(format!(
                "installed-version schema {} is invalid JSON: {error}",
                schema_path.display()
            ))
        })?;
        if !json_contains_string(&schema, method) {
            return Err(ProviderError::Incompatible(format!(
                "installed Codex schema does not advertise {method}"
            )));
        }
        Ok(())
    }

    async fn materialize_new_thread(
        &self,
        client: &CodexRpcClient,
        thread_id: &str,
    ) -> Result<(), ProviderError> {
        client
            .request(
                "thread/inject_items",
                json!({
                    "threadId": thread_id,
                    "items": [new_thread_materialization_item()]
                }),
            )
            .await?;
        let response = client
            .request(
                "thread/read",
                json!({
                    "threadId": thread_id,
                    "includeTurns": true,
                }),
            )
            .await?;
        let transcript = parse_native_transcript(thread_id, &response.result)?;
        if !transcript.turns.is_empty() {
            return Err(ProviderError::Incompatible(
                "Codex exposed agentctl's rollout-materialization marker as a native turn"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    async fn create_or_resume_thread(
        &self,
        context: &SessionContext,
    ) -> Result<String, ProviderError> {
        let client = self.client().await?;
        if let Some(thread_id) = self
            .sessions
            .read()
            .await
            .get(&context.unified_session_id)
            .cloned()
        {
            if client.is_thread_loaded(&thread_id).await {
                return Ok(thread_id);
            }
            let response = client
                .request(
                    "thread/resume",
                    json!({
                        "threadId": thread_id,
                        "cwd": context.workspace_root
                    }),
                )
                .await?;
            let resumed = resumed_thread_id(&thread_id, &response.result)?;
            client.mark_thread_loaded(&resumed).await;
            return Ok(resumed);
        }

        // Codex 0.144 only reserves an in-memory id for an otherwise empty
        // `thread/start`; no rollout exists for a later native `codex resume`
        // until at least one model-history item is persisted. Validate the two
        // no-model endpoints before creating the thread so an incompatible
        // installation does not leave an avoidable orphan id.
        self.require_installed_client_method("thread/inject_items")
            .await?;
        self.require_installed_client_method("thread/read").await?;
        let response = client
            .request(
                "thread/start",
                json!({
                    "cwd": context.workspace_root
                }),
            )
            .await?;
        let thread_id = extract_thread_id(&response.result).ok_or_else(|| {
            ProviderError::Protocol("thread/start omitted the thread id".to_owned())
        })?;
        self.materialize_new_thread(&client, &thread_id).await?;
        client.mark_thread_loaded(&thread_id).await;
        self.sessions
            .write()
            .await
            .insert(context.unified_session_id, thread_id.clone());
        Ok(thread_id)
    }

    async fn answer_approval(
        &self,
        approval_id: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), ProviderError> {
        let pending = self
            .pending_approvals
            .lock()
            .await
            .remove(&approval_id)
            .ok_or_else(|| ProviderError::Protocol(format!("unknown approval {approval_id}")))?;
        let result = approval_result(&pending, decision);
        let client = pending.client.as_ref().ok_or_else(|| {
            ProviderError::Protocol("approval has no app-server connection".to_owned())
        })?;
        if client.is_closed() {
            return Err(ProviderError::Process(
                "approval's Codex app-server connection closed".to_owned(),
            ));
        }
        client.respond(pending.rpc_id, result).await?;
        if decision == ApprovalDecision::CancelTurn {
            // The approval response asks Codex to cancel; the explicit interrupt is a
            // belt-and-suspenders fallback for protocol versions that only decline.
            let _ = self
                .client()
                .await?
                .request(
                    "turn/interrupt",
                    json!({
                        "threadId": pending.params.get("threadId").and_then(Value::as_str),
                        "turnId": pending.native_turn_id
                    }),
                )
                .await;
        }
        Ok(())
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl AgentProvider for CodexAdapter {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn supports_turn_mode(&self, _mode: TurnExecutionMode) -> bool {
        true
    }

    async fn probe(&self) -> Result<ProviderHealth, ProviderError> {
        self.probe_live().await
    }

    async fn ensure_session(
        &self,
        context: &SessionContext,
    ) -> Result<NativeSession, ProviderError> {
        let installation = self.installation().await?;
        let native_session_id = self.create_or_resume_thread(context).await?;
        Ok(NativeSession {
            id: ProviderSessionId::new(),
            provider: ProviderKind::Codex,
            native_session_id,
            native_version: Some(installation.version),
            capabilities: codex_capabilities(),
        })
    }

    async fn restore_session(
        &self,
        context: &SessionContext,
        persisted: NativeSession,
    ) -> Result<NativeSession, ProviderError> {
        validate_session(&persisted)?;
        self.attach_native_session(
            context.unified_session_id,
            persisted.native_session_id.clone(),
        )
        .await;
        let resumed_id = self.create_or_resume_thread(context).await?;
        if resumed_id != persisted.native_session_id {
            return Err(ProviderError::Protocol(format!(
                "Codex resumed unexpected thread {resumed_id}"
            )));
        }
        Ok(persisted)
    }

    async fn attach_existing_session(
        &self,
        context: &SessionContext,
        native_session_id: &str,
    ) -> Result<NativeSession, ProviderError> {
        validate_native_session_id(native_session_id)?;
        self.attach_native_session(context.unified_session_id, native_session_id)
            .await;
        // `ensure_session` performs the official app-server `thread/resume`
        // request and verifies that the returned thread id is unchanged.
        self.ensure_session(context).await
    }

    async fn read_native_history(
        &self,
        session: &NativeSession,
    ) -> Result<NativeTranscript, ProviderError> {
        validate_session(session)?;
        self.require_installed_client_method("thread/read").await?;
        let client = self.client().await?;
        let response = client
            .request(
                "thread/read",
                json!({
                    "threadId": session.native_session_id,
                    "includeTurns": true,
                }),
            )
            .await?;
        parse_native_transcript(&session.native_session_id, &response.result)
    }

    async fn sync_context(
        &self,
        session: &NativeSession,
        batch: SyncBatch,
    ) -> Result<SyncReceipt, ProviderError> {
        validate_session(session)?;
        let key = (
            session.native_session_id.clone(),
            batch.through_seq_inclusive,
            batch.projection_version,
        );
        if self.applied_syncs.lock().await.contains(&key) {
            return Ok(SyncReceipt {
                through_seq: batch.through_seq_inclusive,
                projection_version: batch.projection_version,
                native_receipt: Some("codex:idempotent-local-hit".to_owned()),
            });
        }

        let items = projection_items(&batch);
        let native_receipt = if items.is_empty() {
            None
        } else {
            let client = self.client().await?;
            self.ensure_thread_loaded(&client, session, None).await?;
            let response = client
                .request(
                    "thread/inject_items",
                    json!({
                        "threadId": session.native_session_id,
                        "items": items
                    }),
                )
                .await?;
            Some(format!("codex:rpc:{}", response.request_id))
        };
        self.applied_syncs.lock().await.insert(key);
        Ok(SyncReceipt {
            through_seq: batch.through_seq_inclusive,
            projection_version: batch.projection_version,
            native_receipt,
        })
    }

    async fn run_turn(
        &self,
        session: &NativeSession,
        request: TurnRequest,
    ) -> Result<ProviderEventStream, ProviderError> {
        validate_session(session)?;
        let client = self.client().await?;
        self.ensure_thread_loaded(&client, session, Some(&request.cwd))
            .await?;
        let mut inbound = client.subscribe();
        let response = client
            .request("turn/start", turn_start_params(session, &request))
            .await?;
        let native_turn_id = response
            .result
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .ok_or_else(|| ProviderError::Protocol("turn/start omitted turn.id".to_owned()))?
            .to_owned();
        let start_response_raw = json!({
            "id": response.request_id,
            "result": response.result
        });
        let thread_id = session.native_session_id.clone();
        self.active_turns
            .lock()
            .await
            .insert(thread_id.clone(), native_turn_id.clone());
        let approvals = Arc::clone(&self.pending_approvals);
        let active_turns = Arc::clone(&self.active_turns);
        let (sender, receiver) = mpsc::channel(self.config.channel_capacity.max(1));

        tokio::spawn(async move {
            if sender
                .send(Ok(AgentEvent::ProviderSpecific {
                    provider: ProviderKind::Codex,
                    kind: "raw:turn/start:response".to_owned(),
                    payload: start_response_raw,
                }))
                .await
                .is_err()
            {
                active_turns.lock().await.remove(&thread_id);
                return;
            }
            loop {
                match inbound.recv().await {
                    Ok(RpcInbound::Notification { method, params }) => {
                        if !notification_matches(&params, &thread_id, &native_turn_id) {
                            continue;
                        }
                        let done = method == "turn/completed";
                        let terminal_error =
                            done.then(|| classify_terminal_turn(&params)).flatten();
                        for event in map_notification(&method, &params) {
                            if sender.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                        if done {
                            active_turns.lock().await.remove(&thread_id);
                            if let Some(error) = terminal_error {
                                let _ = sender.send(Err(error)).await;
                            }
                            return;
                        }
                    }
                    Ok(RpcInbound::Request { id, method, params }) => {
                        if !notification_matches(&params, &thread_id, &native_turn_id) {
                            continue;
                        }
                        let raw = AgentEvent::ProviderSpecific {
                            provider: ProviderKind::Codex,
                            kind: format!("raw:{method}"),
                            payload: json!({
                                "id": id,
                                "method": method,
                                "params": params
                            }),
                        };
                        if sender.send(Ok(raw)).await.is_err() {
                            return;
                        }
                        if is_approval_method(&method) {
                            let approval_id = ApprovalId::new();
                            let approval =
                                approval_request(approval_id, &method, &params, &request);
                            approvals.lock().await.insert(
                                approval_id,
                                PendingApproval {
                                    client: Some(Arc::clone(&client)),
                                    rpc_id: id,
                                    method,
                                    params,
                                    native_turn_id: native_turn_id.clone(),
                                },
                            );
                            if sender
                                .send(Ok(AgentEvent::ApprovalRequested { request: approval }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        } else {
                            let event = AgentEvent::ProviderSpecific {
                                provider: ProviderKind::Codex,
                                kind: method,
                                payload: params,
                            };
                            if sender.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(RpcInbound::Malformed { raw, error }) => {
                        if sender
                            .send(Ok(AgentEvent::ProviderSpecific {
                                provider: ProviderKind::Codex,
                                kind: "raw:malformed_json".to_owned(),
                                payload: json!({"raw": raw, "parse_error": error}),
                            }))
                            .await
                            .is_err()
                        {
                            active_turns.lock().await.remove(&thread_id);
                            return;
                        }
                        let _ = sender
                            .send(Err(ProviderError::Protocol(
                                "Codex emitted malformed JSONL".to_owned(),
                            )))
                            .await;
                        active_turns.lock().await.remove(&thread_id);
                        return;
                    }
                    Ok(RpcInbound::TransportError { message }) => {
                        let _ = sender.send(Err(ProviderError::Process(message))).await;
                        active_turns.lock().await.remove(&thread_id);
                        return;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let _ = sender
                            .send(Err(ProviderError::Protocol(format!(
                                "Codex event consumer lagged by {skipped} frames"
                            ))))
                            .await;
                        active_turns.lock().await.remove(&thread_id);
                        return;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        let _ = sender
                            .send(Err(ProviderError::Process(
                                "Codex app-server event stream closed".to_owned(),
                            )))
                            .await;
                        active_turns.lock().await.remove(&thread_id);
                        return;
                    }
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver).map(|result| result)))
    }

    async fn interrupt(
        &self,
        session: &NativeSession,
        native_turn_id: &str,
    ) -> Result<(), ProviderError> {
        validate_session(session)?;
        let native_turn_id = self
            .active_turns
            .lock()
            .await
            .get(&session.native_session_id)
            .cloned()
            .unwrap_or_else(|| native_turn_id.to_owned());
        self.client()
            .await?
            .request(
                "turn/interrupt",
                json!({
                    "threadId": session.native_session_id,
                    "turnId": native_turn_id
                }),
            )
            .await?;
        self.active_turns
            .lock()
            .await
            .remove(&session.native_session_id);
        Ok(())
    }

    async fn respond_approval(
        &self,
        session: &NativeSession,
        approval_id: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), ProviderError> {
        validate_session(session)?;
        self.answer_approval(approval_id, decision).await
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        self.pending_approvals.lock().await.clear();
        self.active_turns.lock().await.clear();
        if let Some(client) = self.connection.lock().await.take() {
            client.shutdown().await?;
        }
        Ok(())
    }
}

impl ThreadSnapshotAccumulator {
    fn accept_page(
        &mut self,
        response: &Value,
        expected_cwd: &Path,
    ) -> Result<Option<String>, ProviderError> {
        self.pages = self.pages.saturating_add(1);
        if self.pages > MAX_THREAD_SNAPSHOT_PAGES {
            return Err(ProviderError::Incompatible(format!(
                "thread/list exceeded the bounded {MAX_THREAD_SNAPSHOT_PAGES}-page snapshot"
            )));
        }
        let data = response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ProviderError::Protocol("thread/list response omitted data".to_owned())
            })?;
        if self.threads.len().saturating_add(data.len()) > MAX_INTERACTIVE_THREAD_SNAPSHOT {
            return Err(ProviderError::Incompatible(format!(
                "thread/list exceeded the bounded {MAX_INTERACTIVE_THREAD_SNAPSHOT}-thread snapshot"
            )));
        }
        for thread in data {
            let thread_id = required_thread_list_string(thread, "id")?;
            validate_native_session_id(thread_id)?;
            if !self.seen_threads.insert(thread_id.to_owned()) {
                return Err(ProviderError::Protocol(format!(
                    "thread/list repeated thread id {thread_id}"
                )));
            }
            let session_id = required_thread_list_string(thread, "sessionId")?;
            validate_native_session_id(session_id)?;
            let returned_cwd = required_thread_list_string(thread, "cwd")?;
            let returned_cwd = std::fs::canonicalize(returned_cwd).map_err(|error| {
                ProviderError::Incompatible(format!(
                    "thread/list returned unavailable cwd {returned_cwd}: {error}"
                ))
            })?;
            if returned_cwd != expected_cwd {
                return Err(ProviderError::Protocol(format!(
                    "thread/list cwd filter returned unexpected workspace {}",
                    returned_cwd.display()
                )));
            }
            let forked_from_id = match thread.get("forkedFromId") {
                None | Some(Value::Null) => None,
                Some(Value::String(value)) => {
                    validate_native_session_id(value)?;
                    Some(value.clone())
                }
                Some(_) => {
                    return Err(ProviderError::Protocol(format!(
                        "thread/list thread {thread_id} has invalid forkedFromId"
                    )));
                }
            };
            let updated_at = thread
                .get("updatedAt")
                .and_then(Value::as_i64)
                .filter(|value| *value >= 0)
                .ok_or_else(|| {
                    ProviderError::Protocol(format!(
                        "thread/list thread {thread_id} has invalid updatedAt"
                    ))
                })?;
            self.threads.push(InteractiveThreadIdentity {
                thread_id: thread_id.to_owned(),
                session_id: session_id.to_owned(),
                forked_from_id,
                updated_at,
            });
        }
        match response.get("nextCursor") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(cursor))
                if !cursor.is_empty()
                    && cursor.len() <= 4096
                    && !cursor.chars().any(char::is_control) =>
            {
                if !self.seen_cursors.insert(cursor.clone()) {
                    return Err(ProviderError::Protocol(
                        "thread/list repeated a pagination cursor".to_owned(),
                    ));
                }
                if self.threads.len() >= MAX_INTERACTIVE_THREAD_SNAPSHOT {
                    return Err(ProviderError::Incompatible(format!(
                        "thread/list has more than the bounded {MAX_INTERACTIVE_THREAD_SNAPSHOT} threads"
                    )));
                }
                Ok(Some(cursor.clone()))
            }
            Some(_) => Err(ProviderError::Protocol(
                "thread/list returned an invalid nextCursor".to_owned(),
            )),
        }
    }

    fn finish(mut self, cwd: PathBuf) -> InteractiveThreadSnapshot {
        self.threads.sort();
        InteractiveThreadSnapshot {
            cwd,
            threads: self.threads,
        }
    }
}

fn required_thread_list_string<'a>(
    thread: &'a Value,
    field: &str,
) -> Result<&'a str, ProviderError> {
    thread
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 4096)
        .ok_or_else(|| ProviderError::Protocol(format!("thread/list thread omitted valid {field}")))
}

fn validate_session(session: &NativeSession) -> Result<(), ProviderError> {
    if session.provider == ProviderKind::Codex {
        validate_native_session_id(&session.native_session_id)
    } else {
        Err(ProviderError::Protocol(format!(
            "Codex adapter received a {} session",
            session.provider
        )))
    }
}

fn validate_native_session_id(native_session_id: &str) -> Result<(), ProviderError> {
    if native_session_id.is_empty()
        || native_session_id.len() > 512
        || native_session_id.trim() != native_session_id
        || native_session_id.chars().any(char::is_control)
    {
        return Err(ProviderError::Incompatible(
            "invalid Codex thread id".to_owned(),
        ));
    }
    Ok(())
}

fn turn_start_params(session: &NativeSession, request: &TurnRequest) -> Value {
    let mut params = json!({
        "threadId": session.native_session_id,
        "input": [{"type": "text", "text": request.prompt}],
        "cwd": request.cwd,
        "clientUserMessageId": request.turn_id.to_string()
    });
    if request.execution_mode == TurnExecutionMode::ReviewReadOnly {
        params["sandboxPolicy"] = json!({
            "type": "readOnly",
            "networkAccess": false
        });
        params["approvalPolicy"] = Value::String("never".to_owned());
    }
    params
}

fn extract_thread_id(value: &Value) -> Option<String> {
    value
        .pointer("/thread/id")
        .or_else(|| value.get("threadId"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn resumed_thread_id(requested: &str, result: &Value) -> Result<String, ProviderError> {
    let resumed = extract_thread_id(result)
        .ok_or_else(|| ProviderError::Protocol("thread/resume omitted the thread id".to_owned()))?;
    if resumed != requested {
        return Err(ProviderError::Protocol(format!(
            "thread/resume returned unexpected thread {resumed}; requested {requested}"
        )));
    }
    Ok(resumed)
}

fn json_contains_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, expected)),
        Value::Object(values) => values
            .values()
            .any(|value| json_contains_string(value, expected)),
        _ => false,
    }
}

fn codex_capabilities() -> BTreeMap<String, bool> {
    [
        "app_server",
        "streaming",
        "resume",
        "context_injection",
        "assistant_role_injection",
        "interrupt",
        "approvals",
        "rate_limits",
        "raw_events",
    ]
    .into_iter()
    .map(|capability| (capability.to_owned(), true))
    .collect()
}

#[allow(clippy::too_many_lines)]
fn parse_native_transcript(
    native_session_id: &str,
    response: &Value,
) -> Result<NativeTranscript, ProviderError> {
    let thread = response
        .get("thread")
        .ok_or_else(|| ProviderError::Protocol("thread/read response omitted thread".to_owned()))?;
    let returned_id = thread.get("id").and_then(Value::as_str).ok_or_else(|| {
        ProviderError::Protocol("thread/read response omitted thread.id".to_owned())
    })?;
    if returned_id != native_session_id {
        return Err(ProviderError::Protocol(format!(
            "thread/read returned unexpected thread {returned_id}"
        )));
    }
    let workspace_cwd = thread
        .get("cwd")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| {
            ProviderError::Protocol("thread/read response omitted thread.cwd".to_owned())
        })?;
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ProviderError::Protocol(
                "thread/read did not include turns; installed protocol is incompatible".to_owned(),
            )
        })?;
    let mut imported = Vec::with_capacity(turns.len());
    for turn in turns {
        let native_turn_id = turn
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ProviderError::Protocol("native turn omitted id".to_owned()))?
            .to_owned();
        let status = match turn.get("status").and_then(Value::as_str) {
            Some("completed") => TurnStatus::Completed,
            Some("interrupted") => TurnStatus::Interrupted,
            Some("failed") => TurnStatus::Failed,
            Some("inProgress") => TurnStatus::Running,
            Some(other) => {
                return Err(ProviderError::Protocol(format!(
                    "unsupported native turn status {other}"
                )));
            }
            None => {
                return Err(ProviderError::Protocol(
                    "native turn omitted status".to_owned(),
                ));
            }
        };
        match turn.get("itemsView").and_then(Value::as_str) {
            None | Some("full") => {}
            Some("notLoaded" | "summary") => {
                return Err(ProviderError::Incompatible(format!(
                    "native turn {native_turn_id} returned a partial items view"
                )));
            }
            Some(other) => {
                return Err(ProviderError::Protocol(format!(
                    "native turn {native_turn_id} returned unsupported itemsView {other}"
                )));
            }
        }
        let items = turn
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| ProviderError::Protocol("native turn omitted items".to_owned()))?;
        let mut normalized = Vec::new();
        let mut native_item_ids = HashSet::new();
        for item in items {
            let native_item_id = item
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| ProviderError::Protocol("native item omitted id".to_owned()))?
                .to_owned();
            if !native_item_ids.insert(native_item_id.clone()) {
                return Err(ProviderError::Protocol(format!(
                    "native turn {native_turn_id} repeated item id {native_item_id}"
                )));
            }
            match item.get("type").and_then(Value::as_str) {
                Some("userMessage") => {
                    let (text, attachments, raw) =
                        parse_native_user_message(item, &native_item_id)?;
                    normalized.push(NativeTranscriptItem::UserPrompt {
                        native_item_id,
                        text,
                        attachments,
                        raw,
                    });
                }
                Some("agentMessage") => {
                    let text = item.get("text").and_then(Value::as_str).ok_or_else(|| {
                        ProviderError::Incompatible(format!(
                            "native agentMessage {native_item_id} omitted string text"
                        ))
                    })?;
                    normalized.push(NativeTranscriptItem::AssistantMessage {
                        native_item_id,
                        text: text.to_owned(),
                        final_answer: item.get("phase").and_then(Value::as_str)
                            == Some("final_answer"),
                        raw: json!({
                            "type": "agentMessage",
                            "text": text,
                            "phase": item.get("phase").cloned().unwrap_or(Value::Null),
                        }),
                    });
                }
                Some("plan") => {
                    let text = item.get("text").and_then(Value::as_str).ok_or_else(|| {
                        ProviderError::Incompatible(format!(
                            "native plan {native_item_id} omitted string text"
                        ))
                    })?;
                    normalized.push(NativeTranscriptItem::Plan {
                        native_item_id,
                        text: text.to_owned(),
                        raw: json!({"type": "plan", "text": text}),
                    });
                }
                Some("commandExecution") => {
                    let command = item
                        .get("command")
                        .and_then(Value::as_str)
                        .filter(|command| !command.is_empty())
                        .ok_or_else(|| {
                            ProviderError::Incompatible(format!(
                                "native commandExecution {native_item_id} omitted a non-empty command"
                            ))
                        })?;
                    let status = parse_native_effect_status(item, "commandExecution")?;
                    let cwd = item
                        .get("cwd")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    let exit_code = item
                        .get("exitCode")
                        .and_then(Value::as_i64)
                        .and_then(|value| i32::try_from(value).ok());
                    normalized.push(NativeTranscriptItem::Command {
                        native_item_id,
                        command: command.to_owned(),
                        cwd: cwd.clone(),
                        exit_code,
                        status,
                        // Aggregated stdout/stderr is intentionally omitted.
                        raw: json!({
                            "type": "commandExecution",
                            "command": command,
                            "cwd": cwd,
                            "exitCode": exit_code,
                            "status": status,
                        }),
                    });
                }
                Some("fileChange") => {
                    let status = parse_native_effect_status(item, "fileChange")?;
                    let changes = item
                        .get("changes")
                        .and_then(Value::as_array)
                        .filter(|changes| !changes.is_empty())
                        .ok_or_else(|| {
                            ProviderError::Incompatible(format!(
                                "native fileChange {native_item_id} omitted a non-empty changes array"
                            ))
                        })?;
                    let paths = changes
                        .iter()
                        .map(|change| {
                            change
                                .get("path")
                                .and_then(Value::as_str)
                                .filter(|path| !path.is_empty() && !path.contains('\0'))
                                .map(ToOwned::to_owned)
                                .ok_or_else(|| {
                                    ProviderError::Incompatible(format!(
                                        "native fileChange {native_item_id} contains a change without a valid path"
                                    ))
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    normalized.push(NativeTranscriptItem::FilesChanged {
                        native_item_id,
                        paths: paths.clone(),
                        status,
                        // Diffs can be huge and the current workspace is authoritative.
                        raw: json!({
                            "type": "fileChange",
                            "paths": paths,
                            "status": status,
                        }),
                    });
                }
                Some("mcpToolCall") => {
                    normalized.push(parse_mcp_tool_call(item, native_item_id)?);
                }
                Some("dynamicToolCall") => {
                    normalized.push(parse_dynamic_tool_call(item, native_item_id)?);
                }
                Some("webSearch") => {
                    normalized.push(parse_web_search(item, native_item_id)?);
                }
                Some("imageGeneration") => {
                    normalized.push(parse_image_generation(item, native_item_id)?);
                }
                Some("imageView") => {
                    normalized.push(parse_image_view(item, native_item_id)?);
                }
                Some("sleep") => {
                    let duration =
                        item.get("durationMs")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| {
                                ProviderError::Incompatible(format!(
                                    "native sleep {native_item_id} omitted durationMs"
                                ))
                            })?;
                    normalized.push(native_tool_call(
                        native_item_id,
                        "sleep".to_owned(),
                        &json!({"durationMs": duration}),
                        NativeEffectStatus::Completed,
                        None,
                        Vec::new(),
                        false,
                        "sleep",
                    )?);
                }
                Some("collabAgentToolCall") => {
                    normalized.push(parse_collab_tool_call(item, native_item_id)?);
                }
                Some("subAgentActivity") => {
                    normalized.push(parse_subagent_activity(item, native_item_id)?);
                }
                Some("hookPrompt") => {
                    normalized.push(parse_hook_prompt(item, native_item_id)?);
                }
                Some("enteredReviewMode" | "exitedReviewMode") => {
                    normalized.push(parse_review_marker(item, native_item_id)?);
                }
                Some("contextCompaction") => {
                    normalized.push(NativeTranscriptItem::ContextMarker {
                        native_item_id,
                        marker_kind: "context_compaction".to_owned(),
                        summary: "Codex compacted its native context.".to_owned(),
                        content_digest: None,
                        raw: json!({"type": "contextCompaction"}),
                    });
                }
                // Private reasoning is a known provider item but is deliberately
                // excluded from the canonical public transcript.
                Some("reasoning") => {}
                Some(item_type) => {
                    return Err(ProviderError::Incompatible(format!(
                        "native item {native_item_id} has unsupported type {item_type}; refusing to import an incomplete canonical transcript"
                    )));
                }
                None => {
                    return Err(ProviderError::Protocol(format!(
                        "native item {native_item_id} omitted type"
                    )));
                }
            }
        }
        if status == TurnStatus::Completed
            && !normalized.iter().any(|item| {
                matches!(
                    item,
                    NativeTranscriptItem::AssistantMessage {
                        final_answer: true,
                        ..
                    }
                )
            })
            && let Some(NativeTranscriptItem::AssistantMessage { final_answer, .. }) = normalized
                .iter_mut()
                .rev()
                .find(|item| matches!(item, NativeTranscriptItem::AssistantMessage { .. }))
        {
            *final_answer = true;
        }
        imported.push(NativeTranscriptTurn {
            native_turn_id,
            status,
            items: normalized,
        });
    }
    Ok(NativeTranscript {
        provider: ProviderKind::Codex,
        native_session_id: native_session_id.to_owned(),
        workspace_cwd,
        turns: imported,
    })
}

fn parse_native_user_message(
    item: &Value,
    native_item_id: &str,
) -> Result<(String, Vec<NativeAttachment>, Value), ProviderError> {
    let content = item
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ProviderError::Incompatible(format!(
                "native userMessage {native_item_id} omitted a content array"
            ))
        })?;
    if content.is_empty() {
        return Err(ProviderError::Incompatible(format!(
            "native userMessage {native_item_id} has empty content"
        )));
    }
    let mut text = Vec::new();
    let mut attachments = Vec::new();
    for (index, part) in content.iter().enumerate() {
        match part.get("type").and_then(Value::as_str) {
            Some("text") => text.push(
                part.get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ProviderError::Incompatible(format!(
                            "native userMessage {native_item_id} contains text content without a string text field"
                        ))
                    })?
                    .to_owned(),
            ),
            Some("image") => {
                let url = required_public_string(part, "url", native_item_id, "user image")?;
                attachments.push(attachment_from_source(
                    "remote_image",
                    None,
                    image_media_type(url),
                    url,
                    json!({
                        "content_index": index,
                        "detail": part.get("detail").cloned().unwrap_or(Value::Null),
                        "source_bytes": url.len(),
                    }),
                )?);
            }
            Some("localImage") => {
                let path = required_public_string(part, "path", native_item_id, "local image")?;
                validate_public_path(path, native_item_id, "local image")?;
                attachments.push(attachment_from_source(
                    "local_image",
                    file_name(path),
                    None,
                    path,
                    json!({
                        "content_index": index,
                        "detail": part.get("detail").cloned().unwrap_or(Value::Null),
                    }),
                )?);
            }
            Some(kind @ ("skill" | "mention")) => {
                let path = required_public_string(part, "path", native_item_id, kind)?;
                validate_public_path(path, native_item_id, kind)?;
                let name = required_public_string(part, "name", native_item_id, kind)?;
                attachments.push(attachment_from_source(
                    kind,
                    Some(name.to_owned()),
                    None,
                    path,
                    json!({"content_index": index}),
                )?);
            }
            Some(other) => {
                return Err(ProviderError::Incompatible(format!(
                    "native userMessage {native_item_id} has unsupported content type {other}"
                )));
            }
            None => {
                return Err(ProviderError::Incompatible(format!(
                    "native userMessage {native_item_id} contains content without type"
                )));
            }
        }
    }
    let text = text.join("\n");
    let raw = json!({
        "type": "userMessage",
        "text": text,
        "attachments": attachments,
    });
    Ok((text, attachments, raw))
}

fn parse_mcp_tool_call(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let server = required_public_string(item, "server", &native_item_id, "mcpToolCall")?;
    let tool = required_public_string(item, "tool", &native_item_id, "mcpToolCall")?;
    let arguments = item.get("arguments").ok_or_else(|| {
        ProviderError::Incompatible(format!(
            "native mcpToolCall {native_item_id} omitted arguments"
        ))
    })?;
    let status = parse_native_effect_status(item, "mcpToolCall")?;
    let result = item.get("result").cloned().unwrap_or(Value::Null);
    let error = item.get("error").cloned().unwrap_or(Value::Null);
    let output_digest = digest_non_null(&json!({"result": result, "error": error}))?;
    let artifacts = extract_mcp_artifacts(&result)?;
    native_tool_call(
        native_item_id,
        format!("mcp.{server}.{tool}"),
        arguments,
        status,
        output_digest,
        artifacts,
        true,
        "mcpToolCall",
    )
}

fn parse_dynamic_tool_call(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let tool = required_public_string(item, "tool", &native_item_id, "dynamicToolCall")?;
    let arguments = item.get("arguments").ok_or_else(|| {
        ProviderError::Incompatible(format!(
            "native dynamicToolCall {native_item_id} omitted arguments"
        ))
    })?;
    let status = parse_native_effect_status(item, "dynamicToolCall")?;
    let namespace = optional_public_string(item, "namespace", &native_item_id, "dynamicToolCall")?;
    let name = namespace.map_or_else(
        || format!("dynamic.{tool}"),
        |namespace| format!("dynamic.{namespace}.{tool}"),
    );
    let content = item.get("contentItems").cloned().unwrap_or(Value::Null);
    let success = item.get("success").cloned().unwrap_or(Value::Null);
    let output_digest = digest_non_null(&json!({"contentItems": content, "success": success}))?;
    let artifacts = extract_dynamic_artifacts(&content, &native_item_id)?;
    native_tool_call(
        native_item_id,
        name,
        arguments,
        status,
        output_digest,
        artifacts,
        true,
        "dynamicToolCall",
    )
}

fn parse_web_search(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let query = required_public_string(item, "query", &native_item_id, "webSearch")?;
    let input = json!({
        "query": query,
        "action": item.get("action").cloned().unwrap_or(Value::Null),
    });
    native_tool_call(
        native_item_id,
        "web.search".to_owned(),
        &input,
        NativeEffectStatus::Completed,
        None,
        Vec::new(),
        false,
        "webSearch",
    )
}

fn parse_image_generation(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let status = parse_native_effect_status(item, "imageGeneration")?;
    let result = required_public_string(item, "result", &native_item_id, "imageGeneration")?;
    let revised_prompt =
        optional_public_string(item, "revisedPrompt", &native_item_id, "imageGeneration")?;
    let saved_path = optional_public_string(item, "savedPath", &native_item_id, "imageGeneration")?;
    if let Some(path) = saved_path {
        validate_public_path(path, &native_item_id, "imageGeneration")?;
    }
    let mut artifacts = vec![attachment_from_source(
        "generated_image_result",
        None,
        image_media_type(result),
        result,
        json!({"source_bytes": result.len()}),
    )?];
    if let Some(path) = saved_path {
        artifacts.push(attachment_from_source(
            "generated_image_file",
            file_name(path),
            None,
            path,
            Value::Null,
        )?);
    }
    native_tool_call(
        native_item_id,
        "image.generate".to_owned(),
        &json!({"revisedPrompt": revised_prompt}),
        status,
        Some(digest_text(result)),
        artifacts,
        true,
        "imageGeneration",
    )
}

fn parse_image_view(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let path = required_public_string(item, "path", &native_item_id, "imageView")?;
    validate_public_path(path, &native_item_id, "imageView")?;
    native_tool_call(
        native_item_id,
        "image.view".to_owned(),
        &json!({"path": path}),
        NativeEffectStatus::Completed,
        None,
        vec![attachment_from_source(
            "viewed_image",
            file_name(path),
            None,
            path,
            Value::Null,
        )?],
        false,
        "imageView",
    )
}

fn parse_collab_tool_call(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let tool = required_public_string(item, "tool", &native_item_id, "collabAgentToolCall")?;
    let status = parse_native_effect_status(item, "collabAgentToolCall")?;
    let receivers = item
        .get("receiverThreadIds")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ProviderError::Incompatible(format!(
                "native collabAgentToolCall {native_item_id} omitted receiverThreadIds"
            ))
        })?;
    if receivers.iter().any(|receiver| receiver.as_str().is_none()) {
        return Err(ProviderError::Incompatible(format!(
            "native collabAgentToolCall {native_item_id} has invalid receiverThreadIds"
        )));
    }
    let sender = required_public_string(
        item,
        "senderThreadId",
        &native_item_id,
        "collabAgentToolCall",
    )?;
    let agents = item
        .get("agentsStates")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            ProviderError::Incompatible(format!(
                "native collabAgentToolCall {native_item_id} omitted agentsStates"
            ))
        })?;
    let input = json!({
        "prompt": item.get("prompt").cloned().unwrap_or(Value::Null),
        "model": item.get("model").cloned().unwrap_or(Value::Null),
        "receiverThreadIds": receivers,
        "senderThreadId": sender,
    });
    let output_digest = Some(digest_value(&Value::Object(agents.clone()))?);
    native_tool_call(
        native_item_id,
        format!("collab.{tool}"),
        &input,
        status,
        output_digest,
        Vec::new(),
        true,
        "collabAgentToolCall",
    )
}

fn parse_subagent_activity(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let kind = required_public_string(item, "kind", &native_item_id, "subAgentActivity")?;
    let agent_path =
        required_public_string(item, "agentPath", &native_item_id, "subAgentActivity")?;
    let agent_thread_id =
        required_public_string(item, "agentThreadId", &native_item_id, "subAgentActivity")?;
    let public = json!({
        "kind": kind,
        "agentPath": agent_path,
        "agentThreadId": agent_thread_id,
    });
    let content_digest = digest_value(&public)?;
    Ok(NativeTranscriptItem::ContextMarker {
        native_item_id,
        marker_kind: "subagent_activity".to_owned(),
        summary: format!("Native subagent activity: {kind}."),
        content_digest: Some(content_digest.clone()),
        raw: json!({
            "type": "subAgentActivity",
            "kind": kind,
            "content_digest": content_digest,
        }),
    })
}

fn parse_hook_prompt(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let fragments = item
        .get("fragments")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ProviderError::Incompatible(format!(
                "native hookPrompt {native_item_id} omitted fragments"
            ))
        })?;
    for fragment in fragments {
        required_public_string(fragment, "hookRunId", &native_item_id, "hookPrompt")?;
        required_public_string(fragment, "text", &native_item_id, "hookPrompt")?;
    }
    let content_digest = digest_value(&Value::Array(fragments.clone()))?;
    Ok(NativeTranscriptItem::ContextMarker {
        native_item_id,
        marker_kind: "hook_prompt".to_owned(),
        summary: format!(
            "Native hooks supplied {} prompt fragment(s); content omitted.",
            fragments.len()
        ),
        content_digest: Some(content_digest.clone()),
        raw: json!({
            "type": "hookPrompt",
            "fragment_count": fragments.len(),
            "content_digest": content_digest,
        }),
    })
}

fn parse_review_marker(
    item: &Value,
    native_item_id: String,
) -> Result<NativeTranscriptItem, ProviderError> {
    let item_type = required_public_string(item, "type", &native_item_id, "review marker")?;
    let review = required_public_string(item, "review", &native_item_id, item_type)?;
    let content_digest = digest_text(review);
    Ok(NativeTranscriptItem::ContextMarker {
        native_item_id,
        marker_kind: item_type.to_owned(),
        summary: review.to_owned(),
        content_digest: Some(content_digest.clone()),
        raw: json!({
            "type": item_type,
            "review": review,
            "content_digest": content_digest,
        }),
    })
}

#[allow(clippy::too_many_arguments)]
fn native_tool_call(
    native_item_id: String,
    name: String,
    input: &Value,
    status: NativeEffectStatus,
    output_digest: Option<String>,
    artifacts: Vec<NativeAttachment>,
    may_have_side_effects: bool,
    provider_type: &str,
) -> Result<NativeTranscriptItem, ProviderError> {
    let input_summary = summarize_value(input)?;
    let raw = json!({
        "type": provider_type,
        "name": name,
        "input_summary": input_summary,
        "status": status,
        "output_digest": output_digest,
        "artifacts": artifacts,
        "may_have_side_effects": may_have_side_effects,
    });
    Ok(NativeTranscriptItem::ToolCall {
        native_item_id,
        name,
        input_summary,
        status,
        output_digest,
        artifacts,
        may_have_side_effects,
        raw,
    })
}

fn summarize_value(value: &Value) -> Result<Value, ProviderError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        ProviderError::Protocol(format!("failed to summarize native tool input: {error}"))
    })?;
    let (shape, keys, item_count) = match value {
        Value::Object(object) => {
            let mut keys = object.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            let item_count = keys.len();
            keys.truncate(32);
            ("object", keys, item_count)
        }
        Value::Array(items) => ("array", Vec::new(), items.len()),
        Value::String(_) => ("string", Vec::new(), 1),
        Value::Number(_) => ("number", Vec::new(), 1),
        Value::Bool(_) => ("boolean", Vec::new(), 1),
        Value::Null => ("null", Vec::new(), 0),
    };
    Ok(json!({
        "shape": shape,
        "keys": keys,
        "item_count": item_count,
        "size_bytes": bytes.len(),
        "digest": digest_bytes(&bytes),
    }))
}

fn extract_mcp_artifacts(result: &Value) -> Result<Vec<NativeAttachment>, ProviderError> {
    let Some(content) = result.get("content").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut artifacts = Vec::new();
    for item in content {
        if item.get("type").and_then(Value::as_str) != Some("image") {
            continue;
        }
        let source = item
            .get("data")
            .or_else(|| item.get("url"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderError::Incompatible(
                    "native mcpToolCall returned image content without data or url".to_owned(),
                )
            })?;
        let media_type = item
            .get("mimeType")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| image_media_type(source));
        artifacts.push(attachment_from_source(
            "mcp_image",
            None,
            media_type,
            source,
            json!({"source_bytes": source.len()}),
        )?);
    }
    Ok(artifacts)
}

fn extract_dynamic_artifacts(
    content: &Value,
    native_item_id: &str,
) -> Result<Vec<NativeAttachment>, ProviderError> {
    if content.is_null() {
        return Ok(Vec::new());
    }
    let items = content.as_array().ok_or_else(|| {
        ProviderError::Incompatible(format!(
            "native dynamicToolCall {native_item_id} returned non-array contentItems"
        ))
    })?;
    let mut artifacts = Vec::new();
    for item in items {
        match item.get("type").and_then(Value::as_str) {
            Some("inputText") => {
                required_public_string(item, "text", native_item_id, "dynamicToolCall output")?;
            }
            Some("inputImage") => {
                let url = required_public_string(
                    item,
                    "imageUrl",
                    native_item_id,
                    "dynamicToolCall image output",
                )?;
                artifacts.push(attachment_from_source(
                    "dynamic_tool_image",
                    None,
                    image_media_type(url),
                    url,
                    json!({"source_bytes": url.len()}),
                )?);
            }
            Some(other) => {
                return Err(ProviderError::Incompatible(format!(
                    "native dynamicToolCall {native_item_id} returned unsupported content item {other}"
                )));
            }
            None => {
                return Err(ProviderError::Incompatible(format!(
                    "native dynamicToolCall {native_item_id} returned content without type"
                )));
            }
        }
    }
    Ok(artifacts)
}

fn attachment_from_source(
    kind: &str,
    name: Option<String>,
    media_type: Option<String>,
    source: &str,
    metadata: Value,
) -> Result<NativeAttachment, ProviderError> {
    if source.is_empty() {
        return Err(ProviderError::Incompatible(format!(
            "native {kind} attachment has an empty source"
        )));
    }
    Ok(NativeAttachment {
        kind: kind.to_owned(),
        name,
        media_type,
        source_digest: digest_text(source),
        metadata,
        omitted: true,
    })
}

fn digest_non_null(value: &Value) -> Result<Option<String>, ProviderError> {
    let has_value = value
        .as_object()
        .is_some_and(|object| object.values().any(|value| !value.is_null()));
    has_value.then(|| digest_value(value)).transpose()
}

fn digest_value(value: &Value) -> Result<String, ProviderError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        ProviderError::Protocol(format!("failed to digest native provider value: {error}"))
    })?;
    Ok(digest_bytes(&bytes))
}

fn digest_text(value: &str) -> String {
    digest_bytes(value.as_bytes())
}

fn digest_bytes(value: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(value)))
}

fn required_public_string<'a>(
    value: &'a Value,
    field: &str,
    native_item_id: &str,
    item_kind: &str,
) -> Result<&'a str, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ProviderError::Incompatible(format!(
                "native {item_kind} {native_item_id} omitted non-empty string {field}"
            ))
        })
}

fn optional_public_string<'a>(
    value: &'a Value,
    field: &str,
    native_item_id: &str,
    item_kind: &str,
) -> Result<Option<&'a str>, ProviderError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(ProviderError::Incompatible(format!(
            "native {item_kind} {native_item_id} has non-string {field}"
        ))),
    }
}

fn validate_public_path(
    path: &str,
    native_item_id: &str,
    item_kind: &str,
) -> Result<(), ProviderError> {
    if path.contains('\0') {
        return Err(ProviderError::Incompatible(format!(
            "native {item_kind} {native_item_id} contains an invalid path"
        )));
    }
    Ok(())
}

fn file_name(path: &str) -> Option<String> {
    std::path::Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
}

fn image_media_type(source: &str) -> Option<String> {
    source
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(';').map(|(media_type, _)| media_type))
        .filter(|media_type| media_type.starts_with("image/"))
        .map(ToOwned::to_owned)
}

fn parse_native_effect_status(
    item: &Value,
    item_kind: &str,
) -> Result<NativeEffectStatus, ProviderError> {
    match item.get("status").and_then(Value::as_str) {
        Some("completed") => Ok(NativeEffectStatus::Completed),
        Some("failed") => Ok(NativeEffectStatus::Failed),
        Some("declined") => Ok(NativeEffectStatus::Declined),
        Some("inProgress") => Err(ProviderError::Incompatible(format!(
            "thread/read returned non-terminal {item_kind} item"
        ))),
        Some(other) => Err(ProviderError::Protocol(format!(
            "thread/read returned unsupported {item_kind} status {other}"
        ))),
        None => Err(ProviderError::Protocol(format!(
            "thread/read {item_kind} item omitted status"
        ))),
    }
}

fn projection_items(batch: &SyncBatch) -> Vec<Value> {
    let mut items = batch
        .events
        .iter()
        .filter_map(|event| {
            let normalized = event.kind.to_ascii_lowercase();
            let (role, content_type) = if normalized.contains("user")
                && (normalized.contains("prompt") || normalized.contains("message"))
            {
                ("user", "input_text")
            } else if (normalized.contains("assistant") && normalized.contains("final"))
                || (normalized.contains("checkpoint") && batch.handoff.is_none())
            {
                ("assistant", "output_text")
            } else {
                return None;
            };
            let text = ["text", "prompt", "result", "response", "summary"]
                .into_iter()
                .find_map(|key| event.payload.get(key).and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .or_else(|| {
                    normalized
                        .contains("checkpoint")
                        .then(|| event.payload.get("checkpoint").map(Value::to_string))
                        .flatten()
                })?;
            Some(json!({
                "type": "message",
                "role": role,
                "content": [{"type": content_type, "text": text}]
            }))
        })
        .collect::<Vec<_>>();
    if let Some(handoff) = &batch.handoff
        && !handoff.is_empty()
    {
        items.push(json!({
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": handoff}]
        }));
    }
    items
}

fn new_thread_materialization_item() -> Value {
    json!({
        "type": "message",
        "role": "assistant",
        "content": [{
            "type": "output_text",
            "text": NEW_THREAD_MATERIALIZATION_MARKER
        }]
    })
}

fn notification_matches(params: &Value, thread_id: &str, turn_id: &str) -> bool {
    let event_thread = params
        .get("threadId")
        .and_then(Value::as_str)
        .or_else(|| params.pointer("/thread/id").and_then(Value::as_str));
    if event_thread.is_some_and(|value| value != thread_id) {
        return false;
    }
    let event_turn = params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| params.pointer("/turn/id").and_then(Value::as_str));
    event_turn.is_none_or(|value| value == turn_id)
}

fn classify_terminal_turn(params: &Value) -> Option<ProviderError> {
    let status = params.pointer("/turn/status").and_then(Value::as_str)?;
    if status == "completed" {
        return None;
    }
    if status == "interrupted" {
        return Some(ProviderError::Interrupted);
    }
    let error = params.pointer("/turn/error").unwrap_or(&Value::Null);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Codex turn failed")
        .to_owned();
    let details = format!(
        "{} {}",
        message,
        error
            .get("codexErrorInfo")
            .map(Value::to_string)
            .unwrap_or_default()
    )
    .to_ascii_lowercase();
    if details.contains("usagelimitexceeded") || details.contains("usage limit") {
        Some(ProviderError::RateLimited { resets_at: None })
    } else if details.contains("serveroverloaded") || details.contains("overloaded") {
        Some(ProviderError::Overloaded(message))
    } else if details.contains("unauthorized") || details.contains("authentication") {
        Some(ProviderError::Authentication(message))
    } else {
        Some(ProviderError::Process(message))
    }
}

fn is_approval_method(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "execCommandApproval"
            | "applyPatchApproval"
    )
}

fn approval_request(
    id: ApprovalId,
    method: &str,
    params: &Value,
    turn: &TurnRequest,
) -> ApprovalRequest {
    let command = params
        .get("command")
        .map(|command| {
            command.as_str().map_or_else(
                || {
                    command
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(" ")
                },
                ToOwned::to_owned,
            )
        })
        .filter(|command| !command.is_empty());
    let files: Vec<PathBuf> = params
        .get("fileChanges")
        .and_then(Value::as_object)
        .map(|changes| changes.keys().map(PathBuf::from).collect())
        .unwrap_or_default();
    let network_host = params
        .pointer("/networkApprovalContext/host")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let action = if network_host.is_some() {
        ApprovalAction::Network { host: network_host }
    } else if method.contains("command") || method == "execCommandApproval" {
        ApprovalAction::Command
    } else if method.contains("fileChange") || method == "applyPatchApproval" {
        ApprovalAction::FileChange
    } else {
        ApprovalAction::Permission {
            name: "codex_permission_profile".to_owned(),
        }
    };
    ApprovalRequest {
        id,
        provider: ProviderKind::Codex,
        session_id: turn.session_id,
        turn_id: turn.turn_id,
        action,
        risk: if command.is_some() || !files.is_empty() {
            RiskLevel::High
        } else {
            RiskLevel::Medium
        },
        cwd: params.get("cwd").and_then(Value::as_str).map(PathBuf::from),
        command,
        files,
        reason: params
            .get("reason")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    }
}

fn approval_result(pending: &PendingApproval, decision: ApprovalDecision) -> Value {
    if pending.method == "item/permissions/requestApproval" {
        return match decision {
            ApprovalDecision::AllowOnce | ApprovalDecision::AllowSession => json!({
                "permissions": pending.params.get("permissions").cloned().unwrap_or_else(|| json!({})),
                "scope": if decision == ApprovalDecision::AllowSession { "session" } else { "turn" }
            }),
            ApprovalDecision::Deny | ApprovalDecision::CancelTurn => json!({
                "permissions": {},
                "scope": "turn"
            }),
        };
    }
    let legacy = matches!(
        pending.method.as_str(),
        "execCommandApproval" | "applyPatchApproval"
    );
    let decision = if legacy {
        match decision {
            ApprovalDecision::AllowOnce => "approved",
            ApprovalDecision::AllowSession => "approved_for_session",
            ApprovalDecision::Deny => "denied",
            ApprovalDecision::CancelTurn => "abort",
        }
    } else {
        match decision {
            ApprovalDecision::AllowOnce => "accept",
            ApprovalDecision::AllowSession => "acceptForSession",
            ApprovalDecision::Deny => "decline",
            ApprovalDecision::CancelTurn => "cancel",
        }
    };
    json!({"decision": decision})
}

#[cfg(test)]
mod tests {
    use super::{
        CodexAdapter, CodexConfig, MAX_THREAD_SNAPSHOT_PAGES, NEW_THREAD_MATERIALIZATION_MARKER,
        PendingApproval, ThreadSnapshotAccumulator, approval_result, classify_terminal_turn,
        new_thread_materialization_item, parse_native_transcript, projection_items,
        resumed_thread_id, turn_start_params, validate_native_session_id,
    };
    use agentctl_core::{
        AgentProvider, ApprovalDecision, CanonicalEvent, EventId, EventVisibility,
        NativeEffectStatus, NativeSession, NativeTranscriptItem, ProviderError, ProviderKind,
        ProviderSessionId, SyncBatch, TurnExecutionMode, TurnId, TurnRequest, TurnStatus,
        UnifiedSessionId,
    };
    use chrono::Utc;
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, path::PathBuf};

    #[test]
    fn resumed_thread_id_must_match_the_requested_thread() {
        assert_eq!(
            resumed_thread_id("thr_requested", &json!({"thread": {"id": "thr_requested"}}))
                .unwrap(),
            "thr_requested"
        );

        let error = resumed_thread_id("thr_requested", &json!({"thread": {"id": "thr_other"}}))
            .unwrap_err();
        assert!(matches!(error, ProviderError::Protocol(_)));
        assert!(error.to_string().contains("requested thr_requested"));
    }

    #[tokio::test]
    async fn native_history_requires_thread_read_in_the_installed_version_schema() {
        let directory = tempfile::tempdir().unwrap();
        let schema_directory = directory.path().join("test");
        tokio::fs::create_dir(&schema_directory).await.unwrap();
        let schema_path = schema_directory.join("ClientRequest.json");
        tokio::fs::write(
            &schema_path,
            serde_json::to_vec(
                &json!({"oneOf": [{"properties": {"method": {"const": "thread/read"}}}]}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let adapter = CodexAdapter::from_config(CodexConfig {
            binary: PathBuf::from("binary-must-not-run"),
            schema_cache_root: Some(directory.path().to_path_buf()),
            ..CodexConfig::default()
        });
        *adapter.installation.write().await = Some(crate::schema::CodexInstallation {
            binary: PathBuf::from("binary-must-not-run"),
            version: "test".to_owned(),
        });
        adapter
            .require_installed_client_method("thread/read")
            .await
            .unwrap();

        tokio::fs::write(
            &schema_path,
            serde_json::to_vec(
                &json!({"oneOf": [{"properties": {"method": {"const": "thread/start"}}}]}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let error = adapter
            .require_installed_client_method("thread/read")
            .await
            .unwrap_err();
        assert!(matches!(error, ProviderError::Incompatible(_)));
    }

    #[test]
    fn projection_maps_roles_and_avoids_unrelated_events() {
        let event = CanonicalEvent {
            schema_version: 1,
            session_id: UnifiedSessionId::new(),
            seq: 1,
            event_id: EventId::new(),
            turn_id: None,
            origin_provider: None,
            kind: "assistant_final".to_owned(),
            visibility: EventVisibility::Projection,
            payload: json!({"text": "finished"}),
            content_hash: "sha256:test".to_owned(),
            raw_event_id: None,
            created_at: Utc::now(),
        };
        let items = projection_items(&SyncBatch {
            from_seq_exclusive: 0,
            through_seq_inclusive: 1,
            projection_version: 1,
            events: vec![event],
            handoff: None,
        });
        assert_eq!(items[0]["role"], "assistant");
        assert_eq!(items[0]["content"][0]["type"], "output_text");
    }

    #[test]
    fn projection_keeps_native_roles_and_appends_effect_handoff() {
        let event = CanonicalEvent {
            schema_version: 1,
            session_id: UnifiedSessionId::new(),
            seq: 1,
            event_id: EventId::new(),
            turn_id: None,
            origin_provider: None,
            kind: "assistant_final".to_owned(),
            visibility: EventVisibility::Projection,
            payload: json!({"text": "finished"}),
            content_hash: "sha256:test".to_owned(),
            raw_event_id: None,
            created_at: Utc::now(),
        };
        let items = projection_items(&SyncBatch {
            from_seq_exclusive: 0,
            through_seq_inclusive: 1,
            projection_version: 1,
            events: vec![event],
            handoff: Some("<agent-handoff/>".to_owned()),
        });
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["role"], "assistant");
        assert_eq!(items[1]["content"][0]["text"], "<agent-handoff/>");
    }

    #[test]
    fn new_thread_materialization_uses_one_neutral_assistant_item() {
        let item = new_thread_materialization_item();
        assert_eq!(item["type"], "message");
        assert_eq!(item["role"], "assistant");
        assert_eq!(item["content"].as_array().map(Vec::len), Some(1));
        assert_eq!(item["content"][0]["type"], "output_text");
        assert_eq!(
            item["content"][0]["text"],
            NEW_THREAD_MATERIALIZATION_MARKER
        );
        assert!(NEW_THREAD_MATERIALIZATION_MARKER.starts_with("<agentctl-session-marker "));
        assert!(NEW_THREAD_MATERIALIZATION_MARKER.ends_with(" />"));
    }

    #[test]
    fn native_history_import_keeps_public_context_and_drops_reasoning_and_logs() {
        let transcript = parse_native_transcript(
            "thr_1",
            &json!({
                "thread": {
                    "id": "thr_1",
                    "cwd": "/repo",
                    "turns": [{
                        "id": "turn_1",
                        "status": "completed",
                        "items": [
                            {"type": "userMessage", "id": "u", "content": [{"type": "text", "text": "fix it"}]},
                            {"type": "reasoning", "id": "r", "content": ["private chain"]},
                            {"type": "commandExecution", "id": "c", "command": "cargo test", "cwd": "/repo", "exitCode": 0, "status": "completed", "aggregatedOutput": "very large secret output"},
                            {"type": "agentMessage", "id": "a", "text": "done", "phase": "final_answer"}
                        ]
                    }]
                }
            }),
        )
        .unwrap();
        assert_eq!(transcript.turns[0].status, TurnStatus::Completed);
        assert_eq!(transcript.turns[0].items.len(), 3);
        assert!(matches!(
            &transcript.turns[0].items[0],
            NativeTranscriptItem::UserPrompt { text, .. } if text == "fix it"
        ));
        let encoded = serde_json::to_string(&transcript).unwrap();
        assert!(!encoded.contains("private chain"));
        assert!(!encoded.contains("very large secret output"));
    }

    #[test]
    fn failed_native_turn_does_not_promote_partial_message_to_final_answer() {
        let transcript = parse_native_transcript(
            "thr_1",
            &json!({
                "thread": {
                    "id": "thr_1",
                    "cwd": "/repo",
                    "turns": [{
                        "id": "turn_failed",
                        "status": "failed",
                        "items": [
                            {"type": "userMessage", "id": "u", "content": [{"type": "text", "text": "change it"}]},
                            {"type": "agentMessage", "id": "a", "text": "I will start by inspecting the workspace."}
                        ]
                    }]
                }
            }),
        )
        .unwrap();

        assert!(matches!(
            &transcript.turns[0].items[1],
            NativeTranscriptItem::AssistantMessage {
                final_answer: false,
                ..
            }
        ));
    }

    #[test]
    fn native_history_rejects_partial_items_view() {
        let error = parse_native_transcript(
            "thr_1",
            &json!({
                "thread": {
                    "id": "thr_1",
                    "cwd": "/repo",
                    "turns": [{
                        "id": "turn_summary",
                        "status": "completed",
                        "itemsView": "summary",
                        "items": []
                    }]
                }
            }),
        )
        .unwrap_err();
        assert!(matches!(error, ProviderError::Incompatible(_)));
    }

    #[test]
    fn native_history_rejects_unknown_multimodal_input_instead_of_silently_dropping_it() {
        let error = parse_native_transcript(
            "thr_1",
            &json!({
                "thread": {
                    "id": "thr_1",
                    "cwd": "/repo",
                    "turns": [{
                        "id": "turn_image",
                        "status": "completed",
                        "items": [{
                            "type": "userMessage",
                            "id": "u",
                            "content": [{"type": "text", "text": "inspect this"}, {"type": "audio", "path": "/tmp/audio.wav"}]
                        }]
                    }]
                }
            }),
        )
        .unwrap_err();
        assert!(matches!(error, ProviderError::Incompatible(_)));
    }

    #[test]
    fn native_history_normalizes_current_tools_and_multimodal_items_without_raw_outputs() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../fixtures/codex/0.144/thread-read-rich-items.json"
        ))
        .unwrap();
        let transcript = parse_native_transcript("thr_rich", &fixture).unwrap();
        let items = &transcript.turns[0].items;

        let NativeTranscriptItem::UserPrompt {
            text, attachments, ..
        } = &items[0]
        else {
            panic!("first rich item was not the user prompt");
        };
        assert_eq!(text, "Inspect these inputs");
        assert_eq!(attachments.len(), 4);
        assert!(attachments.iter().all(|attachment| {
            attachment.omitted && attachment.source_digest.starts_with("sha256:")
        }));

        let tools = items
            .iter()
            .filter_map(|item| {
                if let NativeTranscriptItem::ToolCall {
                    name,
                    input_summary,
                    status,
                    output_digest,
                    artifacts,
                    ..
                } = item
                {
                    Some((
                        name.as_str(),
                        input_summary,
                        status,
                        output_digest,
                        artifacts,
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(tools.len(), 7);
        assert_eq!(tools[0].0, "mcp.github.get_issue");
        assert_eq!(tools[1].0, "dynamic.browser.inspect");
        assert_eq!(tools[2].0, "web.search");
        assert_eq!(tools[3].0, "image.generate");
        assert_eq!(tools[4].0, "image.view");
        assert_eq!(tools[5].0, "sleep");
        assert_eq!(tools[6].0, "collab.spawnAgent");
        assert!(tools.iter().all(|(_, summary, status, _, _)| {
            **status == NativeEffectStatus::Completed
                && summary["digest"]
                    .as_str()
                    .is_some_and(|value| value.starts_with("sha256:"))
        }));
        assert!(
            tools[0]
                .3
                .as_deref()
                .is_some_and(|value| value.starts_with("sha256:"))
        );
        assert_eq!(tools[0].4.len(), 1);
        assert_eq!(tools[1].4.len(), 1);
        assert_eq!(tools[3].4.len(), 2);
        assert_eq!(tools[4].4.len(), 1);
        assert_eq!(
            items
                .iter()
                .filter(|item| matches!(item, NativeTranscriptItem::ContextMarker { .. }))
                .count(),
            5
        );

        let serialized = serde_json::to_string(&transcript).unwrap();
        for private in [
            "PRIVATE_REMOTE_BYTES",
            "PRIVATE_MCP_OUTPUT",
            "PRIVATE_MCP_IMAGE",
            "PRIVATE_STRUCTURED_OUTPUT",
            "PRIVATE_DYNAMIC_OUTPUT",
            "PRIVATE_DYNAMIC_IMAGE",
            "private acquisition target",
            "PRIVATE_GENERATED_IMAGE",
            "PRIVATE_REVISED_PROMPT",
            "PRIVATE_HOOK_PROMPT",
            "secret-owner",
            "secret-repo",
            "PRIVATE_SUBAGENT_OUTPUT",
            "PRIVATE_SUBAGENT_PROMPT",
        ] {
            assert!(
                !serialized.contains(private),
                "private native payload leaked into normalized transcript: {private}"
            );
        }
    }

    #[test]
    fn interactive_thread_snapshot_accumulates_all_pages_and_preserves_fork_identity() {
        let directory = tempfile::tempdir().unwrap();
        let cwd = std::fs::canonicalize(directory.path()).unwrap();
        let cwd_text = cwd.to_str().unwrap();
        let mut snapshot = ThreadSnapshotAccumulator::default();
        let cursor = snapshot
            .accept_page(
                &json!({
                    "data": [{
                        "id": "thread_b",
                        "sessionId": "session_tree",
                        "updatedAt": 11,
                        "forkedFromId": null,
                        "cwd": cwd_text
                    }],
                    "nextCursor": "cursor-1"
                }),
                &cwd,
            )
            .unwrap();
        assert_eq!(cursor.as_deref(), Some("cursor-1"));
        assert!(
            snapshot
                .accept_page(
                    &json!({
                        "data": [{
                            "id": "thread_a",
                            "sessionId": "session_tree",
                            "updatedAt": 12,
                            "forkedFromId": "thread_b",
                            "cwd": cwd_text
                        }],
                        "nextCursor": null
                    }),
                    &cwd,
                )
                .unwrap()
                .is_none()
        );
        let snapshot = snapshot.finish(cwd.clone());
        assert_eq!(snapshot.cwd, cwd);
        assert_eq!(snapshot.threads.len(), 2);
        assert_eq!(snapshot.threads[0].thread_id, "thread_a");
        assert_eq!(snapshot.threads[0].session_id, "session_tree");
        assert_eq!(
            snapshot.threads[0].forked_from_id.as_deref(),
            Some("thread_b")
        );
    }

    #[test]
    fn interactive_thread_snapshot_fails_closed_on_cursor_cycles_and_workspace_leaks() {
        let directory = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let cwd = std::fs::canonicalize(directory.path()).unwrap();
        let other = std::fs::canonicalize(other.path()).unwrap();
        let mut cursor_cycle = ThreadSnapshotAccumulator::default();
        let page = json!({"data": [], "nextCursor": "same-cursor"});
        cursor_cycle.accept_page(&page, &cwd).unwrap();
        assert!(matches!(
            cursor_cycle.accept_page(&page, &cwd),
            Err(ProviderError::Protocol(_))
        ));

        let mut leaked = ThreadSnapshotAccumulator::default();
        let error = leaked
            .accept_page(
                &json!({
                    "data": [{
                        "id": "thread_other",
                        "sessionId": "session_other",
                        "updatedAt": 13,
                        "forkedFromId": null,
                        "cwd": other
                    }],
                    "nextCursor": null
                }),
                &cwd,
            )
            .unwrap_err();
        assert!(matches!(error, ProviderError::Protocol(_)));
    }

    #[test]
    fn interactive_thread_snapshot_has_a_hard_page_bound() {
        let directory = tempfile::tempdir().unwrap();
        let cwd = std::fs::canonicalize(directory.path()).unwrap();
        let mut snapshot = ThreadSnapshotAccumulator::default();
        for page in 0..MAX_THREAD_SNAPSHOT_PAGES {
            snapshot
                .accept_page(
                    &json!({"data": [], "nextCursor": format!("cursor-{page}")}),
                    &cwd,
                )
                .unwrap();
        }
        assert!(matches!(
            snapshot.accept_page(&json!({"data": [], "nextCursor": null}), &cwd),
            Err(ProviderError::Incompatible(_))
        ));
    }

    #[test]
    fn native_history_rejects_unknown_item_types_instead_of_silently_dropping_them() {
        let error = parse_native_transcript(
            "thr_1",
            &json!({
                "thread": {
                    "id": "thr_1",
                    "cwd": "/repo",
                    "turns": [{
                        "id": "turn_future",
                        "status": "completed",
                        "items": [{
                            "type": "futurePublicToolCall",
                            "id": "future_item",
                            "result": "state-changing provider output"
                        }]
                    }]
                }
            }),
        )
        .unwrap_err();

        assert!(matches!(error, ProviderError::Incompatible(_)));
        assert!(error.to_string().contains("futurePublicToolCall"));
    }

    #[test]
    fn native_history_rejects_known_public_items_with_missing_payload_fields() {
        let malformed = [
            (
                "user content",
                json!({"type": "userMessage", "id": "user_missing_content"}),
            ),
            (
                "user text",
                json!({
                    "type": "userMessage",
                    "id": "user_missing_text",
                    "content": [{"type": "text"}]
                }),
            ),
            (
                "agent text",
                json!({"type": "agentMessage", "id": "agent_missing_text"}),
            ),
            (
                "plan text",
                json!({"type": "plan", "id": "plan_missing_text"}),
            ),
            (
                "command",
                json!({
                    "type": "commandExecution",
                    "id": "command_missing_command",
                    "status": "completed"
                }),
            ),
            (
                "changes",
                json!({
                    "type": "fileChange",
                    "id": "file_missing_changes",
                    "status": "completed"
                }),
            ),
            (
                "change path",
                json!({
                    "type": "fileChange",
                    "id": "file_missing_path",
                    "status": "completed",
                    "changes": [{"kind": "update"}]
                }),
            ),
        ];

        for (label, item) in malformed {
            let error = parse_native_transcript(
                "thr_1",
                &json!({
                    "thread": {
                        "id": "thr_1",
                        "cwd": "/repo",
                        "turns": [{
                            "id": format!("turn_{label}"),
                            "status": "completed",
                            "items": [item]
                        }]
                    }
                }),
            )
            .unwrap_err();
            assert!(
                matches!(error, ProviderError::Incompatible(_)),
                "{label} was not rejected as an incompatible public item: {error}"
            );
        }
    }

    #[test]
    fn native_history_preserves_declined_effect_status_without_claiming_completion() {
        let transcript = parse_native_transcript(
            "thr_1",
            &json!({
                "thread": {
                    "id": "thr_1",
                    "cwd": "/repo",
                    "turns": [{
                        "id": "turn_declined",
                        "status": "completed",
                        "items": [
                            {"type": "commandExecution", "id": "c", "command": "rm file", "cwd": "/repo", "status": "declined"},
                            {"type": "fileChange", "id": "f", "status": "declined", "changes": [{"path": "file"}]}
                        ]
                    }]
                }
            }),
        )
        .unwrap();

        assert!(matches!(
            &transcript.turns[0].items[0],
            NativeTranscriptItem::Command {
                status: NativeEffectStatus::Declined,
                ..
            }
        ));
        assert!(matches!(
            &transcript.turns[0].items[1],
            NativeTranscriptItem::FilesChanged {
                status: NativeEffectStatus::Declined,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn repeated_empty_sync_is_an_idempotent_local_hit_without_starting_app_server() {
        let adapter = CodexAdapter::new("binary-must-not-run", None);
        let session = NativeSession {
            id: ProviderSessionId::new(),
            provider: ProviderKind::Codex,
            native_session_id: "thr_test".to_owned(),
            native_version: Some("test".to_owned()),
            capabilities: BTreeMap::new(),
        };
        let batch = SyncBatch {
            from_seq_exclusive: 0,
            through_seq_inclusive: 7,
            projection_version: 3,
            events: Vec::new(),
            handoff: None,
        };
        let first = adapter.sync_context(&session, batch.clone()).await.unwrap();
        let repeated = adapter.sync_context(&session, batch).await.unwrap();
        assert_eq!(first.through_seq, 7);
        assert_eq!(first.native_receipt, None);
        assert_eq!(
            repeated.native_receipt.as_deref(),
            Some("codex:idempotent-local-hit")
        );
    }

    #[test]
    fn approval_response_uses_current_protocol_spelling() {
        let pending = PendingApproval {
            client: None,
            rpc_id: Value::from(9),
            method: "item/commandExecution/requestApproval".to_owned(),
            params: Value::Null,
            native_turn_id: "turn".to_owned(),
        };
        assert_eq!(
            approval_result(&pending, ApprovalDecision::AllowSession)["decision"],
            "acceptForSession"
        );
        assert_eq!(
            approval_result(&pending, ApprovalDecision::CancelTurn)["decision"],
            "cancel"
        );
    }

    #[test]
    fn terminal_usage_limit_is_a_routable_error() {
        let error = classify_terminal_turn(&json!({
            "turn": {
                "status": "failed",
                "error": {
                    "message": "limit reached",
                    "codexErrorInfo": "usageLimitExceeded"
                }
            }
        }))
        .unwrap();
        assert!(matches!(
            error,
            agentctl_core::ProviderError::RateLimited { .. }
        ));
    }

    #[test]
    fn review_turn_uses_non_escalating_read_only_sandbox() {
        let session = NativeSession {
            id: ProviderSessionId::new(),
            provider: ProviderKind::Codex,
            native_session_id: "thread-123".to_owned(),
            native_version: None,
            capabilities: BTreeMap::new(),
        };
        let request = TurnRequest {
            session_id: UnifiedSessionId::new(),
            turn_id: TurnId::new(),
            prompt: "review".to_owned(),
            cwd: PathBuf::from("/repo"),
            continuation: false,
            execution_mode: TurnExecutionMode::ReviewReadOnly,
            metadata: Value::Null,
        };
        let params = turn_start_params(&session, &request);
        assert_eq!(params["sandboxPolicy"]["type"], "readOnly");
        assert_eq!(params["sandboxPolicy"]["networkAccess"], false);
        assert_eq!(params["approvalPolicy"], "never");
    }

    #[test]
    fn malformed_thread_ids_are_rejected_before_app_server() {
        assert!(validate_native_session_id("").is_err());
        assert!(validate_native_session_id(" thread").is_err());
        assert!(validate_native_session_id("thread\nother").is_err());
        assert!(validate_native_session_id("01900000-0000-7000-8000-000000000000").is_ok());
    }
}
