// ABOUTME: Persists canonical sessions and native lifecycle evidence in SQLite.
// ABOUTME: Validates bounded records and enforces durable state transitions.
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    str::FromStr,
};

use agentctl_core::{
    AuthMode, CanonicalEvent, EventId, EventVisibility, ProviderHealth, ProviderKind,
    ProviderSessionId, ProviderStatus, SessionStatus, SideEffectState, TurnId, TurnStatus,
    UnifiedSession, UnifiedSessionId,
};
use chrono::{DateTime, Utc};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, types::Type,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{BlobRef, Result, StorageError, error::io_error};

const CURRENT_SCHEMA_VERSION: u32 = 2;
const INITIAL_MIGRATION: &str = include_str!("../migrations/0001_initial.sql");
const NATIVE_CLI_MIGRATION: &str = include_str!("../migrations/0002_native_cli_bridge.sql");

#[derive(Clone, Copy, Debug)]
pub struct StorageLimits {
    pub max_event_bytes: usize,
    pub max_json_depth: usize,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            max_event_bytes: 8 * 1024 * 1024,
            max_json_depth: 128,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteStore {
    path: PathBuf,
    limits: StorageLimits,
}

/// Complete local persistence surface. `Deref` exposes the canonical `SQLite` API while
/// `blobs` handles large content-addressed payloads.
#[derive(Clone, Debug)]
pub struct AgentctlStore {
    pub sqlite: SqliteStore,
    pub blobs: crate::BlobStore,
}

impl AgentctlStore {
    pub fn open(database_path: impl AsRef<Path>, blob_root: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            sqlite: SqliteStore::open(database_path)?,
            blobs: crate::BlobStore::open(blob_root)?,
        })
    }
}

impl std::ops::Deref for AgentctlStore {
    type Target = SqliteStore;

    fn deref(&self) -> &Self::Target {
        &self.sqlite
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderSessionRecord {
    pub id: ProviderSessionId,
    pub unified_session_id: UnifiedSessionId,
    pub provider: ProviderKind,
    pub native_session_id: String,
    pub native_version: Option<String>,
    pub last_synced_seq: u64,
    pub status: ProviderStatus,
    pub reset_at: Option<DateTime<Utc>>,
    pub capabilities: BTreeMap<String, bool>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeLaunchState {
    Started,
    CaptureReady,
    Exited,
    Uncertain,
    Captured,
    Failed,
}

impl NativeLaunchState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::CaptureReady => "capture_ready",
            Self::Exited => "exited",
            Self::Uncertain => "uncertain",
            Self::Captured => "captured",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "started" => Ok(Self::Started),
            "capture_ready" => Ok(Self::CaptureReady),
            "exited" => Ok(Self::Exited),
            "uncertain" => Ok(Self::Uncertain),
            "captured" => Ok(Self::Captured),
            "failed" => Ok(Self::Failed),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid native launch state {other}"),
                )),
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativeLaunchRecord {
    pub id: uuid::Uuid,
    pub session_id: UnifiedSessionId,
    pub provider: ProviderKind,
    pub native_session_id: String,
    pub workspace_lease_key: String,
    pub child_pid: Option<u32>,
    pub state: NativeLaunchState,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub metadata: serde_json::Value,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeHandoffState {
    Staged,
    Delivering,
    Delivered,
    Uncertain,
}

impl NativeHandoffState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Staged => "staged",
            Self::Delivering => "delivering",
            Self::Delivered => "delivered",
            Self::Uncertain => "uncertain",
        }
    }

    fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "staged" => Ok(Self::Staged),
            "delivering" => Ok(Self::Delivering),
            "delivered" => Ok(Self::Delivered),
            "uncertain" => Ok(Self::Uncertain),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid native handoff state {other}"),
                )),
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativeHandoffRecord {
    pub launch_id: uuid::Uuid,
    pub provider_session_id: ProviderSessionId,
    pub session_id: UnifiedSessionId,
    pub native_session_id: String,
    pub through_seq: u64,
    pub capsule: String,
    pub content_digest: String,
    pub state: NativeHandoffState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TurnRecord {
    pub id: TurnId,
    pub session_id: UnifiedSessionId,
    pub provider: Option<ProviderKind>,
    pub prompt_seq: u64,
    pub status: TurnStatus,
    pub side_effect_state: SideEffectState,
    pub native_turn_id: Option<String>,
    pub continuation: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawProviderEvent {
    pub event_id: EventId,
    pub session_id: UnifiedSessionId,
    pub turn_id: Option<TurnId>,
    pub provider: ProviderKind,
    pub kind: String,
    pub payload: serde_json::Value,
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}

impl RawProviderEvent {
    pub fn new(
        session_id: UnifiedSessionId,
        turn_id: Option<TurnId>,
        provider: ProviderKind,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<Self> {
        let encoded = serde_json::to_vec(&payload)?;
        Ok(Self {
            event_id: EventId::new(),
            session_id,
            turn_id,
            provider,
            kind: kind.into(),
            payload,
            content_hash: sha256_digest(&encoded),
            created_at: Utc::now(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkspaceSnapshotRecord {
    pub id: EventId,
    pub session_id: UnifiedSessionId,
    pub turn_id: Option<TurnId>,
    pub phase: String,
    pub fingerprint: String,
    pub snapshot: serde_json::Value,
    pub diff_digest: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContextCheckpointRecord {
    pub id: EventId,
    pub session_id: UnifiedSessionId,
    pub through_seq: u64,
    pub projection_version: u32,
    pub checkpoint: serde_json::Value,
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProtocolCapabilityRecord {
    pub provider: ProviderKind,
    pub native_version: String,
    pub capability: String,
    pub supported: bool,
    pub evidence: serde_json::Value,
    pub probed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncReceiptEntry {
    pub canonical_event_id: EventId,
    pub projection_version: u32,
    pub native_receipt: Option<String>,
    pub state: String,
    pub applied_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncIntentEntry {
    pub canonical_event_id: EventId,
    pub projection_version: u32,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncWriteResult {
    pub inserted: usize,
    pub duplicate: usize,
    pub last_synced_seq: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProjectionRebuildResult {
    pub provider_sessions_removed: usize,
    pub receipts_removed: usize,
    pub pending_intents_removed: usize,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        reject_symlink(&path)?;
        if let Some(parent) = path.parent() {
            create_private_dir(parent)?;
        }
        if !path.exists() {
            create_private_file(&path)?;
        }
        set_mode(&path, 0o600)?;
        let store = Self {
            path,
            limits: StorageLimits::default(),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(test)]
    #[must_use]
    fn with_limits(mut self, limits: StorageLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn schema_version(&self) -> Result<u32> {
        let connection = self.connection()?;
        let version = connection
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                [],
                |row| row.get::<_, u32>(0),
            )
            .unwrap_or(0);
        Ok(version)
    }

    pub fn integrity_check(&self) -> Result<String> {
        let connection = self.connection()?;
        connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(Into::into)
    }

    pub fn create_session(&self, session: &UnifiedSession) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO unified_sessions (
                id, name, workspace_path, workspace_fingerprint, active_provider_json, routing_policy,
                auth_mode_json, status_json, parent_session_id, created_at, updated_at,
                schema_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                session.id.to_string(),
                session.name,
                session.workspace_path.to_string_lossy(),
                session.workspace_fingerprint,
                encode_option(session.active_provider.as_ref())?,
                session.routing_policy,
                encode(&session.auth_mode)?,
                encode(&session.status)?,
                session.parent_session_id.map(|id| id.to_string()),
                timestamp(session.created_at),
                timestamp(session.updated_at),
                session.schema_version,
            ],
        )?;
        Ok(())
    }

    pub fn get_session(&self, id: UnifiedSessionId) -> Result<Option<UnifiedSession>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, name, workspace_path, workspace_fingerprint, active_provider_json, routing_policy,
                    auth_mode_json, status_json, parent_session_id, created_at, updated_at,
                    schema_version
             FROM unified_sessions WHERE id = ?1",
        )?;
        statement
            .query_row([id.to_string()], session_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn list_sessions(&self) -> Result<Vec<UnifiedSession>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, name, workspace_path, workspace_fingerprint, active_provider_json, routing_policy,
                    auth_mode_json, status_json, parent_session_id, created_at, updated_at,
                    schema_version
             FROM unified_sessions ORDER BY updated_at DESC, id",
        )?;
        let rows = statement.query_map([], session_from_row)?;
        collect_rows(rows)
    }

    pub fn delete_session(&self, id: UnifiedSessionId) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_no_open_native_launch(&transaction, id, "delete")?;
        transaction.execute(
            "INSERT OR IGNORE INTO deletion_authorizations (session_id) VALUES (?1)",
            [id.to_string()],
        )?;
        expect_one(
            transaction.execute(
                "DELETE FROM unified_sessions WHERE id = ?1",
                [id.to_string()],
            )?,
            format!("session {id}"),
        )?;
        transaction.execute(
            "DELETE FROM deletion_authorizations WHERE session_id = ?1",
            [id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn next_seq(&self, session_id: UnifiedSessionId) -> Result<u64> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE session_id = ?1",
                [session_id.to_string()],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn update_session_state(
        &self,
        id: UnifiedSessionId,
        active_provider: Option<&ProviderKind>,
        status: SessionStatus,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let connection = self.connection()?;
        expect_one(
            connection.execute(
                "UPDATE unified_sessions SET active_provider_json = ?2, status_json = ?3,
                 updated_at = ?4 WHERE id = ?1",
                params![
                    id.to_string(),
                    encode_option(active_provider)?,
                    encode(&status)?,
                    timestamp(updated_at),
                ],
            )?,
            format!("session {id}"),
        )
    }

    pub fn update_session_routing(
        &self,
        id: UnifiedSessionId,
        active_provider: Option<&ProviderKind>,
        routing_policy: &str,
        status: SessionStatus,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        if routing_policy.trim().is_empty() {
            return Err(StorageError::InvalidData(
                "routing policy cannot be empty".to_owned(),
            ));
        }
        let connection = self.connection()?;
        expect_one(
            connection.execute(
                "UPDATE unified_sessions SET active_provider_json = ?2, routing_policy = ?3,
                 status_json = ?4, updated_at = ?5 WHERE id = ?1",
                params![
                    id.to_string(),
                    encode_option(active_provider)?,
                    routing_policy,
                    encode(&status)?,
                    timestamp(updated_at),
                ],
            )?,
            format!("session {id}"),
        )
    }

    pub fn upsert_provider_session(&self, record: &ProviderSessionRecord) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO provider_sessions (
                id, unified_session_id, provider_json, native_session_id, native_version,
                last_synced_seq, status_json, reset_at, capabilities_json, metadata_json,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(unified_session_id, provider_json) DO UPDATE SET
                native_session_id = excluded.native_session_id,
                native_version = excluded.native_version,
                last_synced_seq = MAX(provider_sessions.last_synced_seq, excluded.last_synced_seq),
                status_json = excluded.status_json,
                reset_at = excluded.reset_at,
                capabilities_json = excluded.capabilities_json,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at",
            params![
                record.id.to_string(),
                record.unified_session_id.to_string(),
                encode(&record.provider)?,
                record.native_session_id,
                record.native_version,
                record.last_synced_seq,
                encode(&record.status)?,
                record.reset_at.map(timestamp),
                encode(&record.capabilities)?,
                encode(&record.metadata)?,
                timestamp(record.created_at),
                timestamp(record.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn provider_session(
        &self,
        session_id: UnifiedSessionId,
        provider: &ProviderKind,
    ) -> Result<Option<ProviderSessionRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, unified_session_id, provider_json, native_session_id, native_version,
                    last_synced_seq, status_json, reset_at, capabilities_json, metadata_json,
                    created_at, updated_at
             FROM provider_sessions WHERE unified_session_id = ?1 AND provider_json = ?2",
        )?;
        statement
            .query_row(
                params![session_id.to_string(), encode(provider)?],
                provider_session_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_provider_sessions(
        &self,
        session_id: UnifiedSessionId,
    ) -> Result<Vec<ProviderSessionRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, unified_session_id, provider_json, native_session_id, native_version,
                    last_synced_seq, status_json, reset_at, capabilities_json, metadata_json,
                    created_at, updated_at
             FROM provider_sessions WHERE unified_session_id = ?1 ORDER BY provider_json",
        )?;
        let rows = statement.query_map([session_id.to_string()], provider_session_from_row)?;
        collect_rows(rows)
    }

    pub fn start_native_launch(&self, record: &NativeLaunchRecord) -> Result<()> {
        if record.state != NativeLaunchState::Started {
            return Err(StorageError::InvalidData(
                "a native launch must begin in started state".to_owned(),
            ));
        }
        validate_json(&record.metadata, self.limits)?;
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO native_launches (
                id, session_id, provider_json, native_session_id, workspace_lease_key,
                child_pid, state, exit_code, error, metadata_json, started_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, NULL, NULL, ?7, ?8, ?9)",
            params![
                record.id.to_string(),
                record.session_id.to_string(),
                encode(&record.provider)?,
                record.native_session_id,
                record.workspace_lease_key,
                record.state.as_str(),
                encode(&record.metadata)?,
                timestamp(record.started_at),
                timestamp(record.updated_at),
            ],
        )?;
        Ok(())
    }

    /// Persists launch-specific Codex observations without replacing other lifecycle metadata.
    pub fn update_codex_launch_evidence(
        &self,
        id: uuid::Uuid,
        evidence: &serde_json::Value,
    ) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (provider, state, mut metadata): (ProviderKind, NativeLaunchState, serde_json::Value) =
            transaction
                .query_row(
                    "SELECT provider_json, state, metadata_json FROM native_launches WHERE id = ?1",
                    [id.to_string()],
                    |row| {
                        Ok((
                            decode_row(row.get::<_, String>(0)?, 0)?,
                            NativeLaunchState::parse(&row.get::<_, String>(1)?)?,
                            decode_row(row.get::<_, String>(2)?, 2)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| StorageError::NotFound(format!("native launch {id}")))?;
        if provider != ProviderKind::Codex
            || !matches!(
                state,
                NativeLaunchState::Started | NativeLaunchState::Exited
            )
        {
            return Err(StorageError::InvalidData(
                "Codex evidence requires an active Codex launch".to_owned(),
            ));
        }
        metadata["codex_evidence"] = evidence.clone();
        validate_json(&metadata, self.limits)?;
        expect_one(
            transaction.execute(
                "UPDATE native_launches SET metadata_json = ?2, updated_at = ?3 WHERE id = ?1",
                params![id.to_string(), encode(&metadata)?, timestamp(Utc::now())],
            )?,
            format!("native launch {id}"),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn update_native_launch(
        &self,
        id: uuid::Uuid,
        state: NativeLaunchState,
        exit_code: Option<i32>,
        error: Option<&str>,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        if error.is_some_and(|error| error.len() > self.limits.max_event_bytes) {
            return Err(StorageError::InvalidData(
                "native launch error exceeds storage limit".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT state FROM native_launches WHERE id = ?1",
                [id.to_string()],
                |row| NativeLaunchState::parse(&row.get::<_, String>(0)?),
            )
            .optional()?
            .ok_or_else(|| StorageError::NotFound(format!("native launch {id}")))?;
        let allowed = match current {
            NativeLaunchState::Started => true,
            NativeLaunchState::CaptureReady => matches!(
                state,
                NativeLaunchState::CaptureReady
                    | NativeLaunchState::Captured
                    | NativeLaunchState::Uncertain
                    | NativeLaunchState::Failed
            ),
            NativeLaunchState::Exited => matches!(
                state,
                NativeLaunchState::Exited
                    | NativeLaunchState::Captured
                    | NativeLaunchState::Uncertain
                    | NativeLaunchState::Failed
            ),
            NativeLaunchState::Uncertain => matches!(
                state,
                NativeLaunchState::Uncertain
                    | NativeLaunchState::Captured
                    | NativeLaunchState::Failed
            ),
            NativeLaunchState::Captured => state == NativeLaunchState::Captured,
            NativeLaunchState::Failed => state == NativeLaunchState::Failed,
        };
        if !allowed {
            return Err(StorageError::InvalidData(format!(
                "invalid native launch transition {current:?} -> {state:?} for {id}"
            )));
        }
        expect_one(
            transaction.execute(
                "UPDATE native_launches
                 SET state = ?2, exit_code = ?3, error = ?4, updated_at = ?5
                 WHERE id = ?1",
                params![
                    id.to_string(),
                    state.as_str(),
                    exit_code,
                    error,
                    timestamp(updated_at),
                ],
            )?,
            format!("native launch {id}"),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_native_launch_pid(&self, id: uuid::Uuid, child_pid: u32) -> Result<()> {
        let connection = self.connection()?;
        expect_one(
            connection.execute(
                "UPDATE native_launches SET child_pid = ?2, updated_at = ?3
                 WHERE id = ?1 AND state = 'started' AND child_pid IS NULL",
                params![id.to_string(), child_pid, timestamp(Utc::now())],
            )?,
            format!("unstarted native launch {id}"),
        )
    }

    pub fn native_launch(&self, id: uuid::Uuid) -> Result<Option<NativeLaunchRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id, session_id, provider_json, native_session_id,
                        workspace_lease_key, child_pid, state, exit_code, error, metadata_json,
                        started_at, updated_at
                 FROM native_launches WHERE id = ?1",
                [id.to_string()],
                native_launch_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn open_native_launches(
        &self,
        session_id: UnifiedSessionId,
    ) -> Result<Vec<NativeLaunchRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, session_id, provider_json, native_session_id,
                    workspace_lease_key, child_pid, state, exit_code, error, metadata_json,
                    started_at, updated_at
             FROM native_launches
             WHERE session_id = ?1 AND state IN ('started', 'capture_ready', 'exited', 'uncertain')
             ORDER BY started_at, id",
        )?;
        let rows = statement.query_map([session_id.to_string()], native_launch_from_row)?;
        collect_rows(rows)
    }

    #[cfg(test)]
    fn latest_open_native_launch(
        &self,
        session_id: UnifiedSessionId,
        provider: &ProviderKind,
    ) -> Result<Option<NativeLaunchRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id, session_id, provider_json, native_session_id,
                        workspace_lease_key, child_pid, state, exit_code, error, metadata_json,
                        started_at, updated_at
                 FROM native_launches
                 WHERE session_id = ?1 AND provider_json = ?2
                   AND state IN ('started', 'capture_ready', 'exited', 'uncertain')
                 ORDER BY started_at DESC, id DESC LIMIT 1",
                params![session_id.to_string(), encode(provider)?],
                native_launch_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn open_native_launch_for_workspace(
        &self,
        workspace_lease_key: &str,
    ) -> Result<Option<NativeLaunchRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id, session_id, provider_json, native_session_id,
                        workspace_lease_key, child_pid, state, exit_code, error, metadata_json,
                        started_at, updated_at
                 FROM native_launches
                 WHERE workspace_lease_key = ?1 AND state IN ('started', 'capture_ready', 'exited', 'uncertain')
                 ORDER BY started_at DESC, id DESC LIMIT 1",
                [workspace_lease_key],
                native_launch_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn stage_native_handoff(&self, record: &NativeHandoffRecord) -> Result<()> {
        if record.state != NativeHandoffState::Staged {
            return Err(StorageError::InvalidData(
                "a native handoff must begin in staged state".to_owned(),
            ));
        }
        if record.capsule.len() > self.limits.max_event_bytes {
            return Err(StorageError::EventTooLarge(self.limits.max_event_bytes));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let launch_matches: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM native_launches
             WHERE id = ?1 AND session_id = ?2 AND native_session_id = ?3
               AND state = 'started')",
            params![
                record.launch_id.to_string(),
                record.session_id.to_string(),
                record.native_session_id,
            ],
            |row| row.get(0),
        )?;
        if !launch_matches {
            return Err(StorageError::InvalidData(
                "native handoff does not match an open launch".to_owned(),
            ));
        }
        let provider_matches: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM provider_sessions
             WHERE id = ?1 AND unified_session_id = ?2 AND native_session_id = ?3)",
            params![
                record.provider_session_id.to_string(),
                record.session_id.to_string(),
                record.native_session_id,
            ],
            |row| row.get(0),
        )?;
        if !provider_matches {
            return Err(StorageError::InvalidData(
                "native handoff provider session mismatch".to_owned(),
            ));
        }
        let latest: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session_id = ?1",
            [record.session_id.to_string()],
            |row| row.get(0),
        )?;
        if record.through_seq > latest {
            return Err(StorageError::InvalidData(format!(
                "native handoff cursor {} exceeds canonical latest {latest}",
                record.through_seq
            )));
        }
        transaction.execute(
            "INSERT INTO native_handoffs (
                launch_id, provider_session_id, session_id, native_session_id,
                through_seq, capsule, content_digest, state, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.launch_id.to_string(),
                record.provider_session_id.to_string(),
                record.session_id.to_string(),
                record.native_session_id,
                record.through_seq,
                record.capsule,
                record.content_digest,
                record.state.as_str(),
                timestamp(record.created_at),
                timestamp(record.updated_at),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn native_handoff(&self, launch_id: uuid::Uuid) -> Result<Option<NativeHandoffRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT launch_id, provider_session_id, session_id, native_session_id,
                        through_seq, capsule, content_digest, state, created_at, updated_at
                 FROM native_handoffs WHERE launch_id = ?1",
                [launch_id.to_string()],
                native_handoff_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn begin_native_handoff_delivery(
        &self,
        launch_id: uuid::Uuid,
    ) -> Result<NativeHandoffRecord> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        expect_one(
            transaction.execute(
                "UPDATE native_handoffs SET state = 'delivering', updated_at = ?2
                 WHERE launch_id = ?1 AND state = 'staged'",
                params![launch_id.to_string(), timestamp(Utc::now())],
            )?,
            format!("staged native handoff {launch_id}"),
        )?;
        let record = transaction.query_row(
            "SELECT launch_id, provider_session_id, session_id, native_session_id,
                    through_seq, capsule, content_digest, state, created_at, updated_at
             FROM native_handoffs WHERE launch_id = ?1",
            [launch_id.to_string()],
            native_handoff_from_row,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    pub fn complete_native_handoff_delivery(
        &self,
        launch_id: uuid::Uuid,
        updated_at: DateTime<Utc>,
    ) -> Result<u64> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (provider_session_id, session_id, through_seq): (String, String, u64) = transaction
            .query_row(
                "SELECT provider_session_id, session_id, through_seq
                 FROM native_handoffs WHERE launch_id = ?1 AND state = 'delivering'",
                [launch_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| {
                StorageError::InvalidData(format!(
                    "native handoff {launch_id} is not in delivering state"
                ))
            })?;
        let latest: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session_id = ?1",
            [&session_id],
            |row| row.get(0),
        )?;
        if through_seq > latest {
            return Err(StorageError::InvalidData(format!(
                "native handoff cursor {through_seq} exceeds canonical latest {latest}"
            )));
        }
        expect_one(
            transaction.execute(
                "UPDATE native_handoffs SET state = 'delivered', updated_at = ?2
                 WHERE launch_id = ?1 AND state = 'delivering'",
                params![launch_id.to_string(), timestamp(updated_at)],
            )?,
            format!("delivering native handoff {launch_id}"),
        )?;
        let provider_session_id: ProviderSessionId =
            provider_session_id.parse().map_err(|error| {
                StorageError::InvalidData(format!("invalid provider session id: {error}"))
            })?;
        let last_synced = update_provider_cursor(&transaction, provider_session_id, through_seq)?;
        transaction.commit()?;
        Ok(last_synced)
    }

    pub fn mark_native_handoff_uncertain(&self, launch_id: uuid::Uuid) -> Result<()> {
        let connection = self.connection()?;
        expect_one(
            connection.execute(
                "UPDATE native_handoffs SET state = 'uncertain', updated_at = ?2
                 WHERE launch_id = ?1 AND state = 'delivering'",
                params![launch_id.to_string(), timestamp(Utc::now())],
            )?,
            format!("delivering native handoff {launch_id}"),
        )
    }

    pub fn update_provider_session_health(
        &self,
        session_id: UnifiedSessionId,
        provider: &ProviderKind,
        status: &ProviderStatus,
        reset_at: Option<DateTime<Utc>>,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE provider_sessions SET status_json = ?3, reset_at = ?4, updated_at = ?5
             WHERE unified_session_id = ?1 AND provider_json = ?2",
            params![
                session_id.to_string(),
                encode(provider)?,
                encode(status)?,
                reset_at.map(timestamp),
                timestamp(updated_at),
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn create_turn(&self, turn: &TurnRecord) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO turns (
                id, session_id, provider_json, prompt_seq, status_json, side_effect_state_json,
                native_turn_id, continuation, started_at, completed_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                turn.id.to_string(),
                turn.session_id.to_string(),
                encode_option(turn.provider.as_ref())?,
                turn.prompt_seq,
                encode(&turn.status)?,
                encode(&turn.side_effect_state)?,
                turn.native_turn_id,
                turn.continuation,
                turn.started_at.map(timestamp),
                turn.completed_at.map(timestamp),
                timestamp(turn.created_at),
                timestamp(turn.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn update_turn_state(
        &self,
        id: TurnId,
        status: TurnStatus,
        side_effect_state: SideEffectState,
        native_turn_id: Option<&str>,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let connection = self.connection()?;
        let completed_at = matches!(
            status,
            TurnStatus::Completed | TurnStatus::Interrupted | TurnStatus::Failed
        )
        .then(|| timestamp(updated_at));
        expect_one(
            connection.execute(
                "UPDATE turns SET status_json = ?2,
                 side_effect_state_json = CASE
                    WHEN side_effect_state_json = '\"confirmed\"' THEN side_effect_state_json
                    WHEN side_effect_state_json = '\"possible\"' AND ?3 = '\"none\"'
                        THEN side_effect_state_json
                    ELSE ?3 END,
                 native_turn_id = COALESCE(?4, native_turn_id),
                 started_at = CASE WHEN ?2 = '\"running\"' THEN COALESCE(started_at, ?5)
                                   ELSE started_at END,
                 completed_at = COALESCE(?6, completed_at), updated_at = ?5
                 WHERE id = ?1",
                params![
                    id.to_string(),
                    encode(&status)?,
                    encode(&side_effect_state)?,
                    native_turn_id,
                    timestamp(updated_at),
                    completed_at,
                ],
            )?,
            format!("turn {id}"),
        )
    }

    pub fn get_turn(&self, id: TurnId) -> Result<Option<TurnRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id, session_id, provider_json, prompt_seq, status_json,
                        side_effect_state_json, native_turn_id, continuation, started_at,
                        completed_at, created_at, updated_at
                 FROM turns WHERE id = ?1",
                [id.to_string()],
                turn_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn recovery_candidates(&self) -> Result<Vec<TurnRecord>> {
        let connection = self.connection()?;
        let states = [
            encode(&TurnStatus::Pending)?,
            encode(&TurnStatus::Running)?,
            encode(&TurnStatus::WaitingOnApproval)?,
            encode(&TurnStatus::Uncertain)?,
        ];
        let mut statement = connection.prepare(
            "SELECT id, session_id, provider_json, prompt_seq, status_json,
                    side_effect_state_json, native_turn_id, continuation, started_at,
                    completed_at, created_at, updated_at
             FROM turns WHERE status_json IN (?1, ?2, ?3, ?4) ORDER BY updated_at",
        )?;
        let rows = statement.query_map(
            params![states[0], states[1], states[2], states[3]],
            turn_from_row,
        )?;
        collect_rows(rows)
    }

    /// Returns non-terminal turns for one exact provider projection without
    /// scanning unrelated sessions. Native lifecycle hooks call this under a
    /// short provider timeout, so the query is backed by
    /// `idx_turns_session_status`.
    pub fn recovery_candidates_for(
        &self,
        session_id: UnifiedSessionId,
        provider: &ProviderKind,
    ) -> Result<Vec<TurnRecord>> {
        let connection = self.connection()?;
        let states = [
            encode(&TurnStatus::Pending)?,
            encode(&TurnStatus::Running)?,
            encode(&TurnStatus::WaitingOnApproval)?,
            encode(&TurnStatus::Uncertain)?,
        ];
        let mut statement = connection.prepare(
            "SELECT id, session_id, provider_json, prompt_seq, status_json,
                    side_effect_state_json, native_turn_id, continuation, started_at,
                    completed_at, created_at, updated_at
             FROM turns
             WHERE session_id = ?1 AND provider_json = ?2
               AND status_json IN (?3, ?4, ?5, ?6)
             ORDER BY updated_at",
        )?;
        let rows = statement.query_map(
            params![
                session_id.to_string(),
                encode(provider)?,
                states[0],
                states[1],
                states[2],
                states[3]
            ],
            turn_from_row,
        )?;
        collect_rows(rows)
    }

    pub fn append_event(
        &self,
        event: &CanonicalEvent,
        raw: Option<&RawProviderEvent>,
    ) -> Result<()> {
        if event.seq == 0 {
            return Err(StorageError::InvalidSequence {
                expected: 1,
                actual: 0,
            });
        }
        if event.content_hash.is_empty() {
            return Err(StorageError::InvalidData(
                "canonical event content_hash cannot be empty".to_owned(),
            ));
        }
        validate_json(&event.payload, self.limits)?;
        if raw.map(|item| item.event_id) != event.raw_event_id {
            return Err(StorageError::InvalidData(
                "raw event must match canonical raw_event_id".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let next: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE session_id = ?1",
            [event.session_id.to_string()],
            |row| row.get(0),
        )?;
        if event.seq != next {
            return Err(StorageError::InvalidSequence {
                expected: next,
                actual: event.seq,
            });
        }
        if let Some(raw) = raw {
            validate_json(&raw.payload, self.limits)?;
            insert_raw_event(&transaction, raw)?;
        }
        transaction.execute(
            "INSERT INTO events (
                session_id, seq, event_id, turn_id, origin_provider_json, kind,
                visibility_json, payload_json, content_hash, raw_event_id, created_at,
                schema_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                event.session_id.to_string(),
                event.seq,
                event.event_id.to_string(),
                event.turn_id.map(|id| id.to_string()),
                encode_option(event.origin_provider.as_ref())?,
                event.kind,
                encode(&event.visibility)?,
                encode(&event.payload)?,
                event.content_hash,
                event.raw_event_id.map(|id| id.to_string()),
                timestamp(event.created_at),
                event.schema_version,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Allocates the next canonical sequence and appends in one IMMEDIATE
    /// transaction. Native provider hooks can execute concurrently, so callers
    /// must use this instead of a separate `next_seq`/`append_event` pair.
    pub fn append_event_allocating_seq(
        &self,
        mut event: CanonicalEvent,
        raw: Option<&RawProviderEvent>,
    ) -> Result<CanonicalEvent> {
        if event.content_hash.is_empty() {
            return Err(StorageError::InvalidData(
                "canonical event content_hash cannot be empty".to_owned(),
            ));
        }
        validate_json(&event.payload, self.limits)?;
        if raw.map(|item| item.event_id) != event.raw_event_id {
            return Err(StorageError::InvalidData(
                "raw event must match canonical raw_event_id".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        event.seq = transaction.query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE session_id = ?1",
            [event.session_id.to_string()],
            |row| row.get(0),
        )?;
        if let Some(raw) = raw {
            validate_json(&raw.payload, self.limits)?;
            insert_raw_event(&transaction, raw)?;
        }
        transaction.execute(
            "INSERT INTO events (
                session_id, seq, event_id, turn_id, origin_provider_json, kind,
                visibility_json, payload_json, content_hash, raw_event_id, created_at,
                schema_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                event.session_id.to_string(),
                event.seq,
                event.event_id.to_string(),
                event.turn_id.map(|id| id.to_string()),
                encode_option(event.origin_provider.as_ref())?,
                event.kind,
                encode(&event.visibility)?,
                encode(&event.payload)?,
                event.content_hash,
                event.raw_event_id.map(|id| id.to_string()),
                timestamp(event.created_at),
                event.schema_version,
            ],
        )?;
        transaction.commit()?;
        Ok(event)
    }

    /// Atomically creates a provider turn and its first canonical prompt.
    /// The deterministic event id makes a retried native hook an idempotent
    /// read, while an IMMEDIATE transaction prevents prompt/turn sequence gaps.
    pub fn create_turn_with_allocated_event(
        &self,
        mut turn: TurnRecord,
        mut event: CanonicalEvent,
        raw: Option<&RawProviderEvent>,
    ) -> Result<(TurnRecord, CanonicalEvent, bool)> {
        if event.turn_id != Some(turn.id) || event.session_id != turn.session_id {
            return Err(StorageError::InvalidData(
                "turn and first event identity mismatch".to_owned(),
            ));
        }
        validate_json(&event.payload, self.limits)?;
        if raw.map(|item| item.event_id) != event.raw_event_id {
            return Err(StorageError::InvalidData(
                "raw event must match canonical raw_event_id".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = transaction
            .query_row(
                "SELECT session_id, seq, event_id, turn_id, origin_provider_json, kind,
                        visibility_json, payload_json, content_hash, raw_event_id, created_at,
                        schema_version
                 FROM events WHERE event_id = ?1",
                [event.event_id.to_string()],
                event_from_row,
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing.session_id != event.session_id
                || existing.turn_id != event.turn_id
                || existing.kind != event.kind
                || existing.content_hash != event.content_hash
            {
                return Err(StorageError::InvalidData(
                    "deterministic native hook event id collision".to_owned(),
                ));
            }
            let existing_turn = transaction.query_row(
                "SELECT id, session_id, provider_json, prompt_seq, status_json,
                        side_effect_state_json, native_turn_id, continuation, started_at,
                        completed_at, created_at, updated_at
                 FROM turns WHERE id = ?1",
                [turn.id.to_string()],
                turn_from_row,
            )?;
            transaction.commit()?;
            return Ok((existing_turn, existing, true));
        }
        event.seq = transaction.query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE session_id = ?1",
            [event.session_id.to_string()],
            |row| row.get(0),
        )?;
        turn.prompt_seq = event.seq;
        transaction.execute(
            "INSERT INTO turns (
                id, session_id, provider_json, prompt_seq, status_json,
                side_effect_state_json, native_turn_id, continuation, started_at,
                completed_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                turn.id.to_string(),
                turn.session_id.to_string(),
                encode_option(turn.provider.as_ref())?,
                turn.prompt_seq,
                encode(&turn.status)?,
                encode(&turn.side_effect_state)?,
                turn.native_turn_id,
                turn.continuation,
                turn.started_at.map(timestamp),
                turn.completed_at.map(timestamp),
                timestamp(turn.created_at),
                timestamp(turn.updated_at),
            ],
        )?;
        if let Some(raw) = raw {
            validate_json(&raw.payload, self.limits)?;
            insert_raw_event(&transaction, raw)?;
        }
        transaction.execute(
            "INSERT INTO events (
                session_id, seq, event_id, turn_id, origin_provider_json, kind,
                visibility_json, payload_json, content_hash, raw_event_id, created_at,
                schema_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                event.session_id.to_string(),
                event.seq,
                event.event_id.to_string(),
                event.turn_id.map(|id| id.to_string()),
                encode_option(event.origin_provider.as_ref())?,
                event.kind,
                encode(&event.visibility)?,
                encode(&event.payload)?,
                event.content_hash,
                event.raw_event_id.map(|id| id.to_string()),
                timestamp(event.created_at),
                event.schema_version,
            ],
        )?;
        transaction.commit()?;
        Ok((turn, event, false))
    }

    pub fn list_events(
        &self,
        session_id: UnifiedSessionId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT session_id, seq, event_id, turn_id, origin_provider_json, kind,
                    visibility_json, payload_json, content_hash, raw_event_id, created_at,
                    schema_version
             FROM events WHERE session_id = ?1 AND seq > ?2 ORDER BY seq LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![
                session_id.to_string(),
                after_seq,
                i64::try_from(limit.max(1)).unwrap_or(i64::MAX)
            ],
            event_from_row,
        )?;
        collect_rows(rows)
    }

    /// Returns true when the latest native-import start marker has no later
    /// completion marker. Runtime startup uses this journal boundary to avoid
    /// exposing a partially imported canonical transcript.
    pub fn has_incomplete_native_import(&self, session_id: UnifiedSessionId) -> Result<bool> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT
                    COALESCE(MAX(CASE WHEN kind = 'native_session_import_started' THEN seq END), 0)
                    >
                    COALESCE(MAX(CASE WHEN kind = 'native_session_imported' THEN seq END), 0)
                 FROM events WHERE session_id = ?1",
                [session_id.to_string()],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn event_by_id(&self, id: EventId) -> Result<Option<CanonicalEvent>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT session_id, seq, event_id, turn_id, origin_provider_json, kind,
                        visibility_json, payload_json, content_hash, raw_event_id, created_at,
                        schema_version
                 FROM events WHERE event_id = ?1",
                [id.to_string()],
                event_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Indexed lookup for an idempotent native-hook family. The base key is a
    /// provider-generated digest stored inside the bounded canonical payload.
    pub fn has_native_hook_base_key(
        &self,
        session_id: UnifiedSessionId,
        base_key: &str,
    ) -> Result<bool> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM events
                    WHERE session_id = ?1
                      AND json_extract(payload_json, '$.native_hook.base_key') = ?2
                 )",
                params![session_id.to_string(), base_key],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn raw_event(&self, id: EventId) -> Result<Option<RawProviderEvent>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT event_id, session_id, turn_id, provider_json, kind, payload_json,
                        content_hash, created_at FROM raw_provider_events WHERE event_id = ?1",
                [id.to_string()],
                |row| {
                    Ok(RawProviderEvent {
                        event_id: parse_id(row.get::<_, String>(0)?, 0)?,
                        session_id: parse_id(row.get::<_, String>(1)?, 1)?,
                        turn_id: parse_optional_id(row.get(2)?, 2)?,
                        provider: decode_row(row.get::<_, String>(3)?, 3)?,
                        kind: row.get(4)?,
                        payload: decode_row(row.get::<_, String>(5)?, 5)?,
                        content_hash: row.get(6)?,
                        created_at: parse_timestamp(row.get(7)?, 7)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn pending_sync_intents(
        &self,
        provider_session_id: ProviderSessionId,
    ) -> Result<Vec<SyncReceiptEntry>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT canonical_event_id, projection_version, native_receipt, state, applied_at
             FROM sync_receipts
             WHERE provider_session_id = ?1 AND state = 'pending'
             ORDER BY applied_at, canonical_event_id, projection_version",
        )?;
        let rows = statement.query_map([provider_session_id.to_string()], |row| {
            Ok(SyncReceiptEntry {
                canonical_event_id: parse_id(row.get(0)?, 0)?,
                projection_version: row.get(1)?,
                native_receipt: row.get(2)?,
                state: row.get(3)?,
                applied_at: parse_timestamp(row.get(4)?, 4)?,
            })
        })?;
        collect_rows(rows)
    }

    /// Persists the send intent before any provider call. A pre-existing pending
    /// intent is deliberately treated as uncertain rather than retried.
    pub fn begin_sync_intents(
        &self,
        provider_session_id: ProviderSessionId,
        intents: &[SyncIntentEntry],
    ) -> Result<usize> {
        if intents.is_empty() {
            return Ok(0);
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let pending: usize = transaction.query_row(
            "SELECT COUNT(*) FROM sync_receipts
             WHERE provider_session_id = ?1 AND state = 'pending'",
            [provider_session_id.to_string()],
            |row| row.get(0),
        )?;
        if pending != 0 {
            return Err(StorageError::InvalidData(format!(
                "provider session {provider_session_id} has {pending} uncertain projection intents"
            )));
        }
        for intent in intents {
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT state FROM sync_receipts
                     WHERE provider_session_id = ?1 AND canonical_event_id = ?2
                       AND projection_version = ?3",
                    params![
                        provider_session_id.to_string(),
                        intent.canonical_event_id.to_string(),
                        intent.projection_version,
                    ],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(state) = existing {
                return Err(StorageError::InvalidData(format!(
                    "sync intent already exists in state {state} for event {}",
                    intent.canonical_event_id
                )));
            }
            transaction.execute(
                "INSERT INTO sync_receipts (
                    provider_session_id, canonical_event_id, projection_version,
                    native_receipt, state, applied_at
                 ) VALUES (?1, ?2, ?3, NULL, 'pending', ?4)",
                params![
                    provider_session_id.to_string(),
                    intent.canonical_event_id.to_string(),
                    intent.projection_version,
                    timestamp(intent.created_at),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(intents.len())
    }

    /// Cancels a provider-declared deferred delivery. No cursor is advanced.
    pub fn cancel_sync_intents(
        &self,
        provider_session_id: ProviderSessionId,
        intents: &[SyncIntentEntry],
    ) -> Result<usize> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let mut removed = 0;
        for intent in intents {
            removed += transaction.execute(
                "DELETE FROM sync_receipts
                 WHERE provider_session_id = ?1 AND canonical_event_id = ?2
                   AND projection_version = ?3 AND state = 'pending'",
                params![
                    provider_session_id.to_string(),
                    intent.canonical_event_id.to_string(),
                    intent.projection_version,
                ],
            )?;
        }
        if removed != intents.len() {
            return Err(StorageError::InvalidData(format!(
                "expected to cancel {} pending sync intents, removed {removed}",
                intents.len()
            )));
        }
        transaction.commit()?;
        Ok(removed)
    }

    /// Atomically turns pending intents into durable receipts and advances the
    /// provider cursor. Failure leaves the pending rows as crash evidence.
    pub fn finalize_sync_intents(
        &self,
        provider_session_id: ProviderSessionId,
        receipts: &[SyncReceiptEntry],
        through_seq: u64,
    ) -> Result<SyncWriteResult> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for receipt in receipts {
            if receipt.state != "applied" {
                return Err(StorageError::InvalidData(
                    "finalized sync receipt state must be applied".to_owned(),
                ));
            }
            let updated = transaction.execute(
                "UPDATE sync_receipts
                 SET native_receipt = ?4, state = 'applied', applied_at = ?5
                 WHERE provider_session_id = ?1 AND canonical_event_id = ?2
                   AND projection_version = ?3 AND state = 'pending'",
                params![
                    provider_session_id.to_string(),
                    receipt.canonical_event_id.to_string(),
                    receipt.projection_version,
                    receipt.native_receipt,
                    timestamp(receipt.applied_at),
                ],
            )?;
            if updated != 1 {
                return Err(StorageError::InvalidData(format!(
                    "pending sync intent is missing for event {}",
                    receipt.canonical_event_id
                )));
            }
        }
        let last_synced_seq =
            update_provider_cursor(&transaction, provider_session_id, through_seq)?;
        transaction.commit()?;
        Ok(SyncWriteResult {
            inserted: receipts.len(),
            duplicate: 0,
            last_synced_seq,
        })
    }

    pub fn advance_provider_cursor(
        &self,
        provider_session_id: ProviderSessionId,
        through_seq: u64,
    ) -> Result<u64> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let pending: usize = transaction.query_row(
            "SELECT COUNT(*) FROM sync_receipts
             WHERE provider_session_id = ?1 AND state = 'pending'",
            [provider_session_id.to_string()],
            |row| row.get(0),
        )?;
        if pending != 0 {
            return Err(StorageError::InvalidData(format!(
                "provider session {provider_session_id} has uncertain projection intents"
            )));
        }
        let last_synced_seq =
            update_provider_cursor(&transaction, provider_session_id, through_seq)?;
        transaction.commit()?;
        Ok(last_synced_seq)
    }

    pub fn record_sync_receipts(
        &self,
        provider_session_id: ProviderSessionId,
        receipts: &[SyncReceiptEntry],
        through_seq: u64,
    ) -> Result<SyncWriteResult> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let pending: usize = transaction.query_row(
            "SELECT COUNT(*) FROM sync_receipts
             WHERE provider_session_id = ?1 AND state = 'pending'",
            [provider_session_id.to_string()],
            |row| row.get(0),
        )?;
        if pending != 0 {
            return Err(StorageError::InvalidData(format!(
                "provider session {provider_session_id} has uncertain projection intents"
            )));
        }
        let mut inserted = 0;
        for receipt in receipts {
            if receipt.state != "applied" {
                return Err(StorageError::InvalidData(
                    "recorded sync receipt state must be applied".to_owned(),
                ));
            }
            inserted += transaction.execute(
                "INSERT OR IGNORE INTO sync_receipts (
                    provider_session_id, canonical_event_id, projection_version,
                    native_receipt, state, applied_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    provider_session_id.to_string(),
                    receipt.canonical_event_id.to_string(),
                    receipt.projection_version,
                    receipt.native_receipt,
                    receipt.state,
                    timestamp(receipt.applied_at),
                ],
            )?;
        }
        let last_synced_seq =
            update_provider_cursor(&transaction, provider_session_id, through_seq)?;
        transaction.commit()?;
        Ok(SyncWriteResult {
            inserted,
            duplicate: receipts.len().saturating_sub(inserted),
            last_synced_seq,
        })
    }

    pub fn has_sync_receipt(
        &self,
        provider_session_id: ProviderSessionId,
        event_id: EventId,
        projection_version: u32,
    ) -> Result<bool> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sync_receipts
                 WHERE provider_session_id = ?1 AND canonical_event_id = ?2
                   AND projection_version = ?3 AND state = 'applied')",
                params![
                    provider_session_id.to_string(),
                    event_id.to_string(),
                    projection_version
                ],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn record_health(
        &self,
        session_id: Option<UnifiedSessionId>,
        health: &ProviderHealth,
    ) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO provider_health_snapshots
             (session_id, provider_json, health_json, checked_at) VALUES (?1, ?2, ?3, ?4)",
            params![
                session_id.map(|id| id.to_string()),
                encode(&health.provider)?,
                encode(health)?,
                timestamp(health.checked_at),
            ],
        )?;
        Ok(())
    }

    pub fn latest_health(&self, provider: &ProviderKind) -> Result<Option<ProviderHealth>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT health_json FROM provider_health_snapshots
                 WHERE provider_json = ?1 ORDER BY checked_at DESC, id DESC LIMIT 1",
                [encode(provider)?],
                |row| decode_row(row.get::<_, String>(0)?, 0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn recent_failure_count(
        &self,
        session_id: UnifiedSessionId,
        provider: &ProviderKind,
        since: DateTime<Utc>,
    ) -> Result<u32> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT health_json FROM provider_health_snapshots
             WHERE session_id = ?1 AND provider_json = ?2 AND checked_at >= ?3
             ORDER BY checked_at",
        )?;
        let rows = statement.query_map(
            params![session_id.to_string(), encode(provider)?, timestamp(since)],
            |row| decode_row::<ProviderHealth>(row.get::<_, String>(0)?, 0),
        )?;
        let mut failures = 0_u32;
        for row in rows {
            let health = row?;
            if matches!(
                health.status,
                ProviderStatus::Exhausted { .. }
                    | ProviderStatus::Overloaded
                    | ProviderStatus::AuthError
                    | ProviderStatus::Offline
                    | ProviderStatus::Incompatible
            ) {
                failures = failures.saturating_add(1);
            }
        }
        Ok(failures)
    }

    pub fn record_workspace_snapshot(&self, record: &WorkspaceSnapshotRecord) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO workspace_snapshots
             (id, session_id, turn_id, phase, fingerprint, snapshot_json, diff_digest, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                record.id.to_string(),
                record.session_id.to_string(),
                record.turn_id.map(|id| id.to_string()),
                record.phase,
                record.fingerprint,
                encode(&record.snapshot)?,
                record.diff_digest,
                timestamp(record.created_at),
            ],
        )?;
        Ok(())
    }

    pub fn list_workspace_snapshots(
        &self,
        session_id: UnifiedSessionId,
        turn_id: Option<TurnId>,
    ) -> Result<Vec<WorkspaceSnapshotRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, session_id, turn_id, phase, fingerprint, snapshot_json,
                    diff_digest, created_at
             FROM workspace_snapshots
             WHERE session_id = ?1 AND (?2 IS NULL OR turn_id = ?2)
             ORDER BY created_at, id",
        )?;
        let rows = statement.query_map(
            params![session_id.to_string(), turn_id.map(|id| id.to_string())],
            workspace_snapshot_from_row,
        )?;
        collect_rows(rows)
    }

    pub fn record_checkpoint(&self, record: &ContextCheckpointRecord) -> Result<bool> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "INSERT OR IGNORE INTO context_checkpoints
             (id, session_id, through_seq, projection_version, checkpoint_json,
              content_hash, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                record.id.to_string(),
                record.session_id.to_string(),
                record.through_seq,
                record.projection_version,
                encode(&record.checkpoint)?,
                record.content_hash,
                timestamp(record.created_at),
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn latest_checkpoint(
        &self,
        session_id: UnifiedSessionId,
    ) -> Result<Option<ContextCheckpointRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id, session_id, through_seq, projection_version, checkpoint_json,
                        content_hash, created_at
                 FROM context_checkpoints WHERE session_id = ?1
                 ORDER BY through_seq DESC, projection_version DESC LIMIT 1",
                [session_id.to_string()],
                |row| {
                    Ok(ContextCheckpointRecord {
                        id: parse_id(row.get::<_, String>(0)?, 0)?,
                        session_id: parse_id(row.get::<_, String>(1)?, 1)?,
                        through_seq: row.get(2)?,
                        projection_version: row.get(3)?,
                        checkpoint: decode_row(row.get::<_, String>(4)?, 4)?,
                        content_hash: row.get(5)?,
                        created_at: parse_timestamp(row.get(6)?, 6)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn record_protocol_capability(&self, record: &ProtocolCapabilityRecord) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO protocol_capabilities
             (provider_json, native_version, capability, supported, evidence_json, probed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(provider_json, native_version, capability) DO UPDATE SET
                supported = excluded.supported,
                evidence_json = excluded.evidence_json,
                probed_at = excluded.probed_at",
            params![
                encode(&record.provider)?,
                record.native_version,
                record.capability,
                record.supported,
                encode(&record.evidence)?,
                timestamp(record.probed_at),
            ],
        )?;
        Ok(())
    }

    pub fn register_blob(&self, blob: &BlobRef, created_at: DateTime<Utc>) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "INSERT OR IGNORE INTO blobs
             (digest, size, compression, media_type, redacted, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                blob.digest,
                blob.size,
                blob.compression,
                blob.media_type,
                blob.redacted,
                timestamp(created_at),
            ],
        )?;
        Ok(())
    }

    pub fn list_registered_blobs_before(&self, cutoff: DateTime<Utc>) -> Result<Vec<BlobRef>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT digest, size, compression, media_type, redacted
             FROM blobs WHERE created_at < ?1 ORDER BY digest",
        )?;
        let rows = statement.query_map([timestamp(cutoff)], |row| {
            Ok(BlobRef {
                digest: row.get(0)?,
                size: row.get(1)?,
                compression: row.get(2)?,
                media_type: row.get(3)?,
                redacted: row.get(4)?,
            })
        })?;
        collect_rows(rows)
    }

    pub fn unregister_blob(&self, digest: &str) -> Result<bool> {
        let connection = self.connection()?;
        Ok(connection.execute("DELETE FROM blobs WHERE digest = ?1", [digest])? == 1)
    }

    /// Copies the canonical prefix into an already-created child session using fresh event IDs.
    /// Provider-native raw events are intentionally not copied; the child projections are rebuilt.
    pub fn copy_events_for_fork(
        &self,
        parent_session_id: UnifiedSessionId,
        child_session_id: UnifiedSessionId,
        through_seq: u64,
    ) -> Result<usize> {
        let events = self.list_events(parent_session_id, 0, usize::MAX)?;
        let selected: Vec<_> = events
            .into_iter()
            .filter(|event| event.seq <= through_seq)
            .collect();
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for event in &selected {
            transaction.execute(
                "INSERT INTO events (
                    session_id, seq, event_id, turn_id, origin_provider_json, kind,
                    visibility_json, payload_json, content_hash, raw_event_id, created_at,
                    schema_version
                 ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10)",
                params![
                    child_session_id.to_string(),
                    event.seq,
                    EventId::new().to_string(),
                    encode_option(event.origin_provider.as_ref())?,
                    event.kind,
                    encode(&event.visibility)?,
                    encode(&event.payload)?,
                    event.content_hash,
                    timestamp(event.created_at),
                    event.schema_version,
                ],
            )?;
        }
        transaction.execute(
            "INSERT INTO session_branches
             (child_session_id, parent_session_id, through_seq, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                child_session_id.to_string(),
                parent_session_id.to_string(),
                through_seq,
                timestamp(Utc::now()),
            ],
        )?;
        transaction.commit()?;
        Ok(selected.len())
    }

    pub fn fork_session(
        &self,
        parent_session_id: UnifiedSessionId,
        child: &UnifiedSession,
        through_seq: u64,
    ) -> Result<usize> {
        if child.parent_session_id != Some(parent_session_id) {
            return Err(StorageError::InvalidData(
                "forked session must reference its parent".to_owned(),
            ));
        }
        if let Some(launch) = self
            .open_native_launches(parent_session_id)?
            .into_iter()
            .next()
        {
            return Err(StorageError::InvalidData(format!(
                "cannot fork session {parent_session_id} while native launch {} is {:?}",
                launch.id, launch.state
            )));
        }
        self.create_session(child)?;
        match self.copy_events_for_fork(parent_session_id, child.id, through_seq) {
            Ok(count) => Ok(count),
            Err(error) => {
                let _ = self.delete_session(child.id);
                Err(error)
            }
        }
    }

    /// Drops provider-native session references and their receipts. The canonical log is
    /// untouched; the next use creates fresh native sessions before lazily rebuilding each
    /// projection. Reusing an existing native session here would duplicate its transcript.
    pub fn rebuild_projection_state(
        &self,
        session_id: UnifiedSessionId,
    ) -> Result<ProjectionRebuildResult> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_no_open_native_launch(&transaction, session_id, "rebuild projections for")?;
        let receipts_removed: usize = transaction.query_row(
            "SELECT COUNT(*) FROM sync_receipts WHERE provider_session_id IN
             (SELECT id FROM provider_sessions WHERE unified_session_id = ?1)",
            [session_id.to_string()],
            |row| row.get(0),
        )?;
        let pending_intents_removed: usize = transaction.query_row(
            "SELECT COUNT(*) FROM sync_receipts WHERE state = 'pending'
             AND provider_session_id IN
             (SELECT id FROM provider_sessions WHERE unified_session_id = ?1)",
            [session_id.to_string()],
            |row| row.get(0),
        )?;
        let provider_sessions_removed = transaction.execute(
            "DELETE FROM provider_sessions WHERE unified_session_id = ?1",
            [session_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(ProjectionRebuildResult {
            provider_sessions_removed,
            receipts_removed,
            pending_intents_removed,
        })
    }

    pub fn repair_indexes(&self) -> Result<()> {
        let connection = self.connection()?;
        connection.execute_batch("REINDEX; PRAGMA optimize;")?;
        Ok(())
    }

    fn migrate(&self) -> Result<()> {
        let mut connection = self.connection()?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );",
        )?;
        let current: u32 = connection.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?;
        if current > CURRENT_SCHEMA_VERSION {
            return Err(StorageError::UnsupportedSchema {
                found: current,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }
        if current < 1 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(INITIAL_MIGRATION)?;
            transaction.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?1)",
                [timestamp(Utc::now())],
            )?;
            transaction.commit()?;
        }
        if current < 2 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(NATIVE_CLI_MIGRATION)?;
            transaction.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (2, ?1)",
                [timestamp(Utc::now())],
            )?;
            transaction.commit()?;
        }
        Ok(())
    }

    fn connection(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA temp_store = MEMORY;",
        )?;
        set_mode(&self.path, 0o600)?;
        for suffix in ["-wal", "-shm"] {
            let auxiliary = PathBuf::from(format!("{}{suffix}", self.path.display()));
            if auxiliary.exists() {
                set_mode(&auxiliary, 0o600)?;
            }
        }
        Ok(connection)
    }
}

pub fn canonical_content_hash(
    kind: &str,
    visibility: EventVisibility,
    payload: &serde_json::Value,
) -> Result<String> {
    #[derive(Serialize)]
    struct HashInput<'a> {
        kind: &'a str,
        visibility: EventVisibility,
        payload: &'a serde_json::Value,
    }
    Ok(sha256_digest(&serde_json::to_vec(&HashInput {
        kind,
        visibility,
        payload,
    })?))
}

fn update_provider_cursor(
    transaction: &Transaction<'_>,
    provider_session_id: ProviderSessionId,
    through_seq: u64,
) -> Result<u64> {
    expect_one(
        transaction.execute(
            "UPDATE provider_sessions
             SET last_synced_seq = MAX(last_synced_seq, ?2), updated_at = ?3
             WHERE id = ?1",
            params![
                provider_session_id.to_string(),
                through_seq,
                timestamp(Utc::now())
            ],
        )?,
        format!("provider session {provider_session_id}"),
    )?;
    transaction
        .query_row(
            "SELECT last_synced_seq FROM provider_sessions WHERE id = ?1",
            [provider_session_id.to_string()],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn insert_raw_event(transaction: &Transaction<'_>, raw: &RawProviderEvent) -> Result<()> {
    transaction.execute(
        "INSERT INTO raw_provider_events
         (event_id, session_id, turn_id, provider_json, kind, payload_json, content_hash, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            raw.event_id.to_string(),
            raw.session_id.to_string(),
            raw.turn_id.map(|id| id.to_string()),
            encode(&raw.provider)?,
            raw.kind,
            encode(&raw.payload)?,
            raw.content_hash,
            timestamp(raw.created_at),
        ],
    )?;
    Ok(())
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CanonicalEvent> {
    Ok(CanonicalEvent {
        session_id: parse_id(row.get::<_, String>(0)?, 0)?,
        seq: row.get(1)?,
        event_id: parse_id(row.get::<_, String>(2)?, 2)?,
        turn_id: parse_optional_id(row.get(3)?, 3)?,
        origin_provider: decode_optional_row(row.get(4)?, 4)?,
        kind: row.get(5)?,
        visibility: decode_row(row.get::<_, String>(6)?, 6)?,
        payload: decode_row(row.get::<_, String>(7)?, 7)?,
        content_hash: row.get(8)?,
        raw_event_id: parse_optional_id(row.get(9)?, 9)?,
        created_at: parse_timestamp(row.get(10)?, 10)?,
        schema_version: row.get(11)?,
    })
}

fn session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<UnifiedSession> {
    Ok(UnifiedSession {
        id: parse_id(row.get::<_, String>(0)?, 0)?,
        name: row.get(1)?,
        workspace_path: PathBuf::from(row.get::<_, String>(2)?),
        workspace_fingerprint: row.get(3)?,
        active_provider: decode_optional_row(row.get(4)?, 4)?,
        routing_policy: row.get(5)?,
        auth_mode: decode_row::<AuthMode>(row.get(6)?, 6)?,
        status: decode_row(row.get(7)?, 7)?,
        parent_session_id: parse_optional_id(row.get(8)?, 8)?,
        created_at: parse_timestamp(row.get(9)?, 9)?,
        updated_at: parse_timestamp(row.get(10)?, 10)?,
        schema_version: row.get(11)?,
    })
}

fn provider_session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProviderSessionRecord> {
    Ok(ProviderSessionRecord {
        id: parse_id(row.get::<_, String>(0)?, 0)?,
        unified_session_id: parse_id(row.get::<_, String>(1)?, 1)?,
        provider: decode_row(row.get::<_, String>(2)?, 2)?,
        native_session_id: row.get(3)?,
        native_version: row.get(4)?,
        last_synced_seq: row.get(5)?,
        status: decode_row(row.get::<_, String>(6)?, 6)?,
        reset_at: parse_optional_timestamp(row.get(7)?, 7)?,
        capabilities: decode_row(row.get::<_, String>(8)?, 8)?,
        metadata: decode_row(row.get::<_, String>(9)?, 9)?,
        created_at: parse_timestamp(row.get(10)?, 10)?,
        updated_at: parse_timestamp(row.get(11)?, 11)?,
    })
}

fn native_launch_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<NativeLaunchRecord> {
    Ok(NativeLaunchRecord {
        id: parse_id(row.get::<_, String>(0)?, 0)?,
        session_id: parse_id(row.get::<_, String>(1)?, 1)?,
        provider: decode_row(row.get::<_, String>(2)?, 2)?,
        native_session_id: row.get(3)?,
        workspace_lease_key: row.get(4)?,
        child_pid: row.get(5)?,
        state: NativeLaunchState::parse(&row.get::<_, String>(6)?)?,
        exit_code: row.get(7)?,
        error: row.get(8)?,
        metadata: decode_row(row.get::<_, String>(9)?, 9)?,
        started_at: parse_timestamp(row.get(10)?, 10)?,
        updated_at: parse_timestamp(row.get(11)?, 11)?,
    })
}

fn native_handoff_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<NativeHandoffRecord> {
    Ok(NativeHandoffRecord {
        launch_id: parse_id(row.get::<_, String>(0)?, 0)?,
        provider_session_id: parse_id(row.get::<_, String>(1)?, 1)?,
        session_id: parse_id(row.get::<_, String>(2)?, 2)?,
        native_session_id: row.get(3)?,
        through_seq: row.get(4)?,
        capsule: row.get(5)?,
        content_digest: row.get(6)?,
        state: NativeHandoffState::parse(&row.get::<_, String>(7)?)?,
        created_at: parse_timestamp(row.get(8)?, 8)?,
        updated_at: parse_timestamp(row.get(9)?, 9)?,
    })
}

fn turn_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnRecord> {
    Ok(TurnRecord {
        id: parse_id(row.get::<_, String>(0)?, 0)?,
        session_id: parse_id(row.get::<_, String>(1)?, 1)?,
        provider: decode_optional_row(row.get(2)?, 2)?,
        prompt_seq: row.get(3)?,
        status: decode_row(row.get::<_, String>(4)?, 4)?,
        side_effect_state: decode_row(row.get::<_, String>(5)?, 5)?,
        native_turn_id: row.get(6)?,
        continuation: row.get(7)?,
        started_at: parse_optional_timestamp(row.get(8)?, 8)?,
        completed_at: parse_optional_timestamp(row.get(9)?, 9)?,
        created_at: parse_timestamp(row.get(10)?, 10)?,
        updated_at: parse_timestamp(row.get(11)?, 11)?,
    })
}

fn workspace_snapshot_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<WorkspaceSnapshotRecord> {
    Ok(WorkspaceSnapshotRecord {
        id: parse_id(row.get::<_, String>(0)?, 0)?,
        session_id: parse_id(row.get::<_, String>(1)?, 1)?,
        turn_id: parse_optional_id(row.get(2)?, 2)?,
        phase: row.get(3)?,
        fingerprint: row.get(4)?,
        snapshot: decode_row(row.get::<_, String>(5)?, 5)?,
        diff_digest: row.get(6)?,
        created_at: parse_timestamp(row.get(7)?, 7)?,
    })
}

fn encode<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(Into::into)
}

fn encode_option<T: Serialize + ?Sized>(value: Option<&T>) -> Result<Option<String>> {
    value.map(encode).transpose()
}

#[allow(clippy::needless_pass_by_value)]
fn decode_row<T: DeserializeOwned>(value: String, column: usize) -> rusqlite::Result<T> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

fn decode_optional_row<T: DeserializeOwned>(
    value: Option<String>,
    column: usize,
) -> rusqlite::Result<Option<T>> {
    value.map(|item| decode_row(item, column)).transpose()
}

#[allow(clippy::needless_pass_by_value)]
fn parse_id<T>(value: String, column: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

fn parse_optional_id<T>(value: Option<String>, column: usize) -> rusqlite::Result<Option<T>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value.map(|item| parse_id(item, column)).transpose()
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

#[allow(clippy::needless_pass_by_value)]
fn parse_timestamp(value: String, column: usize) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|date| date.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
        })
}

fn parse_optional_timestamp(
    value: Option<String>,
    column: usize,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    value.map(|item| parse_timestamp(item, column)).transpose()
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>> {
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn ensure_no_open_native_launch(
    transaction: &Transaction<'_>,
    session_id: UnifiedSessionId,
    operation: &str,
) -> Result<()> {
    let open = transaction
        .query_row(
            "SELECT id, state FROM native_launches
             WHERE session_id = ?1 AND state IN ('started', 'capture_ready', 'exited', 'uncertain')
             ORDER BY started_at DESC, id DESC LIMIT 1",
            [session_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((launch_id, state)) = open {
        return Err(StorageError::InvalidData(format!(
            "cannot {operation} session {session_id} while native launch {launch_id} is {state}"
        )));
    }
    Ok(())
}

fn expect_one(changed: usize, name: String) -> Result<()> {
    if changed == 1 {
        Ok(())
    } else {
        Err(StorageError::NotFound(name))
    }
}

fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn validate_json(value: &serde_json::Value, limits: StorageLimits) -> Result<()> {
    if serde_json::to_vec(value)?.len() > limits.max_event_bytes {
        return Err(StorageError::EventTooLarge(limits.max_event_bytes));
    }
    let mut stack = vec![(value, 1_usize)];
    while let Some((current, depth)) = stack.pop() {
        if depth > limits.max_json_depth {
            return Err(StorageError::JsonTooDeep(limits.max_json_depth));
        }
        match current {
            serde_json::Value::Array(items) => {
                stack.extend(items.iter().map(|item| (item, depth.saturating_add(1))));
            }
            serde_json::Value::Object(items) => {
                stack.extend(items.values().map(|item| (item, depth.saturating_add(1))));
            }
            _ => {}
        }
    }
    Ok(())
}

fn create_private_dir(path: &Path) -> Result<()> {
    reject_symlink(path)?;
    fs::create_dir_all(path).map_err(|error| io_error(path, error))?;
    set_mode(path, 0o700)
}

fn reject_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(StorageError::UnsafeSymlink(path.to_path_buf()))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(path, error)),
    }
}

fn create_private_file(path: &Path) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map(|_| ())
        .map_err(|error| io_error(path, error))
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .map_err(|error| io_error(path, error))
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use agentctl_core::{EventVisibility, ProviderKind};

    use super::*;

    fn session() -> UnifiedSession {
        let now = Utc::now();
        UnifiedSession {
            id: UnifiedSessionId::new(),
            name: "test".to_owned(),
            workspace_path: PathBuf::from("/tmp/test"),
            workspace_fingerprint: "sha256:test".to_owned(),
            active_provider: Some(ProviderKind::Codex),
            routing_policy: "sticky-balanced".to_owned(),
            auth_mode: AuthMode::NativeLocal,
            status: SessionStatus::Active,
            parent_session_id: None,
            created_at: now,
            updated_at: now,
            schema_version: 1,
        }
    }

    fn event(session_id: UnifiedSessionId, seq: u64) -> CanonicalEvent {
        let payload = serde_json::json!({"text": format!("event {seq}")});
        CanonicalEvent {
            schema_version: 1,
            session_id,
            seq,
            event_id: EventId::new(),
            turn_id: None,
            origin_provider: None,
            kind: "user_prompt".to_owned(),
            visibility: EventVisibility::User,
            content_hash: canonical_content_hash("user_prompt", EventVisibility::User, &payload)
                .unwrap(),
            payload,
            raw_event_id: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn creates_schema_in_wal_mode() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        assert_eq!(store.schema_version().unwrap(), 2);
        assert_eq!(store.integrity_check().unwrap(), "ok");
        let connection = store.connection().unwrap();
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    #[test]
    fn native_launch_journal_survives_reopen_until_capture() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("agentctl.db");
        let store = SqliteStore::open(&database).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let id = uuid::Uuid::now_v7();
        let now = Utc::now();
        store
            .start_native_launch(&NativeLaunchRecord {
                id,
                session_id: session.id,
                provider: ProviderKind::Codex,
                native_session_id: "thread-native".to_owned(),
                workspace_lease_key: "lease:test".to_owned(),
                child_pid: None,
                state: NativeLaunchState::Started,
                exit_code: None,
                error: None,
                metadata: serde_json::json!({"preserved": true}),
                started_at: now,
                updated_at: now,
            })
            .unwrap();
        let evidence = serde_json::json!({"selected_threads": ["thread-native"], "complete": true});
        store.update_codex_launch_evidence(id, &evidence).unwrap();
        drop(store);

        let reopened = SqliteStore::open(&database).unwrap();
        assert_eq!(
            reopened.native_launch(id).unwrap().unwrap().metadata["codex_evidence"],
            evidence
        );
        assert_eq!(
            reopened.native_launch(id).unwrap().unwrap().metadata["preserved"],
            true
        );
        assert_eq!(reopened.open_native_launches(session.id).unwrap().len(), 1);
        reopened
            .update_native_launch(id, NativeLaunchState::Exited, Some(130), None, Utc::now())
            .unwrap();
        assert_eq!(
            reopened
                .latest_open_native_launch(session.id, &ProviderKind::Codex)
                .unwrap()
                .unwrap()
                .state,
            NativeLaunchState::Exited
        );
        reopened
            .update_native_launch(id, NativeLaunchState::Captured, Some(130), None, Utc::now())
            .unwrap();
        assert!(
            reopened
                .update_codex_launch_evidence(id, &evidence)
                .is_err()
        );
        assert!(
            reopened
                .open_native_launches(session.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn destructive_storage_mutations_reject_open_native_launches() {
        for state in [
            NativeLaunchState::Started,
            NativeLaunchState::CaptureReady,
            NativeLaunchState::Exited,
            NativeLaunchState::Uncertain,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
            let parent = session();
            store.create_session(&parent).unwrap();
            let id = uuid::Uuid::now_v7();
            let now = Utc::now();
            store
                .start_native_launch(&NativeLaunchRecord {
                    id,
                    session_id: parent.id,
                    provider: ProviderKind::Codex,
                    native_session_id: format!("native-{id}"),
                    workspace_lease_key: format!("lease-{id}"),
                    child_pid: None,
                    state: NativeLaunchState::Started,
                    exit_code: None,
                    error: None,
                    metadata: serde_json::json!({}),
                    started_at: now,
                    updated_at: now,
                })
                .unwrap();
            if state != NativeLaunchState::Started {
                store
                    .update_native_launch(id, state, None, None, Utc::now())
                    .unwrap();
            }

            let delete_error = store.delete_session(parent.id).unwrap_err().to_string();
            assert!(delete_error.contains("cannot delete session"));
            assert!(delete_error.contains(&id.to_string()));
            assert!(store.get_session(parent.id).unwrap().is_some());

            let rebuild_error = store
                .rebuild_projection_state(parent.id)
                .unwrap_err()
                .to_string();
            assert!(rebuild_error.contains("cannot rebuild projections for session"));
            assert!(rebuild_error.contains(&id.to_string()));

            let mut child = session();
            child.parent_session_id = Some(parent.id);
            let fork_error = store
                .fork_session(parent.id, &child, 0)
                .unwrap_err()
                .to_string();
            assert!(fork_error.contains("cannot fork session"));
            assert!(fork_error.contains(&id.to_string()));
            assert!(store.get_session(child.id).unwrap().is_none());
        }
    }

    #[test]
    fn terminal_native_launch_does_not_block_session_deletion() {
        for state in [NativeLaunchState::Captured, NativeLaunchState::Failed] {
            let directory = tempfile::tempdir().unwrap();
            let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
            let session = session();
            store.create_session(&session).unwrap();
            let id = uuid::Uuid::now_v7();
            let now = Utc::now();
            store
                .start_native_launch(&NativeLaunchRecord {
                    id,
                    session_id: session.id,
                    provider: ProviderKind::Claude,
                    native_session_id: format!("native-{id}"),
                    workspace_lease_key: format!("lease-{id}"),
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
                .update_native_launch(id, state, Some(0), None, Utc::now())
                .unwrap();
            store.delete_session(session.id).unwrap();
            assert!(store.get_session(session.id).unwrap().is_none());
        }
    }

    #[test]
    fn schema_one_database_upgrades_to_native_bridge_schema_two() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("agentctl.db");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                 );",
            )
            .unwrap();
        connection.execute_batch(INITIAL_MIGRATION).unwrap();
        connection
            .execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?1)",
                [timestamp(Utc::now())],
            )
            .unwrap();
        drop(connection);

        let upgraded = SqliteStore::open(&database).unwrap();
        assert_eq!(upgraded.schema_version().unwrap(), 2);
        let table_exists: bool = upgraded
            .connection()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'native_launches')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(table_exists);
    }

    #[test]
    fn events_are_monotonic_and_append_only() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        store.append_event(&event(session.id, 1), None).unwrap();
        let error = store.append_event(&event(session.id, 3), None).unwrap_err();
        assert!(matches!(
            error,
            StorageError::InvalidSequence {
                expected: 2,
                actual: 3
            }
        ));
        let duplicate = store.append_event(&event(session.id, 1), None).unwrap_err();
        assert!(matches!(
            duplicate,
            StorageError::InvalidSequence {
                expected: 2,
                actual: 1
            }
        ));
        assert_eq!(store.list_events(session.id, 0, 10).unwrap().len(), 1);
        let connection = store.connection().unwrap();
        assert!(
            connection
                .execute(
                    "UPDATE events SET kind = 'changed' WHERE session_id = ?1",
                    [session.id.to_string()]
                )
                .is_err()
        );
    }

    #[test]
    fn concurrent_native_hooks_allocate_sequences_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let session_id = session.id;
        let store = std::sync::Arc::new(store);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(12));
        let workers = (0..12)
            .map(|index| {
                let store = std::sync::Arc::clone(&store);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let payload = serde_json::json!({"tool_use_id": format!("tool-{index}")});
                    let event = CanonicalEvent {
                        schema_version: 1,
                        session_id,
                        seq: 0,
                        event_id: EventId::new(),
                        turn_id: None,
                        origin_provider: Some(ProviderKind::Claude),
                        kind: "tool_completed".to_owned(),
                        visibility: EventVisibility::User,
                        content_hash: canonical_content_hash(
                            "tool_completed",
                            EventVisibility::User,
                            &payload,
                        )
                        .unwrap(),
                        payload,
                        raw_event_id: None,
                        created_at: Utc::now(),
                    };
                    barrier.wait();
                    store.append_event_allocating_seq(event, None).unwrap().seq
                })
            })
            .collect::<Vec<_>>();
        let mut sequences = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=12).collect::<Vec<_>>());
    }

    #[test]
    fn receipts_are_idempotent_and_sync_never_regresses() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let event = event(session.id, 1);
        store.append_event(&event, None).unwrap();
        let provider_id = ProviderSessionId::new();
        let now = Utc::now();
        store
            .upsert_provider_session(&ProviderSessionRecord {
                id: provider_id,
                unified_session_id: session.id,
                provider: ProviderKind::Codex,
                native_session_id: "thread".to_owned(),
                native_version: Some("1".to_owned()),
                last_synced_seq: 0,
                status: ProviderStatus::Ready,
                reset_at: None,
                capabilities: BTreeMap::new(),
                metadata: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        let receipt = SyncReceiptEntry {
            canonical_event_id: event.event_id,
            projection_version: 1,
            native_receipt: None,
            state: "applied".to_owned(),
            applied_at: now,
        };
        let first = store
            .record_sync_receipts(provider_id, std::slice::from_ref(&receipt), 1)
            .unwrap();
        let second = store
            .record_sync_receipts(provider_id, &[receipt], 0)
            .unwrap();
        assert_eq!(first.inserted, 1);
        assert_eq!(second.duplicate, 1);
        assert_eq!(second.last_synced_seq, 1);
    }

    #[test]
    fn pending_sync_intent_blocks_cursor_until_atomic_finalize() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let event = event(session.id, 1);
        store.append_event(&event, None).unwrap();
        let provider_id = ProviderSessionId::new();
        let now = Utc::now();
        store
            .upsert_provider_session(&ProviderSessionRecord {
                id: provider_id,
                unified_session_id: session.id,
                provider: ProviderKind::Codex,
                native_session_id: "thread".to_owned(),
                native_version: Some("1".to_owned()),
                last_synced_seq: 0,
                status: ProviderStatus::Ready,
                reset_at: None,
                capabilities: BTreeMap::new(),
                metadata: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        let intent = SyncIntentEntry {
            canonical_event_id: event.event_id,
            projection_version: 1,
            created_at: now,
        };
        assert_eq!(
            store
                .begin_sync_intents(provider_id, std::slice::from_ref(&intent))
                .unwrap(),
            1
        );
        assert_eq!(store.pending_sync_intents(provider_id).unwrap().len(), 1);
        assert!(
            !store
                .has_sync_receipt(provider_id, event.event_id, 1)
                .unwrap()
        );
        assert!(store.advance_provider_cursor(provider_id, 1).is_err());

        let result = store
            .finalize_sync_intents(
                provider_id,
                &[SyncReceiptEntry {
                    canonical_event_id: event.event_id,
                    projection_version: 1,
                    native_receipt: Some("native-applied".to_owned()),
                    state: "applied".to_owned(),
                    applied_at: now,
                }],
                1,
            )
            .unwrap();
        assert_eq!(result.last_synced_seq, 1);
        assert!(store.pending_sync_intents(provider_id).unwrap().is_empty());
        assert!(
            store
                .has_sync_receipt(provider_id, event.event_id, 1)
                .unwrap()
        );
    }

    #[test]
    fn projection_rebuild_discards_native_sessions_before_reprojection() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let event = event(session.id, 1);
        store.append_event(&event, None).unwrap();
        let provider_id = ProviderSessionId::new();
        let now = Utc::now();
        store
            .upsert_provider_session(&ProviderSessionRecord {
                id: provider_id,
                unified_session_id: session.id,
                provider: ProviderKind::Codex,
                native_session_id: "existing-native-transcript".to_owned(),
                native_version: Some("1".to_owned()),
                last_synced_seq: 0,
                status: ProviderStatus::Ready,
                reset_at: None,
                capabilities: BTreeMap::new(),
                metadata: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            })
            .unwrap();
        store
            .record_sync_receipts(
                provider_id,
                &[SyncReceiptEntry {
                    canonical_event_id: event.event_id,
                    projection_version: 1,
                    native_receipt: Some("native-receipt".to_owned()),
                    state: "applied".to_owned(),
                    applied_at: now,
                }],
                1,
            )
            .unwrap();

        store
            .begin_sync_intents(
                provider_id,
                &[SyncIntentEntry {
                    canonical_event_id: event.event_id,
                    projection_version: 2,
                    created_at: now,
                }],
            )
            .unwrap();
        let removed = store.rebuild_projection_state(session.id).unwrap();
        assert_eq!(removed.provider_sessions_removed, 1);
        assert_eq!(removed.receipts_removed, 2);
        assert_eq!(removed.pending_intents_removed, 1);
        assert!(
            store
                .provider_session(session.id, &ProviderKind::Codex)
                .unwrap()
                .is_none()
        );
        assert!(
            !store
                .has_sync_receipt(provider_id, event.event_id, 1)
                .unwrap()
        );
    }

    #[test]
    fn reboot_preserves_crash_evidence_and_rebuilds_projection_from_canonical_log() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("agentctl.db");
        let session = session();
        let canonical = event(session.id, 1);
        let provider_id = ProviderSessionId::new();
        let running_turn = TurnId::new();
        let now = Utc::now();
        {
            let store = SqliteStore::open(&database).unwrap();
            store.create_session(&session).unwrap();
            store.append_event(&canonical, None).unwrap();
            store
                .create_turn(&TurnRecord {
                    id: running_turn,
                    session_id: session.id,
                    provider: Some(ProviderKind::Claude),
                    prompt_seq: 1,
                    status: TurnStatus::Running,
                    side_effect_state: SideEffectState::Possible,
                    native_turn_id: Some("native-running-turn".to_owned()),
                    continuation: false,
                    started_at: Some(now),
                    completed_at: None,
                    created_at: now,
                    updated_at: now,
                })
                .unwrap();
            store
                .upsert_provider_session(&ProviderSessionRecord {
                    id: provider_id,
                    unified_session_id: session.id,
                    provider: ProviderKind::Codex,
                    native_session_id: "thr_before_crash".to_owned(),
                    native_version: Some("1".to_owned()),
                    last_synced_seq: 0,
                    status: ProviderStatus::Ready,
                    reset_at: None,
                    capabilities: BTreeMap::new(),
                    metadata: serde_json::json!({}),
                    created_at: now,
                    updated_at: now,
                })
                .unwrap();
            store
                .record_sync_receipts(
                    provider_id,
                    &[SyncReceiptEntry {
                        canonical_event_id: canonical.event_id,
                        projection_version: 1,
                        native_receipt: Some("receipt-before-crash".to_owned()),
                        state: "applied".to_owned(),
                        applied_at: now,
                    }],
                    1,
                )
                .unwrap();
        }

        let reopened = SqliteStore::open(&database).unwrap();
        assert_eq!(reopened.integrity_check().unwrap(), "ok");
        assert_eq!(reopened.list_events(session.id, 0, 10).unwrap().len(), 1);
        assert!(
            reopened
                .has_sync_receipt(provider_id, canonical.event_id, 1)
                .unwrap()
        );
        assert_eq!(
            reopened
                .recovery_candidates()
                .unwrap()
                .iter()
                .map(|turn| turn.id)
                .collect::<Vec<_>>(),
            vec![running_turn]
        );

        let rebuilt = reopened.rebuild_projection_state(session.id).unwrap();
        assert_eq!(rebuilt.provider_sessions_removed, 1);
        assert_eq!(rebuilt.receipts_removed, 1);
        assert!(
            reopened
                .provider_session(session.id, &ProviderKind::Codex)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            reopened.list_events(session.id, 0, 10).unwrap()[0].event_id,
            canonical.event_id
        );
        assert_eq!(reopened.next_seq(session.id).unwrap(), 2);
        assert_eq!(reopened.recovery_candidates().unwrap()[0].id, running_turn);
    }

    #[test]
    fn pending_turns_are_crash_recovery_candidates() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let now = Utc::now();
        let turn_id = TurnId::new();
        store
            .create_turn(&TurnRecord {
                id: turn_id,
                session_id: session.id,
                provider: None,
                prompt_seq: 1,
                status: TurnStatus::Pending,
                side_effect_state: SideEffectState::None,
                native_turn_id: None,
                continuation: false,
                started_at: None,
                completed_at: None,
                created_at: now,
                updated_at: now,
            })
            .unwrap();

        assert_eq!(store.recovery_candidates().unwrap()[0].id, turn_id);
    }

    #[test]
    fn persists_raw_and_normalized_events_together() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let raw = RawProviderEvent::new(
            session.id,
            None,
            ProviderKind::Claude,
            "result",
            serde_json::json!({"result": "ok"}),
        )
        .unwrap();
        let mut event = event(session.id, 1);
        event.raw_event_id = Some(raw.event_id);
        store.append_event(&event, Some(&raw)).unwrap();
        assert_eq!(store.list_events(session.id, 0, 10).unwrap().len(), 1);
        assert_eq!(
            store.raw_event(raw.event_id).unwrap().unwrap().kind,
            "result"
        );
    }

    #[test]
    fn explicit_delete_removes_append_only_session_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let session = session();
        store.create_session(&session).unwrap();
        let raw = RawProviderEvent::new(
            session.id,
            None,
            ProviderKind::Codex,
            "event",
            serde_json::json!({"raw": true}),
        )
        .unwrap();
        let mut event = event(session.id, 1);
        event.raw_event_id = Some(raw.event_id);
        store.append_event(&event, Some(&raw)).unwrap();
        store.delete_session(session.id).unwrap();
        assert!(store.get_session(session.id).unwrap().is_none());
        assert!(store.raw_event(raw.event_id).unwrap().is_none());
    }

    #[test]
    fn fork_copies_canonical_prefix_with_fresh_event_ids() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db")).unwrap();
        let parent = session();
        store.create_session(&parent).unwrap();
        let original = event(parent.id, 1);
        store.append_event(&original, None).unwrap();
        let mut child = session();
        child.parent_session_id = Some(parent.id);
        let copied = store.fork_session(parent.id, &child, 1).unwrap();
        let child_events = store.list_events(child.id, 0, 10).unwrap();
        assert_eq!(copied, 1);
        assert_eq!(child_events.len(), 1);
        assert_ne!(child_events[0].event_id, original.event_id);
        assert_eq!(child_events[0].content_hash, original.content_hash);
        assert_eq!(store.next_seq(child.id).unwrap(), 2);
    }

    #[test]
    fn configured_event_limits_are_enforced() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(directory.path().join("agentctl.db"))
            .unwrap()
            .with_limits(StorageLimits {
                max_event_bytes: 1024,
                max_json_depth: 2,
            });
        let session = session();
        store.create_session(&session).unwrap();
        let mut nested = event(session.id, 1);
        nested.payload = serde_json::json!({"one": {"two": true}});
        assert!(matches!(
            store.append_event(&nested, None),
            Err(StorageError::JsonTooDeep(2))
        ));
    }
}
