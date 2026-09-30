// ABOUTME: Attributes native Codex thread selections to one CLI connection.
// ABOUTME: Persists bounded selection evidence before forwarding native protocol traffic.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct Evidence {
    pub selected_threads: Vec<String>,
    pub pending_requests: usize,
    pub complete: bool,
    pub error: Option<String>,
}

impl Evidence {
    pub(crate) fn validate(&self, mapped: &str) -> Result<()> {
        ensure!(
            self.complete && self.pending_requests == 0 && self.error.is_none(),
            "native Codex thread-selection evidence is incomplete or failed; automatic capture is blocked"
        );
        ensure!(
            !self.selected_threads.is_empty(),
            "native Codex launch has no observed thread selection"
        );
        ensure!(
            self.selected_threads.iter().all(|thread| thread == mapped),
            "Codex selected additional native threads during the mapped launch; resume the original mapped thread before switching providers"
        );
        Ok(())
    }
}

#[derive(Default)]
struct Selections {
    evidence: Evidence,
    pending: std::collections::HashSet<String>,
}

impl Selections {
    fn request(&mut self, message: &serde_json::Value) -> Result<bool> {
        if !matches!(
            message["method"].as_str(),
            Some("thread/start" | "thread/resume" | "thread/fork")
        ) {
            return Ok(false);
        }
        // Codex generates titles in a temporary thread without changing the conversation.
        if message["method"] == "thread/start"
            && message["params"]["ephemeral"] == true
            && message["params"]["threadSource"] == "thread_title"
        {
            return Ok(false);
        }
        let id = message
            .get("id")
            .filter(|id| id.is_string() || id.is_number())
            .ok_or_else(|| anyhow::anyhow!("native Codex selection has no request identity"))?;
        ensure!(
            self.pending.len() < 4096,
            "too many pending native Codex selections"
        );
        ensure!(
            self.pending.insert(id.to_string()),
            "duplicate native Codex selection identity"
        );
        self.evidence.pending_requests = self.pending.len();
        Ok(true)
    }

    fn response(&mut self, message: &serde_json::Value) -> Result<bool> {
        if message.get("method").is_some() {
            return Ok(false);
        }
        let Some(id) = message.get("id") else {
            return Ok(false);
        };
        if !self.pending.contains(&id.to_string()) {
            return Ok(false);
        }
        if message.get("error").is_none() {
            let thread = message["result"]["thread"]["id"]
                .as_str()
                .filter(|id| !id.is_empty() && id.len() <= 256)
                .ok_or_else(|| {
                    anyhow::anyhow!("native Codex selection response has no thread identity")
                })?;
            if !self
                .evidence
                .selected_threads
                .iter()
                .any(|known| known == thread)
            {
                ensure!(
                    self.evidence.selected_threads.len() < 4096,
                    "too many native Codex selected threads"
                );
                self.evidence.selected_threads.push(thread.to_owned());
            }
        }
        self.pending.remove(&id.to_string());
        self.evidence.pending_requests = self.pending.len();
        Ok(true)
    }
}

pub(crate) struct Relay {
    directory: tempfile::TempDir,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl Relay {
    pub(crate) async fn prepare(
        binary: &std::path::Path,
        store: agentctl_storage::AgentctlStore,
        launch_id: uuid::Uuid,
    ) -> Result<Option<Self>> {
        agentctl_provider_codex::interactive_endpoint(binary)
            .await?
            .map(|endpoint| Self::start(&endpoint, store, launch_id))
            .transpose()
    }

    fn start(
        endpoint: &std::path::Path,
        store: agentctl_storage::AgentctlStore,
        launch_id: uuid::Uuid,
    ) -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new().prefix("acl-").tempdir()?;
        let socket = directory.path().join("native.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        store
            .update_codex_launch_evidence(launch_id, &serde_json::to_value(Evidence::default())?)?;
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let endpoint = endpoint.to_path_buf();
        let task = tokio::spawn(async move {
            let mut selections = Selections::default();
            let outcome = async {
                loop {
                    tokio::select! {
                        _ = &mut stopped => return Ok::<_, anyhow::Error>(()),
                        accepted = listener.accept() => {
                            let (stream, _) = accepted?;
                            relay_connection(stream, &endpoint, &mut selections, &store, launch_id).await?;
                        }
                    }
                }
            }.await;
            selections.evidence.complete = outcome.is_ok();
            if outcome.is_err() {
                selections.evidence.error =
                    Some("native Codex relay failed; automatic capture is blocked".to_owned());
            }
            store.update_codex_launch_evidence(
                launch_id,
                &serde_json::to_value(&selections.evidence)?,
            )?;
            outcome
        });
        Ok(Self {
            directory,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub(crate) fn endpoint(&self) -> std::path::PathBuf {
        self.directory.path().join("native.sock")
    }

    pub(crate) async fn finish(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            ensure!(
                stop.send(()).is_ok()
                    || self
                        .task
                        .as_ref()
                        .is_some_and(tokio::task::JoinHandle::is_finished),
                "native Codex relay stopped unexpectedly"
            );
        }
        let task = self
            .task
            .as_mut()
            .expect("relay task exists until completion");
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .map_err(|_| anyhow::anyhow!("native Codex relay did not close after CLI exit"))???;
        Ok(())
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn relay_connection(
    stream: tokio::net::UnixStream,
    endpoint: &std::path::Path,
    selections: &mut Selections,
    store: &agentctl_storage::AgentctlStore,
    launch_id: uuid::Uuid,
) -> Result<()> {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        accept_async_with_config, client_async_with_config,
        tungstenite::{Message, protocol::WebSocketConfig},
    };
    let configuration = WebSocketConfig::default()
        .max_message_size(Some(64 * 1024 * 1024))
        .max_frame_size(Some(64 * 1024 * 1024));
    let mut native = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        accept_async_with_config(stream, Some(configuration)),
    )
    .await??;
    let upstream = tokio::net::UnixStream::connect(endpoint).await?;
    let (mut server, _) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        client_async_with_config("ws://localhost/", upstream, Some(configuration)),
    )
    .await??;
    loop {
        tokio::select! {
            frame = native.next() => {
                let Some(frame) = frame else { return Ok(()); };
                let frame = frame?;
                if matches!(frame, Message::Close(_)) { return Ok(()); }
                if let Message::Text(text) = &frame
                    && selections.request(&serde_json::from_str(text)?)? {
                    store.update_codex_launch_evidence(launch_id, &serde_json::to_value(&selections.evidence)?)?;
                }
                server.send(frame).await?;
            }
            frame = server.next() => {
                let frame = frame.ok_or_else(|| anyhow::anyhow!("native Codex service closed during the launch"))??;
                if matches!(frame, Message::Close(_)) { anyhow::bail!("native Codex service closed during the launch"); }
                if let Message::Text(text) = &frame
                    && selections.response(&serde_json::from_str(text)?)? {
                    store.update_codex_launch_evidence(launch_id, &serde_json::to_value(&selections.evidence)?)?;
                }
                native.send(frame).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_complete_evidence_for_the_mapped_thread() {
        let evidence = Evidence {
            selected_threads: vec!["mapped".to_owned()],
            complete: true,
            ..Evidence::default()
        };
        evidence.validate("mapped").unwrap();
        assert!(evidence.validate("other").is_err());
        assert!(Evidence::default().validate("mapped").is_err());
        let pending = Evidence {
            pending_requests: 1,
            ..evidence.clone()
        };
        assert!(pending.validate("mapped").is_err());
        let failed = Evidence {
            error: Some("connection failed".to_owned()),
            ..evidence.clone()
        };
        assert!(failed.validate("mapped").is_err());
    }

    #[test]
    fn observes_only_successful_selections_and_retains_pending_requests() {
        use serde_json::json;
        let mut selections = Selections::default();
        assert!(
            !selections
                .request(&json!({"id":0,"method":"thread/read","params":{"threadId":"sibling"}}))
                .unwrap()
        );
        for method in ["thread/start", "thread/fork", "thread/resume"] {
            assert!(
                selections
                    .request(&json!({"id":1,"method":method}))
                    .unwrap()
            );
            assert_eq!(selections.evidence.pending_requests, 1);
            assert!(
                selections
                    .response(&json!({"id":1,"result":{"thread":{"id":"mapped"}}}))
                    .unwrap()
            );
            assert_eq!(selections.evidence.pending_requests, 0);
        }
        assert_eq!(selections.evidence.selected_threads, vec!["mapped"]);
        selections
            .request(&json!({"id":"failed","method":"thread/resume"}))
            .unwrap();
        selections
            .response(&json!({"id":"failed","error":{"message":"not found"}}))
            .unwrap();
        assert_eq!(selections.evidence.selected_threads, vec!["mapped"]);
        selections
            .request(&json!({"id":2,"method":"thread/start"}))
            .unwrap();
        assert!(selections.response(&json!({"id":2,"result":{}})).is_err());
        assert_eq!(selections.evidence.pending_requests, 1);
    }

    #[test]
    fn background_title_generation_is_not_a_conversation_selection() {
        let mut selections = Selections::default();
        assert!(
            !selections
                .request(&serde_json::json!({
                    "id":"temporary-structured-title", "method":"thread/start",
                    "params":{"ephemeral":true,"threadSource":"thread_title"}
                }))
                .unwrap()
        );
        assert_eq!(selections.evidence.pending_requests, 0);
        assert!(selections.request(&serde_json::json!({
            "id":1,"method":"thread/start", "params":{"ephemeral":false,"threadSource":"thread_title"}
        })).unwrap());
        assert!(selections.request(&serde_json::json!({
            "id":2,"method":"thread/start", "params":{"ephemeral":true,"threadSource":"unknown"}
        })).unwrap());
    }

    #[test]
    fn server_requests_cannot_resolve_a_native_selection() {
        let mut selections = Selections::default();
        selections
            .request(&serde_json::json!({"id":1,"method":"thread/resume"}))
            .unwrap();
        assert!(!selections.response(&serde_json::json!({"id":1,"method":"item/commandExecution/requestApproval","params":{}})).unwrap());
        assert_eq!(selections.evidence.pending_requests, 1);
    }

    #[test]
    fn detects_switches_even_when_the_cli_returns_to_the_mapped_thread() {
        let evidence = Evidence {
            selected_threads: vec!["mapped".to_owned(), "other".to_owned()],
            complete: true,
            ..Evidence::default()
        };
        assert!(evidence.validate("mapped").is_err());
    }
}
