// ABOUTME: Exchanges bounded Codex protocol messages over owned or shared connections.
// ABOUTME: Correlates requests and closes only resources owned by this client.
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use agentctl_core::{ProviderError, ProviderKind};
use agentctl_workspace::{ProcessTree, configure_tokio_process_group};
use futures::{Sink, SinkExt, Stream, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::BufReader,
    process::{Child, Command},
    sync::{Mutex, broadcast, mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

const MAX_PROTOCOL_LINE_BYTES: usize = 8 * 1024 * 1024;
type PendingSender = oneshot::Sender<Result<Value, ProviderError>>;
type PendingRequests = Arc<Mutex<HashMap<u64, PendingSender>>>;

/// A message initiated by the app-server.
#[derive(Clone, Debug)]
pub(crate) enum RpcInbound {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Malformed {
        raw: String,
        error: String,
    },
    TransportError {
        message: String,
    },
}

/// Successful JSON-RPC result together with its client-generated request id.
#[derive(Clone, Debug)]
pub(crate) struct RpcResponse {
    pub request_id: u64,
    pub result: Value,
}

#[derive(Debug)]
struct OutboundLine(Value);

/// Bounded, multiplexed protocol connection to a Codex session service.
#[derive(Debug)]
pub(crate) struct CodexRpcClient {
    outbound: mpsc::Sender<OutboundLine>,
    pending: PendingRequests,
    inbound: broadcast::Sender<RpcInbound>,
    child: Mutex<Option<Child>>,
    process_tree: Option<ProcessTree>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    next_id: std::sync::atomic::AtomicU64,
    request_timeout: Duration,
    closed: Arc<AtomicBool>,
    loaded_threads: Mutex<HashSet<String>>,
}

impl CodexRpcClient {
    pub(crate) async fn spawn(
        binary: &std::path::Path,
        channel_capacity: usize,
        request_timeout: Duration,
    ) -> Result<Arc<Self>, ProviderError> {
        let mut command = Command::new(binary);
        command
            .args(["app-server", "--stdio"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        configure_tokio_process_group(&mut command);
        let mut child = command.spawn().map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => {
                ProviderError::BinaryNotFound(binary.display().to_string())
            }
            _ => ProviderError::Io(error),
        })?;
        let process_tree = match ProcessTree::attach(&child) {
            Ok(process_tree) => process_tree,
            Err(error) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(ProviderError::Process(error.to_string()));
            }
        };

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ProviderError::Process("Codex stdin was not piped".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProviderError::Process("Codex stdout was not piped".to_owned()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ProviderError::Process("Codex stderr was not piped".to_owned()))?;

        let (outbound, outbound_rx) = mpsc::channel(channel_capacity.max(1));
        let (inbound, _) = broadcast::channel(channel_capacity.max(16));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));

        let client = Arc::new(Self {
            outbound,
            pending: Arc::clone(&pending),
            inbound: inbound.clone(),
            child: Mutex::new(Some(child)),
            process_tree: Some(process_tree),
            tasks: Mutex::new(Vec::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            request_timeout,
            closed: Arc::clone(&closed),
            loaded_threads: Mutex::new(HashSet::new()),
        });

        let writer = FramedWrite::new(
            stdin,
            LinesCodec::new_with_max_length(MAX_PROTOCOL_LINE_BYTES),
        )
        .with(|text: String| {
            futures::future::ready(Ok::<String, tokio_util::codec::LinesCodecError>(text))
        })
        .sink_map_err(|error| {
            ProviderError::Process(format!("Codex protocol write failed: {error}"))
        });
        let reader = FramedRead::new(
            BufReader::new(stdout),
            LinesCodec::new_with_max_length(MAX_PROTOCOL_LINE_BYTES),
        )
        .map(|line| line.map_err(|error| error.to_string()));
        let mut tasks = client.tasks.lock().await;
        tasks.push(
            tokio::spawn(write_loop(writer, outbound_rx, Arc::clone(&closed))).abort_handle(),
        );
        tasks.push(tokio::spawn(read_loop(reader, pending, inbound, closed)).abort_handle());
        tasks.push(tokio::spawn(async move {
            let mut lines = FramedRead::new(
                BufReader::new(stderr),
                LinesCodec::new_with_max_length(MAX_PROTOCOL_LINE_BYTES),
            );
            while let Some(line) = lines.next().await {
                match line {
                    Ok(line) => tracing::debug!(
                        provider = %ProviderKind::Codex,
                        bytes = line.len(),
                        "provider stderr line suppressed"
                    ),
                    Err(error) => {
                        tracing::warn!(provider = %ProviderKind::Codex, %error, "failed to read provider stderr");
                        break;
                    }
                }
            }
        }).abort_handle());
        drop(tasks);

        client.initialize().await?;
        Ok(client)
    }

    #[cfg(unix)]
    pub(crate) async fn connect(
        socket_path: &std::path::Path,
        channel_capacity: usize,
        request_timeout: Duration,
    ) -> Result<Arc<Self>, ProviderError> {
        use tokio::net::UnixStream;
        use tokio_tungstenite::{
            client_async_with_config,
            tungstenite::{Message, protocol::WebSocketConfig},
        };

        let stream = timeout(request_timeout, UnixStream::connect(socket_path))
            .await
            .map_err(|_| {
                ProviderError::Process("native Codex socket connection timed out".to_owned())
            })??;
        let configuration = WebSocketConfig::default()
            .max_message_size(Some(MAX_PROTOCOL_LINE_BYTES))
            .max_frame_size(Some(MAX_PROTOCOL_LINE_BYTES));
        let (websocket, _) = timeout(
            request_timeout,
            client_async_with_config("ws://localhost/", stream, Some(configuration)),
        )
        .await
        .map_err(|_| ProviderError::Process("native Codex handshake timed out".to_owned()))?
        .map_err(|error| {
            ProviderError::Protocol(format!("native Codex handshake failed: {error}"))
        })?;
        let (writer, reader) = websocket.split();
        let writer = writer
            .with(|text: String| {
                futures::future::ready(Ok::<Message, tokio_tungstenite::tungstenite::Error>(
                    Message::text(text),
                ))
            })
            .sink_map_err(|error| {
                ProviderError::Process(format!("native Codex write failed: {error}"))
            });
        let reader = reader.filter_map(|frame| {
            futures::future::ready(match frame {
                Ok(Message::Text(text)) => Some(Ok(text.to_string())),
                Ok(Message::Ping(_) | Message::Pong(_)) => None,
                Ok(Message::Close(_)) => Some(Err("native Codex connection closed".to_owned())),
                Ok(_) => Some(Err("native Codex sent a non-text protocol frame".to_owned())),
                Err(error) => Some(Err(error.to_string())),
            })
        });
        let (outbound, outbound_rx) = mpsc::channel(channel_capacity.max(1));
        let (inbound, _) = broadcast::channel(channel_capacity.max(16));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let client = Arc::new(Self {
            outbound,
            pending: Arc::clone(&pending),
            inbound: inbound.clone(),
            child: Mutex::new(None),
            process_tree: None,
            tasks: Mutex::new(Vec::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            request_timeout,
            closed: Arc::clone(&closed),
            loaded_threads: Mutex::new(HashSet::new()),
        });
        let mut tasks = client.tasks.lock().await;
        tasks.push(
            tokio::spawn(write_loop(writer, outbound_rx, Arc::clone(&closed))).abort_handle(),
        );
        tasks.push(tokio::spawn(read_loop(reader, pending, inbound, closed)).abort_handle());
        drop(tasks);
        client.initialize().await?;
        Ok(client)
    }

    async fn initialize(&self) -> Result<(), ProviderError> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "agentctl",
                    "title": "agentctl",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": true
                }
            }),
        )
        .await?;
        self.notify("initialized", Value::Null).await
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<RpcInbound> {
        self.inbound.subscribe()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) async fn is_thread_loaded(&self, thread_id: &str) -> bool {
        self.loaded_threads.lock().await.contains(thread_id)
    }

    pub(crate) async fn mark_thread_loaded(&self, thread_id: &str) {
        self.loaded_threads
            .lock()
            .await
            .insert(thread_id.to_owned());
    }

    pub(crate) async fn request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<RpcResponse, ProviderError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id, sender);

        // Some app-server releases require an object even where the generated
        // request schema represents an empty params payload as null/optional.
        let params = if params.is_null() { json!({}) } else { params };
        let request = json!({"id": id, "method": method, "params": params});
        if self.outbound.send(OutboundLine(request)).await.is_err() {
            self.pending.lock().await.remove(&id);
            return Err(ProviderError::Process(
                "Codex app-server writer stopped".to_owned(),
            ));
        }

        match timeout(self.request_timeout, receiver).await {
            Ok(Ok(Ok(result))) => Ok(RpcResponse {
                request_id: id,
                result,
            }),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(ProviderError::Process(
                "Codex response channel closed".to_owned(),
            )),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(ProviderError::Process(format!(
                    "Codex request {method} timed out"
                )))
            }
        }
    }

    pub(crate) async fn notify(&self, method: &str, params: Value) -> Result<(), ProviderError> {
        let mut notification = json!({"method": method});
        if !params.is_null() {
            notification["params"] = params;
        }
        self.outbound
            .send(OutboundLine(notification))
            .await
            .map_err(|_| ProviderError::Process("Codex app-server writer stopped".to_owned()))
    }

    pub(crate) async fn respond(&self, id: Value, result: Value) -> Result<(), ProviderError> {
        self.outbound
            .send(OutboundLine(json!({"id": id, "result": result})))
            .await
            .map_err(|_| ProviderError::Process("Codex app-server writer stopped".to_owned()))
    }

    pub(crate) async fn shutdown(&self) -> Result<(), ProviderError> {
        self.closed.store(true, Ordering::Release);
        for task in self.tasks.lock().await.iter() {
            task.abort();
        }
        fail_pending(&self.pending, "Codex connection shut down".to_owned()).await;
        let mut child = self.child.lock().await;
        if let (Some(child), Some(process_tree)) = (child.as_mut(), self.process_tree.as_ref()) {
            let root_running = child.try_wait().map_err(ProviderError::Io)?.is_none();
            let terminated = process_tree
                .terminate()
                .map_err(|error| ProviderError::Process(error.to_string()));
            if root_running {
                child.wait().await.map_err(ProviderError::Io)?;
            }
            terminated?;
        }
        Ok(())
    }
}

impl Drop for CodexRpcClient {
    fn drop(&mut self) {
        for task in self.tasks.get_mut().iter() {
            task.abort();
        }
    }
}

async fn write_loop(
    mut writer: impl Sink<String, Error = ProviderError> + Unpin,
    mut receiver: mpsc::Receiver<OutboundLine>,
    closed: Arc<AtomicBool>,
) {
    while let Some(OutboundLine(value)) = receiver.recv().await {
        let encoded = match serde_json::to_string(&value) {
            Ok(encoded) => encoded,
            Err(error) => {
                tracing::error!(provider = %ProviderKind::Codex, %error, "failed to encode request");
                continue;
            }
        };
        if let Err(error) = writer.send(encoded).await {
            tracing::warn!(provider = %ProviderKind::Codex, %error, "Codex connection write failed");
            break;
        }
    }
    closed.store(true, Ordering::Release);
}

async fn read_loop(
    mut lines: impl Stream<Item = Result<String, String>> + Unpin,
    pending: PendingRequests,
    inbound: broadcast::Sender<RpcInbound>,
    closed: Arc<AtomicBool>,
) {
    while let Some(line) = lines.next().await {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                let message = format!("invalid Codex protocol line: {error}");
                let _ = inbound.send(RpcInbound::TransportError {
                    message: message.clone(),
                });
                fail_pending(&pending, message).await;
                closed.store(true, Ordering::Release);
                return;
            }
        };
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                let _ = inbound.send(RpcInbound::Malformed {
                    raw: line,
                    error: error.to_string(),
                });
                continue;
            }
        };
        if let Some(method) = value.get("method").and_then(Value::as_str) {
            let params = value.get("params").cloned().unwrap_or(Value::Null);
            let message = if let Some(id) = value.get("id") {
                RpcInbound::Request {
                    id: id.clone(),
                    method: method.to_owned(),
                    params,
                }
            } else {
                RpcInbound::Notification {
                    method: method.to_owned(),
                    params,
                }
            };
            let _ = inbound.send(message);
            continue;
        }

        let Some(id) = value.get("id").and_then(Value::as_u64) else {
            tracing::debug!(provider = %ProviderKind::Codex, frame = %value, "ignored uncorrelated frame");
            continue;
        };
        if let Some(sender) = pending.lock().await.remove(&id) {
            let result = if let Some(error) = value.get("error") {
                Err(classify_rpc_error(error))
            } else {
                Ok(value.get("result").cloned().unwrap_or(Value::Null))
            };
            let _ = sender.send(result);
        }
    }
    let _ = inbound.send(RpcInbound::TransportError {
        message: "Codex app-server exited".to_owned(),
    });
    closed.store(true, Ordering::Release);
    fail_pending(&pending, "Codex app-server exited".to_owned()).await;
}

async fn fail_pending(
    pending: &Mutex<HashMap<u64, oneshot::Sender<Result<Value, ProviderError>>>>,
    message: String,
) {
    let senders = std::mem::take(&mut *pending.lock().await);
    for sender in senders.into_values() {
        let _ = sender.send(Err(ProviderError::Process(message.clone())));
    }
}

fn classify_rpc_error(error: &Value) -> ProviderError {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown Codex JSON-RPC error");
    let serialized = error.to_string();
    let lower = format!("{message} {serialized}").to_ascii_lowercase();
    if lower.contains("usagelimitexceeded") || lower.contains("usage limit") {
        ProviderError::RateLimited { resets_at: None }
    } else if lower.contains("serveroverloaded") || lower.contains("overloaded") {
        ProviderError::Overloaded(message.to_owned())
    } else if lower.contains("unauthorized") || lower.contains("authentication") {
        ProviderError::Authentication(message.to_owned())
    } else if lower.contains("method not found") || lower.contains("invalid params") {
        ProviderError::Incompatible(message.to_owned())
    } else {
        ProviderError::Protocol(format!("{message}: {serialized}"))
    }
}

#[cfg(test)]
mod tests {
    use super::classify_rpc_error;
    #[cfg(unix)]
    use super::{CodexRpcClient, RpcInbound};
    use agentctl_core::ProviderError;
    use serde_json::json;
    #[cfg(unix)]
    use std::time::Duration;

    #[test]
    fn classifies_usage_limit_from_structured_error() {
        let error = classify_rpc_error(&json!({
            "message": "request failed",
            "data": {"codexErrorInfo": "usageLimitExceeded"}
        }));
        assert!(matches!(error, ProviderError::RateLimited { .. }));
    }

    #[test]
    fn classifies_incompatible_method() {
        let error = classify_rpc_error(&json!({"message": "Method not found"}));
        assert!(matches!(error, ProviderError::Incompatible(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_json_is_reported_without_losing_the_next_valid_notification() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let fake = directory.path().join("fake-codex");
        std::fs::write(
            &fake,
            r#"#!/bin/sh
IFS= read -r initialize
printf '%s\n' '{"id":1,"result":{"capabilities":{}}}'
IFS= read -r initialized
sleep 0.2
printf '%s\n' 'not-json'
printf '%s\n' '{"method":"test/afterMalformed","params":{"seq":2}}'
IFS= read -r request
printf '%s\n' '{"id":2,"result":{"winner":"first"}}'
printf '%s\n' '{"id":2,"result":{"winner":"duplicate"}}'
printf '%s\n' '{"method":"test/afterDuplicate","params":{"seq":3}}'
sleep 1
"#,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&fake, permissions).unwrap();

        let client = CodexRpcClient::spawn(&fake, 16, Duration::from_secs(2))
            .await
            .unwrap();
        let mut inbound = client.subscribe();
        let malformed = tokio::time::timeout(Duration::from_secs(2), inbound.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            malformed,
            RpcInbound::Malformed { raw, error }
                if raw == "not-json" && !error.is_empty()
        ));
        let next = tokio::time::timeout(Duration::from_secs(2), inbound.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            next,
            RpcInbound::Notification { method, params }
                if method == "test/afterMalformed" && params["seq"] == 2
        ));
        let response = client.request("test/request", json!({})).await.unwrap();
        assert_eq!(response.result["winner"], "first");
        let after_duplicate = tokio::time::timeout(Duration::from_secs(2), inbound.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            after_duplicate,
            RpcInbound::Notification { method, params }
                if method == "test/afterDuplicate" && params["seq"] == 3
        ));
        client.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_kills_descendant_even_after_provider_root_exited() {
        use agentctl_workspace::process_group_exists;
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let fake = directory.path().join("exited-codex");
        let descendant_file = directory.path().join("descendant.pid");
        std::fs::write(
            &fake,
            format!(
                "#!/bin/sh\nIFS= read -r initialize\nprintf '%s\\n' '{{\"id\":1,\"result\":{{\"capabilities\":{{}}}}}}'\nIFS= read -r initialized\nsleep 30 >/dev/null 2>&1 &\nprintf '%s' \"$!\" > \"{}\"\n",
                descendant_file.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&fake, permissions).unwrap();

        let client = CodexRpcClient::spawn(&fake, 4, Duration::from_secs(5))
            .await
            .unwrap();
        for _ in 0..50 {
            if descendant_file.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !std::fs::read_to_string(&descendant_file)
                .unwrap()
                .trim()
                .is_empty()
        );
        let group = client.process_tree.as_ref().unwrap().id();
        tokio::time::timeout(
            Duration::from_secs(2),
            client.child.lock().await.as_mut().unwrap().wait(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(process_group_exists(group).unwrap());

        client.shutdown().await.unwrap();

        let mut gone = false;
        for _ in 0..50 {
            if !process_group_exists(group).unwrap() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            gone,
            "provider descendant survived shutdown after root exit"
        );
    }
}
