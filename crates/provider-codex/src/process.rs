// ABOUTME: Opens owned Codex helpers and discovers the native session service.
// ABOUTME: Selects connections without interrupting other native sessions.
use std::{path::Path, sync::Arc, time::Duration};

use agentctl_core::ProviderError;

use crate::jsonrpc::CodexRpcClient;

pub(crate) async fn spawn_app_server(
    binary: &Path,
    channel_capacity: usize,
    request_timeout: Duration,
) -> Result<Arc<CodexRpcClient>, ProviderError> {
    CodexRpcClient::spawn(binary, channel_capacity, request_timeout).await
}

#[cfg(unix)]
pub async fn interactive_endpoint(
    binary: &Path,
) -> Result<Option<std::path::PathBuf>, ProviderError> {
    use directories::BaseDirs;
    use serde_json::Value;
    use std::os::unix::fs::FileTypeExt;
    use tokio::process::Command;

    let home = match std::env::var_os("CODEX_HOME") {
        Some(home) => std::path::PathBuf::from(home),
        None => BaseDirs::new()
            .ok_or_else(|| ProviderError::Process("could not determine Codex home".to_owned()))?
            .home_dir()
            .join(".codex"),
    };
    let socket = home.join("app-server-control/app-server-control.sock");
    match tokio::fs::symlink_metadata(&socket).await {
        Ok(_) => {
            let metadata = tokio::fs::metadata(&socket).await?;
            if !metadata.file_type().is_socket() {
                return Err(ProviderError::Incompatible(
                    "native Codex control endpoint is not a Unix socket".to_owned(),
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ProviderError::Io(error)),
    }
    let mut command = Command::new(binary);
    command.args(["app-server", "daemon", "version"]);
    let output =
        crate::schema::run_output(command, binary, "native Codex daemon discovery").await?;
    if !output.status.success() {
        return Err(ProviderError::Process(format!(
            "native Codex daemon discovery failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let report: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        ProviderError::Protocol(format!("invalid native daemon report: {error}"))
    })?;
    validate_native_daemon(&report, &socket)?;
    Ok(Some(socket))
}

#[cfg(unix)]
fn validate_native_daemon(report: &serde_json::Value, socket: &Path) -> Result<(), ProviderError> {
    if report["status"] != "running" || report["socketPath"].as_str() != socket.to_str() {
        return Err(ProviderError::Incompatible(
            "native Codex daemon report does not match its control socket".to_owned(),
        ));
    }
    if report["cliVersion"].as_str().is_none() || report["cliVersion"] != report["appServerVersion"]
    {
        return Err(ProviderError::Incompatible(
            "native Codex daemon and selected CLI versions differ; use matching versions before switching providers".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use serde_json::json;

    fn report() -> serde_json::Value {
        json!({
            "status": "running",
            "socketPath": "/tmp/native-codex.sock",
            "cliVersion": "0.159.2",
            "appServerVersion": "0.159.2"
        })
    }

    #[test]
    fn native_daemon_requires_matching_cli_version_and_socket() {
        validate_native_daemon(&report(), Path::new("/tmp/native-codex.sock")).unwrap();
    }

    #[test]
    fn native_daemon_rejects_missing_or_mismatched_versions() {
        let mut response = report();
        response["appServerVersion"] = json!("0.158.0");
        assert!(validate_native_daemon(&response, Path::new("/tmp/native-codex.sock")).is_err());
        response["cliVersion"] = serde_json::Value::Null;
        response["appServerVersion"] = serde_json::Value::Null;
        assert!(validate_native_daemon(&response, Path::new("/tmp/native-codex.sock")).is_err());
    }

    #[test]
    fn native_daemon_rejects_other_sockets_and_unconfirmed_running_state() {
        assert!(validate_native_daemon(&report(), Path::new("/tmp/other.sock")).is_err());
        let mut response = report();
        response["status"] = json!("unknown");
        assert!(validate_native_daemon(&response, Path::new("/tmp/native-codex.sock")).is_err());
    }
}
