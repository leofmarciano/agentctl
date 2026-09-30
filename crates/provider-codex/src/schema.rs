// ABOUTME: Detects Codex installations and caches their official protocol schemas.
// ABOUTME: Runs bounded discovery commands and protects private schema artifacts.
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

use agentctl_core::ProviderError;
use agentctl_workspace::{ProcessTree, configure_tokio_process_group};
use directories::ProjectDirs;
use fs2::FileExt;
use tokio::{process::Command, time::timeout};

const COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

#[derive(Clone, Debug)]
pub struct CodexInstallation {
    pub binary: PathBuf,
    pub version: String,
}

pub async fn detect_installation(binary: &Path) -> Result<CodexInstallation, ProviderError> {
    let mut command = Command::new(binary);
    command.arg("--version");
    let output = run_output(command, binary, "`codex --version`").await?;
    if !output.status.success() {
        return Err(ProviderError::Process(format!(
            "`codex --version` exited with {}",
            output.status
        )));
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if version.is_empty() {
        return Err(ProviderError::Incompatible(
            "`codex --version` returned an empty version".to_owned(),
        ));
    }
    Ok(CodexInstallation {
        binary: binary.to_path_buf(),
        version,
    })
}

pub async fn generate_schema_cache(
    installation: &CodexInstallation,
) -> Result<PathBuf, ProviderError> {
    let project = ProjectDirs::from("dev", "agentctl", "agentctl").ok_or_else(|| {
        ProviderError::Process("could not determine agentctl data directory".to_owned())
    })?;
    let parent = project.cache_dir().join("protocols").join("codex");
    generate_schema_cache_under(installation, &parent).await
}

/// Generates or reuses the installed-version schema below an explicit cache root.
///
/// Callers that provide their own state directory should use this entrypoint so
/// schema discovery never escapes that directory. Generation is serialized
/// across processes and published with an atomic directory rename.
pub async fn generate_schema_cache_under(
    installation: &CodexInstallation,
    parent: &Path,
) -> Result<PathBuf, ProviderError> {
    let safe_version = normalized_version_component(&installation.version);
    generate_schema_cache_in(installation, parent, &safe_version).await
}

fn normalized_version_component(version: &str) -> String {
    version
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

async fn generate_schema_cache_in(
    installation: &CodexInstallation,
    parent: &Path,
    safe_version: &str,
) -> Result<PathBuf, ProviderError> {
    tokio::fs::create_dir_all(parent).await?;
    set_private_directory(parent)?;
    let directory = parent.join(safe_version);
    let lock_path = parent.join(format!("{safe_version}.lock"));
    let lock = tokio::task::spawn_blocking(move || acquire_schema_lock(&lock_path))
        .await
        .map_err(|error| ProviderError::Process(format!("schema lock task failed: {error}")))??;

    let result = generate_schema_cache_locked(installation, parent, safe_version, &directory).await;
    let unlocked = FileExt::unlock(&lock).map_err(ProviderError::Io);
    match (result, unlocked) {
        (Ok(directory), Ok(())) => Ok(directory),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

async fn generate_schema_cache_locked(
    installation: &CodexInstallation,
    parent: &Path,
    safe_version: &str,
    directory: &Path,
) -> Result<PathBuf, ProviderError> {
    if valid_client_request_schema(directory).await {
        set_private_tree(directory)?;
        return Ok(directory.to_path_buf());
    }

    let staging = parent.join(format!(".{safe_version}.staging-{}", std::process::id()));
    if tokio::fs::symlink_metadata(&staging).await.is_ok() {
        tokio::fs::remove_dir_all(&staging).await?;
    }
    tokio::fs::create_dir(&staging).await?;
    set_private_directory(&staging)?;

    let mut command = Command::new(&installation.binary);
    command
        .args(["app-server", "generate-json-schema", "--out"])
        .arg(&staging);
    let output = run_output(command, &installation.binary, "Codex schema generation").await?;
    if !output.status.success() {
        let _ = tokio::fs::remove_dir_all(&staging).await;
        return Err(ProviderError::Incompatible(format!(
            "Codex schema generation failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    if !valid_client_request_schema(&staging).await {
        let _ = tokio::fs::remove_dir_all(&staging).await;
        return Err(ProviderError::Incompatible(
            "Codex schema generation produced an invalid ClientRequest.json".to_owned(),
        ));
    }
    set_private_tree(&staging)?;
    if tokio::fs::symlink_metadata(directory).await.is_ok() {
        tokio::fs::remove_dir_all(directory).await?;
    }
    tokio::fs::rename(&staging, directory).await?;
    Ok(directory.to_path_buf())
}

fn acquire_schema_lock(path: &Path) -> Result<File, ProviderError> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    set_private_file(&file)?;
    FileExt::lock_exclusive(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn set_private_directory(path: &Path) -> Result<(), ProviderError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ProviderError::Incompatible(format!(
            "Codex schema cache path is not a real directory: {}",
            path.display()
        )));
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory(_path: &Path) -> Result<(), ProviderError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(file: &File) -> Result<(), ProviderError> {
    use std::os::unix::fs::PermissionsExt;

    if !file.metadata()?.is_file() {
        return Err(ProviderError::Incompatible(
            "Codex schema cache lock is not a regular file".to_owned(),
        ));
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_file(_file: &File) -> Result<(), ProviderError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_tree(path: &Path) -> Result<(), ProviderError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(ProviderError::Incompatible(format!(
            "Codex schema generation emitted a symlink: {}",
            path.display()
        )));
    }
    if metadata.is_dir() {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        for entry in std::fs::read_dir(path)? {
            set_private_tree(&entry?.path())?;
        }
        return Ok(());
    }
    if metadata.is_file() {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        return Ok(());
    }
    Err(ProviderError::Incompatible(format!(
        "Codex schema generation emitted a non-file entry: {}",
        path.display()
    )))
}

#[cfg(not(unix))]
fn set_private_tree(_path: &Path) -> Result<(), ProviderError> {
    Ok(())
}

async fn valid_client_request_schema(directory: &Path) -> bool {
    let Ok(encoded) = tokio::fs::read(directory.join("ClientRequest.json")).await else {
        return false;
    };
    serde_json::from_slice::<serde_json::Value>(&encoded).is_ok()
}

pub(crate) async fn run_output(
    mut command: Command,
    binary: &Path,
    description: &str,
) -> Result<std::process::Output, ProviderError> {
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    configure_tokio_process_group(&mut command);
    let mut child = command.spawn().map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => ProviderError::BinaryNotFound(binary.display().to_string()),
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
    let Ok(output) = timeout(COMMAND_TIMEOUT, child.wait_with_output()).await else {
        let _ = process_tree.terminate();
        return Err(ProviderError::Process(format!("{description} timed out")));
    };
    output.map_err(ProviderError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_component_normalization_is_stable_and_path_safe() {
        assert_eq!(
            normalized_version_component("codex-cli 0.144.0 (preview/arm64)"),
            "codex-cli_0.144.0__preview_arm64_"
        );
        assert_eq!(
            normalized_version_component("0.144.0-beta_1"),
            "0.144.0-beta_1"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explicit_root_is_private_versioned_atomic_and_idempotent() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("isolated-home/protocols/codex");
        let counter = temporary.path().join("generations");
        let binary = temporary.path().join("fake-codex");
        let quoted_counter = format!(
            "'{}'",
            counter.display().to_string().replace('\'', "'\"'\"'")
        );
        std::fs::write(
            &binary,
            format!(
                r#"#!/bin/sh
test "$1" = "app-server"
test "$2" = "generate-json-schema"
test "$3" = "--out"
mkdir -p "$4/nested"
printf '%s\n' '{{"methods":["thread/read"]}}' > "$4/ClientRequest.json"
printf '%s\n' 'fixture' > "$4/nested/methods.json"
printf x >> {quoted_counter}
"#
            ),
        )
        .unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        let installation = CodexInstallation {
            binary,
            version: "codex-cli 0.144.0/arm64".to_owned(),
        };

        let (first, second) = tokio::join!(
            generate_schema_cache_under(&installation, &root),
            generate_schema_cache_under(&installation, &root)
        );
        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first, root.join("codex-cli_0.144.0_arm64"));
        assert_eq!(second, first);
        assert_eq!(std::fs::read_to_string(counter).unwrap(), "x");
        assert_eq!(mode(&root), 0o700);
        assert_eq!(mode(&first), 0o700);
        assert_eq!(mode(&first.join("nested")), 0o700);
        assert_eq!(mode(&first.join("ClientRequest.json")), 0o600);
        assert_eq!(mode(&first.join("nested/methods.json")), 0o600);
        assert_eq!(mode(&root.join("codex-cli_0.144.0_arm64.lock")), 0o600);
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;

        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }
}
