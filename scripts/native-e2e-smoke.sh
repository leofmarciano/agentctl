#!/usr/bin/env bash
# ABOUTME: Verifies native provider lifecycle and synchronization in a disposable workspace.
# ABOUTME: Checks writer exclusion, transcript capture, mappings, and repeated synchronization.
set -Eeuo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
EXPECT_DRIVER="$ROOT_DIR/scripts/native-pty-driver.exp"
SESSION_NAME=${AGENTCTL_NATIVE_E2E_SESSION_NAME:-native-bridge-smoke}
KEEP_ARTIFACTS=${AGENTCTL_NATIVE_E2E_KEEP:-0}
STARTUP_SECONDS=${AGENTCTL_NATIVE_E2E_STARTUP_SECONDS:-12}
READY_TIMEOUT_SECONDS=${AGENTCTL_NATIVE_E2E_READY_TIMEOUT_SECONDS:-45}

usage() {
  cat <<'EOF'
Usage: scripts/native-e2e-smoke.sh

Opt-in smoke test for the real native CLI bridge. It opens Claude Code, then
Codex, then Claude Code again in PTYs. It never submits a chat prompt and does
not intentionally start a model turn.

Environment:
  AGENTCTL_BIN                         agentctl executable (default: build target/debug/agentctl)
  AGENTCTL_NATIVE_E2E_KEEP=1           retain the temporary workspace and logs
  AGENTCTL_NATIVE_E2E_STARTUP_SECONDS  native UI startup grace (default: 12)
  AGENTCTL_NATIVE_E2E_READY_TIMEOUT_SECONDS
                                       wrapper readiness timeout (default: 45)

Prerequisites: authenticated/configured `claude` and `codex`, `expect`, `jq`,
and `git`. The provider CLIs must be able to open an interactive session in a
new temporary Git worktree without an onboarding prompt.
EOF
}

die() {
  printf 'native E2E smoke: %s\n' "$*" >&2
  exit 1
}

note() {
  printf '==> %s\n' "$*"
}

if [[ ${1:-} == "--help" || ${1:-} == "-h" ]]; then
  usage
  exit 0
fi
[[ $# -eq 0 ]] || die "unexpected argument: $1 (use --help)"

for dependency in expect jq git; do
  command -v "$dependency" >/dev/null 2>&1 || die "required command not found: $dependency"
done
for provider in claude codex; do
  command -v "$provider" >/dev/null 2>&1 || die "provider CLI not found on PATH: $provider"
done
[[ -x "$EXPECT_DRIVER" ]] || die "PTY driver is not executable: $EXPECT_DRIVER"

if [[ -n ${AGENTCTL_BIN:-} ]]; then
  AGENTCTL_BIN=$(cd -- "$(dirname -- "$AGENTCTL_BIN")" && pwd -P)/$(basename -- "$AGENTCTL_BIN")
else
  note "building agentctl debug binary"
  cargo build --locked --package agentctl --manifest-path "$ROOT_DIR/Cargo.toml"
  AGENTCTL_BIN="$ROOT_DIR/target/debug/agentctl"
fi
[[ -x "$AGENTCTL_BIN" ]] || die "agentctl executable is not runnable: $AGENTCTL_BIN"

TMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/agentctl-native-e2e.XXXXXX")
STATE_DIR="$TMP_ROOT/state"
WORKSPACE="$TMP_ROOT/workspace"
LOG_DIR="$TMP_ROOT/logs"
mkdir -p "$WORKSPACE" "$LOG_DIR"

CURRENT_NATIVE_DRIVER_PID=''
CURRENT_NATIVE_RELEASE_FILE=''
CURRENT_NATIVE_LOG=''

print_native_log() {
  local log=${1:-$CURRENT_NATIVE_LOG}
  if [[ -n $log && -f $log ]]; then
    printf '%s\n' '--- native PTY log ---' >&2
    sed -n '1,320p' "$log" >&2 || true
  fi
}

cleanup() {
  local status=$?
  trap - EXIT
  set +e
  if [[ -n $CURRENT_NATIVE_DRIVER_PID ]]; then
    if kill -0 "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null; then
      if [[ -n $CURRENT_NATIVE_RELEASE_FILE ]]; then
        touch "$CURRENT_NATIVE_RELEASE_FILE"
      fi
      local elapsed=0
      while kill -0 "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null && ((elapsed < 35)); do
        sleep 1
        ((elapsed += 1))
      done
      if kill -0 "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null; then
        kill -TERM "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null || true
        sleep 1
      fi
      if kill -0 "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null; then
        kill -KILL "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null || true
      fi
    fi
    wait "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null || true
    if [[ $status -ne 0 ]]; then
      print_native_log
    fi
  fi
  if [[ $KEEP_ARTIFACTS == 1 || $status -ne 0 ]]; then
    printf 'native E2E artifacts: %s\n' "$TMP_ROOT" >&2
  else
    rm -rf -- "$TMP_ROOT"
  fi
  exit "$status"
}
trap cleanup EXIT

wait_for_file() {
  local marker=$1
  local owner_pid=$2
  local log=$3
  local elapsed=0
  while [[ ! -f $marker ]]; do
    if ! kill -0 "$owner_pid" 2>/dev/null; then
      print_native_log "$log"
      die "native wrapper exited before becoming ready"
    fi
    ((elapsed += 1))
    if ((elapsed >= READY_TIMEOUT_SECONDS)); then
      print_native_log "$log"
      die "timed out waiting for native wrapper readiness"
    fi
    sleep 1
  done
}

run_native_held() {
  local label=$1
  local exit_text=$2
  local mutation_file=$3
  local mutation_content=$4
  shift 4

  local ready="$TMP_ROOT/$label.ready"
  local release="$TMP_ROOT/$label.release"
  local log="$LOG_DIR/$label.pty.log"
  rm -f -- "$ready" "$release"

  note "opening native $label UI"
  (
    cd -- "$WORKSPACE"
    AGENTCTL_E2E_READY_FILE="$ready" \
      AGENTCTL_E2E_RELEASE_FILE="$release" \
      AGENTCTL_E2E_EXIT_TEXT="$exit_text" \
      AGENTCTL_E2E_PROVIDER="${label%%-*}" \
      AGENTCTL_E2E_STARTUP_SECONDS="$STARTUP_SECONDS" \
      AGENTCTL_E2E_NATIVE_START_SECONDS="$READY_TIMEOUT_SECONDS" \
      "$EXPECT_DRIVER" "$@"
  ) >"$log" 2>&1 &
  local driver_pid=$!
  CURRENT_NATIVE_DRIVER_PID=$driver_pid
  CURRENT_NATIVE_RELEASE_FILE=$release
  CURRENT_NATIVE_LOG=$log
  wait_for_file "$ready" "$driver_pid" "$log"

  if [[ -n $mutation_file ]]; then
    printf '%s\n' "$mutation_content" >"$WORKSPACE/$mutation_file"
  fi

}

release_native() {
  touch "$CURRENT_NATIVE_RELEASE_FILE"
  local exit_status=0
  wait "$CURRENT_NATIVE_DRIVER_PID" || exit_status=$?
  local completed_log=$CURRENT_NATIVE_LOG
  CURRENT_NATIVE_DRIVER_PID=''
  CURRENT_NATIVE_RELEASE_FILE=''
  CURRENT_NATIVE_LOG=''
  if ((exit_status != 0)); then
    print_native_log "$completed_log"
    die "native launch did not complete cleanly"
  fi
}

status_json() {
  (
    cd -- "$WORKSPACE"
    "$AGENTCTL_BIN" --home "$STATE_DIR" --json status "$SESSION_NAME"
  )
}

history_json() {
  (
    cd -- "$WORKSPACE"
    "$AGENTCTL_BIN" --home "$STATE_DIR" --json history "$SESSION_NAME" --raw
  )
}

note "creating disposable Git worktree and canonical session"
git -C "$WORKSPACE" init --quiet
git -C "$WORKSPACE" config user.name agentctl-e2e
git -C "$WORKSPACE" config user.email agentctl-e2e@invalid.example
printf '%s\n' '# agentctl native bridge smoke' >"$WORKSPACE/README.md"
git -C "$WORKSPACE" add README.md
git -C "$WORKSPACE" commit --quiet -m init

NEW_SESSION=$(
  cd -- "$WORKSPACE"
  "$AGENTCTL_BIN" --home "$STATE_DIR" --json new \
    --name "$SESSION_NAME" \
    --workspace "$WORKSPACE" \
    --provider claude \
    --no-launch
)
SESSION_ID=$(jq -er '.id' <<<"$NEW_SESSION")
[[ -n $SESSION_ID ]] || die "new did not return a canonical session id"

run_native_held \
  claude-first \
  /exit \
  claude-native-snapshot.txt \
  provider=claude-native-ui \
  "$AGENTCTL_BIN" --home "$STATE_DIR" open claude --session "$SESSION_NAME" -- --ax-screen-reader

note "verifying the per-worktree writer lock"
set +e
LOCK_OUTPUT=$(
  cd -- "$WORKSPACE"
  "$AGENTCTL_BIN" --home "$STATE_DIR" switch codex --session "$SESSION_NAME" -- --no-alt-screen 2>&1
)
LOCK_STATUS=$?
set -e
[[ $LOCK_STATUS -ne 0 ]] || die "a second native writer opened while Claude held the worktree"
grep -Eiq 'another native agentctl session already owns|lease.*busy|already owns this worktree' <<<"$LOCK_OUTPUT" \
  || die "concurrent launch failed for an unexpected reason: $LOCK_OUTPUT"
kill -0 "$CURRENT_NATIVE_DRIVER_PID" 2>/dev/null \
  || die "lock check disturbed the original Claude native process"
release_native

STATUS_AFTER_CLAUDE=$(status_json)
CLAUDE_NATIVE_ID=$(jq -er '
  .providers[]
  | select(.session.provider.kind == "claude")
  | .session.native_session_id
' <<<"$STATUS_AFTER_CLAUDE")
jq -e '
  any(.providers[];
    .session.provider.kind == "claude"
    and (.session.metadata | has("native_materialized")))
' <<<"$STATUS_AFTER_CLAUDE" >/dev/null \
  || die "Claude launch did not persist its native materialization state"
jq -e '
  any(.[].event;
    .kind == "workspace_snapshot"
    and .origin_provider.kind == "claude")
' <<<"$(history_json)" >/dev/null \
  || die "Claude native workspace effect was not recorded with Claude origin"

run_native_held \
  codex \
  /exit \
  codex-native-snapshot.txt \
  provider=codex-native-ui \
  "$AGENTCTL_BIN" --home "$STATE_DIR" switch codex --session "$SESSION_NAME" -- --no-alt-screen
release_native

STATUS_AFTER_CODEX=$(status_json)
jq -e '
  .session.active_provider.kind == "codex"
  and any(.providers[]; .session.provider.kind == "codex")
' <<<"$STATUS_AFTER_CODEX" >/dev/null \
  || die "Codex did not become the active native provider"
jq -e '
  any(.[].event;
    .kind == "workspace_snapshot"
    and .origin_provider.kind == "codex")
' <<<"$(history_json)" >/dev/null \
  || die "Codex native workspace effect was not recorded with Codex origin"

run_native_held \
  claude-return \
  /exit \
  '' \
  '' \
  "$AGENTCTL_BIN" --home "$STATE_DIR" switch claude --session "$SESSION_NAME" -- --ax-screen-reader
release_native

STATUS_BEFORE_SYNC=$(status_json)
CLAUDE_NATIVE_ID_AFTER=$(jq -er '
  .providers[]
  | select(.session.provider.kind == "claude")
  | .session.native_session_id
' <<<"$STATUS_BEFORE_SYNC")
[[ $CLAUDE_NATIVE_ID_AFTER == "$CLAUDE_NATIVE_ID" ]] \
  || die "return to Claude changed the mapped native session id"
jq -e '
  .session.active_provider.kind == "claude"
  and ([.providers[].session.provider.kind] | sort == ["claude", "codex"])
  and any(.providers[];
    .session.provider.kind == "claude"
    and .sync_lag > 0)
' <<<"$STATUS_BEFORE_SYNC" >/dev/null \
  || die "round trip did not preserve both mappings and Claude's prompt-bound pending delta"

note "running projection synchronization twice"
(
  cd -- "$WORKSPACE"
  "$AGENTCTL_BIN" --home "$STATE_DIR" --json sync "$SESSION_NAME" >"$LOG_DIR/sync-one.json"
)
jq -e 'all(.providers[]; has("error") | not)' "$LOG_DIR/sync-one.json" >/dev/null \
  || die "provider synchronization failed: $(cat "$LOG_DIR/sync-one.json")"
STATUS_AFTER_SYNC_ONE=$(status_json)
(
  cd -- "$WORKSPACE"
  "$AGENTCTL_BIN" --home "$STATE_DIR" --json sync "$SESSION_NAME" >"$LOG_DIR/sync-two.json"
)
jq -e 'all(.providers[]; has("error") | not)' "$LOG_DIR/sync-two.json" >/dev/null \
  || die "provider synchronization failed: $(cat "$LOG_DIR/sync-two.json")"
STATUS_AFTER_SYNC_TWO=$(status_json)

jq -en \
  --argjson before "$STATUS_BEFORE_SYNC" \
  --argjson after "$STATUS_AFTER_SYNC_ONE" '
    ($before.providers | map({key: .session.provider.kind, value: .session.last_synced_seq}) | from_entries) as $old
    | all($after.providers[]; .session.last_synced_seq >= $old[.session.provider.kind])
  ' >/dev/null || die "a provider synchronization cursor moved backwards"
jq -en \
  --argjson first "$STATUS_AFTER_SYNC_ONE" \
  --argjson second "$STATUS_AFTER_SYNC_TWO" '
    $first.latest_seq == $second.latest_seq
    and ($first.providers | map([.session.provider.kind, .session.last_synced_seq, .sync_lag]) | sort)
      == ($second.providers | map([.session.provider.kind, .session.last_synced_seq, .sync_lag]) | sort)
  ' >/dev/null || die "the second sync changed canonical state or provider cursors"
jq -e '
  . as $root
  | any($root.providers[];
      .session.provider.kind == "codex"
      and .session.last_synced_seq == $root.latest_seq
      and .sync_lag == 0)
    and any($root.providers[];
      .session.provider.kind == "claude"
      and .session.last_synced_seq < $root.latest_seq
      and .sync_lag == ($root.latest_seq - .session.last_synced_seq))
' <<<"$STATUS_AFTER_SYNC_TWO" >/dev/null \
  || die "final cursors did not preserve Codex sync and Claude's deferred prompt-bound delta"

FINAL_HISTORY=$(history_json)
jq -e '
  length >= 4
  and ([.[].event.seq] == ([.[].event.seq] | sort))
  and ([.[].event.event_id] | length == (unique | length))
  and any(.[].event; .origin_provider.kind == "claude")
  and any(.[].event; .origin_provider.kind == "codex")
' <<<"$FINAL_HISTORY" >/dev/null \
  || die "canonical history sequence, uniqueness, or provider origin invariant failed"

note "native bridge smoke passed"
printf 'session: %s\n' "$SESSION_ID"
printf 'workspace: %s\n' "$WORKSPACE"
printf 'latest canonical seq: %s\n' "$(jq -r '.latest_seq' <<<"$STATUS_AFTER_SYNC_TWO")"
printf 'model prompts submitted: 0\n'
