# Native provider boundaries

The provider's installed interactive CLI is always the user-facing execution
surface. Non-interactive control-protocol calls exist only to prepare, inspect,
synchronize, or validate a native session; they are not an alternative
conversation path.

## Codex

The foreground process is opened as the native equivalent of:

```bash
codex resume THREAD_ID --cd WORKTREE
```

`agentctl` inherits stdin, stdout, and stderr so Codex retains its normal
terminal UI, approvals, tools, MCP servers, configuration, and agent loop.

On Unix, the interactive bridge connects to the running native Codex daemon
through its control socket using WebSocket JSON-RPC. It checks the daemon
report and requires its version to match the selected CLI. Closing the bridge
connection leaves the shared daemon and other native sessions running.

When no native control endpoint exists, the bridge launches its own
`codex app-server` and uses JSON-RPC/JSONL over stdio. A discovered endpoint
that cannot be validated or connected produces an error. Both transports
complete `initialize`/`initialized`. The control connection is used to:

- create or resume the mapped thread;
- inject a missing canonical handoff with `thread/inject_items`;
- read the thread with `thread/read(includeTurns=true)` after native exit;
- inspect account, capability, usage, and rate-limit signals;
- generate and cache the installed version's JSON Schema.

With a running native daemon on Unix, each Codex CLI launch connects through a
private local relay using the CLI's `--remote` option. Relay endpoints last for
one launch; reopen with `agentctl open codex --session NAME` after exit.
The relay forwards native
protocol traffic and journals successful thread selections from that connection.
It stores thread IDs, pending selection count, and completion state; prompts and
tool payloads are not stored in this evidence. Ephemeral `thread_title` requests
are background title generation and do not select a conversation. Other thread
selections remain subject to continuity validation. Unrelated clients can update or
create sibling threads without being attributed to the mapped launch.

Capture and recovery require complete evidence selecting only the mapped thread.
Native `/new`, `/fork`, or `/resume` to another thread remain detectable even if
the CLI returns to the mapped thread before exiting. Missing responses or a failed
relay block automatic capture. Launches without a relay retain the conservative
workspace snapshot check, including launches already journaled with that format.

External Claude messages can be injected as real user and assistant transcript
items without starting a Codex model turn. Synthetic tool calls are never
fabricated; only bounded summaries of their observable effects are projected.

Native capture and import use stable thread, turn, and item identities. A full
items view is required. Unknown public content, non-terminal turns, or an
incompatible schema stops migration before the synchronization cursor advances.
Private reasoning, complete command output, and full diffs are excluded.

## Claude Code

The foreground process is opened as the native equivalent of either:

```bash
claude --session-id SESSION_UUID
claude --resume SESSION_UUID
```

`agentctl` supplies a private, launch-specific settings overlay containing
documented Claude Code lifecycle hooks. It merges rather than replaces native
behavior. Stdin, stdout, and stderr remain attached to the terminal, and the
user continues to use Claude Code's normal UI, tools, MCP servers, permissions,
plugins, configuration, and agent loop.

Hooks capture `SessionStart`, `UserPromptSubmit`, `PreToolUse`, completed or
failed tool use, `Stop`, failure/rate-limit signals, and `SessionEnd`.
`PreToolUse` establishes a possible side effect before execution. Completion
events close the normalized tool lifecycle; recognized shell tools also emit
command start/completion, while recognized write/edit tools emit changed-file
paths. Every hook invocation must match the journaled launch ID, canonical
session, native session UUID, and worktree lease. A mismatch blocks
continuation and marks the launch uncertain.

Stable hook base identities and active-turn status are indexed for bounded
lookup during hook processing. If a legitimate hook payload exceeds canonical
event limits, large inputs and outputs are replaced with bounded summaries,
selected operational fields, original sizes, and SHA-256 digests. The wrapper
does not claim to preserve the omitted output verbatim.

Claude Code has no public API equivalent to inserting an arbitrary external
assistant message. A missing Codex delta is therefore rendered as a structured
handoff capsule and returned as historical `additionalContext` from the first
real `UserPromptSubmit` hook. A stable appended policy tells Claude to treat
the capsule as untrusted historical data, not higher-priority instructions.
Opening and exiting without a prompt leaves the handoff staged and its cursor
unchanged; delivery is journaled before the projection cursor advances.

`claude -p` with stream-json may be used for compatibility probes and an
official resume handshake. It is never used as the user's working interface or
as a hidden conversation runner. Native transcript contents are opaque and are
never read, modified, or synthesized. For launch continuity only, agentctl may
inspect metadata for the official hook-provided `transcript_path` and treat a
regular, non-symlink, non-empty file named for the exact session UUID as
evidence that Claude has reserved that session ID.

Independent of provider event detail, `agentctl` records Git snapshots before
and after each native launch. A changed snapshot contributes a bounded
workspace projection event. This detects observable repository-state changes;
it is not a complete audit of ignored files, non-Git workspaces, background
processes, or external side effects.

## Moving between providers

For canonical prompt sequence `N`, the target provider receives only confirmed
events through `N - 1`; the current native prompt is created by the user inside
that provider and captured once. Projection identity is:

```text
(provider_session_id, canonical_event_id, projection_version)
```

The unique receipt prevents duplicate context when synchronization is retried.
The non-active provider is allowed to lag until the next `open`, `switch`, or
explicit `sync`.

A local enqueue into a disposable Claude stream-json process is not a native
receipt. In particular, `shouldQuery:false` cannot advance the Claude cursor in
the sync-only runtime: shutdown could race the writer before Claude persisted
the frame. The delta stays in the canonical log, and the next native Claude
launch writes it to the launch-bound `native_handoffs` journal. Only the
`UserPromptSubmit` hook's delivery transaction advances that cursor.

Automatic failover is eligible only when the launch provider was selected
implicitly and no native arguments were forwarded. A health-based fallback
requires a completed captured exit. A crash fallback additionally requires the
journaled process group to be proven dead, provider/canonical capture plus a
post-crash workspace snapshot to succeed, and projection/handoff state to be
unambiguous. Before opening the other provider's native CLI, agentctl persists
a deterministic continuation capsule describing `Possible` or `Confirmed`
side effects and explicitly forbidding replay. It never resubmits the prior
prompt or continues provider execution; the user remains responsible for the
next request inside the newly opened CLI.

Provider-native commands or arguments that change session identity, worktree,
settings ownership, or foreground lifecycle are outside this contract. Exit the
native CLI and perform those actions with `agentctl` so capture remains bound to
the correct session. Forwarding is allowlisted and accepts options only;
positional initial prompts are never accepted by the wrapper. With an implicit
or automatic provider, every forwarded option must validate against both
provider allowlists; provider-specific flags require an explicit provider.

## Existing native sessions

`attach` validates an existing native session through the provider's official
resume contract and stores only the mapping. It does not claim that older
history entered the canonical log.

`import-native codex` additionally calls official `thread/read`, and is the
supported way to bring representable older Codex history into an empty
canonical session. Import is transactional and idempotent.

`import-native claude` is intentionally unsupported because Claude Code exposes
no public full-transcript read contract. An attached Claude session can continue
natively, and newly observed turns become transferable, but its pre-attachment
history remains provider-owned.

## Process plugins

The process-plugin protocol is a management extension, not a target for
`agentctl open` or `agentctl switch`. Only providers with a supported installed
interactive CLI can participate in the native launch workflow.
