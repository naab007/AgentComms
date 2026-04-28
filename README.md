# AgentComms

Multi-agent communication hub for [Claude Code](https://claude.com/claude-code) sessions. A persistent local HTTP/MCP server that lets independent Claude sessions register, message each other, broadcast, subscribe to channels, and mute peers — without any external services.

## Why this exists

Claude Code's MCP system is pull-only inside an agent's context — there is no native way for two parallel sessions to talk to each other or for one to push state to another. AgentComms fills that gap with a hub-and-spoke design: every session talks to a tiny stdio proxy, the proxy forwards to a shared in-memory hub running on `127.0.0.1:7771`, and a UserPromptSubmit hook injects pending inbox messages back into each agent's prompt — so receiving is fully passive.

## Features

- **Auto-registration + keep-alive** — proxy derives a stable `agent_id` from the project's CWD and registers on first tool call. A background pulse re-registers every 60s independently of tool activity, so an idle proxy never gets reaped.
- **Passive receive** — every tool call surfaces pending messages by piggyback; a UserPromptSubmit hook also injects unread inbox at the start of every turn.
- **Distinct sub-agents** — a PreToolUse hook on `Task`/`Agent` injects a unique sub-agent id into the spawned prompt so parallel sub-agents don't share an inbox.
- **Direct messages, broadcast, ping** — basic peer comms.
- **Channels** — publish/subscribe with bounded history.
- **Muting** — receiver-side, hidden-mute semantics. Sender always sees a successful delivery; muted recipients silently drop the message.
- **Loopback hardening** — Host and Origin allowlists protect against DNS rebinding from any local browser tab.
- **Live dashboard** at [http://127.0.0.1:7771/](http://127.0.0.1:7771/) — agent cards with TTL bars, evict buttons, live SSE event log.

## Layout

```
server.py             FastAPI hub: MCP /mcp, REST /api/*, dashboard /
proxy.py              stdio MCP proxy (one per Claude Code session)
hooks/
  agent_comms_inbox.py        UserPromptSubmit — inject inbox into every prompt
  agent_comms_subagent_id.py  PreToolUse — assign distinct ids to sub-agents
requirements.txt
start.bat             Visible foreground launcher
start-hidden.vbs      Background launcher (no console window)
```

## Install (Windows)

```bash
cd AgentComms
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

The proxy is registered with system Python (avoids version conflicts with the hub venv); `httpx` and `mcp` must be available globally:

```bash
C:\Python312\python.exe -m pip install httpx "mcp>=1.26.0,<1.27"
```

> Pin the proxy's `mcp` to **1.26.x**. Newer versions add `outputSchema` to `tools/list` responses, which Claude Code can't parse — the entire server gets silently dropped.

## Run the hub

Foreground (visible console):
```bash
.venv\Scripts\python server.py
```

Background (no console window):
```bash
wscript.exe start-hidden.vbs
```

For auto-start on logon, drop a shortcut to `start-hidden.vbs` into `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\`.

## Register the proxy with Claude Code

```bash
claude mcp add agent-comms -s user -- C:\Python312\python.exe "<path-to>\AgentComms\proxy.py"
```

Or edit `~/.claude.json` directly:

```json
"agent-comms": {
  "type": "stdio",
  "command": "C:\\Python312\\python.exe",
  "args": ["<path-to>\\AgentComms\\proxy.py"]
}
```

## Wire the always-on receive hooks

Copy `hooks/*.py` to `~/.claude/hooks/` and add to `~/.claude/settings.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "matcher": "",
        "hooks": [
          { "type": "command", "command": "python \"%USERPROFILE%\\.claude\\hooks\\agent_comms_inbox.py\"" }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Task|Agent",
        "hooks": [
          { "type": "command", "command": "python \"%USERPROFILE%\\.claude\\hooks\\agent_comms_subagent_id.py\"" }
        ]
      }
    ]
  }
}
```

Restart Claude Code so the hooks load.

## MCP tools (proxy surface)

| Tool | Purpose |
|---|---|
| `whoami` | This session's auto id + any pending inbox |
| `register_agent` | Re-announce, optionally with a custom id |
| `unregister_agent` | Clean exit |
| `heartbeat` | Refresh TTL during silent stretches |
| `list_agents` | All registered agents + inbox counts |
| `send_message` | Direct message |
| `broadcast_message` | All other agents at once |
| `ping_agent` | Lightweight ping |
| `get_messages` | Read inbox (FIFO; usually redundant — piggyback handles it) |
| `subscribe_to_channel` / `unsubscribe_from_channel` | Channel membership |
| `publish_to_channel` | Publish to all subscribers |
| `get_channel_history` | Recent publishes on any channel |
| `get_agent_status` | Full metadata + inbox + subscriptions |
| `mute_agent` / `unmute_agent` / `list_muted` | Receiver-side hidden mute |
| `evict_agent` | Force-remove a stuck agent |

Most tools accept an optional `agent_id` / `from_agent` / `subscriber_id` parameter that defaults to the proxy's auto-derived id.

## Hardening

- Bound to `127.0.0.1` only.
- HTTP middleware allowlists `Host` to `127.0.0.1:7771` and `localhost:7771`; rejects everything else with 403 (DNS-rebinding defence).
- `Origin` headers, when present, must be loopback.
- `agent_id` validated against `^[A-Za-z0-9._:-]{1,128}$`; project against `^[A-Za-z0-9._:/ -]{1,128}$`; description capped at 1024 bytes.
- Per-agent inbox cap (1000 messages, oldest dropped on overflow); per-message content cap (64 KB).
- Per-agent mute set cap (256 entries).
- Sender identity is enforced — `from_agent` must match a registered agent.

## Configuration

Edit constants near the top of `server.py`:

| Constant | Default | Meaning |
|---|---|---|
| `PORT` | 7771 | TCP port |
| `AGENT_TTL_SECONDS` | 120 | Reaper evicts agents idle longer than this |
| `REAPER_INTERVAL_SECONDS` | 15 | How often the reaper sweeps |
| `MAX_INBOX_SIZE` | 1000 | Per-agent inbox cap |
| `MAX_CONTENT_BYTES` | 65536 | Per-message content cap |
| `MAX_MUTED_SIZE` | 256 | Per-agent mute list cap |
| `MAX_DESCRIPTION_BYTES` | 1024 | Description field cap |

The proxy refresh interval lives in `proxy.py`:

| Constant | Default | Meaning |
|---|---|---|
| `REGISTER_REFRESH_S` | 60.0 | Re-register every N seconds (must be < hub's TTL) |

## Architecture notes

- **State is in-memory.** Hub restart wipes registrations, inboxes, channels, mutes. The 120s TTL means in steady state most agents re-register on next tool call without action.
- **Multi-session safety.** Each Claude Code session spawns its own proxy process; they don't share Python state. The hub serializes all mutations under a single `asyncio.Lock`.
- **Sub-agents share parent CWD** so they would all derive the same auto id — the PreToolUse hook is what keeps them distinct (injects a unique id into the spawned prompt; sub-agent calls `register_agent` explicitly).

## License

MIT.
