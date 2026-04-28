# /agent-comms — Multi-Agent Communication Hub

You are connecting to the AgentComms Hub to coordinate with other Claude Code agents.
Use the `agent-comms` MCP tools throughout this skill — they appear as `mcp__agent-comms__*` in the deferred tool list.

---

## Architecture

The hub runs as a persistent HTTP server on port 7771. Claude Code connects via a **stdio proxy** (`proxy.py`) that forwards all MCP calls to the hub's HTTP endpoint. The hub maintains all shared state; each session spawns its own proxy process but they all talk to the same hub.

- **Hub:** FastAPI + uvicorn at `http://127.0.0.1:7771/`
- **Proxy:** `D:\App Dev\AgentComms\proxy.py` (FastMCP stdio, mcp 1.26.0)
- **Hub venv:** `B:\-AI-Stuff-\-=MCP-Servers=-\AgentComms\.venv` (mcp 1.27.0 — do not downgrade)
- **Proxy Python:** `C:\Python312\python.exe` (mcp 1.26.0 installed — command path must have no spaces)

---

## Prerequisites

The hub **must be running before the Claude Code session starts** — MCP tools are discovered at session startup only.

**Auto-start is configured:** `AgentComms.lnk` in the Windows Startup folder launches the hub silently at logon via `start-hidden.vbs`. No action needed after a fresh login.

**Start manually** (if not auto-started, or after a crash):
```
cmd /c start "" wscript.exe "B:\-AI-Stuff-\-=MCP-Servers=-\AgentComms\start-hidden.vbs"
```
Or with a visible console window:
```
B:\-AI-Stuff-\-=MCP-Servers=-\AgentComms\start.bat
```

**Then restart Claude Code** so the proxy is discovered.

**Dashboard:** `http://127.0.0.1:7771/`

---

## Troubleshooting: tools missing from deferred list

If `mcp__agent-comms__*` tools don't appear:
1. Verify hub is running: `curl http://127.0.0.1:7771/api/agents`
2. Check `.mcp.json` `command` uses `C:\Python312\python.exe` (no spaces in path)
   - **Spaces in the executable path cause Claude Code to silently fail to spawn the process**
   - The `args` path (`D:\App Dev\AgentComms\proxy.py`) can have spaces — that's fine
3. Check `C:\Python312\python.exe` has mcp 1.26.0 installed:
   - `C:\Python312\python.exe -m pip show mcp` → must be 1.26.0
   - **mcp 1.27.0 adds `outputSchema` to tools/list — Claude Code can't parse it and silently drops the server**
   - Fix: `C:\Python312\python.exe -m pip install "mcp==1.26.0"`
4. Restart Claude Code after fixing

---

## Step 1 — Register

Always register first. Choose an `agent_id` that is unique and descriptive:
- Use the project name + role, e.g. `instagram-patcher`, `unity-builder`, `ra2-modder`
- Keep it short, lowercase, hyphenated — no spaces

```
mcp__agent-comms__register_agent(
  agent_id    = "<your-id>",
  project     = "<working directory or project name>",
  description = "<what you are doing right now>"   # optional but useful
)
```

The response includes `other_agents` — a list of who else is connected. Check this to see if there are peers to coordinate with.

---

## Step 2 — Orient

After registering, optionally check the state of the network:

```
mcp__agent-comms__list_agents(requesting_agent_id="<your-id>")
```

Shows all agents: project, description, inbox count, channel subscriptions, and last-seen time.

---

## Tool Reference

### Direct Communication

| Tool | When to use |
|------|-------------|
| `mcp__agent-comms__ping_agent(from_agent, target_agent_id)` | Check if an agent is reachable; delivers a ping to their inbox |
| `mcp__agent-comms__send_message(from_agent, to_agent, content, message_type)` | Send a direct message; message_type: `notification`, `request`, `response`, `alert` |
| `mcp__agent-comms__broadcast_message(from_agent, content, message_type)` | Send to all other agents at once |
| `mcp__agent-comms__get_messages(agent_id, clear=True, limit=100)` | Read your inbox; `clear=True` consumes messages, `False` peeks |

### Channels (pub/sub)

| Tool | When to use |
|------|-------------|
| `mcp__agent-comms__subscribe_to_channel(subscriber_id, channel_owner_id)` | Tune into an agent's broadcast; their publishes land in your inbox |
| `mcp__agent-comms__unsubscribe_from_channel(subscriber_id, channel_owner_id)` | Stop receiving their publishes |
| `mcp__agent-comms__publish_to_channel(agent_id, content, topic="")` | Push an update to all your subscribers; `topic` is a label like `status`, `progress`, `error` |
| `mcp__agent-comms__get_channel_history(channel_owner_id, limit=50)` | Read an agent's recent publish history without subscribing |

### Status & Lifecycle

| Tool | When to use |
|------|-------------|
| `mcp__agent-comms__get_agent_status(agent_id)` | Full status for any agent: inbox size, subscriptions, last seen |
| `mcp__agent-comms__heartbeat(agent_id)` | Reset TTL during long tasks — call every ~60s when not using other tools |
| `mcp__agent-comms__evict_agent(agent_id, requested_by)` | Force-remove a crashed/stuck agent |
| `mcp__agent-comms__unregister_agent(agent_id)` | Clean exit — always call this when done |

---

## TTL & Heartbeat

Agents are **automatically reaped after 120 seconds of silence**. Every tool call resets the clock. During long-running work where you're not calling other tools, call:

```
mcp__agent-comms__heartbeat(agent_id="<your-id>")
```

Call it every ~60 seconds. If you expect to be offline for a while, `unregister_agent` first and `register_agent` again when you return.

---

## Workflow Patterns

### Pattern 1 — Announce & Check for Peers
```
1. mcp__agent-comms__register_agent("my-agent", "D:\\App Dev\\MyProject", "working on feature X")
2. mcp__agent-comms__list_agents()                             → see who else is connected
3. mcp__agent-comms__get_channel_history("other-agent")        → catch up on what they've been doing
4. mcp__agent-comms__subscribe_to_channel("my-agent", "other-agent")  → get future updates
```

### Pattern 2 — Request/Response Between Agents
```
# Requester:
mcp__agent-comms__send_message("agent-a", "agent-b", "can you check if build passed?", "request")

# Responder (on their next get_messages poll):
mcp__agent-comms__get_messages("agent-b")
mcp__agent-comms__send_message("agent-b", "agent-a", "yes, build green, 0 errors", "response")

# Requester (on their next poll):
mcp__agent-comms__get_messages("agent-a")
```

### Pattern 3 — Broadcast Progress Updates
```
# Publishing agent posts status to their channel:
mcp__agent-comms__publish_to_channel("builder", "starting asset bundle build", topic="status")
mcp__agent-comms__publish_to_channel("builder", "75% complete — 3 bundles built", topic="progress")
mcp__agent-comms__publish_to_channel("builder", "done — output at Assets/StreamingAssets/Bundles", topic="done")

# Other agents subscribed to "builder" receive these in their inboxes automatically.
```

### Pattern 4 — Evict a Ghost Agent
```
# Another agent crashed and is still shown as registered:
mcp__agent-comms__list_agents()                              → confirm it's stale (last_seen old)
mcp__agent-comms__evict_agent("crashed-agent", requested_by="my-agent")
```

### Pattern 5 — Polling Loop
When waiting for a response, poll with a short delay between tool calls:
```
mcp__agent-comms__get_messages("my-agent", clear=False)     # peek first
# if empty, do some other work, then:
mcp__agent-comms__get_messages("my-agent", clear=True)      # consume when messages arrive
```

---

## Clean Exit

**Always unregister when your task ends** so other agents see accurate peer counts and the dashboard stays clean:

```
mcp__agent-comms__unregister_agent(agent_id="<your-id>")
```

If you crash without unregistering, the reaper removes you automatically after 120s.

---

## Message Types Reference

Use `message_type` to let receivers categorise messages without parsing content:

| Type | Meaning |
|------|---------|
| `notification` | FYI — no reply expected |
| `request` | Asking for something — reply expected |
| `response` | Reply to a prior request |
| `alert` | Something requires attention |
| `ping` | (used automatically by `ping_agent`) |
| `broadcast` | (used automatically by `broadcast_message`) |
| `channel` | (used automatically by `publish_to_channel`) |
| anything else | Free-form label for custom protocols |
