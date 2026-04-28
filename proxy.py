#!/usr/bin/env python3
"""
AgentComms stdio proxy for Claude Code.

Each Claude Code session spawns its own proxy. The proxy:
  - derives a stable agent_id from the project cwd at startup,
  - auto-registers with the hub on the first tool call (no manual register_agent needed),
  - forwards tool calls to the hub's /mcp endpoint,
  - piggybacks any pending inbox messages on every tool response so the agent
    sees peer messages without having to call get_messages.

Hub must be running at HUB_URL before Claude Code starts.

.mcp.json entry:
  "agent-comms": {
    "type": "stdio",
    "command": "C:\\Python312\\python.exe",
    "args": ["B:\\-AI-Stuff-\\-=MCP-Servers=-\\AgentComms\\proxy.py"]
  }
"""
from __future__ import annotations
import asyncio, hashlib, json, os, sys, time, uuid
from typing import Optional
import httpx
from mcp.server.fastmcp import FastMCP

HUB_URL = "http://127.0.0.1:7771/mcp"
_HDR = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}

# ── Agent identity (derived once at startup) ──────────────────────────────────

def _derive_agent_id() -> tuple[str, str]:
    """Return (agent_id, project_name) derived from the proxy's cwd.

    Claude Code spawns MCP servers with the project root as cwd. The 8-char
    hash includes the absolute, normcase'd path so two different projects
    with the same folder name don't collide; the project name is kept human-
    readable as the prefix.
    """
    cwd = os.path.abspath(os.getcwd())
    project = os.path.basename(cwd) or "unknown"
    safe_project = "".join(c if c.isalnum() or c in "-_" else "-" for c in project).strip("-") or "unknown"
    h = hashlib.sha1(os.path.normcase(cwd).encode("utf-8")).hexdigest()[:8]
    return f"{safe_project}-{h}", project


_agent_id, _project_name = _derive_agent_id()
_session_id: Optional[str] = None
_registered: bool = False
_initialized: bool = False
_last_register_ts: float = 0.0
# Refresh registration well before the hub's 120s TTL so the reaper never
# evicts us between tool calls. The hub register_agent is idempotent — calling
# it on an already-registered agent just heartbeats.
REGISTER_REFRESH_S = 60.0


def _reset_session() -> None:
    """Forget hub session + registration so the next call re-establishes both.
    Called when the hub responds with a session-invalidation error (typical
    after a hub restart or session timeout)."""
    global _session_id, _initialized, _registered, _last_register_ts
    _session_id = None
    _initialized = False
    _registered = False
    _last_register_ts = 0.0


def _is_session_error(msg: str) -> bool:
    """Heuristic: does this error message indicate the hub forgot our session?"""
    m = (msg or "").lower()
    return any(k in m for k in (
        "session", "not initialized", "invalid session", "unknown session",
    ))


# ── Hub IO ────────────────────────────────────────────────────────────────────

async def _hub(method: str, params: Optional[dict] = None) -> dict:
    """Send one MCP JSON-RPC request to the hub, return the result dict.

    Fresh httpx client per call — the hub closes the SSE stream after each
    response, so reusing connections causes ReadError on the next request.
    """
    global _session_id

    payload: dict = {"jsonrpc": "2.0", "method": method, "id": str(uuid.uuid4())}
    if params:
        payload["params"] = params

    headers = {**_HDR}
    if _session_id:
        headers["Mcp-Session-Id"] = _session_id

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(HUB_URL, json=payload, headers=headers)

    if "mcp-session-id" in r.headers:
        _session_id = r.headers["mcp-session-id"]

    for line in r.text.splitlines():
        if line.startswith("data:"):
            data = json.loads(line[5:].strip())
            if "result" in data:
                return data["result"]
            if "error" in data:
                raise RuntimeError(data["error"].get("message", "Hub error"))

    raise RuntimeError(f"No data in hub response (status {r.status_code}): {r.text[:200]}")


async def _ensure_session() -> None:
    global _initialized
    if _initialized:
        return
    await _hub("initialize", params={
        "protocolVersion": "2025-11-25",
        "capabilities": {},
        "clientInfo": {"name": "AgentComms-proxy", "version": "1.1.0"},
    })
    _initialized = True


async def _ensure_registered() -> None:
    """Register the auto-derived agent_id with the hub. Safe to call repeatedly.
    Re-registers every REGISTER_REFRESH_S seconds (60s) to outpace the hub's
    120s TTL reaper, so an idle proxy is never silently de-registered."""
    global _registered, _last_register_ts
    now = time.time()
    if _registered and (now - _last_register_ts) < REGISTER_REFRESH_S:
        return
    await _ensure_session()
    await _hub("tools/call", params={
        "name": "register_agent",
        "arguments": {
            "agent_id": _agent_id,
            "project": _project_name,
            "description": f"auto-registered from {os.getcwd()}",
        },
    })
    _registered = True
    _last_register_ts = now


def _format_pending(messages: list) -> str:
    """Format inbox messages for piggyback inclusion in tool responses."""
    if not messages:
        return ""
    lines = [f"\n\n📨 {len(messages)} new message(s) for {_agent_id}:"]
    for m in messages:
        ts = (m.get("timestamp") or "")[:19].replace("T", " ")
        sender = m.get("from", "?")
        mtype = m.get("type", "msg")
        content = (m.get("content") or "").strip()
        if len(content) > 400:
            content = content[:400] + " …[truncated]"
        topic = m.get("topic")
        topic_tag = f" #{topic}" if topic else ""
        lines.append(f"  • [{ts}] {sender} ({mtype}{topic_tag}): {content}")
    return "\n".join(lines)


async def _fetch_pending() -> list:
    """Pull-and-clear unread messages for this agent. Used for piggyback."""
    if not _registered:
        return []
    try:
        result = await _hub("tools/call", params={
            "name": "get_messages",
            "arguments": {"agent_id": _agent_id, "clear": True, "limit": 50},
        })
        texts = [c["text"] for c in result.get("content", []) if c.get("type") == "text"]
        if not texts:
            return []
        data = json.loads(texts[0])
        return data.get("messages") or []
    except Exception:
        return []


async def _call(name: str, *, piggyback: bool = True, **kwargs) -> str:
    """Invoke a tool on the hub. Auto-registers, returns text + piggyback inbox.
    On session-invalidation errors (hub restart), resets cached session/registration
    state and retries once before returning an error."""
    async def _attempt() -> str:
        await _ensure_registered()
        result = await _hub("tools/call", params={"name": name, "arguments": kwargs})
        texts = [c["text"] for c in result.get("content", []) if c.get("type") == "text"]
        return "\n".join(texts) if texts else json.dumps(result)

    try:
        out = await _attempt()
    except RuntimeError as e:
        if _is_session_error(str(e)):
            _reset_session()
            try:
                out = await _attempt()
            except RuntimeError as e2:
                return f"ERROR: {e2}"
            except httpx.ConnectError:
                return "ERROR: AgentComms hub is not running at http://127.0.0.1:7771 — start it first."
        else:
            return f"ERROR: {e}"
    except httpx.ConnectError:
        return "ERROR: AgentComms hub is not running at http://127.0.0.1:7771 — start it first."

    # Piggyback pending inbox unless we just consumed it ourselves.
    if piggyback and name != "get_messages":
        pending = await _fetch_pending()
        if pending:
            out += _format_pending(pending)
    return out


def _me(value: Optional[str]) -> str:
    """Resolve an agent_id parameter — fall back to the auto-derived id when blank."""
    return value if value else _agent_id


# ── MCP server ────────────────────────────────────────────────────────────────

mcp = FastMCP("AgentComms")


@mcp.tool()
async def whoami() -> str:
    """
    Return this session's auto-registered agent identity and any pending inbox.
    Use this to discover your agent_id — you don't need to call register_agent
    yourself; the proxy registers automatically on the first tool call.
    """
    await _ensure_registered()
    pending = await _fetch_pending()
    payload = {
        "agent_id": _agent_id,
        "project": _project_name,
        "cwd": os.getcwd(),
        "pid": os.getpid(),
        "registered": _registered,
        "pending_messages": pending,
    }
    out = json.dumps(payload, indent=2)
    if pending:
        out += _format_pending(pending)
    return out


@mcp.tool()
async def register_agent(
    agent_id: Optional[str] = None,
    project: Optional[str] = None,
    description: str = "",
) -> str:
    """
    Re-announce this session, optionally overriding the auto-derived identity.
    Normally you don't need to call this — the proxy auto-registers on first use.
    Useful if you want a custom agent_id or project label.
    """
    aid = agent_id or _agent_id
    proj = project or _project_name
    return await _call("register_agent", agent_id=aid, project=proj, description=description)


@mcp.tool()
async def unregister_agent(agent_id: Optional[str] = None) -> str:
    """Gracefully unregister an agent (defaults to this session's auto-id)."""
    return await _call("unregister_agent", agent_id=_me(agent_id), piggyback=False)


@mcp.tool()
async def heartbeat(agent_id: Optional[str] = None) -> str:
    """
    Refresh the TTL clock for an agent (defaults to this session's auto-id).
    Rarely needed — every tool call already heartbeats. Only useful during long
    silent stretches with no tool activity.
    """
    return await _call("heartbeat", agent_id=_me(agent_id))


@mcp.tool()
async def evict_agent(agent_id: str, requested_by: Optional[str] = None) -> str:
    """Force-remove an agent that crashed or failed to clean up."""
    return await _call("evict_agent", agent_id=agent_id,
                       requested_by=requested_by or _agent_id)


@mcp.tool()
async def list_agents(requesting_agent_id: Optional[str] = None) -> str:
    """List all registered agents with project, description, inbox count, last-seen."""
    return await _call("list_agents", requesting_agent_id=_me(requesting_agent_id))


@mcp.tool()
async def ping_agent(target_agent_id: str, from_agent: Optional[str] = None) -> str:
    """Ping a specific agent (from_agent defaults to this session's auto-id)."""
    return await _call("ping_agent", from_agent=_me(from_agent),
                       target_agent_id=target_agent_id)


@mcp.tool()
async def send_message(
    to_agent: str,
    content: str,
    message_type: str = "notification",
    from_agent: Optional[str] = None,
) -> str:
    """
    Send a direct message to another agent.
    from_agent defaults to this session's auto-id.
    message_type: 'notification', 'request', 'response', 'alert', or any label.
    """
    return await _call("send_message", from_agent=_me(from_agent),
                       to_agent=to_agent, content=content, message_type=message_type)


@mcp.tool()
async def broadcast_message(
    content: str,
    message_type: str = "broadcast",
    from_agent: Optional[str] = None,
) -> str:
    """Broadcast to ALL other agents (from_agent defaults to this session's auto-id)."""
    return await _call("broadcast_message", from_agent=_me(from_agent),
                       content=content, message_type=message_type)


@mcp.tool()
async def get_messages(
    agent_id: Optional[str] = None,
    clear: bool = True,
    limit: int = 100,
) -> str:
    """
    Read pending messages for an agent (defaults to this session's auto-id).
    clear=True consumes them; clear=False peeks. Note that any other tool call
    also flushes the inbox via piggyback, so explicit reads are usually
    redundant — use this if you want the raw JSON or to peek with clear=False.
    """
    return await _call("get_messages", agent_id=_me(agent_id),
                       clear=clear, limit=limit)


@mcp.tool()
async def subscribe_to_channel(
    channel_owner_id: str,
    subscriber_id: Optional[str] = None,
) -> str:
    """Subscribe to another agent's broadcast channel (subscriber defaults to self)."""
    return await _call("subscribe_to_channel",
                       subscriber_id=_me(subscriber_id),
                       channel_owner_id=channel_owner_id)


@mcp.tool()
async def unsubscribe_from_channel(
    channel_owner_id: str,
    subscriber_id: Optional[str] = None,
) -> str:
    """Unsubscribe from a channel (subscriber defaults to self)."""
    return await _call("unsubscribe_from_channel",
                       subscriber_id=_me(subscriber_id),
                       channel_owner_id=channel_owner_id)


@mcp.tool()
async def publish_to_channel(
    content: str,
    topic: str = "",
    agent_id: Optional[str] = None,
) -> str:
    """
    Publish to your own channel (agent_id defaults to this session's auto-id).
    topic is an optional label: 'status', 'progress', 'error', etc.
    """
    return await _call("publish_to_channel", agent_id=_me(agent_id),
                       content=content, topic=topic)


@mcp.tool()
async def mute_agent(target_agent_id: str, agent_id: Optional[str] = None) -> str:
    """
    Mute an agent — silently drop their messages from your inbox.
    The sender continues to see successful sends; you simply don't get them.
    Covers direct messages, broadcasts, pings, and channel publishes.
    agent_id defaults to this session's auto-id.
    """
    return await _call("mute_agent", agent_id=_me(agent_id),
                       target_agent_id=target_agent_id)


@mcp.tool()
async def unmute_agent(target_agent_id: str, agent_id: Optional[str] = None) -> str:
    """Unmute an agent — restore delivery from them. agent_id defaults to self."""
    return await _call("unmute_agent", agent_id=_me(agent_id),
                       target_agent_id=target_agent_id)


@mcp.tool()
async def list_muted(agent_id: Optional[str] = None) -> str:
    """List agents you have muted. agent_id defaults to this session's auto-id."""
    return await _call("list_muted", agent_id=_me(agent_id))


@mcp.tool()
async def get_channel_history(channel_owner_id: str, limit: int = 50) -> str:
    """Read recent publishes on any agent's channel without subscribing."""
    return await _call("get_channel_history",
                       channel_owner_id=channel_owner_id, limit=limit)


@mcp.tool()
async def get_agent_status(agent_id: Optional[str] = None) -> str:
    """Get full status for an agent (defaults to this session's auto-id)."""
    return await _call("get_agent_status", agent_id=_me(agent_id))


if __name__ == "__main__":
    print(f"AgentComms proxy starting — agent_id={_agent_id} project={_project_name}",
          file=sys.stderr, flush=True)
    mcp.run(transport="stdio")
