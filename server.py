#!/usr/bin/env python3
"""
AgentComms MCP Server
Multi-agent communication hub for Claude Code agents.

Runs an HTTP server with:
  - MCP SSE endpoint at  /sse          (for Claude Code integration)
  - REST API at          /api/...      (programmatic access)
  - Web dashboard at     /             (browser monitoring)
"""

import asyncio
import json
import re
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

# ── Config ────────────────────────────────────────────────────────────────────

HOST = "127.0.0.1"
PORT = 7771

# Agents not seen within this window are reaped automatically.
# Any tool call (including heartbeat) resets the clock.
AGENT_TTL_SECONDS = 120

# How often the reaper wakes up to check.
REAPER_INTERVAL_SECONDS = 15

# Per-agent inbox cap. Oldest messages are dropped when exceeded.
MAX_INBOX_SIZE = 1000

# Per-message content cap (UTF-8 bytes). Larger sends are rejected.
MAX_CONTENT_BYTES = 65536

# Per-agent mute set cap. Prevents an attacker from flooding a mute list
# with phantom ids to exhaust memory.
MAX_MUTED_SIZE = 256

# Identity / metadata format. Restrictive enough to render safely in HTML
# without escaping, prevent JS-string-injection in templated handlers, and
# survive logs/JSON without ambiguity.
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")
PROJECT_RE = re.compile(r"^[A-Za-z0-9._:\-/ ]{1,128}$")
MAX_DESCRIPTION_BYTES = 1024

# Loopback hardening — Host and Origin allowlists for the HTTP listener.
# 127.0.0.1 binding alone is not enough: a malicious page can DNS-rebind
# a hostname to 127.0.0.1 and POST from any browser tab.
_ALLOWED_HOSTS = {f"127.0.0.1:{PORT}", f"localhost:{PORT}", "127.0.0.1", "localhost"}
_ALLOWED_ORIGINS = {f"http://127.0.0.1:{PORT}", f"http://localhost:{PORT}"}

# ── State ─────────────────────────────────────────────────────────────────────

_agents: dict[str, dict] = {}           # agent_id → info
_inboxes: dict[str, list] = {}          # agent_id → [msg, ...]
_channels: dict[str, set] = {}          # agent_id → {subscriber_ids}
_channel_history: dict[str, list] = {}  # agent_id → [msg, ...]
_muted: dict[str, set] = {}             # receiver_id → {muted_sender_ids}
_event_log: list[dict] = []             # global ordered event log (capped at 500)
_lock = asyncio.Lock()
_dashboard_clients: list[asyncio.Queue] = []  # SSE dashboard streams
_reaper_task: Optional[asyncio.Task] = None  # held to prevent GC of the background task


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mid() -> str:
    return str(uuid.uuid4())[:8]


async def _log_event(kind: str, data: dict) -> None:
    """Append to global event log and push to dashboard SSE subscribers."""
    entry = {"event": kind, "timestamp": _now(), **data}
    _event_log.append(entry)
    if len(_event_log) > 500:
        _event_log.pop(0)
    for q in list(_dashboard_clients):
        try:
            q.put_nowait(entry)
        except asyncio.QueueFull:
            pass


def _remove_agent(agent_id: str) -> None:
    """Remove an agent and clean up its state. Must be called under _lock."""
    _agents.pop(agent_id, None)
    _inboxes.pop(agent_id, None)
    for subs in _channels.values():
        subs.discard(agent_id)
    _channels.pop(agent_id, None)
    _channel_history.pop(agent_id, None)
    _muted.pop(agent_id, None)
    for muted_set in _muted.values():
        muted_set.discard(agent_id)


def _is_muted(receiver: str, sender: str) -> bool:
    """True if `receiver` has muted `sender`. Must be called under _lock."""
    return sender in _muted.get(receiver, set())


def _enforce_inbox_cap(aid: str) -> int:
    """Drop oldest messages if inbox exceeds MAX_INBOX_SIZE. Must be called under _lock.
    Returns the number of dropped messages (0 if under cap)."""
    inbox = _inboxes.get(aid)
    if not inbox or len(inbox) <= MAX_INBOX_SIZE:
        return 0
    drop = len(inbox) - MAX_INBOX_SIZE
    _inboxes[aid] = inbox[drop:]
    return drop


def _content_too_large(content: str) -> bool:
    """True if content (UTF-8 encoded) exceeds MAX_CONTENT_BYTES."""
    return len(content.encode("utf-8", errors="replace")) > MAX_CONTENT_BYTES


def _validate_identity(agent_id: str, project: str, description: str) -> Optional[str]:
    """Return an error message if any field is malformed, else None.
    agent_id and project use restrictive whitelists so they render in
    dashboard HTML without escaping. Description is free-form but byte-capped."""
    if not isinstance(agent_id, str) or not AGENT_ID_RE.match(agent_id):
        return ("agent_id must be 1-128 chars of [A-Za-z0-9._:-]")
    if not isinstance(project, str) or not PROJECT_RE.match(project):
        return ("project must be 1-128 chars of [A-Za-z0-9._:/ -]")
    if not isinstance(description, str):
        return "description must be a string"
    if len(description.encode("utf-8", errors="replace")) > MAX_DESCRIPTION_BYTES:
        return f"description exceeds {MAX_DESCRIPTION_BYTES} bytes"
    return None


async def _reaper_loop() -> None:
    """Background task: evict agents that haven't been seen within AGENT_TTL_SECONDS."""
    while True:
        await asyncio.sleep(REAPER_INTERVAL_SECONDS)
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=AGENT_TTL_SECONDS)
        evicted = []
        async with _lock:
            for aid, info in list(_agents.items()):
                last = datetime.fromisoformat(info["last_seen"])
                if last < cutoff:
                    _remove_agent(aid)
                    evicted.append(aid)
        for aid in evicted:
            await _log_event("evict", {"agent_id": aid, "reason": "ttl_expired",
                                       "ttl_seconds": AGENT_TTL_SECONDS})


# ── MCP Server ────────────────────────────────────────────────────────────────

mcp = FastMCP("AgentComms")


@mcp.tool()
async def register_agent(agent_id: str, project: str, description: str = "") -> str:
    """
    Register this agent with the comms hub.
    Always call this first. Re-registering updates metadata without losing messages.
    Returns the current list of other connected agents.

    agent_id: 1-128 chars, [A-Za-z0-9._:-] only.
    project: 1-128 chars, [A-Za-z0-9._:/ -] only.
    description: free-form, up to 1024 UTF-8 bytes.
    """
    err = _validate_identity(agent_id, project, description)
    if err:
        return json.dumps({"status": "error", "message": err})
    async with _lock:
        existed = agent_id in _agents
        _agents[agent_id] = {
            "project": project,
            "description": description,
            "registered_at": _agents.get(agent_id, {}).get("registered_at", _now()),
            "last_seen": _now(),
        }
        _inboxes.setdefault(agent_id, [])
        _channels.setdefault(agent_id, set())
        _channel_history.setdefault(agent_id, [])
        _muted.setdefault(agent_id, set())
        others = [aid for aid in _agents if aid != agent_id]

    action = "re-registered" if existed else "registered"
    await _log_event("register", {"agent_id": agent_id, "project": project, "action": action})
    return json.dumps({
        "status": "ok",
        "action": action,
        "agent_id": agent_id,
        "other_agents": others,
    }, indent=2)


@mcp.tool()
async def unregister_agent(agent_id: str) -> str:
    """Gracefully unregister this agent and clean up all subscriptions."""
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "error", "message": f"Agent '{agent_id}' not found"})
        _remove_agent(agent_id)

    await _log_event("unregister", {"agent_id": agent_id})
    return json.dumps({"status": "ok", "message": f"Agent '{agent_id}' unregistered"})


@mcp.tool()
async def heartbeat(agent_id: str) -> str:
    """
    Signal that this agent is still alive without doing anything else.
    Call this periodically during long-running work to avoid being reaped.
    The TTL resets on every tool call, so this is only needed when you're
    not calling other tools for more than ~{ttl}s.
    """.format(ttl=AGENT_TTL_SECONDS)
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{agent_id}' not registered — call register_agent first"})
        _agents[agent_id]["last_seen"] = _now()

    return json.dumps({"status": "ok", "agent_id": agent_id,
                       "ttl_seconds": AGENT_TTL_SECONDS, "timestamp": _now()})


@mcp.tool()
async def evict_agent(agent_id: str, requested_by: str = "admin") -> str:
    """
    Forcibly remove an agent that failed to clean up (crashed, hung, etc.).
    Any agent or an admin can call this. Prefer unregister_agent for clean exits.

    Authorization note: there is no per-caller auth on this endpoint. Any
    process that can reach the hub (now restricted to loopback Host/Origin
    via _loopback_guard) can evict any agent. If the hub is ever exposed
    beyond loopback, gate this on a shared-secret token or self-evict only.
    """
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "not_found",
                               "message": f"Agent '{agent_id}' is not registered"})
        _remove_agent(agent_id)

    await _log_event("evict", {"agent_id": agent_id, "reason": "manual",
                                "requested_by": requested_by})
    return json.dumps({"status": "ok", "message": f"Agent '{agent_id}' evicted",
                       "requested_by": requested_by})


@mcp.tool()
async def list_agents(requesting_agent_id: Optional[str] = None) -> str:
    """
    List all currently registered agents with their project, description,
    inbox size, and last-seen timestamp.
    """
    async with _lock:
        if requesting_agent_id and requesting_agent_id in _agents:
            _agents[requesting_agent_id]["last_seen"] = _now()
        result = [
            {
                "agent_id": aid,
                **info,
                "inbox_count": len(_inboxes.get(aid, [])),
                "channel_subscribers": len(_channels.get(aid, set())),
                "subscribed_to": [ch for ch, subs in _channels.items() if aid in subs],
            }
            for aid, info in _agents.items()
        ]
    return json.dumps({"agents": result, "total": len(result)}, indent=2)


@mcp.tool()
async def ping_agent(from_agent: str, target_agent_id: str) -> str:
    """
    Ping a specific agent — delivers a ping message to their inbox.
    Returns immediately; the target reads it on their next get_messages call.
    """
    async with _lock:
        if from_agent not in _agents:
            return json.dumps({"status": "error",
                               "message": f"sender '{from_agent}' is not registered"})
        _agents[from_agent]["last_seen"] = _now()
        if target_agent_id not in _agents:
            return json.dumps({"status": "not_found",
                               "message": f"Agent '{target_agent_id}' is not registered"})
        muted = _is_muted(target_agent_id, from_agent)
        ping = {"id": _mid(), "type": "ping", "from": from_agent,
                "content": f"Ping from {from_agent}", "timestamp": _now()}
        if not muted:
            _inboxes[target_agent_id].append(ping)
            dropped = _enforce_inbox_cap(target_agent_id)
        else:
            dropped = 0

    if muted:
        await _log_event("delivery_muted", {"from": from_agent, "to": target_agent_id,
                                             "kind": "ping", "id": ping["id"]})
    else:
        await _log_event("ping", {"from": from_agent, "to": target_agent_id, "id": ping["id"]})
    if dropped:
        await _log_event("inbox_overflow", {"agent_id": target_agent_id, "dropped": dropped})
    # Sender always sees ok — mutes are hidden by design.
    return json.dumps({"status": "ok", "ping_id": ping["id"],
                       "message": f"Ping delivered to '{target_agent_id}'"})


@mcp.tool()
async def send_message(
    from_agent: str,
    to_agent: str,
    content: str,
    message_type: str = "notification",
) -> str:
    """
    Send a direct message to a specific agent.
    message_type can be: 'notification', 'request', 'response', 'alert', or any label.
    The target reads it via get_messages().
    """
    if _content_too_large(content):
        return json.dumps({"status": "error",
                           "message": f"content exceeds {MAX_CONTENT_BYTES} bytes"})
    async with _lock:
        if from_agent not in _agents:
            return json.dumps({"status": "error",
                               "message": f"sender '{from_agent}' is not registered"})
        _agents[from_agent]["last_seen"] = _now()
        if to_agent not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{to_agent}' is not registered"})
        muted = _is_muted(to_agent, from_agent)
        msg = {"id": _mid(), "type": message_type, "from": from_agent,
               "to": to_agent, "content": content, "timestamp": _now()}
        if not muted:
            _inboxes[to_agent].append(msg)
            dropped = _enforce_inbox_cap(to_agent)
        else:
            dropped = 0

    if muted:
        await _log_event("delivery_muted", {"from": from_agent, "to": to_agent,
                                             "kind": "message", "type": message_type,
                                             "id": msg["id"]})
    else:
        await _log_event("message", {"from": from_agent, "to": to_agent,
                                     "type": message_type, "id": msg["id"]})
    if dropped:
        await _log_event("inbox_overflow", {"agent_id": to_agent, "dropped": dropped})
    # Sender always sees ok — mutes are hidden by design.
    return json.dumps({"status": "ok", "message_id": msg["id"], "delivered_to": to_agent})


@mcp.tool()
async def broadcast_message(
    from_agent: str, content: str, message_type: str = "broadcast"
) -> str:
    """Broadcast a message to ALL other registered agents at once."""
    if _content_too_large(content):
        return json.dumps({"status": "error",
                           "message": f"content exceeds {MAX_CONTENT_BYTES} bytes"})
    overflows: list[tuple[str, int]] = []
    async with _lock:
        if from_agent not in _agents:
            return json.dumps({"status": "error",
                               "message": f"sender '{from_agent}' is not registered"})
        _agents[from_agent]["last_seen"] = _now()
        mid = _mid()
        ts = _now()
        delivered = []
        muted_recipients = []
        for aid in _agents:
            if aid == from_agent:
                continue
            if _is_muted(aid, from_agent):
                muted_recipients.append(aid)
                continue
            _inboxes[aid].append({"id": mid, "type": message_type, "from": from_agent,
                                  "to": "broadcast", "content": content, "timestamp": ts})
            delivered.append(aid)
            d = _enforce_inbox_cap(aid)
            if d:
                overflows.append((aid, d))

    await _log_event("broadcast", {"from": from_agent, "delivered_to": delivered,
                                   "muted_recipients": muted_recipients, "id": mid})
    for aid, d in overflows:
        await _log_event("inbox_overflow", {"agent_id": aid, "dropped": d})
    # Sender sees the full would-be-delivered list; muted recipients appear
    # there as if delivered. Hides who muted whom (sender can't deduce mutes
    # from absences). Server-side log keeps the truth for the dashboard.
    sender_visible = sorted(delivered + muted_recipients)
    return json.dumps({"status": "ok", "message_id": mid,
                       "delivered_to": sender_visible,
                       "count": len(sender_visible)})


@mcp.tool()
async def get_messages(agent_id: str, clear: bool = True, limit: int = 100) -> str:
    """
    Retrieve pending messages from this agent's inbox.
    clear=True (default): consume and remove the returned messages.
    clear=False: peek without removing.
    Delivery is FIFO: oldest `limit` messages first; remaining stay queued.
    """
    limit = max(0, min(int(limit), MAX_INBOX_SIZE))
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{agent_id}' not registered"})
        _agents[agent_id]["last_seen"] = _now()
        msgs = _inboxes.get(agent_id, [])[:limit]
        if clear:
            _inboxes[agent_id] = _inboxes[agent_id][len(msgs):]

    return json.dumps({"status": "ok", "agent_id": agent_id,
                       "message_count": len(msgs), "messages": msgs}, indent=2)


@mcp.tool()
async def subscribe_to_channel(subscriber_id: str, channel_owner_id: str) -> str:
    """
    Subscribe to another agent's broadcast channel.
    Any message they publish_to_channel will land in your inbox.
    """
    async with _lock:
        if subscriber_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"subscriber '{subscriber_id}' is not registered"})
        _agents[subscriber_id]["last_seen"] = _now()
        if channel_owner_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{channel_owner_id}' is not registered"})
        _channels.setdefault(channel_owner_id, set()).add(subscriber_id)
        count = len(_channels[channel_owner_id])

    await _log_event("subscribe", {"subscriber": subscriber_id, "channel": channel_owner_id})
    return json.dumps({"status": "ok", "channel": channel_owner_id,
                       "message": f"Now subscribed to {channel_owner_id}'s channel",
                       "total_subscribers": count})


@mcp.tool()
async def unsubscribe_from_channel(subscriber_id: str, channel_owner_id: str) -> str:
    """Unsubscribe from a channel you previously subscribed to."""
    async with _lock:
        if subscriber_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"subscriber '{subscriber_id}' is not registered"})
        _agents[subscriber_id]["last_seen"] = _now()
        was_subscribed = (channel_owner_id in _channels and
                          subscriber_id in _channels[channel_owner_id])
        if channel_owner_id in _channels:
            _channels[channel_owner_id].discard(subscriber_id)

    await _log_event("unsubscribe", {"subscriber": subscriber_id, "channel": channel_owner_id})
    return json.dumps({"status": "ok",
                       "was_subscribed": was_subscribed,
                       "message": f"Unsubscribed from {channel_owner_id}'s channel"})


@mcp.tool()
async def mute_agent(agent_id: str, target_agent_id: str) -> str:
    """
    Mute another agent — silently drop their messages from this agent's inbox.
    The sender continues to see successful sends; the receiver simply doesn't
    get them. Affects direct messages, broadcasts, pings, and channel publishes.
    Self-muting is a no-op error.
    """
    if not AGENT_ID_RE.match(target_agent_id or ""):
        return json.dumps({"status": "error",
                           "message": "target_agent_id must be 1-128 chars of [A-Za-z0-9._:-]"})
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{agent_id}' is not registered"})
        _agents[agent_id]["last_seen"] = _now()
        if target_agent_id == agent_id:
            return json.dumps({"status": "error", "message": "cannot mute yourself"})
        muted_set = _muted.setdefault(agent_id, set())
        if target_agent_id not in muted_set and len(muted_set) >= MAX_MUTED_SIZE:
            return json.dumps({"status": "error",
                               "message": f"mute list full ({MAX_MUTED_SIZE} entries) — unmute someone first"})
        muted_set.add(target_agent_id)
        count = len(muted_set)

    await _log_event("mute", {"agent_id": agent_id, "muted": target_agent_id})
    return json.dumps({"status": "ok", "muted": target_agent_id,
                       "total_muted": count})


@mcp.tool()
async def unmute_agent(agent_id: str, target_agent_id: str) -> str:
    """Unmute an agent — restore delivery from them to this agent's inbox."""
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{agent_id}' is not registered"})
        _agents[agent_id]["last_seen"] = _now()
        was_muted = target_agent_id in _muted.get(agent_id, set())
        _muted.setdefault(agent_id, set()).discard(target_agent_id)
        count = len(_muted[agent_id])

    await _log_event("unmute", {"agent_id": agent_id, "unmuted": target_agent_id})
    return json.dumps({"status": "ok", "was_muted": was_muted,
                       "unmuted": target_agent_id, "total_muted": count})


@mcp.tool()
async def list_muted(agent_id: str) -> str:
    """List the agents this agent has muted. Returns a sorted list of agent_ids."""
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{agent_id}' is not registered"})
        _agents[agent_id]["last_seen"] = _now()
        muted = sorted(_muted.get(agent_id, set()))

    return json.dumps({"status": "ok", "agent_id": agent_id,
                       "muted": muted, "count": len(muted)})


@mcp.tool()
async def publish_to_channel(agent_id: str, content: str, topic: str = "") -> str:
    """
    Publish a message to your own channel.
    All subscribers receive a copy in their inbox.
    topic is an optional label (e.g. 'status', 'progress', 'error').
    """
    if _content_too_large(content):
        return json.dumps({"status": "error",
                           "message": f"content exceeds {MAX_CONTENT_BYTES} bytes"})
    overflows: list[tuple[str, int]] = []
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{agent_id}' not registered"})
        _agents[agent_id]["last_seen"] = _now()
        mid = _mid()
        ts = _now()
        ch_msg = {"id": mid, "type": "channel", "from": agent_id,
                  "topic": topic, "content": content, "timestamp": ts}
        hist = _channel_history.setdefault(agent_id, [])
        hist.append(ch_msg)
        if len(hist) > 200:
            _channel_history[agent_id] = hist[-200:]
        subscribers = list(_channels.get(agent_id, set()))
        delivered = []
        muted_recipients = []
        for sid in subscribers:
            if sid in _inboxes:
                if _is_muted(sid, agent_id):
                    muted_recipients.append(sid)
                    continue
                _inboxes[sid].append({**ch_msg, "to": sid})
                delivered.append(sid)
                d = _enforce_inbox_cap(sid)
                if d:
                    overflows.append((sid, d))

    await _log_event("channel_publish", {"from": agent_id, "topic": topic,
                                         "subscribers": subscribers,
                                         "delivered_to": delivered,
                                         "muted_recipients": muted_recipients,
                                         "id": mid})
    for sid, d in overflows:
        await _log_event("inbox_overflow", {"agent_id": sid, "dropped": d})
    # Hide-mute: sender sees would-be delivery list (delivered + muted).
    sender_visible = sorted(delivered + muted_recipients)
    return json.dumps({"status": "ok", "message_id": mid,
                       "delivered_to": sender_visible,
                       "subscriber_count": len(subscribers),
                       "delivered_count": len(sender_visible)})


@mcp.tool()
async def get_channel_history(channel_owner_id: str, limit: int = 50) -> str:
    """
    Read the recent publish history of any agent's channel without subscribing.
    Useful for catching up when you first tune into an agent.
    """
    async with _lock:
        if channel_owner_id not in _agents:
            return json.dumps({"status": "error",
                               "message": f"Agent '{channel_owner_id}' is not registered"})
        history = _channel_history.get(channel_owner_id, [])[-limit:]
        subs = list(_channels.get(channel_owner_id, set()))

    return json.dumps({"status": "ok", "channel": channel_owner_id,
                       "subscribers": subs, "message_count": len(history),
                       "messages": history}, indent=2)


@mcp.tool()
async def get_agent_status(agent_id: str) -> str:
    """Get full status for a specific agent: metadata, inbox size, subscriptions."""
    async with _lock:
        if agent_id not in _agents:
            return json.dumps({"status": "not_found", "agent_id": agent_id})
        info = _agents[agent_id]
        subscribed_to = [ch for ch, subs in _channels.items() if agent_id in subs]

    return json.dumps({
        "status": "ok", "agent_id": agent_id, **info,
        "inbox_count": len(_inboxes.get(agent_id, [])),
        "channel_subscribers": list(_channels.get(agent_id, set())),
        "subscribed_to": subscribed_to,
        "channel_message_count": len(_channel_history.get(agent_id, [])),
    }, indent=2)


# ── FastAPI Web App ────────────────────────────────────────────────────────────

session_manager = StreamableHTTPSessionManager(
    app=mcp._mcp_server,
    json_response=False,
    stateless=False,
)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _reaper_task
    _reaper_task = asyncio.create_task(_reaper_loop())
    try:
        async with session_manager.run():
            yield
    finally:
        if _reaper_task and not _reaper_task.done():
            _reaper_task.cancel()
            try:
                await _reaper_task
            except (asyncio.CancelledError, Exception):
                pass


app = FastAPI(title="AgentComms", lifespan=_lifespan)


@app.middleware("http")
async def _loopback_guard(request: Request, call_next):
    """Reject requests that arrive with non-loopback Host/Origin headers.
    Defends against DNS-rebinding attacks: 127.0.0.1 binding alone allows a
    malicious page to map a hostname to 127.0.0.1 and POST from any browser
    tab. Allowlists Host/Origin to localhost-only values."""
    host = (request.headers.get("host") or "").lower()
    if host not in _ALLOWED_HOSTS:
        return JSONResponse({"error": "host not allowed", "host": host},
                            status_code=403)
    origin = request.headers.get("origin")
    if origin is not None and origin.lower() not in _ALLOWED_ORIGINS:
        return JSONResponse({"error": "origin not allowed", "origin": origin},
                            status_code=403)
    return await call_next(request)


app.add_middleware(CORSMiddleware, allow_origins=list(_ALLOWED_ORIGINS),
                   allow_methods=["*"], allow_headers=["*"])


@app.api_route("/mcp", methods=["GET", "POST", "DELETE"])
async def mcp_endpoint(request: Request):
    """MCP streamable-HTTP endpoint — configure Claude Code to connect here.
    Note: request._send is the documented bridge between Starlette's Request
    and a raw ASGI send callable in MCP SDK examples. It is technically a
    private attribute; if a future Starlette release breaks this, switch to
    a Mount pattern with a raw ASGI handler."""
    await session_manager.handle_request(request.scope, request.receive, request._send)


# ── REST API ──────────────────────────────────────────────────────────────────

@app.get("/api/agents")
async def api_list_agents():
    async with _lock:
        result = [
            {"agent_id": aid, **info,
             "inbox_count": len(_inboxes.get(aid, [])),
             "channel_subscribers": len(_channels.get(aid, set())),
             "subscribed_to": [ch for ch, subs in _channels.items() if aid in subs]}
            for aid, info in _agents.items()
        ]
    return JSONResponse({"agents": result, "total": len(result)})


@app.delete("/api/agents/{agent_id}")
async def api_evict_agent(agent_id: str):
    async with _lock:
        if agent_id not in _agents:
            return JSONResponse({"status": "not_found"}, status_code=404)
        _remove_agent(agent_id)
    await _log_event("evict", {"agent_id": agent_id, "reason": "manual", "requested_by": "dashboard"})
    return JSONResponse({"status": "ok", "evicted": agent_id})


@app.get("/api/agents/{agent_id}/inbox")
async def api_get_inbox(agent_id: str, clear: bool = True, limit: int = 100):
    """Hook-friendly inbox read. Unregistered agents get an empty list, not 404 —
    so a hook firing in a project that hasn't started its proxy yet is a no-op.
    FIFO delivery: oldest `limit` first, remaining stay queued."""
    limit = max(0, min(int(limit), MAX_INBOX_SIZE))
    async with _lock:
        if agent_id not in _agents:
            return JSONResponse({"messages": [], "count": 0, "registered": False})
        _agents[agent_id]["last_seen"] = _now()
        msgs = list(_inboxes.get(agent_id, []))[:limit]
        if clear:
            _inboxes[agent_id] = _inboxes[agent_id][len(msgs):]
    return JSONResponse({"messages": msgs, "count": len(msgs), "registered": True})


@app.get("/api/config")
async def api_config():
    return JSONResponse({"ttl_seconds": AGENT_TTL_SECONDS,
                         "reaper_interval_seconds": REAPER_INTERVAL_SECONDS})


@app.get("/api/events")
async def api_event_log(limit: int = 100):
    return JSONResponse({"events": _event_log[-limit:], "total": len(_event_log)})


@app.get("/api/events/stream")
async def api_event_stream(request: Request):
    """SSE stream of live events for the dashboard."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _dashboard_clients.append(q)

    async def generate():
        # Send recent history first
        for entry in _event_log[-50:]:
            yield {"data": json.dumps(entry)}
        # Then stream live
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=20)
                    yield {"data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"data": json.dumps({"event": "heartbeat", "timestamp": _now()})}
        finally:
            _dashboard_clients.remove(q)

    return EventSourceResponse(generate())


# ── Dashboard HTML ────────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AgentComms Hub</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Cascadia Code', 'Fira Code', monospace; background: #0d1117; color: #c9d1d9; min-height: 100vh; }
  header { background: #161b22; border-bottom: 1px solid #30363d; padding: 14px 24px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.1rem; color: #58a6ff; }
  .badge { background: #21262d; border: 1px solid #30363d; border-radius: 12px; padding: 2px 10px; font-size: 0.75rem; color: #8b949e; }
  .badge.live { border-color: #3fb950; color: #3fb950; }
  main { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 20px 24px; max-width: 1400px; }
  @media (max-width: 900px) { main { grid-template-columns: 1fr; } }
  .panel { background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; }
  .panel-header { padding: 12px 16px; border-bottom: 1px solid #30363d; display: flex; justify-content: space-between; align-items: center; }
  .panel-header h2 { font-size: 0.85rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.05em; }
  .count { background: #21262d; color: #58a6ff; border-radius: 10px; padding: 1px 8px; font-size: 0.75rem; }
  .agents-grid { padding: 12px; display: flex; flex-direction: column; gap: 8px; min-height: 120px; }
  .agent-card { background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 10px 14px; position: relative; }
  .agent-card.online  { border-left: 3px solid #3fb950; }
  .agent-card.warning { border-left: 3px solid #d29922; }
  .agent-card.stale   { border-left: 3px solid #f85149; opacity: 0.75; }
  .agent-name { color: #58a6ff; font-weight: 600; font-size: 0.9rem; }
  .agent-meta { color: #8b949e; font-size: 0.75rem; margin-top: 4px; }
  .agent-stats { display: flex; gap: 12px; margin-top: 6px; font-size: 0.72rem; color: #6e7681; flex-wrap: wrap; }
  .stat { display: flex; gap: 4px; align-items: center; }
  .stat-val { color: #e6edf3; }
  .stat-val.warn { color: #d29922; }
  .stat-val.stale { color: #f85149; }
  .evict-btn {
    position: absolute; top: 10px; right: 10px;
    background: #3d1f1f; border: 1px solid #f8514940; color: #f85149;
    border-radius: 4px; padding: 2px 8px; font-size: 0.7rem; cursor: pointer;
    font-family: inherit;
  }
  .evict-btn:hover { background: #5a2020; }
  .log { padding: 0; list-style: none; font-size: 0.78rem; max-height: 420px; overflow-y: auto; }
  .log li { padding: 6px 16px; border-bottom: 1px solid #21262d; display: flex; gap: 10px; align-items: baseline; }
  .log li:last-child { border-bottom: none; }
  .log li:hover { background: #1c2128; }
  .ts { color: #484f58; white-space: nowrap; flex-shrink: 0; }
  .ev-tag { border-radius: 4px; padding: 1px 6px; font-size: 0.7rem; flex-shrink: 0; }
  .ev-register    { background: #1f3d2f; color: #3fb950; }
  .ev-unregister  { background: #3d1f1f; color: #f85149; }
  .ev-evict       { background: #4a1a1a; color: #ff6b6b; font-weight: 600; }
  .ev-message     { background: #1f2d3d; color: #58a6ff; }
  .ev-ping        { background: #2d2616; color: #d29922; }
  .ev-broadcast   { background: #2d1f3d; color: #bc8cff; }
  .ev-subscribe   { background: #1f3d3d; color: #39d3f0; }
  .ev-unsubscribe { background: #2a2a1f; color: #e3b341; }
  .ev-channel_publish { background: #2a1f3d; color: #bc8cff; }
  .ev-heartbeat   { background: #1c2128; color: #484f58; }
  .ev-text { color: #c9d1d9; }
  .empty { color: #484f58; font-style: italic; padding: 20px 16px; text-align: center; }
  .mcp-info { padding: 12px 16px; font-size: 0.8rem; color: #8b949e; line-height: 1.8; }
  .mcp-info code { background: #21262d; border-radius: 4px; padding: 1px 6px; color: #79c0ff; font-size: 0.78rem; }
  .full-width { grid-column: 1 / -1; }
  .ttl-bar { height: 3px; background: #21262d; border-radius: 2px; margin-top: 8px; overflow: hidden; }
  .ttl-fill { height: 100%; border-radius: 2px; transition: width 1s linear, background 1s; }
</style>
</head>
<body>
<header>
  <h1>AgentComms Hub</h1>
  <span class="badge live" id="conn-badge">● connecting</span>
  <span class="badge" id="agent-count-badge">0 agents</span>
  <span class="badge" id="ttl-badge">TTL: …s</span>
</header>
<main>
  <div class="panel">
    <div class="panel-header">
      <h2>Registered Agents</h2>
      <span class="count" id="agent-count">0</span>
    </div>
    <div class="agents-grid" id="agents-grid">
      <div class="empty">No agents registered yet.</div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-header">
      <h2>Live Event Log</h2>
      <span class="count" id="log-count">0</span>
    </div>
    <ul class="log" id="event-log"></ul>
  </div>

  <div class="panel full-width">
    <div class="panel-header"><h2>MCP Connection</h2></div>
    <div class="mcp-info">
      <strong>MCP endpoint:</strong> <code id="sse-url"></code><br>
      <strong>Add to Claude Code</strong> (<code>~/.claude/.mcp.json</code> or project <code>.mcp.json</code>):<br>
      <code id="mcp-json-snippet"></code><br>
      <strong>Agents are reaped</strong> if silent for longer than the TTL.
      Call <code>heartbeat(agent_id)</code> during long tasks to stay alive.
    </div>
  </div>
</main>

<script>
const origin = location.origin;
document.getElementById('sse-url').textContent = origin + '/mcp';
document.getElementById('mcp-json-snippet').textContent = JSON.stringify({
  mcpServers: { "agent-comms": { type: "http", url: origin + "/mcp" } }
}, null, 2);

let logCount = 0;
let TTL = 120;

// Load config (TTL)
fetch('/api/config').then(r => r.json()).then(cfg => {
  TTL = cfg.ttl_seconds;
  document.getElementById('ttl-badge').textContent = 'TTL: ' + TTL + 's';
});

function fmtTime(iso) {
  return new Date(iso).toLocaleTimeString([], { hour12: false });
}

function secondsAgo(iso) {
  return (Date.now() - new Date(iso).getTime()) / 1000;
}

function eventText(e) {
  switch (e.event) {
    case 'register':        return `${e.agent_id} registered (${e.project})`;
    case 'unregister':      return `${e.agent_id} left`;
    case 'evict':           return `${e.agent_id} evicted — ${e.reason}${e.requested_by ? ' by ' + e.requested_by : ''}`;
    case 'message':         return `${e.from} → ${e.to}: [${e.type}]`;
    case 'ping':            return `${e.from} pinged ${e.to}`;
    case 'broadcast':       return `${e.from} broadcast → ${(e.delivered_to||[]).length} agents`;
    case 'subscribe':       return `${e.subscriber} subscribed to ${e.channel}`;
    case 'unsubscribe':     return `${e.subscriber} left ${e.channel}`;
    case 'channel_publish': return `${e.from} published${e.topic ? ' ['+e.topic+']' : ''} → ${(e.subscribers||[]).length} subs`;
    case 'heartbeat':       return 'heartbeat';
    default:                return JSON.stringify(e);
  }
}

function pushLogEntry(e) {
  if (e.event === 'heartbeat') return;
  logCount++;
  const li = document.createElement('li');
  // Build child spans with textContent so untrusted fields (message_type,
  // arbitrary agent_id substrings, etc.) cannot inject HTML/JS.
  const ts = document.createElement('span');
  ts.className = 'ts';
  ts.textContent = fmtTime(e.timestamp);
  const tag = document.createElement('span');
  // e.event is server-controlled (one of a known-safe set), but limit to
  // [a-z_] just in case to keep the class name well-formed.
  const safeEvent = String(e.event).replace(/[^a-z_]/gi, '');
  tag.className = 'ev-tag ev-' + safeEvent;
  tag.textContent = e.event;
  const text = document.createElement('span');
  text.className = 'ev-text';
  text.textContent = eventText(e);
  li.append(ts, tag, text);
  const ul = document.getElementById('event-log');
  ul.prepend(li);
  if (ul.children.length > 200) ul.lastChild.remove();
  document.getElementById('log-count').textContent = logCount;
}

function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

async function evict(agentId) {
  if (!confirm('Evict agent "' + agentId + '"?')) return;
  await fetch('/api/agents/' + encodeURIComponent(agentId), { method: 'DELETE' });
  refreshAgents();
}

// Single delegated click handler — avoids inline onclick with templated agent_id.
document.getElementById('agents-grid').addEventListener('click', (e) => {
  const btn = e.target.closest('.evict-btn');
  if (btn && btn.dataset.agentId) evict(btn.dataset.agentId);
});

async function refreshAgents() {
  try {
    const res = await fetch('/api/agents');
    const data = await res.json();
    const grid = document.getElementById('agents-grid');
    document.getElementById('agent-count').textContent = data.total;
    document.getElementById('agent-count-badge').textContent =
      data.total + ' agent' + (data.total !== 1 ? 's' : '');

    if (data.total === 0) {
      grid.innerHTML = '<div class="empty">No agents registered yet.</div>';
      return;
    }

    grid.innerHTML = data.agents.map(a => {
      const age = secondsAgo(a.last_seen);
      const pct = Math.max(0, Math.min(100, 100 - (age / TTL) * 100));
      const fillColor = pct > 50 ? '#3fb950' : pct > 20 ? '#d29922' : '#f85149';
      const cardClass = pct > 50 ? 'online' : pct > 20 ? 'warning' : 'stale';
      const ageClass  = pct > 50 ? '' : pct > 20 ? 'warn' : 'stale';
      const ageSec    = age < 60 ? Math.round(age) + 's ago'
                      : Math.round(age/60) + 'm ago';
      const subscribedTo = (a.subscribed_to || []).map(esc).join(', ') || '—';
      return `
      <div class="agent-card ${cardClass}">
        <button class="evict-btn" data-agent-id="${esc(a.agent_id)}">evict</button>
        <div class="agent-name">${esc(a.agent_id)}</div>
        <div class="agent-meta">${esc(a.project)}${a.description ? ' — ' + esc(a.description) : ''}</div>
        <div class="agent-stats">
          <span class="stat">inbox <span class="stat-val">${a.inbox_count|0}</span></span>
          <span class="stat">subs <span class="stat-val">${a.channel_subscribers|0}</span></span>
          <span class="stat">watching <span class="stat-val">${subscribedTo}</span></span>
          <span class="stat">seen <span class="stat-val ${ageClass}">${ageSec}</span></span>
        </div>
        <div class="ttl-bar">
          <div class="ttl-fill" style="width:${pct}%; background:${fillColor}"></div>
        </div>
      </div>`;
    }).join('');
  } catch(e) {}
}

const evtSrc = new EventSource('/api/events/stream');
evtSrc.onopen = () => { document.getElementById('conn-badge').textContent = '● live'; };
evtSrc.onerror = () => { document.getElementById('conn-badge').textContent = '○ disconnected'; };
evtSrc.onmessage = (e) => {
  const data = JSON.parse(e.data);
  pushLogEntry(data);
  if (['register','unregister','evict'].includes(data.event)) refreshAgents();
};

refreshAgents();
setInterval(refreshAgents, 5000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"AgentComms Hub starting on http://{HOST}:{PORT}")
    print(f"  Dashboard :  http://{HOST}:{PORT}/")
    print(f"  MCP HTTP  :  http://{HOST}:{PORT}/mcp")
    print(f"  REST API  :  http://{HOST}:{PORT}/api/agents")
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
