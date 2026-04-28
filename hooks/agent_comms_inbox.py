#!/usr/bin/env python3
"""
UserPromptSubmit hook: pull pending agent-comms messages and inject them
into the prompt as additional context. The agent doesn't need to call any
tool — peer messages just appear in the next turn.

Derives the agent_id from the project cwd the same way proxy.py does, so
the hook and the MCP proxy address the same inbox.

Fails silent: if the hub is down, the agent isn't registered, or anything
errors out, the hook prints nothing and exits 0. Submitting a prompt must
never be blocked by inbox lookup.
"""
from __future__ import annotations
import hashlib, json, os, sys
import urllib.request
import urllib.error

HUB = "http://127.0.0.1:7771"
TIMEOUT_S = 0.6  # short — must not noticeably delay prompt submission


def derive_agent_id(cwd: str) -> str:
    cwd = os.path.abspath(cwd)
    project = os.path.basename(cwd) or "unknown"
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in project).strip("-") or "unknown"
    h = hashlib.sha1(os.path.normcase(cwd).encode("utf-8")).hexdigest()[:8]
    return f"{safe}-{h}"


def fetch_pending(agent_id: str) -> list:
    url = f"{HUB}/api/agents/{agent_id}/inbox?clear=true&limit=50"
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_S) as r:
            data = json.loads(r.read().decode("utf-8", errors="replace"))
        if not data.get("registered"):
            return []
        return data.get("messages") or []
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return []


def format_block(agent_id: str, messages: list) -> str:
    lines = [f"📨 {len(messages)} new agent-comms message(s) for {agent_id}:"]
    for m in messages:
        ts = (m.get("timestamp") or "")[:19].replace("T", " ")
        sender = m.get("from", "?")
        mtype = m.get("type", "msg")
        topic = m.get("topic")
        topic_tag = f" #{topic}" if topic else ""
        content = (m.get("content") or "").strip()
        if len(content) > 600:
            content = content[:600] + " …[truncated]"
        lines.append(f"  • [{ts}] {sender} ({mtype}{topic_tag}): {content}")
    return "\n".join(lines)


def main() -> int:
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
    except Exception:
        return 0

    cwd = payload.get("cwd") or os.getcwd()
    agent_id = derive_agent_id(cwd)
    messages = fetch_pending(agent_id)
    if not messages:
        return 0

    out = {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": format_block(agent_id, messages),
        }
    }
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
