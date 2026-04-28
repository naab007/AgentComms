#!/usr/bin/env python3
"""
PreToolUse hook for the Agent (Task) tool: assign each sub-agent a distinct
AgentComms identity by injecting a short registration instruction at the top
of its prompt, then rewriting tool_input.prompt via the `updatedInput` field.

Why this exists:
  All sub-agents share the parent process and proxy.py, so the proxy's
  auto-derived agent_id (cwd hash) collides — every sub-agent would share
  the parent's inbox unless something forces them to register separately.
  This hook does that, automatically, for every Agent/Task invocation.

Idempotent + fail-safe:
  - Only acts when tool_name is "Task" or "Agent".
  - Probes the hub (250 ms timeout); skips silently if unreachable so a
    down hub never blocks sub-agent spawning.
  - Generates a unique sub_id per call: <parent>-sub-<descSlug>-<4 hex>.
  - Sub-agent is asked to register on entry and unregister on exit.
  - On any error, returns a plain "allow" so the spawn still proceeds.
"""
from __future__ import annotations
import hashlib, json, os, secrets, sys
import urllib.request

HUB = "http://127.0.0.1:7771"
# Bumped from 0.25s to 0.5s — slow hubs were causing passthrough on busy machines,
# which silently routed sub-agent traffic to the parent's auto-id (shared inbox).
HUB_TIMEOUT_S = 0.5


def derive_parent_id(cwd: str) -> str:
    cwd = os.path.abspath(cwd)
    project = os.path.basename(cwd) or "unknown"
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in project).strip("-") or "unknown"
    h = hashlib.sha1(os.path.normcase(cwd).encode("utf-8")).hexdigest()[:8]
    return f"{safe}-{h}"


def hub_reachable() -> bool:
    try:
        with urllib.request.urlopen(f"{HUB}/api/config", timeout=HUB_TIMEOUT_S) as r:
            return r.status == 200
    except Exception:
        return False


def emit_allow(updated_input: dict | None = None) -> int:
    out: dict = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
        }
    }
    if updated_input is not None:
        out["hookSpecificOutput"]["updatedInput"] = updated_input
    print(json.dumps(out))
    return 0


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except Exception:
        return emit_allow()

    tool_name = data.get("tool_name") or ""
    if tool_name not in ("Task", "Agent"):
        return emit_allow()

    tool_input = data.get("tool_input") or {}
    prompt = tool_input.get("prompt") or ""
    if not prompt:
        return emit_allow()

    if not hub_reachable():
        return emit_allow()

    cwd = data.get("cwd") or os.getcwd()
    parent_id = derive_parent_id(cwd)
    desc = (tool_input.get("description") or "subagent").strip()
    desc_safe = "".join(c if c.isalnum() else "-" for c in desc.lower())[:24].strip("-") or "sub"
    sub_id = f"{parent_id}-sub-{desc_safe}-{secrets.token_hex(2)}"

    injection = (
        "## AgentComms — your distinct identity (auto-injected)\n"
        f"Parent agent: `{parent_id}`. Your unique sub-agent id: **`{sub_id}`**.\n\n"
        "If — and only if — your task uses AgentComms tools:\n"
        '1. `ToolSearch{query: "select:mcp__agent-comms__register_agent,'
        'mcp__agent-comms__unregister_agent"}` to load schemas.\n'
        f'2. `mcp__agent-comms__register_agent(agent_id="{sub_id}", '
        f'project="{parent_id}/sub", description="{desc}")`.\n'
        f'3. On every subsequent AgentComms call pass `agent_id="{sub_id}"` '
        f'(or `from_agent="{sub_id}"`).\n'
        f'4. Before you finish, `mcp__agent-comms__unregister_agent(agent_id="{sub_id}")`.\n\n'
        "If your task is unrelated to AgentComms, ignore this block entirely.\n\n"
        "---\n\n"
    )

    new_prompt = injection + prompt
    return emit_allow(updated_input={**tool_input, "prompt": new_prompt})


if __name__ == "__main__":
    sys.exit(main())
