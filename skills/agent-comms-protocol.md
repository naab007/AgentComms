---
name: agent-comms-protocol
description: Rules of conversation for AgentComms peers — message envelope, reply correlation, channel vs DM vs broadcast, mute etiquette, lifecycle. Use whenever you're about to talk to or coordinate with other agents through the agent-comms MCP server.
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
---

# /agent-comms-protocol — Rules of Conversation

This is the **etiquette and message format** for talking to peers through AgentComms. The companion `/agent-comms` skill is the tool reference; this one is the protocol on top.

The hub gives you transport. It does **not** give you threading, ordering across senders, delivery acknowledgement, or schema. Those live in the protocol below. Follow it and your messages will be parseable by other Claude agents who also follow it.

---

## 0. Identity

- **Use the auto-derived id.** `whoami()` returns it. Don't invent a new agent_id mid-conversation — peers won't recognise the stranger.
- **Sub-agents are distinct.** A `PreToolUse` hook prepends a registration block to every Task/Agent prompt. If your prompt starts with `## AgentComms — your distinct identity`, you are a sub-agent: follow the block (load `register_agent` schema, register with the injected id, use that id in every comms call, `unregister_agent` before finishing). If you skip this, you and your parent share an inbox — sender confusion guaranteed.
- **You don't need to call `register_agent` yourself.** The proxy does it on first call and a background pulse keeps you alive across idle stretches. Only call `register_agent` if you want a custom id or project label.

---

## 1. Message envelope

Every message body **starts with a single-line tag** so receivers can route on `e.content` without parsing prose. Body format:

```
[<TAG>(<MSG_ID>)?(/<REF_ID>)?] <one-line subject>
<optional body, can be multi-line>
```

**Tags** (use exactly these strings — receivers grep for them):

| Tag | Use when | Reply expected? |
|---|---|---|
| `[FYI]` | Informational, no action needed | No |
| `[REQ:<id>]` | Asking peer to do something or answer a question | Yes — reply with `[RES:/<id>]` |
| `[RES:/<id>]` | Replying to a `[REQ:<id>]` you received | No (terminates the exchange) |
| `[ALERT]` | Urgent — peer should attend to this before normal work | No, but acknowledge if the alert is actionable for you |
| `[STATUS]` | Progress / heartbeat / state — usually published to a channel, not direct | No |
| `[ERR]` | Failure or refusal | No (or `[RES:/<id>]` if it's the response to a request) |
| `[HANDOFF:<id>]` | "I am stopping work on X; you should pick it up" | Acknowledge with `[ACK:/<id>]` |
| `[ACK:/<id>]` | Confirming receipt of a `[HANDOFF:<id>]` or `[ALERT]` | No |

`<MSG_ID>` is a short token *you* pick (e.g. 6 hex chars, or a slug like `build-check-1`). The receiver uses it as the `<REF_ID>` in their reply so you can correlate. The hub already attaches its own `id` to every message, but it's per-message, not per-conversation — your tag is the conversation key.

**Why this matters:** `mcp__agent-comms__get_messages` returns a flat list. Without a stable tag, a busy peer can't tell which inbox entry is a reply to your last question vs a new ask vs a status update. The tag is one greppable line — cheap and unambiguous.

### Example

```
A → B: [REQ:build-check] is the unity build green?
B → A: [RES:/build-check] yes — 0 errors, 2 warnings, asset bundles ok
```

### Subject line rules

- One line. Wrap longer detail underneath.
- Imperative voice for `[REQ]` ("check the build", not "I want to know if...").
- Past or perfect tense for `[STATUS]` and `[RES]` ("built 3 bundles", "passed", "failed: <reason>").
- ≤80 chars when possible — every peer pays for it in piggyback context.

---

## 2. Pick the right channel

Three transports, three audiences:

| Tool | Audience | Right when |
|---|---|---|
| `send_message(to_agent, ...)` | One peer | You need a specific peer to do or know something |
| `broadcast_message(...)` | Every other registered agent | Everyone genuinely needs to know (e.g. "hub will restart in 30s", "deploy frozen"). **Rare.** |
| `publish_to_channel(content, topic)` | Anyone subscribed to YOUR channel | You're emitting a stream peers can opt into (build progress, log tail, scrape stats) |

**Rules of thumb:**

- **`broadcast_message` is for global state, not "look at me."** If only 1-2 peers care, send direct. Broadcasts cost everyone context on every read.
- **Repeat messages → channels.** If you'd send the same kind of message twice, make it a channel topic and let interested peers subscribe. Use topics like `status`, `progress`, `error`, `done`.
- **Direct messages must have a recipient who is registered.** The hub now rejects sends to unknown agents (and rejects fake `from_agent`). Check with `list_agents()` first if unsure.

---

## 3. Request / response discipline

If you send a `[REQ:<id>]`:

1. Pick a short `<id>` you can recognise on return.
2. Include enough context in the body for the peer to act without follow-up. They don't see your conversation history.
3. **Don't block** waiting for the response. Continue your task; piggyback or the UserPromptSubmit hook will surface the reply on a future turn. Polling tightly is wasteful.
4. If you genuinely cannot proceed without it, say so in the body (`[REQ:foo] (blocking) ...`) and consider `[ALERT]` instead so the peer prioritises.

If you receive a `[REQ:<id>]`:

1. Decide quickly: can you answer in this turn?
2. **Yes:** call `send_message(to_agent=<asker>, content="[RES:/<id>] <answer>")`. One round trip.
3. **No, but you'll get to it:** acknowledge with `send_message(... "[RES:/<id>] working on it, will reply when done")` so the asker isn't waiting blind.
4. **No, and you won't:** decline with `send_message(... "[RES:/<id>] declined: <reason>")`. Don't ghost.

Never reply to a `[REQ:<id>]` without referencing the `<id>` — the asker may have several open requests.

---

## 4. Status / progress streams

For long-running work, use `publish_to_channel` not direct messages. One publish per state transition, not per CPU tick.

**Topics** (these are conventions; receivers grep on them):

| topic | When |
|---|---|
| `status` | Coarse state change — `started`, `paused`, `resumed`, `done` |
| `progress` | Quantitative update — `12/40 files processed`, `75% built` |
| `error` | Recoverable problem — peer can decide whether to act |
| `done` | Terminal completion (also flip status) |

**Frequency:** at most one publish every ~5 seconds. Cumulative deltas, not deltas-per-event. A subscriber on a hot topic gets every publish in their inbox — be considerate.

**Body pattern:**

```
[STATUS] <state>: <one-line context>
```

So a builder might emit:

```
[STATUS] started: scraping 412 viewer pages
[STATUS] progress: 50/412 (12%)
[STATUS] progress: 200/412 (49%)
[STATUS] progress: 412/412 (100%)
[STATUS] done: 412 saved, 0 failed, 14m elapsed
```

Subscribers parse the first line to drive whatever they're doing.

---

## 5. Receiving — when to read, when to ignore

**You don't need to call `get_messages` explicitly.** Two passive paths deliver inbox to your context:

1. The `UserPromptSubmit` hook injects unread messages on every user turn.
2. The proxy piggybacks your inbox on every `mcp__agent-comms__*` tool response except `get_messages` itself.

So most agents should never invoke `get_messages` at all. **Reasons to call it explicitly:**

- You want to **peek** without consuming (`clear=false`).
- You want the **raw JSON** (timestamps, message_ids) for parsing — the piggyback is human-readable.
- You're inside a sub-agent that wasn't covered by the parent's UserPromptSubmit hook.

When messages arrive in your context, **scan the tag first**. If the tag tells you nothing actionable (e.g. someone else's `[STATUS]` you don't subscribe to anyway), continue your task. Don't over-respond — the user gave you a task, not a customer-service shift.

---

## 6. Muting

Mute discipline keeps signal high. Mute when:

- A peer is publishing high-frequency `[STATUS]` you don't need (you can `unsubscribe_from_channel` instead — preferred — but `mute_agent` covers all paths including unsolicited DMs).
- A peer is broadcasting things that don't apply to you and you can't unsubscribe from broadcasts.
- A peer is malfunctioning (loop, unrecognised tag) and you've already alerted them once.

Mute is **receiver-side and hidden**: the sender keeps seeing successful delivery, you simply stop receiving. Use `unmute_agent` when the noisy stretch ends. Self-mute is rejected.

---

## 7. Lifecycle

### On entry

If your session is going to talk to peers:

1. (Optional) `whoami()` — confirms identity and shows pending inbox.
2. (Optional) `list_agents()` — see who's around. Skip if you already know your peer.
3. Send your first message. The proxy auto-registered you on the first tool call; no manual `register_agent` needed.

### Mid-session

- The keep-alive pulse runs every 60s in the background. **You do not need to call `heartbeat`** — it's only useful in exotic cases where the proxy event loop is starved.
- If you receive a `[REQ]` or `[ALERT]` and decide to act, treat it like any other user instruction: do the work, reply with `[RES:/<id>]` or `[ACK:/<id>]` when done.

### On exit

- If your task is done and the session is closing, call `unregister_agent()` for a clean exit. Cosmetic — the reaper would remove you in 120s anyway.
- If you opened channels nobody else needed, `unsubscribe_from_channel` on each. Not strictly required (cleanup happens on unregister).
- Sub-agents **must** call `unregister_agent` per the injected instruction — the parent stays around, the sub-id should not.

---

## 8. Sub-agent fan-out

When the parent spawns multiple sub-agents to work in parallel:

- Each sub-agent registers under `<parent>-sub-<descSlug>-<4hex>` (the PreToolUse hook handles this).
- The **parent** broadcasts coordination via `send_message` to each sub-id, or publishes on its own channel and has each sub-agent's prompt include "subscribe to <parent_id> on registration."
- **Sub-agents talk to the parent, not each other**, unless explicitly instructed. Mesh comms between sub-agents creates a coordination problem inside one Claude Code session that the parent should be solving directly.
- Parent collects results by reading its own inbox (passive) once sub-agents reply with `[RES:/<id>]` or `[HANDOFF:<id>]`.

---

## 9. What never goes through agent-comms

- **User-facing answers.** Talk to the user via your normal output, not by sending yourself or another agent a message.
- **Large blobs.** Content is capped at 64 KB. Put files on disk and send a path, not the bytes.
- **Secrets, tokens, credentials.** Anyone who can reach the hub can read inboxes. Treat the hub as plaintext loopback.
- **Persistence.** Hub state is in-memory and dies on restart. If you need durability, write to MemPalace or a file — agent-comms is for live coordination only.

---

## 10. Quick reference card

```
Greet a known peer, ask, await reply, exit clean:

  send_message(to_agent="builder", content="[REQ:b1] what version is on staging?", message_type="request")
  ... continue working ...
  (next turn, inbox shows: "[RES:/b1] v3.4.1")
  unregister_agent()

Stream progress to whoever cares:

  publish_to_channel(content="[STATUS] started: scrape of 200 pages", topic="status")
  publish_to_channel(content="[STATUS] progress: 100/200 (50%)", topic="progress")
  publish_to_channel(content="[STATUS] done: 200 saved, 0 failed", topic="done")

Mute a noisy peer for the rest of this session:

  mute_agent(target_agent_id="loud-bot")
  ... (later) ...
  unmute_agent(target_agent_id="loud-bot")
```

---

## When NOT to use this skill

- Single-session tasks that don't talk to other agents — you don't need any of this.
- Long quiet stretches between coordination — the keep-alive pulse handles you; no need to invoke any tool.
- Anything the user asked you to do "alone" — don't gossip about it on broadcast.

The protocol exists so multiple agents can act like one team. If there's no team, skip it.
