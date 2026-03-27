# automerge-partyserver

Automerge CRDT sync for [PartyKit](https://www.partykit.io/) / partyserver. The Automerge counterpart to [y-partyserver](../y-partyserver).

## Why both Yjs and Automerge?

Yjs and Automerge are both CRDTs, but they solve different problems and suit different use cases. PartyKit supporting both makes it a universal real-time collaboration platform.

### Yjs — optimized for text and rich content

Yjs is designed for **text editing** and **rich document** collaboration:

- **Data types**: `Y.Text`, `Y.Array`, `Y.Map`, `Y.XmlFragment` — purpose-built for text editors (ProseMirror, TipTap, CodeMirror, Lexical)
- **Awareness protocol**: Built-in cursor positions, selections, user presence — essential for collaborative editing UX
- **Garbage collection**: Aggressively GCs tombstones, keeping docs small for long editing sessions
- **Encoding**: Custom binary encoding (`lib0`) — compact, fast, designed for rapid keystroke-level updates
- **Ecosystem**: Mature editor bindings, dozens of providers, battle-tested in production text editors
- **Best for**: Documents, rich text, code editors, whiteboards — anything where the primary operation is editing text or structured content

### Automerge — optimized for structured data and multi-device sync

Automerge is designed for **application state** and **local-first** architectures:

- **Data model**: JSON-like documents with nested objects and arrays — natural for app state, not just text
- **Change history**: Preserves full causal history — every change is attributable, replayable, and branchable
- **Sync protocol**: Built-in incremental sync with per-peer state tracking — designed for intermittent connectivity and multi-device sync
- **Rust core**: Written in Rust, compiled to WASM — deterministic performance, usable on servers without Node.js
- **Compaction**: `save()` produces a compact binary; incremental `saveIncremental()` for append-only storage
- **Best for**: Application state (CAD models, project data, configuration), offline-first mobile/desktop apps, operation logs, multi-device sync, anything where you need causal history

### When to use which

| Use case | Yjs (y-partyserver) | Automerge (automerge-partyserver) |
|----------|--------------------|---------------------------------|
| Text editor collaboration | **Yes** — purpose-built | Possible but no editor bindings |
| Rich document editing | **Yes** — XML types, awareness | Not designed for this |
| Application state sync | Possible but awkward | **Yes** — JSON data model |
| Operation log / CRDT event sourcing | No history by default | **Yes** — full causal history |
| Offline-first / intermittent connectivity | Basic | **Yes** — sync protocol with state tracking |
| Multi-device sync (mobile, desktop, server) | Basic | **Yes** — Rust core runs anywhere |
| CAD, 3D, design tools | Not ideal | **Yes** — structured op log |
| Gaming / real-time state | Possible | **Yes** — deterministic Rust core |

**They complement each other.** A collaborative design tool might use Yjs for the text fields and Automerge for the design state. PartyKit rooms can run either — or both.

## Server

```typescript
import { withAutomerge, AutomergeServer } from 'automerge-partyserver';
import { Server } from 'partyserver';

// Option A: use the pre-built server
export default AutomergeServer;

// Option B: customize with the mixin
export default class MyServer extends withAutomerge(Server) {
  async onLoad() {
    // Load initial doc state from DO storage
    const bytes = await this.room.storage.get('doc');
    return bytes as Uint8Array | undefined;
  }

  async onSave(bytes: Uint8Array) {
    // Persist doc state (called on debounced interval)
    await this.room.storage.put('doc', bytes);
  }
}
```

## Client (browser)

```typescript
import { AutomergeProvider } from 'automerge-partyserver/provider';
import { next as Automerge } from '@automerge/automerge';

let doc = Automerge.init();

const provider = new AutomergeProvider({
  host: 'localhost:1999',
  room: 'my-model-id',
  doc,
  onUpdate: (newDoc) => {
    doc = newDoc;
    render();
  },
  onEphemeral: (data) => {
    // Handle presence/cursor updates
  },
});

// Make local changes — automatically synced to server + other tabs
provider.change((doc) => {
  doc.title = 'Hello';
  doc.items.push({ name: 'Widget', status: 'active' });
});

// Send ephemeral (presence — not persisted)
provider.sendEphemeral(new TextEncoder().encode(JSON.stringify({
  cursor: { x: 10, y: 20 },
  user: 'Alice',
})));
```

## What you get

- **Automerge incremental sync protocol** over WebSocket — only sends changes since last sync, not the full document
- **Per-connection sync state** — stored in `conn.setState()`, survives WebSocket Hibernation
- **Durable Object persistence** — doc survives restarts via `onLoad`/`onSave`
- **WebSocket Hibernation** — zero compute cost when no clients are connected
- **BroadcastChannel** — cross-tab sync in the browser without server round-trip
- **Ephemeral messages** — presence/cursor data broadcast to peers, not persisted
- **Auto-reconnect** — exponential backoff on disconnect
- **Debounced saves** — batches rapid edits (2s default) to avoid storage thrashing

## How it works

### Server (`withAutomerge` mixin)

Same pattern as `withYjs` in y-partyserver:

1. `onStart()` — loads doc from storage (or starts empty), sets up sync broadcast
2. `onConnect()` — sends initial sync message to new peer
3. `onMessage()` — receives sync message → `Automerge.receiveSyncMessage()` → updates doc → `Automerge.generateSyncMessage()` → replies + broadcasts to other peers
4. `onClose()` — cleans up sync state; if last peer, flushes pending save
5. Persistence — `onSave()` called on debounced interval after changes

### Client (`AutomergeProvider`)

1. Connects to PartyKit WebSocket
2. Server sends sync step 1 → client responds with sync step 2 (its state)
3. On local change → `Automerge.generateSyncMessage()` → send to server + broadcast to other tabs
4. On server message → `Automerge.receiveSyncMessage()` → update doc → notify consumer
5. Reconnects with exponential backoff on disconnect

### Message protocol

Binary: `[type_byte][payload]`
- `0x00` — Automerge sync message (incremental sync protocol)
- `0x01` — Ephemeral message (presence, cursor — not persisted)

### Comparison with y-partyserver internals

| Concern | y-partyserver | automerge-partyserver |
|---------|--------------|----------------------|
| Doc type | `YDoc` | `Automerge.Doc` |
| Sync protocol | `y-protocols/sync` (SyncStep1/2/Update) | `Automerge.generateSyncMessage`/`receiveSyncMessage` |
| Sync state | Implicit in Yjs | Explicit `Automerge.SyncState` per connection |
| Awareness | `y-protocols/awareness` (built-in) | Ephemeral messages (application-defined) |
| Encoding | `lib0/encoding` (custom binary) | Automerge binary + 1-byte type prefix |
| Persistence | `YPartyKitStorage` (incremental update log + compaction) | `onLoad`/`onSave` callbacks (consumer chooses storage) |
| GC | Yjs GC (tombstone removal) | Automerge compaction (`Automerge.save()`) |
| Server mixin | `withYjs(Server)` | `withAutomerge(Server)` |
