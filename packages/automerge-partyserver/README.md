# automerge-partyserver

Automerge CRDT sync for [PartyKit](https://www.partykit.io/) / partyserver. Like [y-partyserver](../y-partyserver) but for [Automerge](https://automerge.org/).

## Server

```typescript
import { withAutomerge, AutomergeServer } from 'automerge-partyserver';
import { Server } from 'partyserver';

// Option A: use the pre-built server
export default AutomergeServer;

// Option B: customize with the mixin
export default class MyServer extends withAutomerge(Server) {
  async onLoad() {
    // Load initial doc state from R2, DO storage, etc.
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
    render(); // re-render your UI
  },
  onEphemeral: (data) => {
    // Handle presence/cursor updates
  },
});

// Make local changes
provider.change((doc) => {
  doc.title = 'Hello';
});

// Send presence
provider.sendEphemeral(new TextEncoder().encode(JSON.stringify({ cursor: { x: 10, y: 20 } })));
```

## What you get

- **Automerge sync protocol** over WebSocket — incremental, only sends changes
- **Durable Object** persistence — doc survives restarts
- **WebSocket Hibernation** — zero cost when idle
- **BroadcastChannel** — cross-tab sync
- **Ephemeral messages** — presence/cursor without persisting
- **Auto-reconnect** — exponential backoff
- **Debounced saves** — batches rapid edits (2s default)

## How it works

- Server uses `withAutomerge(Server)` mixin (same pattern as `withYjs` in y-partyserver)
- Server keeps Automerge doc in memory, runs sync protocol per connection
- Sync state stored in `conn.setState()` — survives WebSocket Hibernation
- Client runs `AutomergeProvider` — WebSocket + BroadcastChannel + reconnect
- Binary message protocol: `[type_byte][payload]` — type 0 = sync, type 1 = ephemeral
