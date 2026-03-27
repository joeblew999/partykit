/**
 * automerge-partyserver — Automerge CRDT sync for PartyKit / partyserver.
 *
 * Provides `withAutomerge(Server)` mixin — same pattern as y-partyserver's `withYjs(Server)`.
 *
 * Usage:
 *   import { withAutomerge, AutomergeServer } from 'automerge-partyserver';
 *
 *   // Option A: use the mixin
 *   class MyServer extends withAutomerge(Server) {
 *     async onLoad() { return loadFromR2(); }
 *     async onSave(doc) { await saveToR2(doc); }
 *   }
 *
 *   // Option B: use the pre-built class
 *   export default AutomergeServer;
 */

import {
  next as Automerge,
  type Doc,
  type Heads,
} from '@automerge/automerge';
import type { Connection, ConnectionContext, WSMessage } from 'partyserver';
import { Server } from 'partyserver';

// ── Message types ────────────────────────────────────────────────────────────
// Simple binary protocol: first byte = message type, rest = payload.

const MSG_SYNC = 0;
const MSG_EPHEMERAL = 1;  // presence/awareness

// ── Per-connection sync state ────────────────────────────────────────────────

const SYNC_STATE_KEY = '__amSyncState';

function getSyncState(conn: Connection): Automerge.SyncState {
  try {
    const state = conn.state as Record<string, unknown> | null;
    const encoded = state?.[SYNC_STATE_KEY] as Uint8Array | undefined;
    if (encoded) {
      return Automerge.decodeSyncState(encoded);
    }
  } catch { /* ignore */ }
  return Automerge.initSyncState();
}

function setSyncState(conn: Connection, syncState: Automerge.SyncState): void {
  try {
    conn.setState((prev: Record<string, unknown> | null) => ({
      ...prev,
      [SYNC_STATE_KEY]: Automerge.encodeSyncState(syncState),
    }));
  } catch { /* ignore — may fail if connection is already closed */ }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function send(conn: Connection, data: Uint8Array): void {
  try {
    if (conn.readyState === 1 /* OPEN */) {
      conn.send(data);
    }
  } catch { /* connection broken, ignore */ }
}

function packMessage(type: number, payload: Uint8Array): Uint8Array {
  const msg = new Uint8Array(1 + payload.length);
  msg[0] = type;
  msg.set(payload, 1);
  return msg;
}

function unpackMessage(data: Uint8Array): { type: number; payload: Uint8Array } {
  return { type: data[0], payload: data.subarray(1) };
}

// ── Mixin ────────────────────────────────────────────────────────────────────

type ServerClass = new (...args: any[]) => Server;

export interface AutomergeInstance {
  /** The current Automerge document. */
  readonly doc: Doc<unknown>;

  /** Called on start — return initial doc state, or void for empty doc. */
  onLoad(): Promise<Uint8Array | void>;

  /** Called periodically when doc changes — persist the doc bytes. */
  onSave(bytes: Uint8Array): Promise<void>;

  /** Process a sync message from a connection. */
  handleMessage(connection: Connection, message: WSMessage): void;

  /** Send an ephemeral message (presence, cursor) to all connections. */
  broadcastEphemeral(data: Uint8Array, exclude?: Connection): void;
}

/**
 * Mixin that adds Automerge sync to a partyserver Server class.
 *
 * Same pattern as y-partyserver's `withYjs(Server)`.
 *
 * The DO keeps the Automerge doc in memory. On each WebSocket message,
 * it runs Automerge's sync protocol (generateSyncMessage/receiveSyncMessage).
 * Sync state per connection is stored in conn.setState() so it survives
 * WebSocket Hibernation.
 */
export function withAutomerge<TBase extends ServerClass>(
  Base: TBase,
): TBase & (new (...args: any[]) => AutomergeInstance) {
  class AutomergeMixin extends Base {
    #doc: Doc<unknown> = Automerge.init();
    #saveTimeout: ReturnType<typeof setTimeout> | null = null;
    #saveDebounceMs = 2000;

    get doc(): Doc<unknown> {
      return this.#doc;
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    async onLoad(): Promise<Uint8Array | void> {
      // Override in subclass to load from R2/DO storage/etc.
      return;
    }

    async onSave(_bytes: Uint8Array): Promise<void> {
      // Override in subclass to persist to R2/DO storage/etc.
    }

    async onStart(): Promise<void> {
      // Load initial state
      const bytes = await this.onLoad();
      if (bytes && bytes.length > 0) {
        this.#doc = Automerge.load(bytes);
      }

      // After hibernation wake-up: doc is empty but connections survive.
      // Send sync step 1 to all connections — they respond with their state.
      for (const conn of this.getConnections()) {
        this.#sendSync(conn);
      }
    }

    // ── Connection lifecycle ───────────────────────────────────────────

    onConnect(conn: Connection<unknown>, _ctx: ConnectionContext): void | Promise<void> {
      // Send initial sync message to the new connection
      this.#sendSync(conn);
    }

    onClose(
      _conn: Connection<unknown>,
      _code: number,
      _reason: string,
      _wasClean: boolean,
    ): void | Promise<void> {
      // Sync state is cleaned up automatically (stored in conn.state).
      // If no connections remain, flush any pending save.
      let hasConnections = false;
      for (const _ of this.getConnections()) {
        hasConnections = true;
        break;
      }
      if (!hasConnections) {
        this.#flushSave();
      }
    }

    // ── Message handling ───────────────────────────────────────────────

    onMessage(conn: Connection, message: WSMessage): void {
      this.handleMessage(conn, message);
    }

    handleMessage(connection: Connection, message: WSMessage): void {
      if (typeof message === 'string') return; // ignore text messages

      const data = message instanceof Uint8Array
        ? message
        : message instanceof ArrayBuffer
          ? new Uint8Array(message)
          : new Uint8Array(message.buffer, message.byteOffset, message.byteLength);

      const { type, payload } = unpackMessage(data);

      switch (type) {
        case MSG_SYNC:
          this.#handleSync(connection, payload);
          break;
        case MSG_EPHEMERAL:
          // Forward ephemeral to all other connections
          this.broadcastEphemeral(payload, connection);
          break;
      }
    }

    // ── Sync protocol ──────────────────────────────────────────────────

    #handleSync(conn: Connection, syncMessage: Uint8Array): void {
      // Receive the sync message
      let syncState = getSyncState(conn);
      const [newDoc, newSyncState] = Automerge.receiveSyncMessage(
        this.#doc,
        syncState,
        syncMessage,
      );
      this.#doc = newDoc;
      syncState = newSyncState;

      // Generate response
      const [nextSyncState, reply] = Automerge.generateSyncMessage(
        this.#doc,
        syncState,
      );
      setSyncState(conn, nextSyncState);

      if (reply) {
        send(conn, packMessage(MSG_SYNC, reply));
      }

      // Broadcast updates to other connections
      for (const other of this.getConnections()) {
        if (other === conn) continue;
        this.#sendSync(other);
      }

      // Schedule debounced save
      this.#scheduleSave();
    }

    #sendSync(conn: Connection): void {
      const syncState = getSyncState(conn);
      const [nextSyncState, syncMessage] = Automerge.generateSyncMessage(
        this.#doc,
        syncState,
      );
      setSyncState(conn, nextSyncState);
      if (syncMessage) {
        send(conn, packMessage(MSG_SYNC, syncMessage));
      }
    }

    // ── Ephemeral (presence/awareness) ─────────────────────────────────

    broadcastEphemeral(data: Uint8Array, exclude?: Connection): void {
      const msg = packMessage(MSG_EPHEMERAL, data);
      for (const conn of this.getConnections()) {
        if (conn === exclude) continue;
        send(conn, msg);
      }
    }

    // ── Persistence ────────────────────────────────────────────────────

    #scheduleSave(): void {
      if (this.#saveTimeout) return;
      this.#saveTimeout = setTimeout(() => {
        this.#saveTimeout = null;
        this.#flushSave();
      }, this.#saveDebounceMs);
    }

    #flushSave(): void {
      if (this.#saveTimeout) {
        clearTimeout(this.#saveTimeout);
        this.#saveTimeout = null;
      }
      const bytes = Automerge.save(this.#doc);
      this.onSave(bytes).catch((err) => {
        console.error('[automerge-partyserver] Failed to save:', err);
      });
    }
  }

  return AutomergeMixin as unknown as TBase & (new (...args: any[]) => AutomergeInstance);
}

/** Pre-built Automerge server — extend or use directly. */
export const AutomergeServer = withAutomerge(Server);
