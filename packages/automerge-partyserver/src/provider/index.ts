/**
 * AutomergeProvider — browser-side WebSocket sync for Automerge + PartyKit.
 *
 * Connects to a PartyKit room running `withAutomerge(Server)`.
 * Runs the Automerge sync protocol over WebSocket.
 * Supports BroadcastChannel for cross-tab sync.
 *
 * Usage:
 *   import { AutomergeProvider } from 'automerge-partyserver/provider';
 *   import { next as Automerge } from '@automerge/automerge';
 *
 *   let doc = Automerge.init();
 *   const provider = new AutomergeProvider({
 *     host: 'localhost:1999',
 *     room: 'my-model-id',
 *     doc,
 *     onUpdate: (newDoc) => { doc = newDoc; render(); },
 *   });
 */

import { next as Automerge, type Doc } from '@automerge/automerge';

// Message types — must match server
const MSG_SYNC = 0;
const MSG_EPHEMERAL = 1;

function packMessage(type: number, payload: Uint8Array): Uint8Array {
  const msg = new Uint8Array(1 + payload.length);
  msg[0] = type;
  msg.set(payload, 1);
  return msg;
}

function unpackMessage(data: Uint8Array): { type: number; payload: Uint8Array } {
  return { type: data[0], payload: data.subarray(1) };
}

// ── Options ──────────────────────────────────────────────────────────────────

export interface AutomergeProviderOptions {
  /** PartyKit host (e.g. 'localhost:1999' or 'my-project.partykit.dev') */
  host: string;
  /** Room name (typically the model/document ID) */
  room: string;
  /** Party name (default: 'main') */
  party?: string;
  /** Initial Automerge document */
  doc: Doc<unknown>;
  /** Called when the document is updated by a remote peer */
  onUpdate: (doc: Doc<unknown>) => void;
  /** Called when an ephemeral message (presence) is received */
  onEphemeral?: (data: Uint8Array) => void;
  /** Called when connection state changes */
  onStatus?: (status: 'connecting' | 'connected' | 'disconnected') => void;
  /** Reconnect delay in ms (default: 2000) */
  reconnectDelay?: number;
  /** Max reconnect delay in ms (default: 30000) */
  maxReconnectDelay?: number;
  /** Use BroadcastChannel for cross-tab sync (default: true) */
  broadcast?: boolean;
  /** WebSocket protocol ('ws' or 'wss', default: auto-detect from host) */
  protocol?: 'ws' | 'wss';
}

// ── Provider ─────────────────────────────────────────────────────────────────

export class AutomergeProvider {
  private ws: WebSocket | null = null;
  private syncState: Automerge.SyncState = Automerge.initSyncState();
  private doc: Doc<unknown>;
  private bc: BroadcastChannel | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectDelay: number;
  private readonly maxReconnectDelay: number;
  private readonly url: string;
  private readonly opts: AutomergeProviderOptions;
  private destroyed = false;

  constructor(opts: AutomergeProviderOptions) {
    this.opts = opts;
    this.doc = opts.doc;
    this.reconnectDelay = opts.reconnectDelay ?? 2000;
    this.maxReconnectDelay = opts.maxReconnectDelay ?? 30000;

    const protocol = opts.protocol ?? (opts.host.startsWith('localhost') ? 'ws' : 'wss');
    const party = opts.party ?? 'main';
    const host = opts.host.replace(/^https?:\/\//, '');
    this.url = `${protocol}://${host}/parties/${party}/${opts.room}`;

    // BroadcastChannel for cross-tab sync
    if (opts.broadcast !== false && typeof BroadcastChannel !== 'undefined') {
      this.bc = new BroadcastChannel(`automerge:${opts.room}`);
      this.bc.onmessage = (event) => {
        const bytes = new Uint8Array(event.data);
        this.doc = Automerge.merge(this.doc, Automerge.load(bytes));
        this.opts.onUpdate(this.doc);
      };
    }

    this.connect();
  }

  // ── Connection ───────────────────────────────────────────────────────

  private connect(): void {
    if (this.destroyed) return;
    this.opts.onStatus?.('connecting');

    this.ws = new WebSocket(this.url);
    this.ws.binaryType = 'arraybuffer';

    this.ws.onopen = () => {
      this.opts.onStatus?.('connected');
      this.reconnectDelay = this.opts.reconnectDelay ?? 2000; // reset backoff
      // Server sends sync step 1 on connect — we respond in onmessage
    };

    this.ws.onmessage = (event) => {
      const data = new Uint8Array(event.data as ArrayBuffer);
      const { type, payload } = unpackMessage(data);

      switch (type) {
        case MSG_SYNC:
          this.handleSync(payload);
          break;
        case MSG_EPHEMERAL:
          this.opts.onEphemeral?.(payload);
          break;
      }
    };

    this.ws.onclose = () => {
      this.opts.onStatus?.('disconnected');
      this.ws = null;
      this.scheduleReconnect();
    };

    this.ws.onerror = () => {
      // onclose will fire after onerror
    };
  }

  private scheduleReconnect(): void {
    if (this.destroyed) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.reconnectDelay);
    // Exponential backoff
    this.reconnectDelay = Math.min(this.reconnectDelay * 1.5, this.maxReconnectDelay);
  }

  // ── Sync protocol ───────────────────────────────────────────────────

  private handleSync(syncMessage: Uint8Array): void {
    // Receive server's sync message
    const [newDoc, newSyncState] = Automerge.receiveSyncMessage(
      this.doc,
      this.syncState,
      syncMessage,
    );
    this.doc = newDoc;
    this.syncState = newSyncState;

    // Notify consumer
    this.opts.onUpdate(this.doc);

    // Generate response
    const [nextSyncState, reply] = Automerge.generateSyncMessage(
      this.doc,
      this.syncState,
    );
    this.syncState = nextSyncState;

    if (reply && this.ws?.readyState === 1) {
      this.ws.send(packMessage(MSG_SYNC, reply));
    }
  }

  // ── Public API ──────────────────────────────────────────────────────

  /** Apply a local change and sync with server + other tabs. */
  change(changeFn: (doc: Doc<unknown>) => void): void {
    this.doc = Automerge.change(this.doc, changeFn);
    this.opts.onUpdate(this.doc);

    // Sync with server
    const [nextSyncState, syncMessage] = Automerge.generateSyncMessage(
      this.doc,
      this.syncState,
    );
    this.syncState = nextSyncState;
    if (syncMessage && this.ws?.readyState === 1) {
      this.ws.send(packMessage(MSG_SYNC, syncMessage));
    }

    // Broadcast to other tabs
    if (this.bc) {
      this.bc.postMessage(Automerge.save(this.doc));
    }
  }

  /** Send an ephemeral message (presence, cursor position, etc). */
  sendEphemeral(data: Uint8Array): void {
    if (this.ws?.readyState === 1) {
      this.ws.send(packMessage(MSG_EPHEMERAL, data));
    }
  }

  /** Get the current document. */
  getDoc(): Doc<unknown> {
    return this.doc;
  }

  /** Clean up — close WebSocket, BroadcastChannel, timers. */
  destroy(): void {
    this.destroyed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.ws) { this.ws.close(); this.ws = null; }
    if (this.bc) { this.bc.close(); this.bc = null; }
  }

  /** Is the WebSocket currently connected? */
  get connected(): boolean {
    return this.ws?.readyState === 1;
  }
}
