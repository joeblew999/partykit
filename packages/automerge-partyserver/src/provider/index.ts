/**
 * AutomergeProvider — browser-side Automerge sync via automerge-repo + PartyKit WebSocket.
 *
 * Wraps automerge-repo's Repo with a PartyKit WebSocket NetworkAdapter.
 * Handles BroadcastChannel for cross-tab sync, reconnect, and ephemeral messages.
 *
 * Usage:
 *   import { AutomergeProvider } from 'automerge-partyserver/provider';
 *
 *   const provider = new AutomergeProvider({
 *     host: 'localhost:1999',
 *     room: 'my-model-id',
 *   });
 *
 *   // Create or find a document
 *   const handle = provider.repo.create();
 *   handle.change((doc) => { doc.title = 'Hello'; });
 *
 *   // Or find an existing one
 *   const handle = provider.repo.find(documentId);
 *   handle.whenReady().then(() => { console.log(handle.doc()); });
 */

import {
  Repo,
  type PeerId,
  type PeerMetadata,
  type DocHandle,
  type AnyDocumentId,
  NetworkAdapter,
  type NetworkAdapterInterface,
  type Message,
} from '@automerge/automerge-repo';
import {
  IndexedDBStorageAdapter,
} from '@automerge/automerge-repo-storage-indexeddb';
import {
  BroadcastChannelNetworkAdapter,
} from '@automerge/automerge-repo-network-broadcastchannel';
import { encode as cborEncode, decode as cborDecode } from 'cborg';

// ── Ephemeral message type ───────────────────────────────────────────────────

const MSG_EPHEMERAL = 0x01;

// ── PartyKit WebSocket NetworkAdapter ────────────────────────────────────────

class PartyKitWebSocketAdapter extends NetworkAdapter {
  private ws: WebSocket | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectDelay: number;
  private readonly maxReconnectDelay: number;
  private readonly url: string;
  private destroyed = false;

  constructor(
    private readonly wsUrl: string,
    private readonly onStatusChange?: (status: 'connecting' | 'connected' | 'disconnected') => void,
    reconnectDelay = 2000,
    maxReconnectDelay = 30000,
  ) {
    super();
    this.url = wsUrl;
    this.reconnectDelay = reconnectDelay;
    this.maxReconnectDelay = maxReconnectDelay;
  }

  connect(peerId: PeerId, peerMetadata?: PeerMetadata): void {
    this.peerId = peerId;
    this.peerMetadata = peerMetadata;
    this.openWebSocket();
  }

  disconnect(): void {
    this.destroyed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.ws) { this.ws.close(); this.ws = null; }
  }

  send(message: Message): void {
    if (!this.ws || this.ws.readyState !== 1) return;
    try {
      this.ws.send(cborEncode(message));
    } catch { /* ignore */ }
  }

  isReady(): boolean {
    return this.ws?.readyState === 1;
  }

  whenReady(): Promise<void> {
    if (this.isReady()) return Promise.resolve();
    return new Promise((resolve) => {
      const check = () => {
        if (this.isReady()) resolve();
        else setTimeout(check, 100);
      };
      check();
    });
  }

  private openWebSocket(): void {
    if (this.destroyed) return;
    this.onStatusChange?.('connecting');

    this.ws = new WebSocket(this.url);
    this.ws.binaryType = 'arraybuffer';

    this.ws.onopen = () => {
      this.onStatusChange?.('connected');
      this.reconnectDelay = 2000; // reset backoff

      // Send join message
      const joinMsg = {
        type: 'join',
        senderId: this.peerId,
        peerMetadata: this.peerMetadata ?? {},
        supportedProtocolVersions: ['1'],
      };
      this.ws!.send(cborEncode(joinMsg));
    };

    this.ws.onmessage = (event) => {
      try {
        const data = new Uint8Array(event.data as ArrayBuffer);

        // Check for ephemeral
        if (data.length > 0 && data[0] === MSG_EPHEMERAL) {
          // Let the provider handle this (not automerge-repo)
          this.emit('ephemeral' as any, data.subarray(1));
          return;
        }

        const message = cborDecode(data);

        // Handle peer announcement
        if ((message as any).type === 'peer' && (message as any).senderId) {
          this.emit('peer-candidate', {
            peerId: (message as any).senderId as PeerId,
            peerMetadata: (message as any).peerMetadata,
          });
          return;
        }

        // Regular sync message
        this.emit('message', message);
      } catch { /* malformed */ }
    };

    this.ws.onclose = () => {
      this.onStatusChange?.('disconnected');
      this.ws = null;
      if (!this.destroyed) this.scheduleReconnect();
    };
  }

  private scheduleReconnect(): void {
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.openWebSocket();
    }, this.reconnectDelay);
    this.reconnectDelay = Math.min(this.reconnectDelay * 1.5, this.maxReconnectDelay);
  }

  /** Send raw ephemeral data (bypasses automerge-repo). */
  sendEphemeral(data: Uint8Array): void {
    if (!this.ws || this.ws.readyState !== 1) return;
    const msg = new Uint8Array(1 + data.length);
    msg[0] = MSG_EPHEMERAL;
    msg.set(data, 1);
    this.ws.send(msg);
  }
}

// ── Provider options ─────────────────────────────────────────────────────────

export interface AutomergeProviderOptions {
  /** PartyKit host (e.g. 'localhost:1999' or 'my-project.partykit.dev') */
  host: string;
  /** Room name (typically the model/document ID) */
  room: string;
  /** Party name (default: 'main') */
  party?: string;
  /** Called when connection state changes */
  onStatus?: (status: 'connecting' | 'connected' | 'disconnected') => void;
  /** Called when ephemeral data arrives (presence, cursor) */
  onEphemeral?: (data: Uint8Array) => void;
  /** Use IndexedDB for local persistence (default: true) */
  indexedDB?: boolean;
  /** IDB database name (default: 'automerge-partyserver') */
  idbName?: string;
  /** Use BroadcastChannel for cross-tab sync (default: true) */
  broadcast?: boolean;
  /** WebSocket protocol ('ws' or 'wss', default: auto-detect) */
  protocol?: 'ws' | 'wss';
}

// ── Provider ─────────────────────────────────────────────────────────────────

export class AutomergeProvider {
  readonly repo: Repo;
  private wsAdapter: PartyKitWebSocketAdapter;

  constructor(opts: AutomergeProviderOptions) {
    const protocol = opts.protocol ?? (opts.host.startsWith('localhost') ? 'ws' : 'wss');
    const party = opts.party ?? 'main';
    const host = opts.host.replace(/^https?:\/\//, '');
    const url = `${protocol}://${host}/parties/${party}/${opts.room}`;

    // Network adapters
    this.wsAdapter = new PartyKitWebSocketAdapter(url, opts.onStatus);
    const networkAdapters: NetworkAdapterInterface[] = [
      this.wsAdapter as unknown as NetworkAdapterInterface,
    ];

    // BroadcastChannel for cross-tab sync
    if (opts.broadcast !== false && typeof BroadcastChannel !== 'undefined') {
      networkAdapters.push(
        new BroadcastChannelNetworkAdapter() as unknown as NetworkAdapterInterface,
      );
    }

    // Storage
    const useIdb = opts.indexedDB !== false && typeof indexedDB !== 'undefined';

    // Create Repo
    this.repo = new Repo({
      network: networkAdapters,
      storage: useIdb ? new IndexedDBStorageAdapter(opts.idbName ?? 'automerge-partyserver') : undefined,
      peerId: `client:${crypto.randomUUID()}` as PeerId,
    });

    // Wire ephemeral events
    if (opts.onEphemeral) {
      this.wsAdapter.on('ephemeral' as any, opts.onEphemeral);
    }
  }

  /** Create a new document. Returns a DocHandle. */
  create<T>(): DocHandle<T> {
    return this.repo.create<T>();
  }

  /** Find an existing document by ID. Returns a DocHandle. */
  find<T>(docId: AnyDocumentId): DocHandle<T> {
    return this.repo.find<T>(docId) as unknown as DocHandle<T>;
  }

  /** Send ephemeral data (presence, cursor) — not persisted. */
  sendEphemeral(data: Uint8Array): void {
    this.wsAdapter.sendEphemeral(data);
  }

  /** Is the WebSocket connected? */
  get connected(): boolean {
    return this.wsAdapter.isReady();
  }

  /** Destroy — close WebSocket, cleanup. */
  destroy(): void {
    this.wsAdapter.disconnect();
    // Repo doesn't have a destroy method, but network disconnect is sufficient
  }
}
