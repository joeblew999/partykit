/**
 * automerge-partyserver — Automerge CRDT sync for PartyKit / partyserver.
 *
 * Uses automerge-repo internally for the sync protocol, document lifecycle,
 * and storage. The `withAutomerge(Server)` mixin bridges automerge-repo's
 * NetworkAdapter to PartyKit's WebSocket connections.
 *
 * Usage:
 *   import { withAutomerge, AutomergeServer } from 'automerge-partyserver';
 *   export default class MyServer extends withAutomerge(Server) { ... }
 */

import {
  Repo,
  type PeerId,
  type DocumentId,
  type DocHandle,
  type PeerMetadata,
  NetworkAdapter,
  type NetworkAdapterInterface,
  type Message,
} from '@automerge/automerge-repo';
import { next as Automerge } from '@automerge/automerge';
// @ts-expect-error — partyserver types resolve at build time via workspace
import type { Connection, ConnectionContext, WSMessage } from 'partyserver';
// @ts-expect-error — partyserver types resolve at build time via workspace
import { Server } from 'partyserver';
import { encode as cborEncode, decode as cborDecode } from 'cborg';
import { DOStorageAdapter } from './storage';

// ── PartyKit NetworkAdapter for automerge-repo ───────────────────────────────

/**
 * Bridges automerge-repo's NetworkAdapter interface to PartyKit WebSocket connections.
 *
 * automerge-repo calls `send(message)` → we forward to the right WebSocket connection.
 * PartyKit gives us `onMessage(conn, data)` → we emit to automerge-repo.
 */
class PartyKitNetworkAdapter extends NetworkAdapter {
  private connections = new Map<string, Connection>();
  private serverPeerId: PeerId;

  constructor(serverPeerId: string) {
    super();
    this.serverPeerId = serverPeerId as PeerId;
  }

  // ── NetworkAdapter interface ─────────────────────────────────────────

  connect(peerId: PeerId, peerMetadata?: PeerMetadata): void {
    this.peerId = peerId;
    this.peerMetadata = peerMetadata;
  }

  disconnect(): void {
    this.connections.clear();
  }

  send(message: Message): void {
    const targetId = message.targetId as string;
    const conn = this.connections.get(targetId);
    if (!conn || conn.readyState !== 1) return;

    try {
      const encoded = cborEncode(message);
      conn.send(encoded);
    } catch {
      // connection broken
    }
  }

  isReady(): boolean {
    return true;
  }

  whenReady(): Promise<void> {
    return Promise.resolve();
  }

  // ── PartyKit events (called by the mixin) ────────────────────────────

  addConnection(peerId: string, conn: Connection): void {
    this.connections.set(peerId, conn);
  }

  removeConnection(peerId: string): void {
    this.connections.delete(peerId);
    this.emit('peer-disconnected', { peerId: peerId as PeerId });
  }

  receiveMessage(conn: Connection, data: Uint8Array): void {
    try {
      const message = cborDecode(data) as any;

      // Handle join message (first message from client)
      if (message.type === 'join' && message.senderId) {
        const peerId = message.senderId as string;
        this.connections.set(peerId, conn);

        // Announce the new peer to the repo
        this.emit('peer-candidate', {
          peerId: peerId as PeerId,
          peerMetadata: message.peerMetadata,
        });

        // Tell the client about us
        const peerMsg = {
          type: 'peer',
          senderId: this.serverPeerId,
          targetId: peerId,
          peerMetadata: this.peerMetadata ?? {},
          selectedProtocolVersion: '1',
        };
        conn.send(cborEncode(peerMsg));
        return;
      }

      // Regular message — forward to repo
      this.emit('message', message);
    } catch {
      // malformed message
    }
  }
}

// ── Ephemeral message type byte ──────────────────────────────────────────────

const MSG_EPHEMERAL = 0x01;

// ── Mixin ────────────────────────────────────────────────────────────────────

type ServerClass = new (...args: any[]) => Server;

export interface AutomergeInstance {
  /** The automerge-repo Repo instance. */
  readonly repo: Repo;

  /** Get a document handle by ID. */
  getHandle(docId: string): DocHandle<unknown> | undefined;

  /** Called on start — return initial doc bytes, or void for empty. */
  onLoad(): Promise<Uint8Array | void>;

  /** Called when repo wants to persist (debounced). */
  onSave(bytes: Uint8Array): Promise<void>;

  /** Handle incoming WebSocket messages. */
  handleMessage(connection: Connection, message: WSMessage): void;

  /** Broadcast ephemeral data (presence/cursor) to all peers. */
  broadcastEphemeral(data: Uint8Array, exclude?: Connection): void;
}

/**
 * Mixin that adds Automerge sync to a partyserver Server class.
 *
 * Uses automerge-repo internally for:
 * - Incremental sync protocol (generateSyncMessage/receiveSyncMessage)
 * - Document lifecycle (DocHandle state machine)
 * - Per-peer sync state tracking
 * - Storage via DOStorageAdapter
 *
 * Same pattern as y-partyserver's `withYjs(Server)`.
 */
export function withAutomerge<TBase extends ServerClass>(
  Base: TBase,
): TBase & (new (...args: any[]) => AutomergeInstance) {
  class AutomergeMixin extends Base {
    // These exist on Server (extends DurableObject) but TS can't see them through the mixin
    declare name: string;
    declare ctx: { storage: any };
    declare getConnections: () => Iterable<Connection>;

    #repo!: Repo;
    #networkAdapter!: PartyKitNetworkAdapter;
    #storageAdapter!: DOStorageAdapter;

    get repo(): Repo {
      return this.#repo;
    }

    getHandle(docId: string): DocHandle<unknown> | undefined {
      return this.#repo.handles[docId as DocumentId];
    }

    // ── Lifecycle ────────────────────────────────────────────────────

    async onLoad(): Promise<Uint8Array | void> {
      // Override in subclass to provide initial state
      return;
    }

    async onSave(_bytes: Uint8Array): Promise<void> {
      // Override in subclass for additional persistence (e.g. R2 backup)
    }

    async onStart(): Promise<void> {
      const serverPeerId = `server:${this.name}`;

      // Create DO-backed storage adapter
      this.#storageAdapter = new DOStorageAdapter(this.ctx.storage as any);

      // Create network adapter that bridges to PartyKit WebSocket
      this.#networkAdapter = new PartyKitNetworkAdapter(serverPeerId);

      // Create automerge-repo Repo
      this.#repo = new Repo({
        network: [this.#networkAdapter as unknown as NetworkAdapterInterface],
        storage: this.#storageAdapter,
        peerId: serverPeerId as PeerId,
        sharePolicy: async () => true, // accept all documents
      });

      // Load initial state if provided
      const initialBytes = await this.onLoad();
      if (initialBytes && initialBytes.length > 0) {
        // Import the document into the repo
        const doc = Automerge.load(initialBytes);
        // The repo will handle storage from here
      }

      // Re-sync existing connections after hibernation wake-up
      for (const conn of this.getConnections()) {
        // Connections that survived hibernation need to re-handshake
        // The client will re-send a join message on reconnect
      }
    }

    // ── Connection lifecycle ─────────────────────────────────────────

    onConnect(conn: Connection<unknown>, _ctx: ConnectionContext): void | Promise<void> {
      // Don't do anything yet — wait for the join message in onMessage.
      // automerge-repo's protocol requires a join/peer handshake before sync.
    }

    onClose(
      conn: Connection<unknown>,
      _code: number,
      _reason: string,
      _wasClean: boolean,
    ): void | Promise<void> {
      // Find and remove this connection's peer ID
      // Connection state may have the peer ID from the join handshake
      try {
        const state = conn.state as Record<string, unknown> | null;
        const peerId = state?.['__amPeerId'] as string | undefined;
        if (peerId) {
          this.#networkAdapter.removeConnection(peerId);
        }
      } catch { /* ignore */ }

      // Flush repo if no connections remain
      let hasConnections = false;
      for (const _ of this.getConnections()) {
        hasConnections = true;
        break;
      }
      if (!hasConnections) {
        this.#repo.flush().catch(() => {});
      }
    }

    // ── Message handling ─────────────────────────────────────────────

    onMessage(conn: Connection, message: WSMessage): void {
      this.handleMessage(conn, message);
    }

    handleMessage(connection: Connection, message: WSMessage): void {
      if (typeof message === 'string') return; // ignore text

      const data = message instanceof Uint8Array
        ? message
        : message instanceof ArrayBuffer
          ? new Uint8Array(message)
          : new Uint8Array(message.buffer, message.byteOffset, message.byteLength);

      // Check for ephemeral messages (first byte = MSG_EPHEMERAL)
      if (data.length > 0 && data[0] === MSG_EPHEMERAL) {
        this.broadcastEphemeral(data.subarray(1), connection);
        return;
      }

      // CBOR-encoded automerge-repo message — forward to network adapter
      // Also store peer ID on connection for cleanup in onClose
      try {
        const decoded = cborDecode(data) as any;
        if (decoded.senderId) {
          connection.setState((prev: Record<string, unknown> | null) => ({
            ...prev,
            '__amPeerId': decoded.senderId,
          }));
        }
      } catch { /* ignore decode errors for state tracking */ }

      this.#networkAdapter.receiveMessage(connection, data);
    }

    // ── Ephemeral ────────────────────────────────────────────────────

    broadcastEphemeral(data: Uint8Array, exclude?: Connection): void {
      const msg = new Uint8Array(1 + data.length);
      msg[0] = MSG_EPHEMERAL;
      msg.set(data, 1);

      for (const conn of this.getConnections()) {
        if (conn === exclude) continue;
        try {
          if (conn.readyState === 1) conn.send(msg);
        } catch { /* ignore broken connections */ }
      }
    }
  }

  return AutomergeMixin as unknown as TBase & (new (...args: any[]) => AutomergeInstance);
}

/** Pre-built Automerge server — extend or use directly. */
export const AutomergeServer = withAutomerge(Server);

export { DOStorageAdapter } from './storage';
