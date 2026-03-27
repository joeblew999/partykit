/**
 * DOStorageAdapter — automerge-repo StorageAdapter backed by DO storage.
 *
 * Maps automerge-repo's hierarchical StorageKey arrays to flat string keys
 * for Durable Object storage. Handles 128KB chunking for large values
 * (following the y-partykit pattern).
 *
 * Key format: segments joined by '.' (e.g. "docId.snapshot.abc123")
 * Chunks: "key.chunk.000", "key.chunk.001", etc.
 */

import type {
  StorageAdapterInterface,
  Chunk,
} from '@automerge/automerge-repo';

type StorageKey = string[];

// DO storage value size limit — chunk above this
const CHUNK_SIZE = 128 * 1024; // 128KB

// PartyKit DO storage interface (subset of what we need)
interface DOStorage {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T = unknown>(key: string, value: T): Promise<void>;
  delete(key: string | string[]): Promise<boolean | number>;
  list<T = unknown>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>>;
}

export class DOStorageAdapter implements StorageAdapterInterface {
  constructor(private storage: DOStorage) {}

  // ── Key encoding ─────────────────────────────────────────────────────

  private encode(key: StorageKey): string {
    return key.join('.');
  }

  private decode(str: string): StorageKey {
    return str.split('.');
  }

  // ── StorageAdapterInterface ──────────────────────────────────────────

  async load(key: StorageKey): Promise<Uint8Array | undefined> {
    const prefix = this.encode(key);

    // Try direct key first (small values stored without chunking)
    const direct = await this.storage.get<Uint8Array>(prefix);
    if (direct) {
      if (direct instanceof Uint8Array) return direct;
      if (direct instanceof ArrayBuffer) return new Uint8Array(direct);
      // DO storage may deserialize as a plain object with numeric keys
      if (typeof direct === 'object' && direct !== null) {
        return new Uint8Array(Object.values(direct as Record<string, number>));
      }
    }

    // Try chunked storage
    const chunks = await this.storage.list<Uint8Array>({ prefix: `${prefix}.chunk.` });
    if (chunks.size === 0) return undefined;

    // Reassemble chunks in order
    const sorted = [...chunks.entries()].sort(([a], [b]) => a.localeCompare(b));
    const totalSize = sorted.reduce((acc, [, v]) => {
      const arr = v instanceof Uint8Array ? v : new Uint8Array(Object.values(v as any));
      return acc + arr.length;
    }, 0);

    const result = new Uint8Array(totalSize);
    let offset = 0;
    for (const [, v] of sorted) {
      const arr = v instanceof Uint8Array ? v : new Uint8Array(Object.values(v as any));
      result.set(arr, offset);
      offset += arr.length;
    }
    return result;
  }

  async save(key: StorageKey, data: Uint8Array): Promise<void> {
    const prefix = this.encode(key);

    // Clean up any existing chunks
    await this.removeChunks(prefix);

    if (data.length <= CHUNK_SIZE) {
      // Small enough — store directly
      await this.storage.put(prefix, data);
    } else {
      // Chunk it
      let chunkIndex = 0;
      for (let i = 0; i < data.length; i += CHUNK_SIZE) {
        const chunk = data.slice(i, i + CHUNK_SIZE);
        const chunkKey = `${prefix}.chunk.${String(chunkIndex).padStart(4, '0')}`;
        await this.storage.put(chunkKey, chunk);
        chunkIndex++;
      }
      // Remove direct key if it existed
      await this.storage.delete(prefix);
    }
  }

  async remove(key: StorageKey): Promise<void> {
    const prefix = this.encode(key);
    await this.storage.delete(prefix);
    await this.removeChunks(prefix);
  }

  async loadRange(keyPrefix: StorageKey): Promise<Chunk[]> {
    const prefix = this.encode(keyPrefix);
    const entries = await this.storage.list<Uint8Array>({ prefix });

    // Group by base key (strip chunk suffixes)
    const grouped = new Map<string, Uint8Array[]>();
    for (const [k, v] of entries) {
      const baseKey = k.replace(/\.chunk\.\d+$/, '');
      if (!grouped.has(baseKey)) grouped.set(baseKey, []);
      const arr = v instanceof Uint8Array ? v : new Uint8Array(Object.values(v as any));
      grouped.get(baseKey)!.push(arr);
    }

    const result: Chunk[] = [];
    for (const [k, chunks] of grouped) {
      if (k.includes('.chunk.')) continue; // skip chunk entries, handled via base key

      let data: Uint8Array;
      if (chunks.length === 1) {
        data = chunks[0];
      } else {
        // Reassemble chunks
        const totalSize = chunks.reduce((acc, c) => acc + c.length, 0);
        data = new Uint8Array(totalSize);
        let offset = 0;
        for (const c of chunks) {
          data.set(c, offset);
          offset += c.length;
        }
      }
      result.push({ key: this.decode(k), data });
    }

    return result;
  }

  async removeRange(keyPrefix: StorageKey): Promise<void> {
    const prefix = this.encode(keyPrefix);
    const entries = await this.storage.list({ prefix });
    const keys = [...entries.keys()];
    if (keys.length > 0) {
      // DO storage.delete accepts an array
      await this.storage.delete(keys);
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────

  private async removeChunks(prefix: string): Promise<void> {
    const chunks = await this.storage.list({ prefix: `${prefix}.chunk.` });
    const keys = [...chunks.keys()];
    if (keys.length > 0) {
      await this.storage.delete(keys);
    }
  }
}
