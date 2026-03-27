/**
 * Example: Automerge sync server with DO storage persistence.
 *
 * Deploy with PartyKit:
 *   npx partykit dev example/server.ts
 *
 * Or add to partykit.json:
 *   { "main": "example/server.ts" }
 */

import { withAutomerge } from '../src/server/index';
import { Server } from 'partyserver';

export default class AutomergeSyncServer extends withAutomerge(Server) {
  /**
   * Load initial doc state from DO storage.
   * Called once on start (or after hibernation wake-up).
   */
  async onLoad(): Promise<Uint8Array | void> {
    const bytes = await this.room.storage.get<Uint8Array>('automerge-doc');
    if (bytes) return bytes;
  }

  /**
   * Persist doc state to DO storage.
   * Called on a debounced interval (2s) after changes.
   */
  async onSave(bytes: Uint8Array): Promise<void> {
    await this.room.storage.put('automerge-doc', bytes);
  }
}
