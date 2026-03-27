/**
 * Example: Automerge sync server with DO storage persistence.
 *
 * Deploy: npx partykit dev example/server.ts
 */

import { withAutomerge } from '../src/server/index';
// @ts-expect-error — partyserver resolves at runtime
import { Server } from 'partyserver';

export default class AutomergeSyncServer extends withAutomerge(Server) {
  async onLoad(): Promise<Uint8Array | void> {
    const bytes = await (this as any).room.storage.get('automerge-doc');
    if (bytes) return bytes as Uint8Array;
  }

  async onSave(bytes: Uint8Array): Promise<void> {
    await (this as any).room.storage.put('automerge-doc', bytes);
  }
}
