/**
 * Example: Browser client that syncs an Automerge doc via PartyKit.
 *
 * Usage:
 *   import { setup } from './client';
 *   const { provider, getDoc } = setup('my-model-id');
 *   provider.change((doc) => { doc.items.push({ text: 'hello' }); });
 */

import { next as Automerge } from '@automerge/automerge';
import { AutomergeProvider } from '../src/provider/index';

export function setup(room: string, host = 'localhost:1999') {
  let doc = Automerge.init<{ items: Array<{ text: string }> }>();

  const provider = new AutomergeProvider({
    host,
    room,
    doc,
    onUpdate: (newDoc) => {
      doc = newDoc as typeof doc;
      console.log('[client] Doc updated:', Automerge.toJS(doc));
    },
    onEphemeral: (data) => {
      console.log('[client] Ephemeral:', new TextDecoder().decode(data));
    },
    onStatus: (status) => {
      console.log('[client] Status:', status);
    },
  });

  return {
    provider,
    getDoc: () => doc,
    addItem: (text: string) => {
      provider.change((d: any) => {
        if (!d.items) d.items = [];
        d.items.push({ text });
      });
    },
  };
}
