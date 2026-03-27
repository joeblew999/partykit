/**
 * Example: Browser client that syncs Automerge docs via PartyKit.
 */

import { AutomergeProvider } from '../src/provider/index';

export function setup(room: string, host = 'localhost:1999') {
  const provider = new AutomergeProvider({
    host,
    room,
    onStatus: (status) => console.log('[client] Status:', status),
    onEphemeral: (data) => console.log('[client] Ephemeral:', new TextDecoder().decode(data)),
  });

  // Create a new document
  const handle = provider.create<{ items: Array<{ text: string }> }>();

  // Make changes
  handle.change((doc) => {
    doc.items = [];
    doc.items.push({ text: 'Hello from client' });
  });

  return { provider, handle };
}
