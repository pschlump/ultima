// Pluggable key/value persistence (browser localStorage by default, an
// in-memory fallback elsewhere) for the WS session id and the auth tokens.
// Keeping this behind an interface is what makes the client
// framework-agnostic (§11.2: bun/Node/browser).

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

/** Non-persistent StorageLike for runtimes without localStorage. */
export class MemoryStorage implements StorageLike {
  private readonly m = new Map<string, string>();
  getItem(key: string): string | null {
    return this.m.get(key) ?? null;
  }
  setItem(key: string, value: string): void {
    this.m.set(key, value);
  }
  removeItem(key: string): void {
    this.m.delete(key);
  }
}

/** localStorage when the runtime has it (and permits access), else memory. */
export function defaultStorage(): StorageLike {
  try {
    const ls = (globalThis as { localStorage?: StorageLike }).localStorage;
    if (ls) {
      // Probe: access can succeed while writes throw (private mode quotas).
      const probe = "__ultima_probe__";
      ls.setItem(probe, "1");
      ls.removeItem(probe);
      return ls;
    }
  } catch {
    // fall through to memory storage
  }
  return new MemoryStorage();
}
