import type { KeyPage, NodeStatus, Register, Snapshot, WriteResult } from './contracts';

export type Fetch = typeof fetch;
export function endpoint(origin: string, path: string, query?: Record<string, string | null>): string {
  const url = new URL(path, origin);
  for (const [name, value] of Object.entries(query ?? {})) {
    if (value !== null) url.searchParams.set(name, value);
  }
  return url.href;
}

export function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class Api {
  constructor(private readonly request: Fetch = (input, init) => fetch(input, init)) {}

  private async json<T>(url: string, signal: AbortSignal): Promise<T> {
    const response = await this.request(url, { signal, cache: 'no-store' });
    if (!response.ok) throw new Error(`Request failed (${response.status}): ${await response.text()}`);
    return response.json() as Promise<T>;
  }

  async status(origin: string, signal: AbortSignal): Promise<NodeStatus> {
    const status = await this.json<NodeStatus>(endpoint(origin, '/v1/admin/node'), signal);
    if (status.schemaVersion !== 1 || !(status.heartbeatMilliseconds > 0) || !status.peers.length) {
      throw new Error('Unsupported node configuration. Check host versions and configured peers.');
    }
    return status;
  }

  async state(origin: string, key: string, signal: AbortSignal): Promise<Snapshot> {
    const state = await this.json<Snapshot>(endpoint(origin, '/v1/admin/state', { key }), signal);
    if (state.schemaVersion !== 1) throw new Error('Unsupported local-state schema.');
    return state;
  }

  keys(origin: string, prefix: string, after: string | null, signal: AbortSignal): Promise<KeyPage> {
    return this.json(endpoint(origin, '/v1/registers', { prefix, after, limit: '50' }), signal);
  }

  async read(origin: string, key: string, signal: AbortSignal): Promise<Register> {
    const response = await this.request(endpoint(origin, '/v1/registers/value', { key }), {
      signal, cache: 'no-store',
    });
    if (response.status === 404) return { present: false };
    if (!response.ok) throw new Error(`Register read failed (${response.status}): ${await response.text()}`);
    const etag = response.headers.get('ETag');
    if (!etag) throw new Error('The host did not expose the register ETag. Check CORS configuration.');
    return { present: true, bytes: new Uint8Array(await response.arrayBuffer()), etag };
  }

  async write(origin: string, key: string, bytes: Uint8Array, etag: string | null, signal: AbortSignal): Promise<WriteResult> {
    try {
      const response = await this.request(endpoint(origin, '/v1/registers/value', { key }), {
        method: 'PUT', signal, cache: 'no-store',
        headers: { 'Content-Type': 'application/octet-stream', [etag === null ? 'If-None-Match' : 'If-Match']: etag ?? '*' },
        body: new Uint8Array(bytes),
      });
      if (response.status === 201 || response.status === 204) {
        const next = response.headers.get('ETag');
        if (!next) return { kind: 'unknown', message: 'The host acknowledged the write but its ETag is unavailable. Refresh to reconcile.' };
        return { kind: 'applied', etag: next };
      }
      if (response.status === 412) return { kind: 'conflict', message: 'Another version won the precondition. Refresh and compare before writing again.' };
      const problem = await response.json() as { outcome?: string; code?: string; detail?: string; currentEtag?: string };
      if (problem.outcome === 'unknown' || problem.code === 'unknown') {
        return { kind: 'unknown', message: problem.detail ?? 'The write may have applied. Reconcile before another write.', currentEtag: problem.currentEtag };
      }
      // An unclassified server/proxy failure may have occurred after dispatch.
      if (response.status >= 500 && problem.outcome !== 'not-applied'
        && !['busy', 'unavailable'].includes(problem.code ?? '')) {
        return { kind: 'unknown', message: problem.detail ?? `Write outcome is unknown (${response.status}).` };
      }
      return { kind: 'rejected', message: problem.detail ?? `Write rejected (${response.status}).` };
    } catch (error) {
      return { kind: 'unknown', message: `Response unavailable: ${errorText(error)}. The write may have applied; reconcile before another write.` };
    }
  }
}

export function toBase64(bytes: Uint8Array): string {
  let binary = '';
  for (let start = 0; start < bytes.length; start += 8192) {
    binary += String.fromCharCode(...bytes.subarray(start, start + 8192));
  }
  return btoa(binary);
}

export function fromBase64(text: string): Uint8Array {
  const canonical = text.replace(/\s/g, '');
  const binary = atob(canonical);
  const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
  if (toBase64(bytes) !== canonical) throw new Error('Use canonical padded Base64.');
  return bytes;
}

export function utf8(bytes: Uint8Array): string | null {
  try { return new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes); }
  catch { return null; }
}

export function textBytes(text: string): Uint8Array {
  const bytes = new TextEncoder().encode(text);
  if (utf8(bytes) !== text) throw new Error('Text contains an unpaired Unicode surrogate. Use well-formed text or Base64.');
  return bytes;
}
