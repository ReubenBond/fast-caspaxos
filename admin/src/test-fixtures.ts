import { vi } from 'vitest';
import { Api } from './api';
import type { EventKind, KeyPreview, NodeEvent, NodeStatus, Peer, Snapshot } from './contracts';
import { ClusterStore } from './store';
import type { Stream } from './store';

export function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((accept, decline) => { resolve = accept; reject = decline; });
  return { promise, resolve, reject };
}

export const peers: Peer[] = [
  { id: 'a', httpEndpoint: 'http://a.test:5001', grpcEndpoint: 'http://a.test:6001', eventsUrl: 'http://a.test:5001/v1/admin/events' },
  { id: 'b', httpEndpoint: 'http://b.test:5002', grpcEndpoint: 'http://b.test:6002', eventsUrl: 'http://b.test:5002/v1/admin/events' },
];

export function status(nodeId = 'a', overrides: Partial<NodeStatus> = {}): NodeStatus {
  return {
    schemaVersion: 1, nodeId, proposerId: nodeId === 'a' ? 1 : 2,
    bootId: `${nodeId}-boot`, startedAt: '2026-10-23T20:00:00.000Z',
    cursor: `${nodeId}-boot:0`, peers, storage: { status: 'ready' },
    lastOperation: null, heartbeatMilliseconds: 1000, historyCapacity: 1000,
    ...overrides,
  };
}

export function snapshot(nodeId = 'a', overrides: Partial<Snapshot> = {}): Snapshot {
  return {
    schemaVersion: 1, nodeId, bootId: `${nodeId}-boot`, cursor: `${nodeId}-boot:0`,
    timestamp: '2026-10-23T20:00:00.000Z', entry: null, ...overrides,
  };
}

export function key(text = 'chosen', overrides: Partial<KeyPreview> = {}): KeyPreview {
  return { text, length: new TextEncoder().encode(text).length, truncated: false, lossy: false, ...overrides };
}

export function event<K extends EventKind>(
  kind: K,
  sequence: string,
  data: Extract<NodeEvent, { kind: K }>['data'],
  nodeId = 'a',
  bootId = `${nodeId}-boot`,
): Extract<NodeEvent, { kind: K }> {
  return {
    schemaVersion: 1, nodeId, bootId, sequence, timestamp: '2026-10-23T20:00:00.000Z',
    kind, data, id: `${bootId}:${sequence}`,
  } as Extract<NodeEvent, { kind: K }>;
}

// The browser owns reconnects. This controllable source never reconnects itself,
// so a test detects if the store replaces it instead of waiting for native open.
export class TestStream implements Stream {
  private readonly listeners = new Map<string, ((event: MessageEvent<string>) => void)[]>();
  readonly close = vi.fn();

  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  emit(type: string, data = ''): void {
    for (const listener of this.listeners.get(type) ?? []) listener(new MessageEvent(type, { data }));
  }

  send(item: NodeEvent): void { this.emit(item.kind, JSON.stringify(item)); }
}

export function harness() {
  const api = new Api(vi.fn<typeof fetch>());
  const statusMock = vi.spyOn(api, 'status').mockImplementation(async (origin) =>
    status(peers.find((peer) => peer.httpEndpoint === origin)?.id ?? 'a'));
  const keys = vi.spyOn(api, 'keys').mockResolvedValue({ keys: ['chosen'], continuation: null });
  const read = vi.spyOn(api, 'read').mockResolvedValue({ present: true, bytes: new Uint8Array([65]), etag: '"v1"' });
  const state = vi.spyOn(api, 'state').mockImplementation(async (origin) =>
    snapshot(peers.find((peer) => peer.httpEndpoint === origin)?.id ?? 'a'));
  const write = vi.spyOn(api, 'write').mockResolvedValue({ kind: 'applied', etag: '"v2"' });
  const sources: TestStream[] = [];
  const streams = vi.fn(() => {
    const source = new TestStream();
    sources.push(source);
    return source;
  });
  const store = new ClusterStore(peers[0]!.httpEndpoint, api, streams, () => Date.now());
  return { store, api, status: statusMock, keys, read, state, write, streams, sources };
}

export type Harness = ReturnType<typeof harness>;

// Drains actual promise continuations as well as the controlled timer queue.
export async function advance(milliseconds: number): Promise<void> {
  await vi.advanceTimersByTimeAsync(milliseconds);
}
