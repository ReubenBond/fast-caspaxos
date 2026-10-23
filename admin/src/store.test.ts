import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { KeyPreview, Register } from './contracts';
import { retention } from './store';
import { advance, deferred, event, harness, key, peers, snapshot, status } from './test-fixtures';
import type { Harness } from './test-fixtures';

function operation(sequence: string, preview: KeyPreview | null = key(), nodeId = 'a', bootId = `${nodeId}-boot`) {
  return event('operation', sequence, {
    operation: 'compare-exchange', operationId: `operation-${sequence}`, key: preview, outcome: 'applied',
  }, nodeId, bootId);
}

function accepted(sequence: string, preview: KeyPreview, version = '"v2"') {
  return event('acceptor-state', sequence, {
    key: preview, promisedBallot: { round: 2, proposer: 1 }, acceptedBallot: { round: 2, proposer: 1 },
    acceptedValue: null, register: { format: 'register', isPresent: true, version, value: null },
  });
}

describe('ClusterStore subscriptions, ordering and refreshes', () => {
  let h: Harness;
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-23T20:00:00.000Z'));
    h = harness();
  });
  afterEach(() => h.store.dispose());

  async function start(selected: string | null = null) {
    await h.store.start();
    h.sources.forEach((source) => source.emit('open'));
    h.store.select(selected);
    await advance(400);
  }

  it('keeps one independent direct SSE subscription per configured host across request-host changes', async () => {
    await start('chosen');
    expect(h.streams.mock.calls).toEqual(peers.map((peer) => [peer.eventsUrl]));
    expect(h.state.mock.calls.map(([origin, selected]) => [origin, selected])).toEqual(
      peers.map((peer) => [peer.httpEndpoint, 'chosen']));
    h.sources[0]!.emit('error');
    h.sources[1]!.send(event('storage', '1', { status: 'ready' }, 'b'));
    expect(h.store.nodes.map((node) => node.connection)).toEqual(['reconnecting', 'connected']);
    h.store.setOrigin(peers[1]!.httpEndpoint);
    await advance(400);
    expect(h.read.mock.calls.at(-1)!.slice(0, 2)).toEqual([peers[1]!.httpEndpoint, 'chosen']);
    expect(h.keys.mock.calls.at(-1)!.slice(0, 3)).toEqual([peers[1]!.httpEndpoint, '', null]);
    expect(h.streams).toHaveBeenCalledTimes(2);
    h.sources.forEach((source) => expect(source.close).not.toHaveBeenCalled());
  });

  it('leaves reconnection to the same native EventSource and restores only the reopened host', async () => {
    await h.store.start();
    h.sources[0]!.emit('error');
    h.sources[1]!.emit('open');
    expect(h.store.nodes.map((node) => node.connection)).toEqual(['unreachable', 'connected']);
    h.sources[0]!.emit('open');
    h.sources[0]!.emit('error');
    expect(h.store.nodes[0]!.connection).toBe('reconnecting');
    await advance(4000);
    expect(h.store.nodes[0]!.connection).toBe('unreachable');
    expect(h.store.nodes[1]!.connection).toBe('stale');
    h.sources[0]!.emit('open');
    await advance(400);
    expect(h.store.nodes[0]!.connection).toBe('connected');
    expect(h.store.nodes[0]!.error).toBeUndefined();
    expect(h.store.nodes[1]!.connection).toBe('stale');
    expect(h.streams).toHaveBeenCalledTimes(2);
    h.sources.forEach((source) => expect(source.close).not.toHaveBeenCalled());
  });

  it('keeps quiet heartbeats healthy without retaining them and separates storage from connection health', async () => {
    await start();
    h.sources[0]!.send(event('storage', '1', { status: 'faulted', error: 'disk unavailable' }));
    h.sources[1]!.send(event('storage', '1', { status: 'ready' }, 'b'));
    await advance(100);
    const received = h.store.received;
    for (let tick = 0; tick < 6; tick++) {
      await advance(1000);
      h.sources[0]!.send(event('heartbeat', '1', { storageStatus: 'faulted' }));
    }
    expect(h.store.nodes[0]!.connection).toBe('connected');
    expect(h.store.nodes[0]!.storage?.status).toBe('faulted');
    expect(h.store.nodes[0]!.storage?.error).toBe('disk unavailable');
    expect(h.store.nodes[1]!.connection).toBe('stale');
    expect(h.store.nodes[1]!.storage?.status).toBe('ready');
    expect(h.store.received).toBe(received);
    expect(h.store.events.map((item) => item.kind)).toEqual(['storage', 'storage']);
    h.sources[1]!.emit('error');
    await advance(1000);
    expect(h.store.nodes[1]!.connection).toBe('unreachable');
    expect(h.store.nodes[1]!.storage?.status).toBe('ready');
  });

  it('uses a strict three-heartbeat stale boundary under the controlled clock', async () => {
    await start();
    await advance(2600);
    expect(h.store.nodes[0]!.connection).toBe('connected');
    await advance(1000);
    expect(h.store.nodes[0]!.connection).toBe('stale');
  });

  it('fences in-flight reads on disconnect and resynchronizes observations on native reopen', async () => {
    await start('chosen');
    const pending = deferred<Register>();
    h.read.mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce({ present: true, bytes: new Uint8Array([66]), etag: '"reconnected"' });
    h.store.current!.invalidate();
    await advance(300);
    h.sources[0]!.emit('error');
    expect(h.store.current!.state.stale).toBe(true);
    expect(h.store.inventory!.state.stale).toBe(true);
    expect(h.store.nodes[0]!.local!.state.stale).toBe(true);
    expect(h.store.nodes[1]!.local!.state.stale).toBe(false);
    pending.resolve({ present: true, bytes: new Uint8Array([99]), etag: '"disconnected"' });
    await advance(1000);
    expect(h.store.current!.state).toMatchObject({ value: initialRegister(), loading: false, stale: true });
    expect(h.read).toHaveBeenCalledTimes(2);
    expect(h.keys).toHaveBeenCalledTimes(1);
    h.sources[0]!.emit('open');
    await advance(400);
    expect(h.store.current!.state).toEqual({
      value: { present: true, bytes: new Uint8Array([66]), etag: '"reconnected"' }, loading: false, stale: false,
    });
    expect(h.store.inventory!.state.stale).toBe(false);
    expect(h.store.nodes[0]!.local!.state.stale).toBe(false);
    expect(h.read).toHaveBeenCalledTimes(3);
    expect(h.streams).toHaveBeenCalledTimes(2);
  });

  it('marks observations stale without polling reads when the heartbeat watchdog expires', async () => {
    await start('chosen');
    await advance(3600);
    expect(h.store.nodes.map((node) => node.connection)).toEqual(['stale', 'stale']);
    expect(h.store.current!.state).toMatchObject({ value: initialRegister(), stale: true });
    expect(h.store.inventory!.state.stale).toBe(true);
    expect(h.store.nodes.map((node) => node.local!.state.stale)).toEqual([true, true]);
    await advance(10_000);
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.state).toHaveBeenCalledTimes(2);
    expect(h.store.nodes.map((node) => node.storage?.status)).toEqual(['ready', 'ready']);
  });

  it('orders adjacent sequences beyond Number precision and rejects duplicate or older observations', async () => {
    await start();
    h.sources[0]!.send(event('reset', '9007199254740992', { reason: 'initial baseline' }));
    h.sources[0]!.send(event('storage', '9007199254740993', { status: 'faulted' }));
    h.sources[0]!.send(event('storage', '9007199254740992', { status: 'ready' }));
    h.sources[0]!.send(event('storage', '9007199254740993', { status: 'ready' }));
    h.sources[1]!.send(event('storage', '9007199254740993', { status: 'ready' }, 'b'));
    await advance(100);
    expect(h.store.nodes[0]!.sequence).toBe(9007199254740993n);
    expect(h.store.nodes[0]!.storage?.status).toBe('faulted');
    expect(h.store.events.map((item) => `${item.nodeId}:${item.kind}:${item.sequence}`)).toEqual([
      'a:reset:9007199254740992', 'a:storage:9007199254740993', 'b:storage:9007199254740993',
    ]);
    expect(h.store.received).toBe(3);
    expect(h.store.nodes[0]!.reset).toBe('initial baseline');
  });

  it('accepts a new boot only on reset, clears old observations and rejects old-boot traffic', async () => {
    await start('chosen');
    h.sources[0]!.send(accepted('1', key()));
    await advance(400);
    expect(h.store.nodes[0]!.versions.get('chosen')).toBe('"v2"');
    h.sources[0]!.send(operation('1', key(), 'a', 'new-boot'));
    h.sources[0]!.send(event('heartbeat', '1', { storageStatus: 'faulted' }, 'a', 'new-boot'));
    expect(h.store.nodes[0]!.boot).toBe('a-boot');
    expect(h.store.received).toBe(1);
    h.sources[0]!.send(event('reset', '0', { reason: 'node restarted' }, 'a', 'new-boot'));
    expect(h.store.nodes[0]!.boot).toBe('new-boot');
    expect(h.store.nodes[0]!.sequence).toBe(0n);
    expect(h.store.nodes[0]!.versions.size).toBe(0);
    expect(h.store.nodes[0]!.storage).toBeUndefined();
    expect(h.store.nodes[0]!.status.state.stale).toBe(true);
    expect(h.store.nodes[0]!.local!.state.stale).toBe(true);
    expect(h.store.inventory!.state.stale).toBe(true);
    expect(h.store.current!.state.stale).toBe(true);
    h.sources[0]!.send(event('storage', '1000', { status: 'faulted' }));
    h.sources[0]!.send(event('storage', '1', { status: 'ready' }, 'a', 'new-boot'));
    await advance(100);
    expect(h.store.nodes[0]!.storage?.status).toBe('ready');
    expect(h.store.events.map((item) => item.id)).toEqual(['a-boot:1', 'new-boot:0', 'new-boot:1']);
  });

  it('invalidates snapshots on a sequence gap even for an unrelated exact key', async () => {
    await start('chosen');
    h.sources[0]!.send(event('reset', '5', { reason: 'baseline' }));
    await advance(400);
    h.keys.mockClear();
    h.read.mockClear();
    h.state.mockClear();
    h.status.mockClear();
    h.sources[0]!.send(operation('7', key('unrelated')));
    expect(h.store.nodes[0]!.reset).toBe('Sequence gap: resynchronizing snapshots');
    expect(h.store.current!.state.stale).toBe(true);
    await advance(400);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.state.mock.calls.map(([origin]) => origin)).toEqual([peers[0]!.httpEndpoint]);
    expect(h.status.mock.calls.map(([origin]) => origin)).toEqual([peers[0]!.httpEndpoint]);
  });

  it.each([
    { name: 'truncated only', preview: key('not-a-match', { truncated: true }) },
    { name: 'lossy only', preview: key('not-a-match', { lossy: true }) },
    { name: 'truncated and lossy', preview: key('not-a-match', { truncated: true, lossy: true }) },
    { name: 'missing operation key', preview: null },
  ])('broadly invalidates logical reads for $name previews', async ({ preview }) => {
    await start('chosen');
    h.store.setQuery('chosen', null);
    await advance(400);
    h.keys.mockClear();
    h.read.mockClear();
    h.sources[0]!.send(operation('1', preview));
    expect(h.store.inventory!.state.stale).toBe(true);
    expect(h.store.current!.state.stale).toBe(true);
    await advance(400);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.read).toHaveBeenCalledTimes(1);
  });

  it('invalidates exact keys only when they match the prefix or selected register', async () => {
    await start('chosen');
    h.store.setQuery('chosen', null);
    await advance(400);
    h.keys.mockClear();
    h.read.mockClear();
    h.sources[0]!.send(operation('1', key('other')));
    await advance(400);
    expect(h.keys).not.toHaveBeenCalled();
    expect(h.read).not.toHaveBeenCalled();
    h.sources[0]!.send(operation('2', key('chosen-child')));
    await advance(400);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.read).not.toHaveBeenCalled();
    h.sources[0]!.send(operation('3', key('chosen')));
    await advance(400);
    expect(h.keys).toHaveBeenCalledTimes(2);
    expect(h.read).toHaveBeenCalledTimes(1);
  });

  it.each([
    { name: 'truncated only', preview: key('other', { truncated: true }) },
    { name: 'lossy only', preview: key('other', { lossy: true }) },
  ])('invalidates local and logical state for $name acceptor keys even at an unchanged version', async ({ preview }) => {
    await start('chosen');
    h.sources[0]!.send(accepted('1', preview));
    await advance(400);
    h.state.mockClear();
    h.read.mockClear();
    h.sources[0]!.send(accepted('2', preview));
    expect(h.store.nodes[0]!.local!.state.stale).toBe(true);
    expect(h.store.current!.state.stale).toBe(true);
    await advance(400);
    expect(h.state).toHaveBeenCalledTimes(1);
    expect(h.read).toHaveBeenCalledTimes(1);
  });

  it('coalesces event storms and suppresses same-version logical read feedback', async () => {
    await start('chosen');
    h.read.mockClear();
    h.keys.mockClear();
    h.state.mockClear();
    for (let index = 1; index <= 100; index++) h.sources[0]!.send(accepted(String(index), key()));
    await advance(400);
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.state).toHaveBeenCalledTimes(1);
    h.sources[0]!.send(accepted('101', key()));
    await advance(400);
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.state).toHaveBeenCalledTimes(2);
    h.sources[0]!.send(accepted('102', key(), '"v3"'));
    await advance(400);
    expect(h.read).toHaveBeenCalledTimes(2);
  });

  it('preserves empty selected keys and empty pagination cursors', async () => {
    await start('');
    h.store.setQuery('', '');
    await advance(400);
    expect(h.store.selected).toBe('');
    expect(h.store.after).toBe('');
    expect(h.read.mock.calls[0]!.slice(0, 2)).toEqual([peers[0]!.httpEndpoint, '']);
    expect(h.state.mock.calls.map(([, selected]) => selected)).toEqual(['', '']);
    expect(h.keys.mock.calls.at(-1)!.slice(0, 3)).toEqual([peers[0]!.httpEndpoint, '', '']);
    h.store.select(null);
    expect(h.store.current).toBeUndefined();
    expect(h.store.nodes.map((node) => node.local)).toEqual([undefined, undefined]);
  });

  it.each([
    { name: 'stream reset', item: event('reset', '0', { reason: 'history gap' }) },
    { name: 'truncated preview', item: operation('1', key('other', { truncated: true })) },
    { name: 'lossy preview', item: operation('1', key('other', { lossy: true })) },
    { name: 'matching exact key', item: operation('1', key('chosen-new')) },
  ])('restarts paginated inventory at a null cursor after $name', async ({ item }) => {
    await start('chosen');
    h.store.setQuery('chosen', '');
    await advance(400);
    h.keys.mockClear();
    h.sources[0]!.send(item);
    expect(h.store.after).toBeNull();
    expect(h.store.prefix).toBe('chosen');
    await advance(400);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.keys.mock.calls[0]!.slice(0, 3)).toEqual([peers[0]!.httpEndpoint, 'chosen', null]);
  });

  it('aborts replaced inventory requests and fences out-of-order query and host completions', async () => {
    await start('chosen');
    const oldPage = deferred<{ keys: string[]; continuation: string | null }>();
    const oldRead = deferred<Register>();
    h.keys.mockReturnValueOnce(oldPage.promise).mockResolvedValueOnce({ keys: ['new-host'], continuation: null });
    h.read.mockReturnValueOnce(oldRead.promise).mockResolvedValueOnce({ present: false });
    h.store.setQuery('old-prefix', 'old-cursor');
    h.store.current!.invalidate();
    await advance(300);
    const pageSignal = h.keys.mock.calls.at(-1)![3];
    const readSignal = h.read.mock.calls.at(-1)![2];
    h.store.setQuery('new-prefix', '');
    h.store.setOrigin(peers[1]!.httpEndpoint);
    expect(pageSignal.aborted).toBe(true);
    expect(readSignal.aborted).toBe(true);
    await advance(400);
    oldPage.resolve({ keys: ['obsolete'], continuation: 'obsolete' });
    oldRead.resolve(initialRegister());
    await advance(100);
    expect(h.store.inventory!.state).toEqual({ value: { keys: ['new-host'], continuation: null }, loading: false, stale: false });
    expect(h.store.current!.state).toEqual({ value: { present: false }, loading: false, stale: false });
    expect(h.keys.mock.calls.at(-1)!.slice(0, 3)).toEqual([peers[1]!.httpEndpoint, 'new-prefix', '']);
    expect(h.store.after).toBe('');
  });

  it('aborts old key reads and ignores their out-of-order completion after selection changes', async () => {
    await start();
    const old = deferred<Register>();
    let oldSignal!: AbortSignal;
    h.read.mockImplementationOnce((_origin, _key, signal) => { oldSignal = signal; return old.promise; })
      .mockResolvedValueOnce({ present: true, bytes: new Uint8Array([66]), etag: '"new"' });
    h.store.select('old');
    await advance(300);
    h.store.select('new');
    expect(oldSignal.aborted).toBe(true);
    await advance(400);
    expect(h.store.current!.state.value).toEqual({ present: true, bytes: new Uint8Array([66]), etag: '"new"' });
    old.resolve({ present: true, bytes: new Uint8Array([65]), etag: '"old"' });
    await advance(100);
    expect(h.store.selected).toBe('new');
    expect(h.store.current!.state.value).toEqual({ present: true, bytes: new Uint8Array([66]), etag: '"new"' });
  });

  it('rejects local snapshots from another boot and fences snapshots invalidated by reset', async () => {
    await start();
    const old = deferred<ReturnType<typeof snapshot>>();
    h.state.mockImplementationOnce(() => old.promise);
    h.store.select('chosen');
    await advance(300);
    h.sources[0]!.send(event('reset', '0', { reason: 'restart' }, 'a', 'new-boot'));
    h.status.mockImplementation(async (origin) => origin === peers[0]!.httpEndpoint
      ? status('a', { bootId: 'new-boot', cursor: 'new-boot:0' }) : status('b'));
    old.resolve(snapshot());
    await advance(0);
    expect(h.store.nodes[0]!.local!.state.value).toBeUndefined();
    await advance(400);
    expect(h.store.nodes[0]!.local!.state.error).toContain('another node generation');
    h.state.mockImplementation(async (origin) => origin === peers[0]!.httpEndpoint
      ? snapshot('a', { bootId: 'new-boot', cursor: 'new-boot:0' }) : snapshot('b'));
    h.store.refresh();
    await advance(400);
    expect(h.store.nodes[0]!.local!.state.value?.bootId).toBe('new-boot');
    expect(h.store.nodes[0]!.local!.state.stale).toBe(false);
  });

  it('does not overwrite newer stream storage with an older status completion', async () => {
    await start();
    const delayed = deferred<ReturnType<typeof status>>();
    h.status.mockImplementationOnce(() => delayed.promise);
    h.store.nodes[0]!.status.invalidate();
    await advance(300);
    h.sources[0]!.send(event('storage', '5', { status: 'faulted', error: 'disk failed' }));
    delayed.resolve(status('a', { cursor: 'a-boot:4', storage: { status: 'ready' } }));
    await advance(100);
    expect(h.store.nodes[0]!.storage).toEqual({ status: 'faulted', error: 'disk failed' });
  });

  it('refreshes every observation intentionally without replacing the streams', async () => {
    await start('chosen');
    h.keys.mockClear();
    h.read.mockClear();
    h.status.mockClear();
    h.state.mockClear();
    h.store.refresh();
    expect(h.store.current!.state.stale).toBe(true);
    await advance(400);
    expect(h.keys).toHaveBeenCalledTimes(1);
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.status).toHaveBeenCalledTimes(2);
    expect(h.state).toHaveBeenCalledTimes(2);
    expect(h.streams).toHaveBeenCalledTimes(2);
  });

  it('bounds pending and retained events and accounts exactly for both eviction phases', async () => {
    await start();
    const listener = vi.fn();
    h.store.subscribe(listener);
    for (let index = 1; index <= retention + 5; index++) {
      h.sources[0]!.send(event('storage', String(index), { status: 'ready' }));
    }
    expect(h.store.received).toBe(1005);
    expect(h.store.discarded).toBe(5);
    expect(h.store.events).toHaveLength(0);
    expect(listener).not.toHaveBeenCalled();
    await advance(100);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(h.store.events).toHaveLength(1000);
    expect(h.store.events[0]!.sequence).toBe('6');
    expect(h.store.events.at(-1)!.sequence).toBe('1005');
    for (let index = 1006; index <= 1015; index++) h.sources[0]!.send(event('storage', String(index), { status: 'ready' }));
    await advance(100);
    expect(h.store.events).toHaveLength(1000);
    expect(h.store.events[0]!.sequence).toBe('16');
    expect(h.store.events.at(-1)!.sequence).toBe('1015');
    expect(h.store.discarded).toBe(15);
    expect(h.store.received).toBe(h.store.events.length + h.store.discarded);
  });

  it('surfaces malformed stream events without corrupting retained observations', async () => {
    await start();
    h.sources[0]!.emit('storage', '{invalid');
    expect(h.store.nodes[0]!.connection).toBe('stale');
    expect(h.store.nodes[0]!.error).toBeTruthy();
    expect(h.store.received).toBe(0);
    h.sources[0]!.send(event('storage', '1', { status: 'ready' }));
    await advance(100);
    expect(h.store.nodes[0]!.connection).toBe('connected');
    expect(h.store.nodes[0]!.error).toBeUndefined();
    expect(h.store.events.map((item) => item.id)).toEqual(['a-boot:1']);
  });

  it('aborts bootstrap on disposal and never opens streams for its late response', async () => {
    const pending = deferred<ReturnType<typeof status>>();
    h.status.mockReturnValueOnce(pending.promise);
    const starting = h.store.start();
    const signal = h.status.mock.calls[0]![1];
    h.store.dispose();
    expect(signal.aborted).toBe(true);
    pending.resolve(status());
    await starting;
    await advance(30_000);
    expect(h.streams).not.toHaveBeenCalled();
    expect(h.store.nodes).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('bounds bootstrap with an abort deadline and reports failure without opening streams', async () => {
    let signal!: AbortSignal;
    h.status.mockImplementationOnce((_origin, value) => {
      signal = value;
      return new Promise((_resolve, reject) => value.addEventListener('abort', () => reject(value.reason)));
    });
    const starting = h.store.start();
    await advance(14_999);
    expect(signal.aborted).toBe(false);
    expect(h.store.loading).toBe(true);
    await advance(1);
    await starting;
    expect(signal.aborted).toBe(true);
    expect(h.store.bootstrapError).toBe('Bootstrap timed out. Check the serving host.');
    expect(h.store.loading).toBe(false);
    expect(h.streams).not.toHaveBeenCalled();
  });

  it('closes all streams, aborts in-flight resources and ignores callbacks after disposal', async () => {
    await start();
    const pending = deferred<Register>();
    let signal!: AbortSignal;
    h.read.mockImplementationOnce((_origin, _key, value) => { signal = value; return pending.promise; });
    h.store.select('chosen');
    await advance(400);
    const listener = vi.fn();
    h.store.subscribe(listener);
    h.store.dispose();
    expect(signal.aborted).toBe(true);
    h.sources.forEach((source) => expect(source.close).toHaveBeenCalledTimes(1));
    h.sources[0]!.send(event('storage', '1', { status: 'faulted' }));
    h.sources[0]!.emit('error');
    pending.resolve({ present: false });
    await advance(30_000);
    expect(h.store.received).toBe(0);
    expect(h.store.nodes[0]!.connection).toBe('connected');
    expect(h.store.current!.state.value).toBeUndefined();
    expect(listener).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});

function initialRegister(): Register {
  return { present: true, bytes: new Uint8Array([65]), etag: '"v1"' };
}
