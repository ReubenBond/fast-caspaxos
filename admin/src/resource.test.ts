import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Resource } from './resource';
import { advance, deferred } from './test-fixtures';

describe('Resource revision and lifetime', () => {
  const resources: Resource<unknown>[] = [];
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => { resources.splice(0).forEach((resource) => resource.dispose()); });

  function create(load: (signal: AbortSignal) => Promise<string>) {
    const changed = vi.fn();
    const resource = new Resource(load, changed);
    resources.push(resource);
    return { resource, changed };
  }

  it('coalesces a burst at a fixed deadline with only one in-flight and one trailing refresh', async () => {
    const first = deferred<string>();
    const second = deferred<string>();
    const load = vi.fn().mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { resource } = create(load);
    resource.invalidate();
    await advance(299);
    for (let index = 0; index < 100; index++) resource.invalidate();
    expect(load).not.toHaveBeenCalled();
    await advance(1);
    expect(load).toHaveBeenCalledTimes(1);
    for (let index = 0; index < 100; index++) resource.invalidate();
    await advance(1000);
    expect(load).toHaveBeenCalledTimes(1);
    first.resolve('intermediate');
    await advance(0);
    expect(resource.state).toEqual({ value: 'intermediate', loading: false, stale: true });
    await advance(299);
    expect(load).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(load).toHaveBeenCalledTimes(2);
    second.resolve('current');
    await advance(0);
    expect(resource.state).toEqual({ value: 'current', loading: false, stale: false });
    await advance(1000);
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('retains the previous observation as stale on failure and clears the error on intentional refresh', async () => {
    const load = vi.fn()
      .mockResolvedValueOnce('last good')
      .mockRejectedValueOnce(new Error('host unavailable'))
      .mockResolvedValueOnce('recovered');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    expect(resource.state).toEqual({ value: 'last good', loading: false, stale: true });
    await advance(300);
    expect(resource.state).toEqual({ value: 'last good', loading: false, stale: true, error: 'host unavailable' });
    resource.invalidate();
    await advance(300);
    expect(resource.state).toEqual({ value: 'recovered', loading: false, stale: false });
  });

  it('surfaces same-generation read failure after soft invalidation and clears it on bounded retry progress', async () => {
    const failing = deferred<string>();
    const recovery = deferred<string>();
    const load = vi.fn().mockResolvedValueOnce('last good')
      .mockReturnValueOnce(failing.promise).mockReturnValueOnce(recovery.promise);
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    await advance(300);
    for (let invalidation = 0; invalidation < 100; invalidation++) resource.invalidate();
    await advance(1000);
    expect(load).toHaveBeenCalledTimes(2);
    failing.reject(new Error('read unavailable'));
    await advance(0);
    expect(resource.state).toEqual({
      value: 'last good', loading: false, stale: true, error: 'read unavailable',
    });
    await advance(299);
    expect(load).toHaveBeenCalledTimes(2);
    expect(resource.state.error).toBe('read unavailable');
    await advance(1);
    expect(load).toHaveBeenCalledTimes(3);
    expect(resource.state).toMatchObject({ value: 'last good', loading: true, stale: true });
    expect(resource.state.error).toBeUndefined();
    recovery.resolve('recovered');
    await advance(0);
    expect(resource.state).toEqual({ value: 'recovered', loading: false, stale: false });
    await advance(1000);
    expect(load).toHaveBeenCalledTimes(3);
  });

  it('publishes stale progress through consecutive invalidation cycles and becomes fresh after quiescence', async () => {
    const reads = Array.from({ length: 4 }, () => deferred<string>());
    const signals: AbortSignal[] = [];
    const load = vi.fn<(signal: AbortSignal) => Promise<string>>((signal) => {
      signals.push(signal);
      return reads[signals.length - 1]!.promise;
    });
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    for (let cycle = 0; cycle < 3; cycle++) {
      for (let invalidation = 0; invalidation < 10; invalidation++) resource.invalidate();
      expect(load).toHaveBeenCalledTimes(cycle + 1);
      expect(signals[cycle]!.aborted).toBe(false);
      reads[cycle]!.resolve(`observation-${cycle + 1}`);
      await advance(0);
      expect(resource.state).toEqual({ value: `observation-${cycle + 1}`, loading: false, stale: true });
      await advance(299);
      expect(load).toHaveBeenCalledTimes(cycle + 1);
      await advance(1);
      expect(load).toHaveBeenCalledTimes(cycle + 2);
    }
    reads[3]!.resolve('quiescent observation');
    await advance(0);
    expect(resource.state).toEqual({ value: 'quiescent observation', loading: false, stale: false });
    await advance(1000);
    expect(load).toHaveBeenCalledTimes(4);
  });

  it.each(['success', 'failure'] as const)('reset fences an old-generation %s and schedules a new read', async (outcome) => {
    const late = deferred<string>();
    let signal!: AbortSignal;
    const load = vi.fn<(signal: AbortSignal) => Promise<string>>()
      .mockResolvedValueOnce('baseline')
      .mockImplementationOnce((value) => { signal = value; return late.promise; })
      .mockResolvedValueOnce('new generation');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    await advance(300);
    resource.reset();
    expect(signal.aborted).toBe(true);
    expect(resource.state).toMatchObject({ value: 'baseline', loading: false, stale: true });
    if (outcome === 'success') late.resolve('old generation');
    else late.reject(new Error('old-generation failure'));
    await advance(0);
    expect(resource.state).toMatchObject({ value: 'baseline', loading: false, stale: true });
    expect(resource.state.error).toBeUndefined();
    await advance(300);
    expect(resource.state).toEqual({ value: 'new generation', loading: false, stale: false });
    expect(load).toHaveBeenCalledTimes(3);
  });

  it('markStale retains the last value and fences an in-flight completion without fetching', async () => {
    const pending = deferred<string>();
    const load = vi.fn().mockResolvedValueOnce('baseline').mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce('reconnected');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.markStale();
    expect(resource.state).toEqual({ value: 'baseline', loading: false, stale: true });
    await advance(1000);
    expect(load).toHaveBeenCalledTimes(1);
    resource.invalidate();
    await advance(300);
    resource.markStale();
    pending.resolve('missed-event-window');
    await advance(1000);
    expect(resource.state).toMatchObject({ value: 'baseline', loading: false, stale: true });
    expect(load).toHaveBeenCalledTimes(2);
    resource.invalidate();
    await advance(300);
    expect(resource.state).toEqual({ value: 'reconnected', loading: false, stale: false });
  });

  it('markStale cancels queued refresh work so disconnected observations cannot become fresh', async () => {
    const load = vi.fn().mockResolvedValueOnce('baseline').mockResolvedValueOnce('reconnected');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    await advance(299);
    resource.markStale();
    await advance(1000);
    expect(resource.state).toEqual({ value: 'baseline', loading: false, stale: true });
    expect(load).toHaveBeenCalledTimes(1);
    resource.invalidate();
    await advance(300);
    expect(resource.state).toEqual({ value: 'reconnected', loading: false, stale: false });
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('markStale cancels a trailing invalidation while a read is in flight', async () => {
    const pending = deferred<string>();
    const load = vi.fn().mockReturnValueOnce(pending.promise).mockResolvedValueOnce('reconnected');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    resource.markStale();
    pending.resolve('obsolete');
    await advance(1000);
    expect(resource.state.value).toBeUndefined();
    expect(resource.state.stale).toBe(true);
    expect(load).toHaveBeenCalledTimes(1);
    resource.invalidate();
    await advance(300);
    expect(resource.state).toEqual({ value: 'reconnected', loading: false, stale: false });
  });

  it('aborts exactly at the read deadline and permits an explicit retry', async () => {
    let signal!: AbortSignal;
    const load = vi.fn<(signal: AbortSignal) => Promise<string>>()
      .mockImplementationOnce((value) => {
        signal = value;
        return new Promise((_resolve, reject) => value.addEventListener('abort', () => reject(value.reason)));
      })
      .mockResolvedValueOnce('recovered');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    await advance(14_999);
    expect(signal.aborted).toBe(false);
    await advance(1);
    expect(signal.aborted).toBe(true);
    expect(resource.state).toMatchObject({ loading: false, stale: true, error: 'Read timed out. Refresh to try again.' });
    await advance(30_000);
    expect(load).toHaveBeenCalledTimes(1);
    resource.invalidate();
    await advance(300);
    expect(resource.state.value).toBe('recovered');
  });

  it('does not publish a successful response delivered after its read deadline', async () => {
    const late = deferred<string>();
    let signal!: AbortSignal;
    const load = vi.fn<(signal: AbortSignal) => Promise<string>>()
      .mockResolvedValueOnce('baseline')
      .mockImplementationOnce((value) => { signal = value; return late.promise; })
      .mockResolvedValueOnce('recovered');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    await advance(15_000);
    expect(signal.aborted).toBe(true);
    late.resolve('too late');
    await advance(0);
    expect(resource.state).toMatchObject({
      value: 'baseline', loading: false, stale: true, error: 'Read timed out. Refresh to try again.',
    });
    await advance(300);
    expect(resource.state).toEqual({ value: 'recovered', loading: false, stale: false });
  });

  it('cancels scheduled work and fences late completion after disposal', async () => {
    const scheduledLoad = vi.fn().mockResolvedValue('unused');
    const scheduled = create(scheduledLoad);
    scheduled.resource.invalidate();
    scheduled.resource.dispose();
    await advance(300);
    expect(scheduledLoad).not.toHaveBeenCalled();

    const late = deferred<string>();
    let signal!: AbortSignal;
    const { resource, changed } = create((value) => { signal = value; return late.promise; });
    resource.invalidate();
    await advance(300);
    resource.dispose();
    expect(signal.aborted).toBe(true);
    changed.mockClear();
    late.resolve('too late');
    await advance(30_000);
    resource.invalidate();
    resource.markStale();
    resource.reset();
    expect(resource.state.value).toBeUndefined();
    expect(changed).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});
