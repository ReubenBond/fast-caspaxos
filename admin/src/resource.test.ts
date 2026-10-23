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
    first.resolve('obsolete');
    await advance(0);
    expect(resource.state.value).toBeUndefined();
    expect(resource.state.stale).toBe(true);
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

  it('fences a stale rejection as well as a stale success', async () => {
    const first = deferred<string>();
    const load = vi.fn().mockReturnValueOnce(first.promise).mockResolvedValueOnce('new');
    const { resource } = create(load);
    resource.invalidate();
    await advance(300);
    resource.invalidate();
    first.reject(new Error('obsolete failure'));
    await advance(0);
    expect(resource.state.error).toBeUndefined();
    await advance(300);
    expect(resource.state).toEqual({ value: 'new', loading: false, stale: false });
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
    expect(resource.state.value).toBeUndefined();
    expect(changed).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});
