import { useSyncExternalStore } from 'react';
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App, WriteEditor } from './App';
import { Timeline } from './Timeline';
import type { LocalState, NodeEvent, Register, WriteResult } from './contracts';
import type { ClusterStore } from './store';
import { advance, deferred, event, harness, key, peers, snapshot } from './test-fixtures';
import type { Harness } from './test-fixtures';

// These children normally receive updates from App's external-store subscription.
function EditorHost({ store, initial, registerKey = 'chosen', onCreated }: {
  store: ClusterStore; initial: Register; registerKey?: string; onCreated?: () => void;
}) {
  useSyncExternalStore(store.subscribe, store.getSnapshot);
  return <WriteEditor store={store} registerKey={registerKey} initial={initial} onCreated={onCreated} />;
}

function TimelineHost({ store }: { store: ClusterStore }) {
  useSyncExternalStore(store.subscribe, store.getSnapshot);
  return <Timeline store={store} />;
}

async function tick(milliseconds: number) {
  await act(async () => { await advance(milliseconds); });
}

const initial: Register = { present: true, bytes: new Uint8Array([65]), etag: '"v1"' };

describe('WriteEditor conditional intent', () => {
  let h: Harness;
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-23T20:00:00.000Z'));
    h = harness();
  });
  afterEach(() => h.store.dispose());

  async function start() {
    await h.store.start();
    h.store.select('chosen');
    await advance(400);
  }

  it.each([
    { name: 'BOM and CRLF text', bytes: [0xef, 0xbb, 0xbf, 65, 13, 10], encoding: 'text', base64: '77u/QQ0K' },
    { name: 'invalid UTF-8 binary', bytes: [0xff, 0, 0xc3, 0x28], encoding: 'base64', base64: '/wDDKA==' },
    { name: 'empty present value', bytes: [], encoding: 'text', base64: '' },
  ])('submits unchanged $name byte-for-byte across encoding switches', async ({ bytes, encoding, base64 }) => {
    await start();
    const refresh = vi.spyOn(h.store, 'refresh');
    render(<EditorHost store={h.store} initial={{ present: true, bytes: new Uint8Array(bytes), etag: '"opaque+/="' }} />);
    expect(screen.getByRole('combobox', { name: 'Encoding' })).toHaveValue(encoding);
    fireEvent.change(screen.getByRole('combobox', { name: 'Encoding' }), { target: { value: 'base64' } });
    expect(screen.getByRole('textbox')).toHaveValue(base64);
    if (encoding === 'text') {
      fireEvent.change(screen.getByRole('combobox', { name: 'Encoding' }), { target: { value: 'text' } });
    }
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(0);
    expect(h.write).toHaveBeenCalledTimes(1);
    const [origin, registerKey, payload, etag, signal] = h.write.mock.calls[0]!;
    expect(origin).toBe(h.store.origin);
    expect(registerKey).toBe('chosen');
    expect([...payload]).toEqual(bytes);
    expect(etag).toBe('"opaque+/="');
    expect(signal.aborted).toBe(false);
    expect(screen.getByRole('status')).toHaveTextContent('Write applied.');
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(screen.getByText('"v2"')).toBeInTheDocument();
  });

  it('keeps invalid UTF-8 in Base64 instead of replacing bytes during a text switch', async () => {
    await start();
    render(<EditorHost store={h.store} initial={{ present: true, bytes: new Uint8Array([0xff]), etag: '"v1"' }} />);
    fireEvent.change(screen.getByRole('combobox', { name: 'Encoding' }), { target: { value: 'text' } });
    expect(screen.getByRole('alert')).toHaveTextContent('not valid UTF-8');
    expect(screen.getByRole('combobox', { name: 'Encoding' })).toHaveValue('base64');
    expect(screen.getByRole('textbox')).toHaveValue('/w==');
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(0);
    expect([...h.write.mock.calls[0]![2]]).toEqual([255]);
  });

  it('validates edited Base64 before dispatch and submits corrected complete bytes', async () => {
    await start();
    render(<EditorHost store={h.store} initial={initial} />);
    fireEvent.change(screen.getByRole('combobox', { name: 'Encoding' }), { target: { value: 'base64' } });
    fireEvent.change(screen.getByRole('textbox'), { target: { value: '/x==' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    expect(screen.getByRole('alert')).toHaveTextContent('canonical padded Base64');
    expect(h.write).not.toHaveBeenCalled();
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'AP8=' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(0);
    expect([...h.write.mock.calls[0]![2]]).toEqual([0, 255]);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it.each([
    { name: 'value', registerKey: 'chosen', draft: '\uD800' },
    { name: 'key', registerKey: '\uD800', draft: 'valid text' },
  ])('rejects unpaired Unicode in the edited $name without dispatching a write', ({ registerKey, draft }) => {
    render(<EditorHost store={h.store} initial={initial} registerKey={registerKey} />);
    fireEvent.change(screen.getByRole('textbox'), { target: { value: draft } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    expect(screen.getByRole('alert')).toHaveTextContent('unpaired Unicode surrogate');
    expect(h.write).not.toHaveBeenCalled();
  });

  it('preserves an edited draft and original precondition when a newer initial observation arrives', async () => {
    await start();
    const ui = render(<EditorHost store={h.store} initial={initial} />);
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'draft 🌍' } });
    ui.rerender(<EditorHost store={h.store} initial={{ present: true, bytes: new Uint8Array([66]), etag: '"v9"' }} />);
    expect(screen.getByRole('textbox')).toHaveValue('draft 🌍');
    expect(screen.getByText('"v1"')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(0);
    expect([...h.write.mock.calls[0]![2]]).toEqual([100, 114, 97, 102, 116, 32, 240, 159, 140, 141]);
    expect(h.write.mock.calls[0]![3]).toBe('"v1"');
  });

  it.each([
    { name: 'conflict', outcome: { kind: 'conflict', message: 'version changed' } as WriteResult, label: 'Version conflict.' },
    { name: 'unknown transport outcome', outcome: { kind: 'unknown', message: 'response lost', currentEtag: '"later-observation"' } as WriteResult, label: 'Write outcome unknown.' },
  ])('blocks $name until an intentional fresh read and explicit version adoption', async ({ outcome, label }) => {
    await start();
    h.write.mockResolvedValueOnce(outcome);
    render(<EditorHost store={h.store} initial={initial} />);
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'my draft' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(0);
    expect(screen.getByRole('status')).toHaveTextContent(label);
    expect(screen.getByRole('button', { name: 'Compare and swap' })).toBeDisabled();
    expect(screen.getByRole('textbox')).toHaveValue('my draft');
    expect(screen.getByText('"v1"')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Use observed version for this draft' })).not.toBeInTheDocument();
    if (outcome.kind === 'unknown') expect(screen.getByText('"later-observation"')).toBeInTheDocument();

    // A background refresh is not the operator's reconciliation decision.
    h.read.mockResolvedValueOnce({ present: true, bytes: new Uint8Array([66]), etag: '"background"' });
    h.store.current!.invalidate();
    await tick(400);
    expect(screen.queryByRole('button', { name: 'Use observed version for this draft' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Compare and swap' })).toBeDisabled();
    expect(h.write).toHaveBeenCalledTimes(1);

    const observed = deferred<Register>();
    h.read.mockReturnValueOnce(observed.promise);
    fireEvent.click(screen.getByRole('button', { name: 'Refresh to reconcile' }));
    await tick(300);
    expect(screen.queryByRole('button', { name: 'Use observed version for this draft' })).not.toBeInTheDocument();
    observed.resolve({ present: true, bytes: new Uint8Array([67]), etag: '"reviewed"' });
    await tick(100);
    expect(screen.getByLabelText('Current value')).toHaveTextContent('C');
    expect(screen.getByRole('button', { name: 'Compare and swap' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'Use observed version for this draft' }));
    expect(screen.getByRole('textbox')).toHaveValue('my draft');
    expect(screen.getByText('"reviewed"')).toBeInTheDocument();
    expect(h.write).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(0);
    expect(h.write).toHaveBeenCalledTimes(2);
    expect(h.write.mock.calls[1]![3]).toBe('"reviewed"');
    expect([...h.write.mock.calls[1]![2]]).toEqual([109, 121, 32, 100, 114, 97, 102, 116]);
    expect(screen.getByRole('status')).toHaveTextContent('Write applied.');
  });

  it('creates an empty key with an absent precondition and reports success only after acknowledgement', async () => {
    const result = deferred<WriteResult>();
    h.write.mockReturnValueOnce(result.promise);
    const onCreated = vi.fn();
    render(<EditorHost store={h.store} registerKey="" initial={{ present: false }} onCreated={onCreated} />);
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'new' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create register' }));
    expect(h.write.mock.calls[0]![1]).toBe('');
    expect(h.write.mock.calls[0]![3]).toBeNull();
    expect(screen.getByRole('button', { name: 'Awaiting outcome...' })).toBeDisabled();
    expect(onCreated).not.toHaveBeenCalled();
    result.resolve({ kind: 'applied', etag: '"created"' });
    await tick(0);
    expect(onCreated).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('button', { name: 'Compare and swap' })).toBeEnabled();
    expect(screen.getByText('"created"')).toBeInTheDocument();
  });

  it('reconciles a failed create against an absent read without silently retrying', async () => {
    await start();
    h.write.mockResolvedValueOnce({ kind: 'unknown', message: 'response lost' });
    render(<EditorHost store={h.store} registerKey="" initial={{ present: false }} />);
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'new' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create register' }));
    await tick(0);
    h.read.mockResolvedValueOnce({ present: false });
    fireEvent.click(screen.getByRole('button', { name: 'Refresh to reconcile' }));
    await tick(400);
    expect(h.store.selected).toBe('');
    expect(screen.getByText('The quorum-established register is absent.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Use observed version for this draft' }));
    expect(screen.getByRole('button', { name: 'Create register' })).toBeEnabled();
    expect(screen.getByRole('textbox')).toHaveValue('new');
    expect(h.write).toHaveBeenCalledTimes(1);
  });

  it('aborts a write at its deadline, preserves uncertain intent and never automatically retries', async () => {
    let signal!: AbortSignal;
    h.write.mockImplementationOnce((_origin, _key, _bytes, _etag, value) => {
      signal = value;
      return new Promise((resolve) => value.addEventListener('abort', () => resolve({ kind: 'unknown', message: 'response unavailable' })));
    });
    render(<EditorHost store={h.store} initial={initial} />);
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(14_999);
    expect(signal.aborted).toBe(false);
    await tick(1);
    expect(signal.aborted).toBe(true);
    expect(screen.getByRole('status')).toHaveTextContent('Write outcome unknown.');
    expect(screen.getByRole('button', { name: 'Compare and swap' })).toBeDisabled();
    expect(screen.getByRole('textbox')).toHaveValue('A');
    expect(screen.getByText('"v1"')).toBeInTheDocument();
    await tick(30_000);
    expect(h.write).toHaveBeenCalledTimes(1);
  });

  it('aborts on unmount and ignores a late applied completion without refreshing or creating', async () => {
    const result = deferred<WriteResult>();
    h.write.mockReturnValueOnce(result.promise);
    const refresh = vi.spyOn(h.store, 'refresh');
    const onCreated = vi.fn();
    const ui = render(<EditorHost store={h.store} initial={{ present: false }} onCreated={onCreated} />);
    fireEvent.click(screen.getByRole('button', { name: 'Create register' }));
    const signal = h.write.mock.calls[0]![4];
    ui.unmount();
    expect(signal.aborted).toBe(true);
    result.resolve({ kind: 'applied', etag: '"late"' });
    await tick(0);
    expect(refresh).not.toHaveBeenCalled();
    expect(onCreated).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('App editor navigation protection', () => {
  let h: Harness;
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-23T20:00:00.000Z'));
    h = harness();
    h.keys.mockResolvedValue({ keys: ['chosen', 'other'], continuation: null });
    h.read.mockImplementation(async (_origin, registerKey) => ({
      present: true, bytes: new Uint8Array([registerKey === 'other' ? 66 : 65]),
      etag: registerKey === 'other' ? '"other-version"' : '"v1"',
    }));
  });
  afterEach(() => h.store.dispose());

  async function openEditor() {
    const ui = render(<App store={h.store} />);
    await tick(400);
    fireEvent.click(screen.getByRole('button', { name: 'chosen' }));
    await tick(400);
    return ui;
  }

  function draft() { return screen.getByRole('textbox', { name: /^New complete value for/ }); }

  function navigate(destination: 'key' | 'host') {
    if (destination === 'key') fireEvent.click(screen.getByRole('button', { name: 'other' }));
    else fireEvent.change(screen.getByRole('combobox', { name: 'Request host' }), {
      target: { value: peers[1]!.httpEndpoint },
    });
  }

  function beforeUnload() {
    const event = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(event);
    return event.defaultPrevented;
  }

  it.each([
    { name: 'pending key', outcome: 'pending', destination: 'key' },
    { name: 'pending host', outcome: 'pending', destination: 'host' },
    { name: 'unknown key', outcome: 'unknown', destination: 'key' },
    { name: 'unknown host', outcome: 'unknown', destination: 'host' },
  ] as const)('blocks $name navigation without losing the editor or aborting its request', async ({ outcome, destination }) => {
    await openEditor();
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
    const result = deferred<WriteResult>();
    h.write.mockReturnValueOnce(result.promise);
    const editor = draft();
    const current = h.store.current;
    fireEvent.change(editor, { target: { value: 'protected draft' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    const signal = h.write.mock.calls[0]![4];
    if (outcome === 'unknown') {
      result.resolve({ kind: 'unknown', message: 'response lost' });
      await tick(0);
    }
    navigate(destination);
    await tick(400);
    expect(h.store.selected).toBe('chosen');
    expect(h.store.origin).toBe(peers[0]!.httpEndpoint);
    expect(h.store.current).toBe(current);
    expect(screen.getByRole('combobox', { name: 'Request host' })).toHaveValue(peers[0]!.httpEndpoint);
    expect(draft()).toBe(editor);
    expect(editor).toHaveValue('protected draft');
    expect(screen.getByRole('region', { name: 'Register inspector' })).toHaveTextContent('Expected ETag "v1"');
    expect(signal.aborted).toBe(false);
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.write).toHaveBeenCalledTimes(1);
    expect(confirm).not.toHaveBeenCalled();
    expect(screen.getByRole('alert')).toHaveTextContent(outcome === 'pending'
      ? 'A write is awaiting its outcome' : 'Reconcile the unknown write outcome');
    expect(beforeUnload()).toBe(true);
    if (outcome === 'pending') {
      expect(screen.getByRole('button', { name: 'Awaiting outcome...' })).toBeDisabled();
      result.resolve({ kind: 'applied', etag: '"acknowledged"' });
      await tick(0);
      expect(screen.getByText('Write applied.')).toBeInTheDocument();
      expect(draft()).toBe(editor);
      expect(screen.getByRole('region', { name: 'Register inspector' })).toHaveTextContent('Expected ETag "acknowledged"');
    } else {
      expect(screen.getByText('Write outcome unknown.')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Compare and swap' })).toBeDisabled();
    }
  });

  it.each([
    { name: 'unsaved', conflict: false, value: 'unsaved draft' },
    { name: 'conflict', conflict: true, value: 'A' },
  ])('preserves the $name draft when discard confirmation is cancelled', async ({ conflict, value }) => {
    const ui = await openEditor();
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
    const editor = draft();
    const current = h.store.current;
    if (conflict) {
      // Even a conflict on an unchanged payload must protect the original intent.
      h.write.mockResolvedValueOnce({ kind: 'conflict', message: 'version changed' });
      fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
      await tick(0);
    } else {
      fireEvent.change(editor, { target: { value } });
    }
    fireEvent.click(screen.getByRole('button', { name: 'chosen' }));
    expect(confirm).not.toHaveBeenCalled();
    expect(h.store.current).toBe(current);
    navigate('key');
    navigate('host');
    await tick(400);
    expect(confirm).toHaveBeenCalledTimes(2);
    expect(confirm).toHaveBeenCalledWith('Discard this unsaved draft and change the editor?');
    expect(h.store.selected).toBe('chosen');
    expect(h.store.origin).toBe(peers[0]!.httpEndpoint);
    expect(h.store.current).toBe(current);
    expect(draft()).toBe(editor);
    expect(editor).toHaveValue(value);
    expect(screen.getByRole('region', { name: 'Register inspector' })).toHaveTextContent('Expected ETag "v1"');
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.write).toHaveBeenCalledTimes(conflict ? 1 : 0);
    expect(beforeUnload()).toBe(true);
    ui.unmount();
    expect(beforeUnload()).toBe(false);
  });

  it.each(['key', 'host'] as const)('allows %s navigation after success marks the edited draft clean', async (destination) => {
    await openEditor();
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
    const editor = draft();
    fireEvent.change(editor, { target: { value: 'saved draft' } });
    expect(beforeUnload()).toBe(true);
    fireEvent.click(screen.getByRole('button', { name: 'Compare and swap' }));
    await tick(400);
    expect(screen.getByText('Write applied.')).toBeInTheDocument();
    expect(beforeUnload()).toBe(false);
    navigate(destination);
    await tick(400);
    expect(confirm).not.toHaveBeenCalled();
    expect(h.store.selected).toBe(destination === 'key' ? 'other' : 'chosen');
    expect(h.store.origin).toBe(peers[destination === 'host' ? 1 : 0]!.httpEndpoint);
    expect(draft()).not.toBe(editor);
    expect(draft()).toHaveValue(destination === 'key' ? 'B' : 'A');
    expect(h.read.mock.calls.at(-1)!.slice(0, 2)).toEqual([h.store.origin, h.store.selected]);
    expect(h.write).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(beforeUnload()).toBe(false);
  });

  it('makes value and local-observation scroll regions keyboard focusable', async () => {
    await openEditor();
    const value = screen.getByLabelText('Current value');
    const local = screen.getByRole('region', { name: 'Scrollable local acceptor observations' });
    expect(value).toHaveAttribute('tabindex', '0');
    expect(local).toHaveAttribute('tabindex', '0');
    value.focus();
    expect(value).toHaveFocus();
    local.focus();
    expect(local).toHaveFocus();
  });
});

describe('Timeline and inventory interactions', () => {
  let h: Harness;
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-23T20:00:00.000Z'));
    h = harness();
  });
  afterEach(() => h.store.dispose());

  async function start() {
    await h.store.start();
    h.sources.forEach((source) => source.emit('open'));
    h.store.select('chosen');
    await advance(400);
  }

  function send(item: NodeEvent, source = 0) { act(() => h.sources[source]!.send(item)); }

  it('inspects and refreshes a stable register with reads without issuing writes or self-invalidating', async () => {
    const entry: LocalState = {
      key: 'chosen', promisedBallot: { round: 8, proposer: 9 }, acceptedBallot: { round: 3, proposer: 9 },
      acceptedValue: { length: 14, base64: 'RkNSMQEAAAAJAAAAAUE=', truncated: false },
      register: { format: 'FCR1', isPresent: true, version: 'AAAACQAAAAE=', value: { length: 1, base64: 'QQ==', truncated: false } },
    };
    h.state.mockImplementation(async (origin) => snapshot(
      peers.find((peer) => peer.httpEndpoint === origin)!.id, { entry }));
    render(<App store={h.store} />);
    await tick(400);
    fireEvent.click(screen.getByRole('button', { name: 'chosen' }));
    await tick(400);
    const inspector = within(screen.getByRole('region', { name: 'Register inspector' }));
    expect(inspector.getByLabelText('Current value')).toHaveTextContent('A');
    expect(inspector.getByText(/Matching quorum observations establish the value in one read round/)).toBeInTheDocument();
    expect(h.read).toHaveBeenCalledTimes(1);

    for (let refresh = 0; refresh < 2; refresh++) {
      fireEvent.click(inspector.getByRole('button', { name: 'Read current value' }));
      await tick(400);
      expect(h.read).toHaveBeenCalledTimes(refresh + 2);
      expect(h.read.mock.calls.at(-1)!.slice(0, 2)).toEqual([h.store.origin, 'chosen']);
      expect(inspector.getByLabelText('Current value')).toHaveTextContent('A');
      send(event('operation', String(refresh + 1), {
        operation: 'read', operationId: `inspection-${refresh}`, key: key(), outcome: 'present',
        isPresent: true, version: 'AAAACQAAAAE=', value: { length: 1, base64: 'QQ==', truncated: false },
      }));
      await tick(400);
      expect(h.read).toHaveBeenCalledTimes(refresh + 2); // Read diagnostics must not schedule another read.
    }

    expect(h.write).not.toHaveBeenCalled();
    expect(h.store.current!.state).toMatchObject({
      loading: false, stale: false, value: { present: true, bytes: new Uint8Array([65]), etag: '"v1"' },
    });
    const rows = within(inspector.getByRole('region', { name: 'Scrollable local acceptor observations' })).getAllByRole('row');
    expect(rows).toHaveLength(3);
    for (const row of rows.slice(1)) {
      expect(row).toHaveTextContent('(8, 9)');
      expect(row).toHaveTextContent('(3, 9)');
    }
  });

  it('shows advancing stale observations under continuous invalidation without enabling unknown-write reconciliation', async () => {
    render(<App store={h.store} />);
    await tick(400);
    fireEvent.click(screen.getByRole('button', { name: 'chosen' }));
    await tick(400);
    const inspector = within(screen.getByRole('region', { name: 'Register inspector' }));
    const editor = inspector.getByRole('textbox', { name: /^New complete value for/ });
    fireEvent.change(editor, { target: { value: 'protected draft' } });
    h.write.mockResolvedValueOnce({ kind: 'unknown', message: 'response lost' });
    fireEvent.click(inspector.getByRole('button', { name: 'Compare and swap' }));
    await tick(0);
    expect(inspector.getByText('Write outcome unknown.')).toBeInTheDocument();

    const reads = Array.from({ length: 4 }, () => deferred<Register>());
    for (const read of reads) h.read.mockReturnValueOnce(read.promise);
    fireEvent.click(inspector.getByRole('button', { name: 'Refresh to reconcile' }));
    for (let cycle = 0; cycle < 3; cycle++) {
      await tick(300);
      send(event('operation', String(cycle + 1), {
        operation: 'compare-exchange', operationId: `concurrent-${cycle + 1}`, key: key(), outcome: 'applied',
      }));
      reads[cycle]!.resolve({
        present: true, bytes: new Uint8Array([66 + cycle]), etag: `"observation-${cycle + 1}"`,
      });
      await tick(100);
      expect(inspector.getByLabelText('Current value').textContent).toBe(['B', 'C', 'D'][cycle]);
      expect(inspector.getByText('Stale observation')).toBeInTheDocument();
      expect(h.store.current!.state).toMatchObject({ loading: false, stale: true });
      expect(h.read).toHaveBeenCalledTimes(cycle + 2);
      expect(inspector.queryByRole('button', { name: 'Use observed version for this draft' })).not.toBeInTheDocument();
      expect(inspector.getByRole('button', { name: 'Compare and swap' })).toBeDisabled();
      expect(editor).toHaveValue('protected draft');
      expect(inspector.getByText('Expected ETag', { exact: false })).toHaveTextContent('Expected ETag "v1"');
      expect(h.write).toHaveBeenCalledTimes(1);
    }
    await tick(300);
    reads[3]!.resolve({ present: true, bytes: new Uint8Array([69]), etag: '"quiescent"' });
    await tick(100);
    expect(inspector.getAllByLabelText('Current value').map((element) => element.textContent)).toEqual(['E', 'E']);
    expect(inspector.queryByText('Stale observation')).not.toBeInTheDocument();
    expect(h.store.current!.state).toMatchObject({ loading: false, stale: false });
    expect(h.read).toHaveBeenCalledTimes(5);
    expect(inspector.getByRole('button', { name: 'Compare and swap' })).toBeDisabled();
    fireEvent.click(inspector.getByRole('button', { name: 'Use observed version for this draft' }));
    expect(inspector.getByText('Expected ETag', { exact: false })).toHaveTextContent('Expected ETag "quiescent"');
    expect(editor).toHaveValue('protected draft');
    expect(inspector.getByRole('button', { name: 'Compare and swap' })).toBeEnabled();
    expect(h.write).toHaveBeenCalledTimes(1);
  });

  it('combines host, event and case-insensitive message filters without changing subscriptions', async () => {
    await start();
    render(<TimelineHost store={h.store} />);
    send(event('operation', '1', { operation: 'compare-exchange', operationId: 'op-A', key: key('alpha'), outcome: 'applied' }));
    send(event('storage', '2', { status: 'faulted' }));
    send(event('operation', '1', { operation: 'compare-exchange', operationId: 'op-B', key: key('beta'), outcome: 'conflict' }, 'b'), 1);
    await tick(100);
    expect(screen.getAllByRole('row')).toHaveLength(4);
    fireEvent.change(screen.getByRole('combobox', { name: 'Node' }), { target: { value: 'a' } });
    fireEvent.change(screen.getByRole('combobox', { name: 'Event' }), { target: { value: 'operation' } });
    fireEvent.change(screen.getByRole('textbox', { name: 'Filter messages' }), { target: { value: 'ALPHA' } });
    const rows = screen.getAllByRole('row');
    expect(rows).toHaveLength(2);
    expect(rows[1]).toHaveTextContent('alpha');
    expect(rows[1]).toHaveTextContent('applied');
    fireEvent.change(screen.getByRole('textbox', { name: 'Filter messages' }), { target: { value: 'no match' } });
    expect(screen.getAllByRole('row')).toHaveLength(1);
    expect(screen.getByText(/Waiting for matching events/)).toBeInTheDocument();
    expect(h.store.events).toHaveLength(3);
    expect(h.streams).toHaveBeenCalledTimes(2);
    h.sources.forEach((source) => expect(source.close).not.toHaveBeenCalled());
  });

  it('freezes only displayed events while subscriptions, node health and register refreshes stay live', async () => {
    await start();
    render(<TimelineHost store={h.store} />);
    send(event('storage', '1', { status: 'ready' }));
    await tick(100);
    fireEvent.click(screen.getByRole('button', { name: 'Pause timeline' }));
    expect(screen.getByRole('button', { name: 'Resume timeline' })).toHaveAttribute('aria-pressed', 'true');
    h.read.mockClear();
    send(event('operation', '2', { operation: 'compare-exchange', operationId: 'new-op', key: key(), outcome: 'applied' }));
    send(event('storage', '1', { status: 'faulted' }, 'b'), 1);
    await tick(400);
    expect(screen.getByRole('status')).toHaveTextContent('2 new events');
    expect(screen.getAllByRole('row')).toHaveLength(2);
    expect(h.store.events).toHaveLength(3);
    expect(h.read).toHaveBeenCalledTimes(1);
    expect(h.store.nodes[1]!.storage?.status).toBe('faulted');
    expect(h.store.nodes[1]!.connection).toBe('connected');
    send(event('heartbeat', '2', { storageStatus: 'ready' }));
    await tick(100);
    expect(screen.getByRole('status')).toHaveTextContent('2 new events');
    expect(h.streams).toHaveBeenCalledTimes(2);
    h.sources.forEach((source) => expect(source.close).not.toHaveBeenCalled());
    fireEvent.click(screen.getByRole('button', { name: 'Resume timeline' }));
    const rows = screen.getAllByRole('row');
    expect(rows).toHaveLength(4);
    expect(rows[1]).toHaveTextContent('faulted');
    expect(rows[2]).toHaveTextContent('compare-exchange');
    expect(rows[3]).toHaveTextContent('ready');
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('renders the latest 200 matching events newest first without reordering retained history', async () => {
    await start();
    render(<TimelineHost store={h.store} />);
    act(() => {
      for (let sequence = 1; sequence <= 205; sequence++) {
        h.sources[0]!.send(event('storage', String(sequence), { status: 'ready' }));
      }
    });
    await tick(100);
    const rows = screen.getAllByRole('row');
    expect(rows).toHaveLength(201);
    expect(within(rows[1]!).getByText('#205')).toBeInTheDocument();
    expect(within(rows[200]!).getByText('#6')).toBeInTheDocument();
    expect(h.store.events[0]!.sequence).toBe('1');
    expect(h.store.events.at(-1)!.sequence).toBe('205');
    expect(screen.queryByText('#5')).not.toBeInTheDocument();
    expect(screen.getByText('205 / 1000 retained · 0 evicted')).toBeInTheDocument();
  });

  it('retains repeated reset controls with the same wire cursor as distinct rendered arrivals', async () => {
    await start();
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    render(<TimelineHost store={h.store} />);
    send(event('reset', '0', { reason: 'first connection' }));
    await tick(100);
    send(event('reset', '0', { reason: 'second connection' }));
    await tick(100);
    expect(screen.getAllByRole('row')).toHaveLength(3);
    expect(screen.getByText('Resynchronize: first connection')).toBeInTheDocument();
    expect(screen.getByText('Resynchronize: second connection')).toBeInTheDocument();
    fireEvent.change(screen.getByRole('combobox', { name: 'Event' }), { target: { value: 'storage' } });
    expect(screen.getAllByRole('row')).toHaveLength(1);
    fireEvent.change(screen.getByRole('combobox', { name: 'Event' }), { target: { value: 'reset' } });
    expect(screen.getAllByRole('row')).toHaveLength(3);
    expect(consoleError).not.toHaveBeenCalled();
  });

  it('draws request and response lanes, filters either endpoint and traces a call', async () => {
    await start();
    render(<TimelineHost store={h.store} />);
    const data = {
      operation: 'prepare', direction: 'outbound', phase: 'request-initiated', peerId: 'b',
      callId: 'call-1', operationId: 'op-1', key: key('alpha'),
      ballot: { round: 2, proposer: 1 }, nextBallot: { round: 0, proposer: 0 },
    };
    send(event('protocol', '1', data));
    send({ ...event('protocol', '2', { ...data, phase: 'response-observed', outcome: 'promised', promisedBallot: { round: 2, proposer: 1 } }),
      timestamp: '2026-10-23T20:00:00.012Z' });
    send(event('storage', '3', { status: 'ready' }));
    await tick(100);
    expect(screen.getByRole('img', { name: 'Node a to Node b: prepare request-initiated' })).toBeInTheDocument();
    expect(screen.getByRole('img', { name: 'Node b to Node a: prepare response-observed · promised' })).toBeInTheDocument();
    expect(screen.getByTitle('12 ms host elapsed')).toHaveTextContent('12 ms');
    expect(screen.getByText('prepare("alpha", b=(2,1))')).toBeInTheDocument();
    expect(screen.getByText('prepare:promised("alpha", b=(2,1), p=(2,1))')).toBeInTheDocument();
    fireEvent.change(screen.getByRole('combobox', { name: 'Node' }), { target: { value: 'b' } });
    expect(screen.getAllByRole('row')).toHaveLength(3);
    const details = screen.getAllByLabelText('Inspect event')[1]!.closest('details')!;
    details.open = true;
    fireEvent(details, new Event('toggle'));
    fireEvent.click(within(details).getByRole('button', { name: 'Trace call' }));
    expect(screen.getByRole('textbox', { name: 'Filter messages' })).toHaveValue('call-1');
    expect(screen.getByRole('combobox', { name: 'Node' })).toHaveValue('');
    expect(screen.getAllByRole('row')).toHaveLength(3);
    fireEvent.click(screen.getByRole('button', { name: 'Clear filter' }));
    expect(screen.getAllByRole('row')).toHaveLength(4);
  });

  it('shows committed ballot changes from retained history before applying text filters', async () => {
    await start();
    render(<TimelineHost store={h.store} />);
    const data = {
      key: key(), promisedBallot: { round: 1, proposer: 1 }, acceptedBallot: { round: 0, proposer: 0 },
      acceptedValue: null, register: null,
    };
    send(event('acceptor-state', '1', data));
    send(event('acceptor-state', '2', { ...data, promisedBallot: { round: 2, proposer: 1 } }));
    await tick(100);
    fireEvent.change(screen.getByRole('textbox', { name: 'Filter messages' }), { target: { value: '"sequence":"2"' } });
    expect(screen.getAllByRole('row')).toHaveLength(2);
    expect(screen.queryByText('Promised: (1, 1) -> (2, 1)')).not.toBeInTheDocument();
    const details = screen.getByLabelText('Inspect event').closest('details')!;
    details.open = true;
    fireEvent(details, new Event('toggle'));
    expect(screen.getByText('Promised: (1, 1) -> (2, 1)')).toBeInTheDocument();
    expect(screen.getByRole('img', { name: 'Node a: Committed acceptor state' })).toBeInTheDocument();
    expect(screen.queryByText('First retained state for this key and node generation')).not.toBeInTheDocument();
  });

  it('subscribes to repair transitions and keeps subsequent sequence history continuous', async () => {
    await start();
    render(<TimelineHost store={h.store} />);
    send(event('storage', '1', { status: 'ready' }));
    send(event('repair', '2', {
      state: 'complete', departing: false, targetReplicas: 2, examined: 5, repaired: 3, pending: 0,
      error: null, timestamp: '2026-10-23T20:00:00.000Z',
    }));
    send(event('storage', '3', { status: 'ready' }));
    await tick(100);
    expect(h.store.events.map((item) => item.kind)).toEqual(['storage', 'repair', 'storage']);
    expect(h.store.nodes[0]!.reset).toBeUndefined();
    fireEvent.change(screen.getByRole('combobox', { name: 'Event' }), { target: { value: 'repair' } });
    expect(screen.getAllByRole('row')).toHaveLength(2);
    expect(screen.getByText('Repair complete')).toBeInTheDocument();
    const details = screen.getByLabelText('Inspect event').closest('details')!;
    details.open = true;
    fireEvent(details, new Event('toggle'));
    expect(screen.getByText('Examined: 5 · Repaired: 3 · Pending: 0')).toBeInTheDocument();
  });

  it('follows new arrivals at the top until the operator scrolls down and resumes explicitly', async () => {
    await start();
    render(<TimelineHost store={h.store} />);
    const viewport = screen.getByRole('region', { name: 'Scrollable event timeline' });
    Object.defineProperties(viewport, {
      scrollHeight: { configurable: true, value: 2000 },
      clientHeight: { configurable: true, value: 640 },
    });
    send(event('storage', '1', { status: 'ready' }));
    await tick(100);
    expect(viewport.scrollTop).toBe(0);
    fireEvent.scroll(viewport, { target: { scrollTop: 100 } });
    expect(screen.getByRole('checkbox', { name: 'Follow latest' })).not.toBeChecked();
    send(event('storage', '2', { status: 'stopped' }));
    await tick(100);
    expect(viewport.scrollTop).toBe(100);
    fireEvent.click(screen.getByRole('checkbox', { name: 'Follow latest' }));
    expect(viewport.scrollTop).toBe(0);
    fireEvent.click(screen.getByRole('button', { name: 'Pause timeline' }));
    viewport.scrollTop = 500;
    send(event('storage', '3', { status: 'ready' }));
    await tick(100);
    expect(viewport.scrollTop).toBe(500);
    fireEvent.click(screen.getByRole('button', { name: 'Resume timeline' }));
    expect(viewport.scrollTop).toBe(0);
  });

  it.each([
    { name: 'reset', item: event('reset', '0', { reason: 'history gap' }) },
    { name: 'lossy key', item: event('operation', '1', {
      operation: 'compare-exchange', operationId: 'new', key: key('other', { lossy: true }), outcome: 'applied',
    }) },
  ])('resets page history when a $name restarts inventory', async ({ item }) => {
    h.keys.mockResolvedValueOnce({ keys: [''], continuation: '' })
      .mockResolvedValueOnce({ keys: ['chosen'], continuation: null })
      .mockResolvedValueOnce({ keys: [''], continuation: '' });
    render(<App store={h.store} />);
    await tick(400);
    fireEvent.click(screen.getByRole('button', { name: 'Next' }));
    await tick(400);
    expect(screen.getByText('Page 2')).toBeInTheDocument();
    send(item);
    await tick(400);
    expect(h.keys.mock.calls[2]![2]).toBeNull();
    expect(screen.getByText('Page 1')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Next' })).toBeEnabled();
  });

  it('preserves an empty continuation through Next and restores null through Previous', async () => {
    h.keys.mockResolvedValueOnce({ keys: [''], continuation: '' })
      .mockResolvedValueOnce({ keys: ['chosen'], continuation: null })
      .mockResolvedValueOnce({ keys: [''], continuation: '' });
    render(<App store={h.store} />);
    await tick(400);
    expect(screen.getByRole('button', { name: '(empty key)' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next' })).toBeEnabled();
    fireEvent.click(screen.getByRole('button', { name: 'Next' }));
    await tick(400);
    expect(h.keys.mock.calls[1]![2]).toBe('');
    expect(screen.getByText('Page 2')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'Previous' }));
    await tick(400);
    expect(h.keys.mock.calls[2]![2]).toBeNull();
    expect(screen.getByText('Page 1')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
  });
});
