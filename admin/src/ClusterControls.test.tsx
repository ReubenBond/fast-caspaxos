import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ClusterControls } from './ClusterControls';
import type { ClusterAdminResult, NodeAdministration, ProposerSettings, ResetRequest } from './contracts';
import { advance, deferred, harness, peers, status } from './test-fixtures';
import type { Harness } from './test-fixtures';

const initial: ProposerSettings = {
  mode: 'adaptive', startInFastMode: true, enableForwarding: true,
  conflictDebtIncrement: 4, conflictDebtDecay: 1, fastReopenThreshold: 0, leaderMissThreshold: 1,
};
const selected: ProposerSettings = {
  mode: 'fast+leader', startInFastMode: false, enableForwarding: false,
  conflictDebtIncrement: 7, conflictDebtDecay: 2, fastReopenThreshold: 3, leaderMissThreshold: 5,
};
const resetRequest: ResetRequest = { operationId: '123456781234123412341234567890ab', expectedEpoch: '0' };
const complete: ClusterAdminResult = {
  state: 'complete', nodes: peers.map(peer => ({ nodeId: peer.id, state: 'active', error: null })), reset: null,
};
const incomplete: ClusterAdminResult = {
  state: 'incomplete', reset: resetRequest,
  nodes: [{ nodeId: 'a', state: 'prepared', error: null }, { nodeId: 'b', state: 'unconfirmed', error: 'HTTP 503: offline' }],
};
function control(nodeId: string, changes: Partial<NodeAdministration> = {}): NodeAdministration {
  return {
    schemaVersion: 1, nodeId, coordinatorId: 'a', epoch: '0', state: 'active',
    settings: initial, pendingReset: null, clusterReset: null, ...changes,
  };
}

describe('ClusterControls', () => {
  let h: Harness;
  beforeEach(() => {
    vi.useFakeTimers();
    h = harness();
    vi.spyOn(h.api, 'administration').mockImplementation(async origin =>
      control(h.store.nodes.find(node => node.peer.httpEndpoint === origin)!.peer.id));
    vi.spyOn(h.api, 'applySettings').mockResolvedValue(complete);
    vi.spyOn(h.api, 'resetCluster').mockResolvedValue({ ...complete, reset: resetRequest });
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    vi.spyOn(crypto, 'randomUUID').mockReturnValue('12345678-1234-1234-1234-1234567890ab');
  });
  afterEach(() => h.store.dispose());

  async function mount(resetBlocked = false) {
    await h.store.start();
    h.store.setOrigin(h.store.nodes.at(-1)!.peer.httpEndpoint);
    const onReset = vi.fn();
    const view = render(<ClusterControls store={h.store} resetBlocked={resetBlocked} onReset={onReset} />);
    const details = screen.getByText('Cluster controls').closest('details')!;
    act(() => { details.open = true; fireEvent(details, new Event('toggle')); });
    await act(() => advance(0));
    return { ...view, onReset };
  }

  function changeAllSettings() {
    fireEvent.change(screen.getByLabelText('Proposer mode'), { target: { value: 'fast+leader' } });
    fireEvent.click(screen.getByLabelText('Start adaptive mode fast'));
    fireEvent.click(screen.getByLabelText('Enable adaptive leader forwarding'));
    for (const [label, value] of [
      ['Conflict debt increment', '7'], ['Conflict debt decay', '2'],
      ['Fast reopen threshold', '3'], ['Leader miss threshold', '5'],
    ]) fireEvent.change(screen.getByLabelText(label!), { target: { value } });
  }
  function row(nodeId: string) {
    return within(screen.getByRole('table', { name: 'Cluster administration progress' }))
      .getAllByRole('row').find(item => within(item).queryByRole('rowheader', { name: nodeId }))!;
  }

  it('offers every supported proposer mode and all adaptive settings', async () => {
    await mount();
    expect(within(screen.getByLabelText('Proposer mode')).getAllByRole('option').map(option =>
      [(option as HTMLOptionElement).value, option.textContent])).toEqual([
      ['classic', 'Classic'], ['fast', 'Fast'], ['leader', 'Leadered'],
      ['fast+leader', 'Fast + leadered'], ['adaptive', 'Adaptive'],
    ]);
    expect(screen.getByLabelText('Start adaptive mode fast')).toBeChecked();
    expect(screen.getByLabelText('Enable adaptive leader forwarding')).toBeChecked();
    for (const [label, value, minimum] of [
      ['Conflict debt increment', 4, '0'], ['Conflict debt decay', 1, '0'],
      ['Fast reopen threshold', 0, '0'], ['Leader miss threshold', 1, '1'],
    ] as const) {
      const input = screen.getByLabelText(label);
      expect(input).toHaveValue(value);
      expect(input).toHaveAttribute('min', minimum);
      expect(input).toHaveAttribute('max', '2147483647');
      expect(input).toHaveAttribute('step', '1');
    }
    expect(h.api.administration).toHaveBeenCalledTimes(2);
  });

  it('applies every policy field through the ordinal-first coordinator rather than the selected host', async () => {
    const members = ['b', 'a2', 'a10'].map((id, index) => ({
      id, httpEndpoint: `http://${id}.test:50${index}`, grpcEndpoint: `http://${id}.test:60${index}`,
      eventsUrl: `http://${id}.test:50${index}/v1/admin/events`,
    }));
    h.status.mockImplementation(async origin =>
      status(members.find(peer => peer.httpEndpoint === origin)?.id ?? 'b', { peers: members }));
    await mount();
    h.store.setOrigin(members[0]!.httpEndpoint);
    changeAllSettings();
    fireEvent.click(screen.getByRole('button', { name: 'Apply settings to all nodes' }));
    await act(() => advance(0));
    expect(h.api.applySettings).toHaveBeenCalledExactlyOnceWith(
      members[2]!.httpEndpoint, selected, expect.any(AbortSignal));
    expect(h.api.resetCluster).not.toHaveBeenCalled();
    expect(screen.getByRole('status')).toHaveTextContent('Cluster command completed on every node.');
  });

  it('shows incomplete member outcomes and preserves the settings draft across status refresh', async () => {
    vi.mocked(h.api.applySettings).mockResolvedValue(incomplete);
    await mount();
    changeAllSettings();
    fireEvent.click(screen.getByRole('button', { name: 'Apply settings to all nodes' }));
    await act(() => advance(0));
    expect(screen.getByRole('status')).toHaveTextContent('Cluster command incomplete');
    expect(screen.queryByText('Cluster command completed on every node.')).not.toBeInTheDocument();
    expect(row('a')).toHaveTextContent('prepared');
    expect(row('b')).toHaveTextContent('unconfirmed');
    expect(row('b')).toHaveTextContent('HTTP 503: offline');
    fireEvent.click(screen.getByRole('button', { name: 'Refresh control status' }));
    await act(() => advance(0));
    expect(screen.getByLabelText('Proposer mode')).toHaveValue('fast+leader');
    expect(screen.getByLabelText('Conflict debt increment')).toHaveValue(7);
    expect(screen.getByLabelText('Start adaptive mode fast')).not.toBeChecked();
    expect(h.api.applySettings).toHaveBeenCalledTimes(1);
  });

  it.each([
    { name: 'negative debt', label: 'Conflict debt increment', value: '-1' },
    { name: 'negative decay', label: 'Conflict debt decay', value: '-1' },
    { name: 'negative threshold', label: 'Fast reopen threshold', value: '-1' },
    { name: 'zero miss threshold', label: 'Leader miss threshold', value: '0' },
    { name: 'fractional debt', label: 'Conflict debt increment', value: '1.5' },
    { name: 'Int32 overflow', label: 'Conflict debt decay', value: '2147483648' },
    { name: 'empty number', label: 'Leader miss threshold', value: '' },
  ])('does not dispatch $name from the settings form', async ({ label, value }) => {
    await mount();
    const input = screen.getByLabelText(label) as HTMLInputElement;
    fireEvent.change(input, { target: { value } });
    expect(input.checkValidity()).toBe(false);
    fireEvent.click(screen.getByRole('button', { name: 'Apply settings to all nodes' }));
    expect(h.api.applySettings).not.toHaveBeenCalled();
    expect(h.api.resetCluster).not.toHaveBeenCalled();
  });

  it('requires explicit destructive confirmation and cancellation sends no reset', async () => {
    const view = await mount();
    vi.mocked(window.confirm).mockReturnValueOnce(false);
    fireEvent.click(screen.getByRole('button', { name: 'Clear entire cluster' }));
    expect(window.confirm).toHaveBeenCalledExactlyOnceWith(
      'Clear all register data and protocol state on every cluster node? Selected proposer settings are retained.');
    expect(h.api.resetCluster).not.toHaveBeenCalled();
    expect(view.onReset).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Clear entire cluster' }));
    await act(() => advance(0));
    expect(h.api.resetCluster).toHaveBeenCalledExactlyOnceWith(
      peers[0]!.httpEndpoint, resetRequest, expect.any(AbortSignal));
    expect(view.onReset).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('status')).toHaveTextContent('Cluster command completed on every node.');
  });

  it.each(['settings', 'reset'] as const)('blocks duplicate and conflicting commands while %s is pending', async kind => {
    const pending = deferred<ClusterAdminResult>();
    vi.mocked(kind === 'settings' ? h.api.applySettings : h.api.resetCluster).mockReturnValue(pending.promise);
    const view = await mount();
    const name = kind === 'settings' ? 'Apply settings to all nodes' : 'Clear entire cluster';
    fireEvent.click(screen.getByRole('button', { name }));
    expect(screen.getByRole('button', { name: 'Apply settings to all nodes' })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Clear entire cluster|Resume cluster reset/ })).toBeDisabled();
    expect(screen.getByLabelText('Proposer mode')).toBeDisabled();
    expect(screen.queryByText('Cluster command completed on every node.')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Apply settings to all nodes' }));
    fireEvent.click(screen.getByRole('button', { name: /Clear entire cluster|Resume cluster reset/ }));
    expect(h.api.applySettings).toHaveBeenCalledTimes(kind === 'settings' ? 1 : 0);
    expect(h.api.resetCluster).toHaveBeenCalledTimes(kind === 'reset' ? 1 : 0);
    expect(view.onReset).not.toHaveBeenCalled();
    await act(async () => { pending.resolve(complete); await advance(0); });
    expect(screen.getByRole('status')).toHaveTextContent('Cluster command completed on every node.');
    expect(view.onReset).toHaveBeenCalledTimes(kind === 'reset' ? 1 : 0);
  });

  it('shows fenced partial progress and resumes exactly the same reset identity', async () => {
    vi.mocked(h.api.resetCluster).mockResolvedValueOnce(incomplete);
    const view = await mount();
    fireEvent.click(screen.getByRole('button', { name: 'Clear entire cluster' }));
    await act(() => advance(0));
    expect(view.onReset).not.toHaveBeenCalled();
    expect(screen.getByRole('status')).toHaveTextContent('Cluster command incomplete');
    expect(row('a')).toHaveTextContent('prepared');
    expect(row('b')).toHaveTextContent('HTTP 503: offline');
    expect(screen.getByRole('button', { name: 'Apply settings to all nodes' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'Resume cluster reset' }));
    await act(() => advance(0));
    expect(h.api.resetCluster).toHaveBeenCalledTimes(2);
    expect(vi.mocked(h.api.resetCluster).mock.calls.map(call => call[1])).toEqual([resetRequest, resetRequest]);
    expect(window.confirm).toHaveBeenCalledTimes(1);
    expect(crypto.randomUUID).toHaveBeenCalledTimes(1);
    expect(view.onReset).toHaveBeenCalledTimes(1);
  });

  it('retains an ambiguous reset for intentional resume without automatic retry', async () => {
    vi.mocked(h.api.resetCluster).mockRejectedValueOnce(new Error('connection lost after dispatch'));
    const view = await mount();
    fireEvent.click(screen.getByRole('button', { name: 'Clear entire cluster' }));
    await act(() => advance(4000));
    expect(screen.getByRole('alert')).toHaveTextContent('The command may have progressed');
    expect(screen.getByRole('alert')).toHaveTextContent('retry the same reset');
    expect(h.api.resetCluster).toHaveBeenCalledTimes(1);
    expect(view.onReset).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Resume cluster reset' }));
    await act(() => advance(0));
    expect(vi.mocked(h.api.resetCluster).mock.calls.map(call => call[1])).toEqual([resetRequest, resetRequest]);
    expect(crypto.randomUUID).toHaveBeenCalledTimes(1);
  });

  it('recovers durable reset progress after remount rather than creating a new operation', async () => {
    vi.mocked(h.api.administration).mockImplementation(async origin =>
      control(origin === peers[0]!.httpEndpoint ? 'a' : 'b', {
        state: 'cleared', pendingReset: resetRequest,
        clusterReset: { request: resetRequest, phase: 'activate', nodes: incomplete.nodes },
      }));
    await mount();
    expect(screen.getByText(/Reset phase: activate/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Resume cluster reset' }));
    await act(() => advance(0));
    expect(h.api.resetCluster).toHaveBeenCalledExactlyOnceWith(
      peers[0]!.httpEndpoint, resetRequest, expect.any(AbortSignal));
    expect(window.confirm).not.toHaveBeenCalled();
    expect(crypto.randomUUID).not.toHaveBeenCalled();
  });

  it('blocks cluster reset while a register edit needs reconciliation', async () => {
    await mount(true);
    expect(screen.getByRole('button', { name: 'Clear entire cluster' })).toBeDisabled();
    expect(screen.getByText('Finish or reconcile the active register edit before resetting the cluster.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Clear entire cluster' }));
    expect(window.confirm).not.toHaveBeenCalled();
    expect(h.api.resetCluster).not.toHaveBeenCalled();
  });

  it('does not enable commands from an unconfirmed coordinator identity', async () => {
    vi.mocked(h.api.administration).mockResolvedValue(control('unexpected'));
    await mount();
    expect(row('a')).toHaveTextContent('Expected node a; received unexpected.');
    expect(row('b')).toHaveTextContent('Expected node b; received unexpected.');
    expect(screen.getByRole('button', { name: 'Clear entire cluster' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Apply settings to all nodes' })).toBeDisabled();
    expect(h.api.resetCluster).not.toHaveBeenCalled();
  });

  it('aborts commands and ignores late completion after unmount', async () => {
    const pending = deferred<ClusterAdminResult>();
    vi.mocked(h.api.resetCluster).mockReturnValue(pending.promise);
    const refresh = vi.spyOn(h.store, 'refresh');
    const view = await mount();
    fireEvent.click(screen.getByRole('button', { name: 'Clear entire cluster' }));
    const signal = vi.mocked(h.api.resetCluster).mock.calls[0]![2];
    expect(signal.aborted).toBe(false);
    view.unmount();
    expect(signal.aborted).toBe(true);
    await act(async () => { pending.resolve(complete); await advance(4000); });
    expect(view.onReset).not.toHaveBeenCalled();
    expect(refresh).not.toHaveBeenCalled();
    expect(h.api.resetCluster).toHaveBeenCalledTimes(1);
    expect(h.api.administration).toHaveBeenCalledTimes(2);
  });
});
