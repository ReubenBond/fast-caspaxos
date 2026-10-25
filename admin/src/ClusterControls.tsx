import { useEffect, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { errorText } from './api';
import type { ClusterAdminResult, NodeAdministration, ProposerSettings, ResetRequest } from './contracts';
import type { ClusterStore } from './store';

const defaults: ProposerSettings = {
  mode: 'adaptive', startInFastMode: true, enableForwarding: true,
  conflictDebtIncrement: 4, conflictDebtDecay: 1, fastReopenThreshold: 0, leaderMissThreshold: 1,
};
const integers = [
  ['conflictDebtIncrement', 'Conflict debt increment', 0],
  ['conflictDebtDecay', 'Conflict debt decay', 0],
  ['fastReopenThreshold', 'Fast reopen threshold', 0],
  ['leaderMissThreshold', 'Leader miss threshold', 1],
] as const;

export function ClusterControls({ store, resetBlocked, onReset }: {
  store: ClusterStore; resetBlocked: boolean; onReset: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [statuses, setStatuses] = useState<Record<string, NodeAdministration>>({});
  const [failures, setFailures] = useState<Record<string, string>>({});
  const [settings, setSettings] = useState<ProposerSettings>(defaults);
  const [busy, setBusy] = useState<'settings' | 'reset'>();
  const [result, setResult] = useState<ClusterAdminResult>();
  const [error, setError] = useState<string>();
  const [resetRequest, setResetRequest] = useState<ResetRequest>();
  const [refresh, setRefresh] = useState(0);
  const initialized = useRef(false);
  const command = useRef<AbortController | null>(null);
  const mounted = useRef(true);
  const coordinator = [...store.nodes].sort((a, b) => a.peer.id < b.peer.id ? -1 : a.peer.id > b.peer.id ? 1 : 0)[0]?.peer;
  const status = coordinator && statuses[coordinator.id];
  const progress = status?.clusterReset;
  const pending = resetRequest ?? (progress && progress.phase !== 'complete' ? progress.request : undefined);

  useEffect(() => () => { mounted.current = false; command.current?.abort(); }, []);
  useEffect(() => {
    if (!open || !coordinator) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const load = async () => {
      const observations = await Promise.all(store.nodes.map(async ({ peer }) => {
        try {
          const value = await store.api.administration(peer.httpEndpoint, AbortSignal.any([controller.signal, AbortSignal.timeout(10_000)]));
          if (value.nodeId !== peer.id) throw new Error(`Expected node ${peer.id}; received ${value.nodeId}.`);
          return { id: peer.id, value };
        } catch (failure) { return { id: peer.id, error: errorText(failure) }; }
      }));
      if (controller.signal.aborted) return;
      setStatuses(Object.fromEntries(observations.flatMap(item => item.value ? [[item.id, item.value]] : [])));
      setFailures(Object.fromEntries(observations.flatMap(item => item.error ? [[item.id, item.error]] : [])));
      const selected = observations.find(item => item.id === coordinator.id)?.value;
      if (selected && !initialized.current) {
        setSettings(selected.settings);
        initialized.current = true;
      }
      timer = setTimeout(() => { void load(); }, 2_000);
    };
    void load();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [open, coordinator?.id, refresh, store]);

  const run = async (kind: 'settings' | 'reset', request?: ResetRequest) => {
    if (!coordinator) return;
    setBusy(kind);
    setError(undefined);
    setResult(undefined);
    const controller = command.current = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 120_000);
    try {
      const outcome = kind === 'settings'
        ? await store.api.applySettings(coordinator.httpEndpoint, settings, controller.signal)
        : await store.api.resetCluster(coordinator.httpEndpoint, request!, controller.signal);
      if (!mounted.current) return;
      setResult(outcome);
      if (kind === 'reset' && outcome.state === 'complete') {
        setResetRequest(undefined);
        onReset();
      }
      store.refresh();
    } catch (failure) {
      if (mounted.current) setError(`${errorText(failure)} The command may have progressed. Refresh node status and retry${kind === 'reset' ? ' the same reset' : ' these settings'}.`);
    } finally {
      clearTimeout(timeout);
      if (mounted.current) { setBusy(undefined); setRefresh(value => value + 1); }
    }
  };

  const apply = (event: FormEvent) => { event.preventDefault(); void run('settings'); };
  const reset = () => {
    if (!status) return;
    if (!pending && !window.confirm('Clear all register data and protocol state on every cluster node? Selected proposer settings are retained.')) return;
    const request = pending ?? { operationId: crypto.randomUUID().replaceAll('-', ''), expectedEpoch: status.epoch };
    setResetRequest(request);
    void run('reset', request);
  };

  return <details className="cluster-controls panel" onToggle={event => setOpen(event.currentTarget.open)}>
    <summary>Cluster controls</summary>
    {open && <div className="cluster-controls-body">
      <p>Apply proposer settings to every configured node. Reset quiesces every member, clears register data and protocol state, then activates a fresh cluster generation with the selected settings.</p>
      <p className="muted">Coordinator: {coordinator?.id ?? 'discovering'}{progress && <> · Reset phase: {progress.phase} · Operation <code>{progress.request.operationId}</code></>}</p>
      <form onSubmit={apply}>
        <fieldset disabled={!!busy || !!pending || !status}>
          <legend>Proposer settings for all nodes</legend>
          <div className="settings-grid">
            <label>Proposer mode<select value={settings.mode} onChange={event => setSettings({ ...settings, mode: event.target.value as ProposerSettings['mode'] })}>
              <option value="classic">Classic</option><option value="fast">Fast</option><option value="leader">Leadered</option>
              <option value="fast+leader">Fast + leadered</option><option value="adaptive">Adaptive</option>
            </select></label>
            {integers.map(([key, label, min]) => <label key={key}>{label}<input type="number" min={min} max={2147483647} step={1} required
              value={Number.isNaN(settings[key]) ? '' : settings[key]}
              onChange={event => setSettings({ ...settings, [key]: event.target.valueAsNumber })} /></label>)}
          </div>
          <div className="settings-checks">
            <label><input type="checkbox" checked={settings.startInFastMode} onChange={event => setSettings({ ...settings, startInFastMode: event.target.checked })} />Start adaptive mode fast</label>
            <label><input type="checkbox" checked={settings.enableForwarding} onChange={event => setSettings({ ...settings, enableForwarding: event.target.checked })} />Enable adaptive leader forwarding</label>
          </div>
          <p className="muted">Adaptive tuning is retained for future adaptive mode selections. Each node drains current register operations before applying its new policy.</p>
          <button type="submit" className="primary">Apply settings to all nodes</button>
        </fieldset>
      </form>
      <div className="write-actions">
        <button onClick={reset} disabled={!!busy || !status || resetBlocked}>{pending ? 'Resume cluster reset' : 'Clear entire cluster'}</button>
        <button onClick={() => setRefresh(value => value + 1)}>Refresh control status</button>
      </div>
      {resetBlocked && <p className="notice">Finish or reconcile the active register edit before resetting the cluster.</p>}
      {busy && <p role="status">{busy === 'settings' ? 'Applying settings across the cluster...' : 'Reset in progress. Nodes remain fenced until the all-member barriers complete.'}</p>}
      {error && <p role="alert" className="notice error">{error}</p>}
      {result && <p role="status" className={`notice ${result.state === 'complete' ? 'success' : 'warning'}`}>
        {result.state === 'complete' ? 'Cluster command completed on every node.' : 'Cluster command incomplete. Successful steps are retained; retry to reach unconfirmed nodes.'}
      </p>}
      {pending && !busy && <p className="notice warning">Resume operation <code>{pending.operationId}</code> to complete the reset. Prepared and cleared nodes retain their durable fence across restarts.</p>}
      <div className="table-scroll"><table aria-label="Cluster administration progress">
        <thead><tr><th>Node</th><th>Serving state / generation</th><th>Selected settings</th><th>Command outcome</th></tr></thead>
        <tbody>{store.nodes.map(({ peer }) => {
          const observed = statuses[peer.id];
          const outcome = result?.nodes.find(node => node.nodeId === peer.id) ?? progress?.nodes.find(node => node.nodeId === peer.id);
          return <tr key={peer.id}><th scope="row">{peer.id}</th>
            <td>{observed ? <>{observed.state}<br /><code>{observed.epoch}</code></> : 'Unconfirmed'}
              {failures[peer.id] && <p className="error-text">{failures[peer.id]}</p>}</td>
            <td>{observed ? <details><summary>{observed.settings.mode}</summary>
              <pre>{JSON.stringify(observed.settings, null, 2)}</pre></details> : 'Unconfirmed'}</td>
            <td>{outcome?.state ?? (busy ? 'In progress' : 'Ready')}{outcome?.error && <p className="error-text">{outcome.error}</p>}</td></tr>;
        })}</tbody>
      </table></div>
    </div>}
  </details>;
}
