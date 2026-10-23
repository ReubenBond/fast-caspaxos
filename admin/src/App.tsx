import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import type { FormEvent } from 'react';
import { errorText, fromBase64, textBytes, toBase64, utf8 } from './api';
import { ballotText } from './contracts';
import type { NodeEvent, PayloadPreview, Register, WriteResult } from './contracts';
import type { ResourceState } from './resource';
import { ClusterStore, retention } from './store';

type NavigationGuard = 'pending' | 'draft' | 'uncertain' | null;

function keyName(key: string): string { return key === '' ? '(empty key)' : key; }
function ResourceNotice({ state }: { state: ResourceState<unknown> | undefined }) {
  if (!state) return null;
  return <>
    {state.loading && <span className="muted" role="status">Refreshing...</span>}
    {state.error && <p className="notice error" role="alert">{state.error}</p>}
    {state.stale && state.value !== undefined && <span className="badge warning">Stale observation</span>}
  </>;
}

function Preview({ value }: { value: PayloadPreview | null | undefined }) {
  if (!value) return <span className="muted">No value</span>;
  return <span className="payload">
    <code>{value.base64 || '(empty bytes)'}</code>
    <span className="muted">{value.length} bytes · Base64{value.truncated ? ' · truncated preview' : ' · complete preview'}</span>
  </span>;
}

export function App({ store }: { store: ClusterStore }) {
  useSyncExternalStore(store.subscribe, store.getSnapshot);
  const [prefix, setPrefix] = useState('');
  const [pageStack, setPageStack] = useState<(string | null)[]>([]);
  const [create, setCreate] = useState(false);
  const [newKey, setNewKey] = useState('');
  const [createKey, setCreateKey] = useState<string | null>(null);
  const [navigationGuard, setNavigationGuard] = useState<NavigationGuard>(null);
  const [navigationNotice, setNavigationNotice] = useState<string>();
  useEffect(() => { void store.start(); return () => store.dispose(); }, [store]);
  useEffect(() => { if (store.after === null) setPageStack([]); }, [store.after]);
  useEffect(() => {
    if (!navigationGuard) return;
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [navigationGuard]);
  const navigate = (action: () => void) => {
    if (navigationGuard === 'pending' || navigationGuard === 'uncertain') {
      setNavigationNotice(navigationGuard === 'pending'
        ? 'A write is awaiting its outcome. Keep this editor open until it completes.'
        : 'Reconcile the unknown write outcome before leaving this editor. Your draft is preserved.');
      return;
    }
    if (navigationGuard === 'draft' && !window.confirm('Discard this unsaved draft and change the editor?')) return;
    setNavigationNotice(undefined);
    action();
  };
  const page = store.inventory?.state.value;
  const current = store.current?.state;
  const select = (key: string) => { setCreate(false); store.select(key); };
  const search = (event: FormEvent) => {
    event.preventDefault();
    setPageStack([]);
    store.setQuery(prefix, null);
  };

  return <div className="app">
    <a className="skip-link" href="#registers">Skip to registers</a>
    <header className="app-header">
      <div className="identity"><span className="mark" aria-hidden="true">F</span>
        <h1>Fast CASPaxos <span>Administration</span></h1></div>
      <div className="header-actions">
        <label>Request host
          <select value={store.origin} onChange={(event) => navigate(() => store.setOrigin(event.target.value))} aria-label="Request host">
            {!store.nodes.some((node) => node.peer.httpEndpoint === store.origin)
              && <option value={store.origin}>Serving host</option>}
            {store.nodes.map((node) => <option key={node.peer.id} value={node.peer.httpEndpoint}>{node.peer.id} · {node.peer.httpEndpoint}</option>)}
          </select>
        </label>
        <button onClick={() => store.refresh()} disabled={store.loading}>Refresh observations</button>
      </div>
    </header>
    {navigationNotice && <p className="notice warning" role="alert">{navigationNotice}</p>}

    {store.bootstrapError && <div className="notice error" role="alert">
      <strong>Cluster configuration unavailable.</strong> {store.bootstrapError}
      <button onClick={() => location.reload()}>Reload console</button>
    </div>}
    <section className="node-strip" aria-label="Node health">
      {store.loading && <p role="status">Discovering configured hosts...</p>}
      {store.nodes.map((node) => <article className="node" key={node.peer.id} data-testid={`node-${node.peer.id}`}>
        <div className="node-heading"><strong>Node {node.peer.id}</strong>
          <span className={`badge ${node.connection === 'connected' ? 'good' : 'warning'}`}><span className="dot" />{node.connection}</span></div>
        <a href={new URL('/admin/', node.peer.httpEndpoint).href}>{node.peer.httpEndpoint}</a>
        <div className="node-facts"><span>Storage <strong className={node.storage?.status === 'ready' ? 'good-text' : ''}>{node.storage?.status ?? 'unconfirmed'}</strong></span>
          <span>Sequence <code>{node.sequence?.toString() ?? '—'}</code></span></div>
        <details><summary>Node details{node.error || node.status.state.error ? ' · attention' : ''}</summary>
          <dl><dt>Boot</dt><dd><code>{node.boot ?? 'awaiting stream'}</code></dd>
            <dt>Proposer</dt><dd>{node.status.state.value?.proposerId ?? 'unconfirmed'}</dd>
            <dt>Stream</dt><dd>{node.peer.eventsUrl}</dd>
            <dt>Heartbeat</dt><dd>{node.status.state.value?.heartbeatMilliseconds ?? 'unknown'} ms</dd>
            <dt>History</dt><dd>{node.reset ?? 'awaiting initial snapshot'}</dd>
            <dt>Last quorum operation</dt><dd>{node.lastOperation
              ? <>{node.lastOperation.data.operation} · {node.lastOperation.data.outcome}
                <br /><time dateTime={node.lastOperation.timestamp}>{new Date(node.lastOperation.timestamp).toLocaleString()}</time></>
              : 'No completed operation observed'}</dd></dl>
          {node.storage?.error && <p className="error-text">{node.storage.error}</p>}
          {node.error && <p className="error-text">{node.error}</p>}
          <ResourceNotice state={node.status.state} />
        </details>
      </article>)}
    </section>

    <main id="registers" className="workbench">
      <section className="inventory panel" aria-labelledby="keys-title">
        <div className="panel-heading"><h2 id="keys-title">Registers</h2>
          <button className="primary compact" onClick={() => navigate(() => { setCreate(true); setCreateKey(null); })}>Create key</button></div>
        <form className="search" onSubmit={search}>
          <label htmlFor="prefix">Key prefix</label>
          <div className="input-action"><input id="prefix" value={prefix} onChange={(event) => setPrefix(event.target.value)} placeholder="Filter by prefix" />
            <button type="submit">Find</button></div>
        </form>
        <div className="inventory-meta"><span>{page ? `${page.keys.length} keys on this page` : 'Loading inventory'}</span><ResourceNotice state={store.inventory?.state} /></div>
        <nav aria-label="Register keys" className="key-list">
          {page?.keys.map((key) => <button key={key} className={store.selected === key && !create ? 'selected key-row' : 'key-row'}
            aria-current={store.selected === key && !create ? 'true' : undefined}
            onClick={() => { if (create || store.selected !== key) navigate(() => select(key)); }} title={key}>
            <span className="key-symbol" aria-hidden="true">#</span><span>{keyName(key)}</span>
          </button>)}
          {page?.keys.length === 0 && <p className="empty">No registers match this prefix. Create a key or search a different prefix.</p>}
        </nav>
        <div className="pagination">
          <button disabled={pageStack.length === 0} onClick={() => {
            const previous = pageStack.at(-1)!;
            setPageStack(pageStack.slice(0, -1));
            store.setQuery(store.prefix, previous);
          }}>Previous</button>
          <span>Page {pageStack.length + 1}</span>
          <button disabled={!page || page.continuation === null || store.inventory?.state.stale} onClick={() => {
            if (page && page.continuation !== null) {
              setPageStack([...pageStack, store.after]);
              store.setQuery(store.prefix, page.continuation);
            }
          }}>Next</button>
        </div>
      </section>

      <section className="inspector panel" aria-labelledby="inspector-title">
        <div className="panel-heading"><h2 id="inspector-title">{create ? 'Create register' : 'Register inspector'}</h2>
          {store.selected !== null && !create && <button className="compact" onClick={() => store.current?.invalidate()}>Read current value</button>}</div>
        {create ? <div className="inspector-body">
          <p>Create an absent key using <code>If-None-Match: *</code>.</p>
          <form onSubmit={(event) => { event.preventDefault(); navigate(() => setCreateKey(newKey)); }}>
            <label htmlFor="new-key">Complete key (empty is valid)</label>
            <div className="input-action"><input autoFocus id="new-key" value={newKey} onChange={(event) => setNewKey(event.target.value)} />
              <button type="submit">Set key</button></div>
          </form>
          {createKey !== null && <WriteEditor key={`create:${createKey}`} store={store} registerKey={createKey}
            initial={{ present: false }} onCreated={() => select(createKey)} onGuardChange={setNavigationGuard} />}
        </div> : store.selected === null ? <div className="empty inspector-empty">
          <h3>Select a register to inspect it</h3>
          <p>Compare its quorum-established value with each node's local accepted state, then follow the messages below.</p>
        </div> : <div className="inspector-body">
          <h3 className="selected-key" title={store.selected}>{keyName(store.selected)}</h3>
          <div className="section-label"><h3>Quorum-established value</h3><ResourceNotice state={current} /></div>
          {current?.value && <RegisterValue value={current.value} />}
          {current?.value && <WriteEditor key={`${store.origin}:${store.selected}`} store={store} registerKey={store.selected}
            initial={current.value} onGuardChange={setNavigationGuard} />}
          <section className="local-section" aria-labelledby="local-title">
            <h3 id="local-title">Local acceptor observations</h3>
            <p className="muted">Each row is one node's local snapshot. The established register value is shown above.</p>
            <div className="table-scroll" tabIndex={0} role="region" aria-label="Scrollable local acceptor observations"><table className="local-table">
              <thead><tr><th>Node</th><th>Promised</th><th>Accepted</th><th>Local accepted register</th></tr></thead>
              <tbody>{store.nodes.map((node) => {
                const state = node.local?.state;
                const entry = state?.value?.entry;
                return <tr key={node.peer.id}><th scope="row">{node.peer.id}<ResourceNotice state={state} /></th>
                  <td><code>{entry ? ballotText(entry.promisedBallot) : '—'}</code></td>
                  <td><code>{entry ? ballotText(entry.acceptedBallot) : '—'}</code></td>
                  <td>{entry ? <details><summary>{entry.register?.isPresent === true ? 'Present' : entry.register?.isPresent === false ? 'Absent' : entry.register?.format ?? 'Promise only'}</summary>
                    <dl><dt>Version</dt><dd><code>{entry.register?.version ?? 'none'}</code></dd>
                      <dt>Register payload</dt><dd><Preview value={entry.register?.value} /></dd>
                      <dt>Accepted bytes</dt><dd><Preview value={entry.acceptedValue} /></dd>
                      <dt>Snapshot cursor</dt><dd><code>{state?.value?.cursor}</code></dd></dl>
                  </details> : state?.value ? 'No local entry' : 'Awaiting snapshot'}</td>
                </tr>;
              })}</tbody>
            </table></div>
          </section>
        </div>}
      </section>
    </main>
    <Timeline store={store} />
    <footer>Direct SSE from every configured host. Connectivity and storage are separate observations.</footer>
  </div>;
}

function RegisterValue({ value }: { value: Register }) {
  const [representation, setRepresentation] = useState<'text' | 'base64'>('text');
  if (!value.present) return <p className="notice">The quorum-established register is absent.</p>;
  const text = utf8(value.bytes);
  const binary = representation === 'base64' || text === null;
  return <div className="register-value">
    <div className="value-toolbar"><span className="badge neutral">Present · {value.bytes.length} bytes</span>
      <label>Display <select aria-label="Display" value={binary ? 'base64' : 'text'} onChange={(event) => setRepresentation(event.target.value as 'text' | 'base64')}>
        <option value="text" disabled={text === null}>UTF-8 text</option><option value="base64">Base64</option>
      </select></label></div>
    {text === null && <p className="muted">Binary value shown losslessly as Base64.</p>}
    <pre className="value-content" tabIndex={0} aria-label="Current value">{(binary ? toBase64(value.bytes) : text) || '(empty bytes)'}</pre>
    <label>ETag <code className="etag">{value.etag}</code></label>
  </div>;
}

export function WriteEditor({ store, registerKey, initial, onCreated, onGuardChange }: {
  store: ClusterStore; registerKey: string; initial: Register; onCreated?: () => void;
  onGuardChange?: (guard: NavigationGuard) => void;
}) {
  const original = useRef(initial.present ? initial.bytes : new Uint8Array());
  const initialText = utf8(original.current);
  const [mode, setMode] = useState<'text' | 'base64'>(initialText === null ? 'base64' : 'text');
  const [draft, setDraft] = useState(initialText ?? toBase64(original.current));
  const [originalDraft, setOriginalDraft] = useState(draft);
  const [expected, setExpected] = useState<string | null>(initial.present ? initial.etag : null);
  const [result, setResult] = useState<WriteResult>();
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [dirty, setDirty] = useState(false);
  const [reconcile, setReconcile] = useState<Register | undefined>();
  const [refreshRequested, setRefreshRequested] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const mounted = useRef(true);
  useEffect(() => () => { mounted.current = false; controller.current?.abort(); }, []);
  useEffect(() => {
    onGuardChange?.(busy ? 'pending' : result?.kind === 'unknown' ? 'uncertain' : dirty || result?.kind === 'conflict' ? 'draft' : null);
    return () => onGuardChange?.(null);
  }, [busy, dirty, result, onGuardChange]);
  const bytes = () => draft === originalDraft ? original.current : mode === 'text' ? textBytes(draft) : fromBase64(draft);
  const blocked = result?.kind === 'unknown' || result?.kind === 'conflict';
  const current = store.selected === registerKey ? store.current?.state : undefined;
  const reviewed = refreshRequested && current?.value && !current.stale && !current.loading && current.value !== reconcile;
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    let payload: Uint8Array;
    try { payload = bytes(); textBytes(registerKey); }
    catch (failure) { setError(errorText(failure)); return; }
    setError(undefined);
    setBusy(true);
    const request = controller.current = new AbortController();
    const timeout = setTimeout(() => request.abort(), 15_000);
    const outcome = await store.api.write(store.origin, registerKey, payload, expected, request.signal);
    clearTimeout(timeout);
    if (!mounted.current) return;
    setBusy(false);
    setResult(outcome);
    setRefreshRequested(false);
    if (outcome.kind === 'applied') {
      original.current = payload;
      setOriginalDraft(draft);
      setDirty(false);
      setExpected(outcome.etag);
      store.refresh();
      onCreated?.();
    }
  };
  return <form className="write-editor" onSubmit={(event) => { void submit(event); }}>
    <div className="section-label"><h3>Conditional edit</h3><label>Encoding <select aria-label="Encoding" value={mode} disabled={busy} onChange={(event) => {
      try {
        const value = bytes();
        const nextMode = event.target.value as 'text' | 'base64';
        const next = nextMode === 'text' ? utf8(value) : toBase64(value);
        if (next === null) throw new Error('These bytes are not valid UTF-8. Keep Base64 to preserve them.');
        original.current = value;
        setOriginalDraft(next);
        setDraft(next);
        setMode(nextMode);
        setError(undefined);
      } catch (failure) { setError(errorText(failure)); }
    }}><option value="text">UTF-8 text</option><option value="base64">Base64</option></select></label></div>
    <label htmlFor="draft">New complete value for <code>{keyName(registerKey)}</code></label>
    <textarea id="draft" value={draft} disabled={busy} spellCheck={false} rows={4}
      onChange={(event) => { setDraft(event.target.value); setDirty(true); }} />
    <p className="precondition">{expected === null ? <>Create precondition <code>If-None-Match: *</code></> : <>Expected ETag <code>{expected}</code></>}</p>
    {error && <p className="notice error" role="alert">{error}</p>}
    {result && <div className={`notice ${result.kind === 'applied' ? 'success' : 'warning'}`} role="status">
      <strong>{result.kind === 'applied' ? 'Write applied.' : result.kind === 'unknown' ? 'Write outcome unknown.' : result.kind === 'conflict' ? 'Version conflict.' : 'Write rejected.'}</strong>
      {result.kind !== 'applied' && <> {result.message}</>}
      {result.kind === 'unknown' && result.currentEtag && <p>Later observed ETag: <code>{result.currentEtag}</code></p>}
      {blocked && <p>Your draft and original precondition are preserved. Read the established state and use application semantics to reconcile.</p>}
    </div>}
    <div className="write-actions">
      <button className="primary" type="submit" disabled={busy || blocked}>{busy ? 'Awaiting outcome...' : expected === null ? 'Create register' : 'Compare and swap'}</button>
      {blocked && <button type="button" onClick={() => {
        setReconcile(current?.value);
        setRefreshRequested(true);
        if (store.selected !== registerKey) store.select(registerKey);
        else store.current?.invalidate();
      }}>Refresh to reconcile</button>}
      {blocked && reviewed && <button type="button" onClick={() => {
        const observed = current.value!;
        setExpected(observed.present ? observed.etag : null);
        setResult(undefined);
        setRefreshRequested(false);
      }}>Use observed version for this draft</button>}
    </div>
    {blocked && reviewed && <RegisterValue value={current.value!} />}
  </form>;
}

export function Timeline({ store }: { store: ClusterStore }) {
  const [paused, setPaused] = useState<{ events: NodeEvent[]; received: number } | null>(null);
  const [host, setHost] = useState('');
  const [kind, setKind] = useState('');
  const [query, setQuery] = useState('');
  const events = paused?.events ?? store.events;
  const filtered = useMemo(() => events.filter((event) =>
    (!host || event.nodeId === host) && (!kind || event.kind === kind)
    && (!query || JSON.stringify(event).toLowerCase().includes(query.toLowerCase()))).slice(-200).reverse(), [events, host, kind, query]);
  return <section className="timeline panel" aria-labelledby="timeline-title">
    <div className="panel-heading"><div><h2 id="timeline-title">Live messages</h2>
      <p className="muted">Arrival order · per-node sequences preserve order · correlate by call / operation ID</p></div>
      <button aria-pressed={paused !== null} onClick={() => setPaused(paused ? null : { events: store.events, received: store.received })}>{paused ? 'Resume timeline' : 'Pause timeline'}</button></div>
    <div className="timeline-toolbar">
      <label>Node<select aria-label="Node" value={host} onChange={(event) => setHost(event.target.value)}><option value="">All nodes</option>
        {store.nodes.map((node) => <option key={node.peer.id}>{node.peer.id}</option>)}</select></label>
      <label>Event<select aria-label="Event" value={kind} onChange={(event) => setKind(event.target.value)}><option value="">All events</option>
        {['protocol', 'operation', 'acceptor-state', 'storage', 'reset'].map((value) => <option key={value}>{value}</option>)}</select></label>
      <label className="event-search">Filter messages<input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Key, peer, call ID, outcome..." /></label>
      <span className="muted">{events.length} / {retention} retained · {store.discarded} evicted</span>
    </div>
    {paused && <p className="pause-note" role="status">Timeline paused · {store.received - paused.received} new events. Node health and register observations remain live.</p>}
    <div className="event-table-wrap" tabIndex={0} aria-label="Scrollable event timeline"><table className="event-table">
      <thead><tr><th>Node / sequence</th><th>Event</th><th>Message / key</th><th>Outcome / details</th></tr></thead>
      <tbody>{filtered.map((event) => <EventRow key={store.receipts.get(event) ?? `${event.nodeId}:${event.id}:${event.kind}:${event.timestamp}`} event={event} />)}</tbody>
    </table>
    {filtered.length === 0 && <p className="empty">Waiting for matching events. Heartbeats keep quiet node connections current.</p>}</div>
    <div className="timeline-foot">Showing latest {filtered.length} matching events (up to 200). Reset events mark a new observation baseline or a history gap.</div>
  </section>;
}

function EventRow({ event }: { event: NodeEvent }) {
  const data = event.data;
  const key = 'key' in data ? data.key : null;
  const message = event.kind === 'protocol' ? `${event.data.direction} ${event.data.operation} ${event.data.phase} · peer ${event.data.peerId ?? 'local'}`
    : event.kind === 'operation' ? event.data.operation
    : event.kind === 'reset' ? `Resynchronize: ${event.data.reason}`
    : event.kind === 'storage' ? event.data.status : 'Local state changed';
  const outcome = 'outcome' in data ? data.outcome : 'error' in data ? data.error : null;
  return <tr className={event.kind === 'reset' ? 'reset-row' : ''}>
    <td><strong>{event.nodeId}</strong> <code>#{event.sequence}</code><time dateTime={event.timestamp}>{new Date(event.timestamp).toLocaleTimeString()}</time></td>
    <td><span className="event-kind">{event.kind}</span></td>
    <td><span>{message}</span>{key && <code className="event-key">{keyName(key.text)}{key.truncated ? ' [truncated key]' : ''}{key.lossy ? ' [lossy key]' : ''}</code>}
      {event.kind === 'protocol' && <span className="muted">Ballot {ballotText(event.data.ballot)} → {ballotText(event.data.nextBallot)}</span>}</td>
    <td><details><summary>{outcome || 'Inspect event'}</summary><pre>{JSON.stringify(event, null, 2)}</pre></details></td>
  </tr>;
}
