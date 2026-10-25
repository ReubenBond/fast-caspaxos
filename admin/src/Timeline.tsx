import { useEffect, useMemo, useRef, useState } from 'react';
import type { NodeEvent } from './contracts';
import { ClusterStore, retention } from './store';
import { buildTimeline, endpoints, formatMessage } from './timeline-model';
import type { TimelineEntry } from './timeline-model';

function nodeLabel(id: string | null): string { return id === null ? 'Unknown peer' : `Node ${id}`; }

function MessageGlyph({ entry, lanes }: { entry: TimelineEntry; lanes: (string | null)[] }) {
  const width = lanes.length * 240;
  const x = (id: string | null) => lanes.indexOf(id) * 240 + 120;
  const from = x(entry.from);
  const to = x(entry.to);
  const sign = to >= from ? 1 : -1;
  const loop = from === to;
  const message = formatMessage(entry.event);
  const labelLeft = loop ? from - 120 : Math.min(from, to);
  const labelWidth = loop ? 240 : Math.abs(from - to);
  const arrow = entry.shape === 'request' || entry.shape === 'response' || entry.shape === 'local';
  const label = arrow
    ? `${nodeLabel(entry.from)} to ${nodeLabel(entry.to)}: ${entry.summary}`
    : `${nodeLabel(entry.event.nodeId)}: ${entry.summary}`;
  return <td colSpan={lanes.length} className={`sequence-path ${entry.shape}`}>
    <div className="lane-guides" aria-hidden="true" style={{ gridTemplateColumns: `repeat(${lanes.length}, 1fr)` }}>
      {lanes.map((lane) => <span key={JSON.stringify(lane)} />)}
    </div>
    <svg className="sequence-glyph" viewBox={`0 0 ${width} 48`} preserveAspectRatio="none" role="img" aria-label={label}>
      {arrow ? <>
        <path className={entry.event.kind === 'protocol' && entry.event.data.phase === 'request-initiated' ? 'initiated-path' : ''}
          d={loop ? `M ${from} 26 h 30 v 12 h -30` : `M ${from} 32 H ${to}`} />
        <path d={loop ? `M ${to + 7} 33 L ${to} 38 L ${to + 7} 43`
          : `M ${to - sign * 8} 27 L ${to} 32 L ${to - sign * 8} 37`} />
        <circle cx={from} cy={loop ? 26 : 32} r="3" />
      </> : entry.shape === 'fault' ? <path d={`M ${from - 6} 26 l 12 12 m -12 0 l 12 -12`} />
        : entry.shape === 'reset' ? <path d={`M ${from - 10} 29 h 20 m -20 6 h 20`} />
        : <rect x={from - 5} y="27" width="10" height="10" />}
      <circle className="observer-dot" cx={x(entry.event.nodeId)} cy="44" r="2" />
    </svg>
    <span className="message-label" title={message.full}
      style={{ left: `${labelLeft / width * 100}%`, width: `${labelWidth / width * 100}%` }}>{message.text}</span>
  </td>;
}

function EventRow({ entry, lanes, trace }: {
  entry: TimelineEntry; lanes: (string | null)[]; trace: (id: string) => void;
}) {
  const { event } = entry;
  const data = event.data;
  const key = 'key' in data ? data.key : null;
  const [expanded, setExpanded] = useState(false);
  return <tr className={event.kind === 'reset' || entry.historyGap ? 'reset-row' : ''}>
    <td className="sequence-time"><time dateTime={event.timestamp} title={event.timestamp}>{new Date(event.timestamp).toISOString().slice(11, 23)}</time>
      <span className="sequence-meta" title={`${nodeLabel(event.nodeId)} #${event.sequence}`}>
        {event.nodeId} <code>#{event.sequence}</code>
        {entry.elapsedMs !== undefined && <span className={entry.elapsedMs < 0 ? 'error-text' : 'elapsed'}
          title={entry.elapsedMs < 0 ? 'Host clock moved backwards' : `${entry.elapsedMs} ms host elapsed`}>
          {' · '}{entry.elapsedMs < 0 ? 'clock reversed' : `${entry.elapsedMs} ms`}
        </span>}
      </span>
    </td>
    <MessageGlyph entry={entry} lanes={lanes} />
    <td className="sequence-evidence">
      {entry.historyGap && <p className="error-text">Sequence gap: comparison baseline reset</p>}
      <details onToggle={(change) => setExpanded(change.currentTarget.open)}>
        <summary aria-label="Inspect event" title={entry.summary}>{entry.summary}</summary>
        {expanded && <>
          {entry.facts.map((fact) => <span className="event-fact" key={fact}>{fact}</span>)}
          <div className="trace-actions">
            {event.kind === 'protocol' && <button className="compact" onClick={() => trace(event.data.callId)}>Trace call</button>}
            {'operationId' in data && data.operationId && <button className="compact" onClick={() => { if (data.operationId) trace(data.operationId); }}>Trace operation</button>}
          </div>
          <pre>{JSON.stringify(event, null, 2)}</pre>
        </>}
      </details>
      <div className="event-context"><span className="event-kind">{event.kind}</span>
        {key && <code className="event-key" title={key.text}>{key.text === '' ? '(empty key)' : key.text}{key.truncated ? ' [truncated key]' : ''}{key.lossy ? ' [lossy key]' : ''}</code>}
      </div>
    </td>
  </tr>;
}

export function Timeline({ store }: { store: ClusterStore }) {
  const [paused, setPaused] = useState<{ events: NodeEvent[]; received: number; discarded: number } | null>(null);
  const [host, setHost] = useState('');
  const [kind, setKind] = useState('');
  const [query, setQuery] = useState('');
  const [follow, setFollow] = useState(true);
  const viewport = useRef<HTMLDivElement>(null);
  const events = paused?.events ?? store.events;
  const entries = useMemo(() => buildTimeline(events), [events]);
  const filtered = useMemo(() => entries.filter(({ event }) =>
    (!host || event.nodeId === host || (event.kind === 'protocol' && event.data.peerId === host))
    && (!kind || event.kind === kind)
    && (!query || JSON.stringify(event).toLowerCase().includes(query.toLowerCase()))), [entries, host, kind, query]);
  const visible = useMemo(() => filtered.slice(-200).reverse(), [filtered]);
  const lanes = useMemo(() => {
    const result: (string | null)[] = store.nodes.map((node) => node.peer.id);
    for (const { event } of entries) {
      for (const id of event.kind === 'protocol' ? endpoints(event.nodeId, event.data) : [event.nodeId]) {
        if (!result.includes(id)) result.push(id);
      }
    }
    return result;
  }, [entries, store.nodes]);
  useEffect(() => {
    if (follow && !paused && viewport.current) viewport.current.scrollTop = 0;
  }, [visible, follow, paused]);
  const trace = (id: string) => { setHost(''); setKind(''); setQuery(id); };
  return <section className="timeline panel" aria-labelledby="timeline-title">
    <div className="panel-heading"><div><h2 id="timeline-title">Cluster timeline</h2>
      <p className="muted">Newest observations first. Arrows show message direction; expand a row for full evidence.</p></div>
      <button aria-pressed={paused !== null} onClick={() => setPaused(paused ? null
        : { events: store.events, received: store.received, discarded: store.discarded })}>{paused ? 'Resume timeline' : 'Pause timeline'}</button></div>
    <div className="timeline-toolbar">
      <label>Node<select aria-label="Node" value={host} onChange={(change) => setHost(change.target.value)}><option value="">All nodes</option>
        {store.nodes.map((node) => <option key={node.peer.id}>{node.peer.id}</option>)}</select></label>
      <label>Event<select aria-label="Event" value={kind} onChange={(change) => setKind(change.target.value)}><option value="">All events</option>
        {['protocol', 'operation', 'acceptor-state', 'storage', 'repair', 'reset'].map((value) => <option key={value}>{value}</option>)}</select></label>
      <label className="event-search">Filter messages<input value={query} onChange={(change) => setQuery(change.target.value)} placeholder="Key, peer, call ID, outcome..." /></label>
      {query && <button onClick={() => setQuery('')}>Clear filter</button>}
      <label className="follow-control"><input type="checkbox" checked={follow} onChange={(change) => setFollow(change.target.checked)} />Follow latest</label>
      <span className="muted">{events.length} / {retention} retained · {paused?.discarded ?? store.discarded} evicted</span>
    </div>
    <div className="timeline-legend">
      <span><i className="legend-request" aria-hidden="true" />Request</span>
      <span><i className="legend-response" aria-hidden="true" />Response</span>
      <span><i className="legend-state" aria-hidden="true" />State / outcome</span>
      <span><i className="legend-fault" aria-hidden="true">×</i>Fault</span>
      <span>Dashed: initiated attempt · Solid: received / completed · Dot: observing host</span>
    </div>
    {paused && <p className="pause-note" role="status">Timeline paused · {store.received - paused.received} new events. Node health and register observations remain live.</p>}
    <div className="event-table-wrap" ref={viewport} tabIndex={0} role="region" aria-label="Scrollable event timeline"
      onScroll={() => {
        const element = viewport.current;
        if (element && element.scrollTop > 24) setFollow(false);
      }}>
      <table className="event-table" style={{ minWidth: `${400 + lanes.length * 240}px` }}>
        <caption className="visually-hidden">Cluster message sequence and committed state transitions, newest to oldest</caption>
        <colgroup><col className="time-column" />{lanes.map((lane) => <col key={JSON.stringify(lane)} />)}<col className="evidence-column" /></colgroup>
        <thead><tr><th>Host time (UTC)<span className="muted">Sequence / elapsed</span></th>
          {lanes.map((lane) => <th className="lane-heading" key={JSON.stringify(lane)} scope="col">{nodeLabel(lane)}</th>)}
          <th>Message / state change</th></tr></thead>
        <tbody>{visible.map((entry) => <EventRow key={store.receipts.get(entry.event) ?? `${entry.event.nodeId}:${entry.event.id}:${entry.event.kind}:${entry.event.timestamp}`}
          entry={entry} lanes={lanes} trace={trace} />)}</tbody>
      </table>
      {visible.length === 0 && <p className="empty">Waiting for matching events. Perform a register read or write to trace activity across the cluster.</p>}
    </div>
    <div className="timeline-foot">Showing latest {visible.length} of {filtered.length} matching events (up to 200), newest first.
      {' '}Host elapsed pairs a call's start and completion on the same node and boot, at millisecond clock resolution.
      {' '}Host clocks are independent; row spacing represents observation order. State comparisons use retained, uninterrupted per-node history.</div>
  </section>;
}
