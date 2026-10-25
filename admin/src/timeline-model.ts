import { ballotText, exactKey } from './contracts';
import type { AcceptorState, NodeEvent, Protocol } from './contracts';

export interface TimelineEntry {
  event: NodeEvent;
  from: string | null;
  to: string | null;
  shape: 'request' | 'response' | 'local' | 'fault' | 'state' | 'reset';
  summary: string;
  facts: string[];
  elapsedMs?: number;
  historyGap: boolean;
}

export function endpoints(nodeId: string, data: Protocol): [string | null, string | null] {
  if (data.direction === 'local') return [nodeId, nodeId];
  return data.direction === 'inbound' ? [data.peerId, nodeId] : [nodeId, data.peerId];
}

function callKey(event: Extract<NodeEvent, { kind: 'protocol' }>): string {
  const data = event.data;
  return JSON.stringify([data.callId, data.operationId, data.operation, data.direction, data.peerId]);
}

function stateFacts(current: AcceptorState, previous?: AcceptorState): string[] {
  const facts: string[] = [];
  for (const [label, field] of [['Promised', 'promisedBallot'], ['Accepted', 'acceptedBallot']] as const) {
    const value = ballotText(current[field]);
    const before = previous && ballotText(previous[field]);
    facts.push(before && before !== value ? `${label}: ${before} -> ${value}` : `${label}: ${value}`);
  }
  const register = current.register;
  facts.push(register?.isPresent === true ? 'Register present'
    : register?.isPresent === false ? 'Register absent'
    : register?.format === 'opaque' ? 'Opaque accepted bytes' : 'Promise only');
  if (register?.version) {
    const before = previous?.register?.version;
    facts.push(before && before !== register.version
      ? `Version: ${before} -> ${register.version}` : `Version: ${register.version}`);
  }
  if (current.acceptedValue) facts.push(`Accepted value: ${current.acceptedValue.length} bytes${current.acceptedValue.truncated ? ' (preview truncated)' : ''}`);
  if (!previous) facts.push('First retained state for this key and node generation');
  return facts;
}

// Derive evidence from the entire retained window before filtering. A reset or sequence
// gap fences comparisons; independent host timestamps never determine display order.
export function buildTimeline(events: readonly NodeEvent[]): TimelineEntry[] {
  const nodes = new Map<string, {
    boot: string; sequence: bigint;
    states: Map<string, AcceptorState>;
    calls: Map<string, Extract<NodeEvent, { kind: 'protocol' }>>;
  }>();
  return events.map((event) => {
    const sequence = BigInt(event.sequence);
    let node = nodes.get(event.nodeId);
    const historyGap = !!node && node.boot === event.bootId && sequence > node.sequence + 1n;
    if (!node || node.boot !== event.bootId || event.kind === 'reset' || historyGap) {
      node = { boot: event.bootId, sequence, states: new Map(), calls: new Map() };
      nodes.set(event.nodeId, node);
    }
    node.sequence = sequence;
    const entry: TimelineEntry = {
      event, from: event.nodeId, to: event.nodeId, shape: 'state',
      summary: '', facts: [], historyGap,
    };
    switch (event.kind) {
      case 'protocol': {
        const data = event.data;
        const [caller, acceptor] = endpoints(event.nodeId, data);
        const request = data.phase === 'request-initiated' || data.phase === 'request-received';
        const response = data.phase === 'response-produced' || data.phase === 'response-observed';
        entry.summary = `${data.operation} ${data.phase}${data.outcome ? ` · ${data.outcome}` : ''}`;
        if (request || response) {
          [entry.from, entry.to] = request ? [caller, acceptor] : [acceptor, caller];
          entry.shape = caller === acceptor ? 'local' : request ? 'request' : 'response';
        } else {
          entry.shape = data.phase === 'fault-observed' ? 'fault' : 'local';
        }
        const id = callKey(event);
        if (request) node.calls.set(id, event);
        else if (response || data.phase === 'fault-observed') {
          const start = node.calls.get(id);
          if (start) {
            entry.elapsedMs = Date.parse(event.timestamp) - Date.parse(start.timestamp);
            node.calls.delete(id);
          }
        }
        if (data.operation !== 'read') entry.facts.push(`Ballot: ${ballotText(data.ballot)}`);
        if (data.operation === 'accept') entry.facts.push(`Next ballot: ${ballotText(data.nextBallot)}`);
        if (data.promisedBallot) entry.facts.push(`Promised: ${ballotText(data.promisedBallot)}`);
        if (data.acceptedBallot) entry.facts.push(`Accepted: ${ballotText(data.acceptedBallot)}`);
        if (data.conflictingBallot) entry.facts.push(`Conflicting: ${ballotText(data.conflictingBallot)}`);
        if (data.error) entry.facts.push(data.error);
        break;
      }
      case 'acceptor-state': {
        const data = event.data;
        const exact = exactKey(data.key);
        entry.summary = 'Committed acceptor state';
        entry.facts = stateFacts(data, exact ? node.states.get(data.key.text) : undefined);
        if (exact) node.states.set(data.key.text, data);
        else node.states.clear();
        break;
      }
      case 'operation':
        entry.summary = `${event.data.operation} · ${event.data.outcome}`;
        if (event.data.version) entry.facts.push(`Version: ${event.data.version}`);
        if (event.data.error) entry.facts.push(event.data.error);
        break;
      case 'storage':
        entry.summary = `Storage ${event.data.status}`;
        if (event.data.error) entry.facts.push(event.data.error);
        break;
      case 'repair':
        entry.summary = `Repair ${event.data.state}`;
        entry.facts.push(`Examined: ${event.data.examined} · Repaired: ${event.data.repaired} · Pending: ${event.data.pending}`,
          `Target replicas: ${event.data.targetReplicas}${event.data.departing ? ' · Departing' : ''}`);
        if (event.data.error) entry.facts.push(event.data.error);
        break;
      case 'reset':
        entry.shape = 'reset';
        entry.summary = `Resynchronize: ${event.data.reason}`;
        break;
      case 'heartbeat':
        entry.summary = `Heartbeat · storage ${event.data.storageStatus}`;
        break;
    }
    return entry;
  });
}
