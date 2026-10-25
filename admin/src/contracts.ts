export interface Ballot { round: number; proposer: number }
export interface KeyPreview { text: string; length: number; truncated: boolean; lossy: boolean }
export interface PayloadPreview { length: number; base64: string; truncated: boolean }
export interface RegisterPreview {
  format: string;
  isPresent: boolean | null;
  version: string | null;
  value: PayloadPreview | null;
}
export interface Peer { id: string; grpcEndpoint: string; httpEndpoint: string; eventsUrl: string }
export interface Storage { status: string; error?: string | null }
export interface Protocol {
  operation: string; direction: string; phase: string; peerId: string | null;
  callId: string; operationId: string | null; key: KeyPreview | null;
  ballot: Ballot; nextBallot: Ballot; outcome?: string | null;
  promisedBallot?: Ballot | null; acceptedBallot?: Ballot | null; conflictingBallot?: Ballot | null;
  value?: PayloadPreview | null; error?: string | null;
}
export interface Operation {
  operation: string; operationId: string; key: KeyPreview | null; outcome: string;
  isPresent?: boolean | null;
  value?: PayloadPreview | null; version?: string | null; error?: string | null;
}
export interface LocalState {
  key: string; promisedBallot: Ballot; acceptedBallot: Ballot;
  acceptedValue: PayloadPreview | null; register: RegisterPreview | null;
}
export interface AcceptorState extends Omit<LocalState, 'key'> { key: KeyPreview }
export interface Repair {
  state: string; departing: boolean; targetReplicas: number;
  examined: number; repaired: number; pending: number; error: string | null; timestamp: string;
}
interface EventData {
  protocol: Protocol;
  operation: Operation;
  'acceptor-state': AcceptorState;
  storage: Storage;
  repair: Repair;
  reset: { reason: string };
  heartbeat: { storageStatus: string };
}
export type EventKind = keyof EventData;
export type NodeEvent = {
  [K in EventKind]: {
    schemaVersion: 1; nodeId: string; bootId: string; sequence: string;
    timestamp: string; kind: K; data: EventData[K]; id: string;
  }
}[EventKind];
export interface NodeStatus {
  schemaVersion: 1; nodeId: string; proposerId: number; bootId: string; startedAt: string;
  cursor: string; peers: Peer[]; storage: Storage;
  lastOperation: Extract<NodeEvent, { kind: 'operation' }> | null;
  heartbeatMilliseconds: number; historyCapacity: number;
}
export interface Snapshot {
  schemaVersion: 1; nodeId: string; bootId: string; cursor: string; timestamp: string;
  entry: LocalState | null;
}
export interface KeyPage { keys: string[]; continuation: string | null }
export type Register = { present: false } | { present: true; bytes: Uint8Array; etag: string };
export type WriteResult =
  | { kind: 'applied'; etag: string }
  | { kind: 'conflict'; message: string }
  | { kind: 'unknown'; message: string; currentEtag?: string }
  | { kind: 'rejected'; message: string };

export const eventKinds: EventKind[] = ['protocol', 'operation', 'acceptor-state', 'storage', 'repair', 'reset', 'heartbeat'];

export function parseEvent(text: string, kind: EventKind, nodeId: string): NodeEvent {
  const item = JSON.parse(text) as NodeEvent;
  if (item.schemaVersion !== 1 || item.nodeId !== nodeId || item.kind !== kind
    || typeof item.bootId !== 'string' || typeof item.sequence !== 'string' || !/^\d+$/.test(item.sequence)
    || item.id !== `${item.bootId}:${item.sequence}` || !item.data) {
    throw new Error('Unsupported or inconsistent node event. Refresh after checking host versions.');
  }
  return item;
}

export function exactKey(key: KeyPreview): boolean { return !key.truncated && !key.lossy; }
export function ballotText(ballot: Ballot): string { return `(${ballot.round}, ${ballot.proposer})`; }
