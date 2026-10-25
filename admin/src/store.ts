import { Api, errorText } from './api';
import { eventKinds, exactKey, parseEvent } from './contracts';
import type { KeyPage, NodeEvent, NodeStatus, Peer, Register, Snapshot, Storage } from './contracts';
import { Resource } from './resource';

export interface Stream {
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void;
  close(): void;
}
export type StreamFactory = (url: string) => Stream;
export type Connection = 'connecting' | 'connected' | 'reconnecting' | 'stale' | 'unreachable';
export interface Node {
  peer: Peer;
  status: Resource<NodeStatus>;
  local?: Resource<Snapshot>;
  stream?: Stream;
  boot?: string;
  sequence?: bigint;
  lastSeen: number;
  opened: boolean;
  connection: Connection;
  storage?: Storage;
  lastOperation?: Extract<NodeEvent, { kind: 'operation' }> | null;
  error?: string;
  reset?: string;
  acceptedEnvelopes: Map<string, string>;
}

export const retention = 1000;

export class ClusterStore {
  readonly nodes: Node[] = [];
  events: NodeEvent[] = [];
  received = 0;
  discarded = 0;
  readonly receipts = new WeakMap<NodeEvent, number>();
  bootstrapError?: string;
  loading = true;
  selected: string | null = null;
  prefix = '';
  after: string | null = null;
  origin: string;
  inventory?: Resource<KeyPage>;
  current?: Resource<Register>;
  private pendingEvents: NodeEvent[] = [];
  private listeners = new Set<() => void>();
  private version = 0;
  private flushTimer?: ReturnType<typeof setTimeout>;
  private healthTimer?: ReturnType<typeof setInterval>;
  private bootstrap = new AbortController();
  private stopped = false;

  constructor(
    origin: string,
    readonly api = new Api(),
    private readonly streams: StreamFactory = (url) => new EventSource(url),
    private readonly now: () => number = () => performance.now(),
  ) { this.origin = origin; }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  };
  getSnapshot = (): number => this.version;
  changed = (): void => {
    if (this.flushTimer || this.stopped) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      const joined = [...this.events, ...this.pendingEvents];
      this.discarded += Math.max(0, joined.length - retention);
      this.events = joined.slice(-retention);
      this.pendingEvents = [];
      this.version++;
      this.listeners.forEach((listener) => listener());
    }, 100);
  };

  async start(): Promise<void> {
    const timeout = setTimeout(() => this.bootstrap.abort(new Error('Bootstrap timed out. Check the serving host.')), 15_000);
    try {
      const config = await this.api.status(this.origin, this.bootstrap.signal);
      if (this.stopped) return;
      for (const peer of config.peers) this.addNode(peer);
      this.setQuery('', null);
      this.healthTimer = setInterval(() => this.checkHealth(), 1000);
    } catch (error) {
      if (!this.stopped) this.bootstrapError = errorText(error);
    } finally {
      clearTimeout(timeout);
      this.loading = false;
      this.changed();
    }
  }

  private addNode(peer: Peer): void {
    const node: Node = {
      peer, lastSeen: this.now(), opened: false, connection: 'connecting', acceptedEnvelopes: new Map(),
      status: new Resource(async (signal) => {
        const status = await this.api.status(peer.httpEndpoint, signal);
        signal.throwIfAborted();
        if (status.nodeId !== peer.id) throw new Error(`Expected node ${peer.id}; host reports ${status.nodeId}.`);
        if (node.boot && status.bootId !== node.boot) throw new Error('Node restarted; awaiting stream resynchronization.');
        node.boot ??= status.bootId;
        const sequence = BigInt(status.cursor.split(':')[1]!);
        if (node.sequence === undefined || sequence >= node.sequence) node.storage = status.storage;
        if (status.lastOperation && (!node.lastOperation
          || BigInt(status.lastOperation.sequence) >= BigInt(node.lastOperation.sequence))) {
          node.lastOperation = status.lastOperation;
        }
        return status;
      }, this.changed),
    };
    this.nodes.push(node);
    node.status.invalidate();
    try {
      const stream = node.stream = this.streams(peer.eventsUrl);
      stream.addEventListener('open', () => {
        if (this.stopped) return;
        node.opened = true;
        node.lastSeen = this.now();
        node.connection = 'connected';
        node.error = undefined;
        node.status.invalidate();
        node.local?.invalidate();
        this.inventory?.invalidate();
        this.current?.invalidate();
        this.changed();
      });
      stream.addEventListener('error', () => {
        if (this.stopped) return;
        node.connection = node.opened ? 'reconnecting' : 'unreachable';
        node.error = 'Stream disconnected. The browser is reconnecting; process health is unconfirmed.';
        node.local?.markStale();
        this.inventory?.markStale();
        this.current?.markStale();
        this.changed();
      });
      for (const kind of eventKinds) stream.addEventListener(kind, (message) => {
        if (this.stopped) return;
        try { this.receive(node, parseEvent(message.data, kind, peer.id)); }
        catch (error) {
          node.error = errorText(error);
          node.connection = 'stale';
          this.changed();
        }
      });
    } catch (error) {
      node.connection = 'unreachable';
      node.error = errorText(error);
    }
  }

  private receive(node: Node, item: NodeEvent): void {
    const sequence = BigInt(item.sequence);
    if (node.boot && item.bootId !== node.boot && item.kind !== 'reset') return;
    if (item.kind !== 'reset' && item.kind !== 'heartbeat'
      && node.sequence !== undefined && sequence <= node.sequence) return;
    const recovering = node.connection === 'stale' || node.connection === 'unreachable' || node.connection === 'reconnecting';
    node.lastSeen = this.now();
    node.opened = true;
    node.connection = 'connected';
    node.error = undefined;
    if (recovering && item.kind !== 'reset') {
      node.status.invalidate();
      node.local?.invalidate();
      this.inventory?.invalidate();
      this.current?.invalidate();
    }
    if (item.kind === 'reset') {
      node.boot = item.bootId;
      node.sequence = sequence;
      node.reset = item.data.reason;
      node.acceptedEnvelopes.clear();
      node.storage = undefined;
      node.lastOperation = undefined;
      node.status.reset();
      node.local?.reset();
      this.resetInventory();
      this.current?.reset();
    } else if (item.kind === 'heartbeat') {
      node.storage = { status: item.data.storageStatus,
        error: item.data.storageStatus === node.storage?.status ? node.storage.error : undefined };
      this.changed();
      return;
    } else {
      node.boot ??= item.bootId;
      if (node.sequence !== undefined && sequence > node.sequence + 1n) {
        node.reset = 'Sequence gap: resynchronizing snapshots';
        node.status.reset();
        node.local?.reset();
        this.resetInventory();
        this.current?.reset();
      }
      node.sequence = sequence;
      if (item.kind === 'storage') node.storage = item.data;
      if (item.kind === 'operation') node.lastOperation = item;
      if (item.kind === 'operation' && item.data.operation === 'compare-exchange') {
        this.invalidateLogical(item.data.key);
      }
      if (item.kind === 'acceptor-state') {
        const key = item.data.key;
        if (!exactKey(key) || key.text === this.selected) node.local?.invalidate();
        // FCR2 contenders can share a revision. The bounded envelope preview includes their
        // distinct request identities; unchanged recovery envelopes suppress read feedback.
        const envelope = JSON.stringify([item.data.register?.version, item.data.acceptedValue]);
        if (!exactKey(key) || node.acceptedEnvelopes.get(key.text) !== envelope) {
          if (exactKey(key)) {
            node.acceptedEnvelopes.set(key.text, envelope);
            if (node.acceptedEnvelopes.size > 500) node.acceptedEnvelopes.delete(node.acceptedEnvelopes.keys().next().value!);
          }
          this.invalidateLogical(key);
        }
      }
    }
    this.received++;
    this.receipts.set(item, this.received);
    if (this.pendingEvents.length === retention) {
      this.pendingEvents.shift();
      this.discarded++;
    }
    this.pendingEvents.push(item);
    this.changed();
  }

  private invalidateLogical(key: { text: string; truncated: boolean; lossy: boolean } | null): void {
    if (!key || key.truncated || key.lossy) {
      this.restartInventory();
      this.current?.invalidate();
      return;
    }
    if (key.text.startsWith(this.prefix)) this.restartInventory();
    if (key.text === this.selected) this.current?.invalidate();
  }

  private restartInventory(): void {
    if (this.after !== null) this.setQuery(this.prefix, null);
    else this.inventory?.invalidate();
  }

  private resetInventory(): void {
    if (this.after !== null) this.setQuery(this.prefix, null);
    else this.inventory?.reset();
  }

  private checkHealth(): void {
    for (const node of this.nodes) {
      const interval = node.status.state.value?.heartbeatMilliseconds;
      if (interval === undefined) continue;
      if (this.now() - node.lastSeen > interval * 3) {
        const connection = node.connection === 'reconnecting' || node.connection === 'unreachable' ? 'unreachable' : 'stale';
        if (node.connection !== connection) {
          node.connection = connection;
          node.local?.markStale();
          this.current?.markStale();
          this.inventory?.markStale();
        }
      }
    }
    this.changed();
  }

  setOrigin(origin: string): void {
    this.origin = origin;
    this.setQuery(this.prefix, this.after);
    this.select(this.selected);
  }

  setQuery(prefix: string, after: string | null): void {
    this.prefix = prefix;
    this.after = after;
    this.inventory?.dispose();
    this.inventory = new Resource((signal) => this.api.keys(this.origin, prefix, after, signal), this.changed);
    this.inventory.invalidate();
  }

  select(key: string | null): void {
    this.selected = key;
    this.current?.dispose();
    this.current = key === null ? undefined : new Resource((signal) => this.api.read(this.origin, key, signal), this.changed);
    this.current?.invalidate();
    for (const node of this.nodes) {
      node.local?.dispose();
      node.local = key === null ? undefined : new Resource(async (signal) => {
        const snapshot = await this.api.state(node.peer.httpEndpoint, key, signal);
        if (snapshot.nodeId !== node.peer.id || snapshot.bootId !== node.boot) {
          throw new Error('Snapshot belongs to another node generation. Waiting for stream resynchronization.');
        }
        return snapshot;
      }, this.changed);
      node.local?.invalidate();
    }
    this.changed();
  }

  refresh(): void {
    this.inventory?.invalidate();
    this.current?.invalidate();
    for (const node of this.nodes) {
      node.status.invalidate();
      node.local?.invalidate();
    }
  }

  dispose(): void {
    this.stopped = true;
    this.bootstrap.abort();
    clearTimeout(this.flushTimer);
    clearInterval(this.healthTimer);
    this.inventory?.dispose();
    this.current?.dispose();
    for (const node of this.nodes) {
      node.stream?.close();
      node.status.dispose();
      node.local?.dispose();
    }
    this.listeners.clear();
  }
}
