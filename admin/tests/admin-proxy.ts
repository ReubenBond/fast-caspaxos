import { createServer, request as forwardRequest } from 'node:http';
import type { ClientRequest, IncomingMessage, OutgoingHttpHeaders, ServerResponse } from 'node:http';
import type { Socket } from 'node:net';

export type ParticipantCommand = 'settings' | 'reset/prepare' | 'reset/clear' | 'reset/activate';
type FaultKind = 'hold' | 'fail' | 'drop-response';
export interface AdminExchange {
  command: ParticipantCommand;
  body: unknown;
  state: 'received' | 'held' | 'forwarding' | 'responded' | 'failed' | 'dropped';
  upstream?: { status: number; body: string };
}

/** Fault on an actual participant HTTP request, never a simulated node. */
export class AdminFault {
  exchange?: AdminExchange;
  active = true;
  private unblock!: (forward: boolean) => void;
  readonly gate = new Promise<boolean>(resolve => { this.unblock = resolve; });

  constructor(readonly kind: FaultKind, readonly message: string) {}
  release(): void { this.active = false; this.unblock(true); }
  cancel(): void { this.active = false; this.unblock(false); }
}

/** CI-only proxy. Construction/importing does not bind a port; only start() does. */
export class AdministrationProxy {
  readonly exchanges: AdminExchange[] = [];
  private readonly faults = new Map<ParticipantCommand, AdminFault>();
  private readonly held = new Set<AdminFault>();
  private readonly sockets = new Set<Socket>();
  private readonly upstream = new Set<ClientRequest>();
  private readonly server = createServer((incoming, outgoing) => {
    void this.forward(incoming, outgoing).catch(error => {
      if (outgoing.destroyed) return;
      if (outgoing.headersSent) outgoing.destroy();
      else { outgoing.writeHead(502); outgoing.end(`Test proxy: ${String(error)}`); }
    });
  });
  private address?: string;

  constructor(private readonly target: string) {
    this.server.on('connection', socket => {
      this.sockets.add(socket);
      socket.once('close', () => this.sockets.delete(socket));
    });
  }

  get origin(): string {
    if (!this.address) throw new Error('Start the administration proxy before advertising it.');
    return this.address;
  }

  async start(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const failed = (error: Error) => reject(error);
      this.server.once('error', failed);
      this.server.listen(0, '127.0.0.1', () => {
        this.server.off('error', failed);
        resolve();
      });
    });
    const address = this.server.address();
    if (!address || typeof address === 'string') throw new Error('Expected an ephemeral TCP proxy address.');
    this.address = `http://127.0.0.1:${address.port}`;
  }

  holdNext(command: ParticipantCommand): AdminFault { return this.arm(command, 'hold'); }
  failNext(command: ParticipantCommand, message: string): AdminFault { return this.arm(command, 'fail', message); }
  // Keep dropping until explicitly released so transparent transport retries cannot
  // turn a deliberately lost acknowledgement into a successful coordinator result.
  dropResponsesUntilReleased(command: ParticipantCommand): AdminFault { return this.arm(command, 'drop-response'); }

  private arm(command: ParticipantCommand, kind: FaultKind, message = 'Injected participant failure'): AdminFault {
    if (this.faults.get(command)?.active) throw new Error(`A fault is already armed for ${command}.`);
    const fault = new AdminFault(kind, message);
    this.faults.set(command, fault);
    return fault;
  }

  private async forward(incoming: IncomingMessage, outgoing: ServerResponse): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
    const body = Buffer.concat(chunks);
    const path = new URL(incoming.url ?? '/', this.target).pathname;
    const command = incoming.method === 'POST' && path.startsWith('/v1/admin/internal/')
      ? path.slice('/v1/admin/internal/'.length) as ParticipantCommand : undefined;
    const exchange: AdminExchange | undefined = command
      ? { command, body: JSON.parse(body.toString('utf8')) as unknown, state: 'received' } : undefined;
    const armed = command ? this.faults.get(command) : undefined;
    const fault = armed?.active ? armed : undefined;
    if (exchange) this.exchanges.push(exchange);
    if (fault && command && exchange) {
      if (fault.kind !== 'drop-response') this.faults.delete(command);
      fault.exchange = exchange;
      if (fault.kind === 'hold') {
        exchange.state = 'held';
        this.held.add(fault);
        const forward = await fault.gate;
        this.held.delete(fault);
        if (!forward || outgoing.destroyed) {
          exchange.state = 'failed';
          if (!outgoing.destroyed) { outgoing.writeHead(503); outgoing.end('Test hold cancelled during cleanup.'); }
          return;
        }
      }
      if (fault.kind === 'fail') {
        exchange.state = 'failed';
        outgoing.writeHead(503, { 'Content-Type': 'text/plain' });
        outgoing.end(fault.message);
        return;
      }
    }
    if (outgoing.destroyed) return;
    if (exchange) exchange.state = 'forwarding';
    const headers: OutgoingHttpHeaders = { ...incoming.headers, host: new URL(this.target).host };
    // The proxy owns this hop's framing. Preserve Origin/CORS, ETags, and all application headers.
    for (const header of ['connection', 'keep-alive', 'proxy-connection', 'transfer-encoding', 'upgrade', 'te', 'trailer'])
      delete headers[header];
    headers['content-length'] = String(body.length);
    const upstream = forwardRequest(new URL(incoming.url ?? '/', this.target), {
      method: incoming.method, headers,
    }, response => {
      const responseChunks: Buffer[] = [];
      if (exchange) response.on('data', (chunk: Buffer) => responseChunks.push(Buffer.from(chunk)));
      response.once('error', error => outgoing.destroy(error));
      response.once('end', () => {
        if (exchange) {
          exchange.upstream = { status: response.statusCode ?? 502, body: Buffer.concat(responseChunks).toString('utf8') };
          exchange.state = fault?.kind === 'drop-response' ? 'dropped' : 'responded';
        }
        if (fault?.kind === 'drop-response') {
          // Consume the complete real Host response first: its durable phase succeeded,
          // but the coordinator receives no acknowledgement at all.
          outgoing.destroy();
        }
      });
      if (fault?.kind === 'drop-response') response.resume();
      else {
        outgoing.writeHead(response.statusCode ?? 502, response.headers);
        outgoing.flushHeaders(); // SSE must stream through the proxy without buffering.
        response.pipe(outgoing);
      }
    });
    this.upstream.add(upstream);
    upstream.once('close', () => this.upstream.delete(upstream));
    upstream.once('error', error => {
      if (exchange && !exchange.upstream) exchange.state = 'failed';
      if (outgoing.destroyed) return;
      if (outgoing.headersSent) outgoing.destroy(error);
      else { outgoing.writeHead(502); outgoing.end(`Owned node unavailable: ${error.message}`); }
    });
    outgoing.once('close', () => {
      if (!outgoing.writableFinished) upstream.destroy(); // Includes browser SSE disconnect and teardown.
    });
    upstream.end(body);
  }

  async dispose(): Promise<void> {
    for (const fault of this.held) fault.cancel();
    this.faults.clear();
    for (const request of this.upstream) request.destroy();
    if (!this.server.listening) return;
    const closed = new Promise<void>((resolve, reject) => this.server.close(error => error ? reject(error) : resolve()));
    this.server.closeAllConnections();
    for (const socket of this.sockets) socket.destroy();
    await closed;
  }
}
