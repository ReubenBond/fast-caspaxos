import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

export class Cluster {
  readonly ids = ['a', 'b', 'c'];
  private processes = new Map<number, ChildProcess>();
  private logs = new Map<number, string>();
  private ports: number[] = [];
  private data = '';
  private readonly dll = resolve('..', 'csharp', 'src', 'FastCASPaxos.Host', 'bin', 'Release', 'net11.0', 'publish', 'FastCASPaxos.Host.dll');
  origin(index: number): string { return `http://127.0.0.1:${this.ports[index + 3]}`; }

  async start(): Promise<void> {
    this.data = await mkdtemp(join(tmpdir(), 'fast-caspaxos-admin-'));
    // Keep reservations until all six independent ports have been allocated.
    const reservations = await Promise.all(Array.from({ length: 6 }, () => new Promise<ReturnType<typeof createServer>>((accept, reject) => {
      const server = createServer();
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => accept(server));
    })));
    this.ports = reservations.map((server) => {
      const address = server.address();
      if (!address || typeof address === 'string') throw new Error('Expected a TCP port reservation.');
      return address.port;
    });
    await Promise.all(reservations.map((server) => new Promise<void>((accept, reject) => server.close((error) => error ? reject(error) : accept()))));
    try { await Promise.all(this.ids.map((_, index) => this.startNode(index))); }
    catch (error) { await this.dispose(); throw error; }
  }

  async startNode(index: number): Promise<void> {
    const id = this.ids[index]!;
    const args = [
      this.dll, '--contentRoot', dirname(this.dll),
      `--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:${this.ports[index]}`,
      `--Kestrel:Endpoints:Rest:Url=${this.origin(index)}`,
      `--FastCASPaxos:AcceptorId=${id}`, `--FastCASPaxos:ProposerId=${index + 1}`,
      `--FastCASPaxos:AcceptorDatabase=${join(this.data, `acceptor-${id}.db`)}`,
      `--FastCASPaxos:ProposerDatabase=${join(this.data, `proposer-${id}.db`)}`,
      '--FastCASPaxos:HeartbeatInterval=00:00:00.250',
      '--FastCASPaxos:OperationTimeout=00:00:03',
      '--Logging:LogLevel:Default=Warning',
      ...this.ids.flatMap((peer, peerIndex) => [
        `--FastCASPaxos:Peers:${peerIndex}:Id=${peer}`,
        `--FastCASPaxos:Peers:${peerIndex}:Endpoint=http://127.0.0.1:${this.ports[peerIndex]}`,
        `--FastCASPaxos:Peers:${peerIndex}:HttpEndpoint=${this.origin(peerIndex)}`,
      ]),
    ];
    const child = spawn('dotnet', args, { cwd: dirname(this.dll), stdio: ['ignore', 'pipe', 'pipe'] });
    this.processes.set(index, child);
    this.logs.set(index, '');
    child.stdout?.on('data', (chunk: Buffer) => this.logs.set(index, (this.logs.get(index) + chunk.toString()).slice(-100_000)));
    child.stderr?.on('data', (chunk: Buffer) => this.logs.set(index, (this.logs.get(index) + chunk.toString()).slice(-100_000)));
    let processError: Error | undefined;
    child.once('error', (error) => { processError = error; });
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      if (processError) throw processError;
      if (child.exitCode !== null) throw new Error(`Node ${id} exited: ${this.logs.get(index)}`);
      try {
        const response = await fetch(`${this.origin(index)}/health/live`, { signal: AbortSignal.timeout(500) });
        if (response.ok) return;
      } catch (error) {
        if (!(error instanceof TypeError) && !(error instanceof DOMException)) throw error;
      }
      await delay(50);
    }
    throw new Error(`Node ${id} readiness timed out: ${this.logs.get(index)}`);
  }

  async stopNode(index: number): Promise<void> {
    const child = this.processes.get(index);
    if (!child) return;
    this.processes.delete(index);
    if (child.exitCode !== null) return;
    const exited = new Promise<void>((accept) => child.once('exit', () => accept()));
    child.kill();
    await exited;
  }

  async dispose(): Promise<void> {
    await Promise.all([...this.processes.keys()].map((index) => this.stopNode(index)));
    if (this.data) await rm(this.data, { recursive: true, force: true });
  }
}
