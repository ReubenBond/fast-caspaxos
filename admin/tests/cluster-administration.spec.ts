import { randomUUID } from 'node:crypto';
import { test as base, expect } from '@playwright/test';
import type { APIRequestContext } from '@playwright/test';
import type { ClusterAdminResult, NodeAdministration, NodeStatus, ProposerSettings, ResetRequest, Snapshot } from '../src/contracts';
import { Cluster } from './cluster';
import type { ParticipantCommand } from './admin-proxy';

// These tests require the published real Host used by the existing browser CI.
// Local authoring validation is tsc only: no test server/process starts at import time.
const test = base.extend<{ cluster: Cluster }>({
  cluster: async ({}, use, testInfo) => {
    const cluster = new Cluster({ administrationProxies: true });
    try {
      await cluster.start();
      await use(cluster);
    } finally {
      try {
        await testInfo.attach('Owned Host logs and participant HTTP history', {
          body: JSON.stringify(cluster.diagnostics(), null, 2), contentType: 'application/json',
        });
      } finally { await cluster.dispose(); }
    }
  },
});
test.setTimeout(120_000);

const selected: ProposerSettings = {
  mode: 'adaptive', startInFastMode: false, enableForwarding: false,
  conflictDebtIncrement: 7, conflictDebtDecay: 2, fastReopenThreshold: 3, leaderMissThreshold: 5,
};
const indexes = [0, 1, 2] as const;
const oldValues = [
  { key: 'reset/empty', bytes: Buffer.alloc(0) },
  { key: 'reset/space + é', bytes: Buffer.from([0, 10, 255]) },
];
const phaseState = { prepare: 'prepared', clear: 'cleared', activate: 'active' } as const;
type ResetPhase = keyof typeof phaseState;

function registerUrl(cluster: Cluster, node: number, key: string): string {
  return `${cluster.origin(node)}/v1/registers/value?key=${encodeURIComponent(key)}`;
}
async function control(request: APIRequestContext, cluster: Cluster, node: number): Promise<NodeAdministration> {
  const response = await request.get(`${cluster.origin(node)}/v1/admin/control`, { timeout: 5000 });
  expect(response.status(), await response.text()).toBe(200);
  expect(response.headers()['cache-control']).toBe('no-store');
  const status = await response.json() as NodeAdministration;
  expect(status.schemaVersion).toBe(1);
  expect(status.nodeId).toBe(cluster.ids[node]);
  expect(status.coordinatorId).toBe('a');
  return status;
}
async function controls(request: APIRequestContext, cluster: Cluster) {
  return Promise.all(indexes.map(node => control(request, cluster, node)));
}
async function local(request: APIRequestContext, cluster: Cluster, node: number, key: string): Promise<Snapshot> {
  const response = await request.get(`${cluster.origin(node)}/v1/admin/state?key=${encodeURIComponent(key)}`, { timeout: 5000 });
  expect(response.status(), await response.text()).toBe(200);
  const snapshot = await response.json() as Snapshot;
  expect(snapshot.nodeId).toBe(cluster.ids[node]);
  return snapshot;
}
async function write(request: APIRequestContext, cluster: Cluster, node: number, key: string, bytes: Buffer): Promise<string> {
  const response = await request.put(registerUrl(cluster, node, key), {
    data: bytes, headers: { 'Content-Type': 'application/octet-stream', 'If-None-Match': '*' },
    timeout: 15_000, maxRetries: 0,
  });
  expect(response.status(), await response.text()).toBe(201);
  const etag = response.headers().etag;
  expect(etag).toMatch(/^"[^"]+"$/);
  return etag!;
}
async function expectValue(request: APIRequestContext, cluster: Cluster, node: number, key: string, bytes: Buffer, etag?: string) {
  const response = await request.get(registerUrl(cluster, node, key), { timeout: 15_000 });
  expect(response.status(), await response.text()).toBe(200);
  expect(await response.body()).toEqual(bytes);
  if (etag !== undefined) expect(response.headers().etag).toBe(etag);
}
async function seedEveryMember(request: APIRequestContext, cluster: Cluster) {
  for (const { key, bytes } of oldValues) await write(request, cluster, 0, key, bytes);
  // A register write only promises a quorum; its third RPC can be cancelled.
  // Request real production repair (target 3 in this opt-in fixture), rather
  // than relying on a lucky write schedule to seed all local acceptors.
  const staged = await request.post(`${cluster.origin(0)}/v1/admin/repair`, { timeout: 15_000, maxRetries: 0 });
  expect(staged.status(), await staged.text()).toBe(202);
  for (const { key, bytes } of oldValues) {
    await expect.poll(async () => Promise.all(indexes.map(async node =>
      (await local(request, cluster, node, key)).entry?.register?.value?.base64)),
    { message: `All owned acceptors contain seeded key ${key}` }).toEqual(indexes.map(() => bytes.toString('base64')));
  }
  await expect.poll(async () => Promise.all(indexes.map(async node => {
    const response = await request.get(`${cluster.origin(node)}/v1/admin/repair`, { timeout: 5000 });
    expect(response.status()).toBe(200);
    const status = await response.json() as { lastRepair: { state: string } | null };
    return status.lastRepair?.state;
  })), { message: 'Seed replication completes before the reset fault schedule starts' }).toEqual(['complete', 'complete', 'complete']);
}
async function expectOldLocalValues(request: APIRequestContext, cluster: Cluster, nodes: readonly number[]) {
  for (const node of nodes)
    for (const { key, bytes } of oldValues) {
      const snapshot = await local(request, cluster, node, key);
      expect(snapshot.entry?.key).toBe(key);
      expect(snapshot.entry?.register?.isPresent).toBe(true);
      expect(snapshot.entry?.register?.value?.base64).toBe(bytes.toString('base64'));
    }
}
async function expectEmptyLocalInventories(request: APIRequestContext, cluster: Cluster, nodes: readonly number[]) {
  for (const node of nodes) {
    const response = await request.get(`${cluster.origin(node)}/v1/admin/states?limit=1`, { timeout: 5000 });
    expect(response.status(), await response.text()).toBe(200);
    expect(await response.json()).toMatchObject({ schemaVersion: 1, nodeId: cluster.ids[node], entries: [], continuation: null });
  }
}
async function expectComplete(request: APIRequestContext, cluster: Cluster, result: ClusterAdminResult, reset: ResetRequest) {
  expect(result).toEqual({
    state: 'complete', reset,
    nodes: cluster.ids.map(nodeId => ({ nodeId, state: 'active', error: null })),
  });
  for (const status of await controls(request, cluster)) {
    expect(status.epoch).toBe((BigInt(reset.expectedEpoch) + 1n).toString());
    expect(status.state).toBe('active');
    expect(status.pendingReset).toBeNull();
  }
  expect((await control(request, cluster, 0)).clusterReset).toEqual({
    request: reset, phase: 'complete', nodes: result.nodes,
  });
}
async function resetRequest(request: APIRequestContext, cluster: Cluster): Promise<ResetRequest> {
  return { operationId: randomUUID().replaceAll('-', ''), expectedEpoch: (await control(request, cluster, 0)).epoch };
}
async function reset(request: APIRequestContext, cluster: Cluster, operation: ResetRequest): Promise<ClusterAdminResult> {
  const response = await request.post(`${cluster.origin(0)}/v1/admin/cluster/reset`, {
    data: operation, timeout: 45_000, maxRetries: 0,
  });
  expect(response.status(), await response.text()).toBe(200);
  return response.json() as Promise<ClusterAdminResult>;
}
async function apply(request: APIRequestContext, cluster: Cluster, settings: ProposerSettings): Promise<ClusterAdminResult> {
  const response = await request.post(`${cluster.origin(0)}/v1/admin/cluster/settings`, {
    data: settings, timeout: 45_000, maxRetries: 0,
  });
  expect(response.status(), await response.text()).toBe(200);
  return response.json() as Promise<ClusterAdminResult>;
}
async function restart(request: APIRequestContext, cluster: Cluster, node: number) {
  const before = await request.get(`${cluster.origin(node)}/v1/admin/node`).then(response => response.json() as Promise<NodeStatus>);
  await cluster.stopNode(node);
  await cluster.startNode(node);
  const after = await request.get(`${cluster.origin(node)}/v1/admin/node`).then(response => response.json() as Promise<NodeStatus>);
  expect(after.nodeId).toBe(cluster.ids[node]);
  expect(after.bootId).not.toBe(before.bootId);
}
function observe<T>(operation: Promise<T>) {
  let settled = false;
  // Observe a rejection immediately, but rethrow it when the test consumes the
  // result. A failed assertion at a hold must not leave an unhandled rejection.
  const result = operation.then(value => ({ ok: true as const, value }), error => ({ ok: false as const, error }))
    .finally(() => { settled = true; });
  return {
    get settled() { return settled; },
    async finish(): Promise<T> {
      const outcome = await result;
      if (!outcome.ok) throw outcome.error;
      return outcome.value;
    },
  };
}
function commands(cluster: Cluster, node: number): ParticipantCommand[] {
  return cluster.administrationProxy(node).exchanges.map(exchange => exchange.command);
}

test('all proposer modes and every adaptive field reach all real nodes and survive restart', async ({ request, cluster }) => {
  await seedEveryMember(request, cluster);
  for (const mode of ['classic', 'fast', 'leader', 'fast+leader', 'adaptive'] as const) {
    const settings = { ...selected, mode };
    expect(await apply(request, cluster, settings)).toEqual({
      state: 'complete', reset: null,
      nodes: cluster.ids.map(nodeId => ({ nodeId, state: 'active', error: null })),
    });
    expect((await controls(request, cluster)).map(status => status.settings)).toEqual(indexes.map(() => settings));
    await expectOldLocalValues(request, cluster, indexes);
  }
  for (const node of indexes) await restart(request, cluster, node);
  expect((await controls(request, cluster)).map(status => status.settings)).toEqual(indexes.map(() => selected));
  await expectOldLocalValues(request, cluster, indexes);
  for (const node of [1, 2])
    expect(cluster.administrationProxy(node).exchanges.filter(item => item.command === 'settings').map(item => item.body))
      .toEqual(['classic', 'fast', 'leader', 'fast+leader', 'adaptive'].map(mode => ({ ...selected, mode })));
});

test('partial real-node settings failure identifies the failed member and retains successful selections', async ({ request, cluster }) => {
  const before = await controls(request, cluster);
  const fault = cluster.administrationProxy(2).failNext('settings', 'injected settings failure at node c');
  const result = await apply(request, cluster, { ...selected, mode: 'leader' });
  expect(result.state).toBe('incomplete');
  expect(result.nodes.map(({ nodeId, state }) => ({ nodeId, state }))).toEqual([
    { nodeId: 'a', state: 'active' }, { nodeId: 'b', state: 'active' }, { nodeId: 'c', state: 'unconfirmed' },
  ]);
  expect(result.nodes.map(node => node.error)).toEqual([null, null, expect.stringContaining('HTTP 503: injected settings failure at node c')]);
  expect(fault.exchange).toMatchObject({ state: 'failed', body: { ...selected, mode: 'leader' } });
  expect(fault.exchange?.upstream).toBeUndefined(); // Failure occurred before the participant was called.
  const after = await controls(request, cluster);
  expect(after.map(status => status.settings)).toEqual([
    { ...selected, mode: 'leader' }, { ...selected, mode: 'leader' }, before[2]!.settings,
  ]);
  expect((await apply(request, cluster, { ...selected, mode: 'leader' })).state).toBe('complete');
  expect((await controls(request, cluster)).map(status => status.settings))
    .toEqual(indexes.map(() => ({ ...selected, mode: 'leader' })));
});

test('reset prepare barrier preserves all real-node data until the third participant acknowledges', async ({ request, cluster }) => {
  await seedEveryMember(request, cluster);
  const operation = await resetRequest(request, cluster);
  const held = cluster.administrationProxy(2).holdNext('reset/prepare');
  const pending = observe(reset(request, cluster, operation));
  try {
    await expect.poll(() => held.exchange?.state).toBe('held');
    expect(held.exchange?.body).toEqual(operation);
    await expect.poll(async () => (await control(request, cluster, 0)).clusterReset).toEqual({
      request: operation, phase: 'prepare',
      nodes: [{ nodeId: 'a', state: 'prepared', error: null }, { nodeId: 'b', state: 'prepared', error: null },
        { nodeId: 'c', state: 'pending', error: null }],
    });
    expect((await controls(request, cluster)).map(status => [status.state, status.epoch])).toEqual([
      ['prepared', '0'], ['prepared', '0'], ['active', '0'],
    ]);
    await expectOldLocalValues(request, cluster, indexes);
    expect(commands(cluster, 1)).toEqual(['reset/prepare']);
    expect(commands(cluster, 2)).toEqual(['reset/prepare']);
    expect(pending.settled).toBe(false);
  } finally { held.release(); }
  await expectComplete(request, cluster, await pending.finish(), operation);
  await expectEmptyLocalInventories(request, cluster, indexes);
  for (const node of [1, 2]) expect(commands(cluster, node)).toEqual(['reset/prepare', 'reset/clear', 'reset/activate']);
});

test('reset clear barrier prevents activation until the third real participant acknowledges', async ({ request, cluster }) => {
  await seedEveryMember(request, cluster);
  const operation = await resetRequest(request, cluster);
  const held = cluster.administrationProxy(2).holdNext('reset/clear');
  const pending = observe(reset(request, cluster, operation));
  try {
    await expect.poll(() => held.exchange?.state).toBe('held');
    expect(held.exchange?.body).toEqual(operation);
    await expect.poll(async () => (await control(request, cluster, 0)).clusterReset).toEqual({
      request: operation, phase: 'clear',
      nodes: [{ nodeId: 'a', state: 'cleared', error: null }, { nodeId: 'b', state: 'cleared', error: null },
        { nodeId: 'c', state: 'pending', error: null }],
    });
    expect((await controls(request, cluster)).map(status => [status.state, status.epoch])).toEqual([
      ['cleared', '0'], ['cleared', '0'], ['prepared', '0'],
    ]);
    await expectEmptyLocalInventories(request, cluster, [0, 1]);
    await expectOldLocalValues(request, cluster, [2]);
    for (const node of indexes)
      expect((await request.get(registerUrl(cluster, node, oldValues[0]!.key))).status()).toBe(503);
    expect(commands(cluster, 1)).toEqual(['reset/prepare', 'reset/clear']);
    expect(commands(cluster, 2)).toEqual(['reset/prepare', 'reset/clear']);
    expect(pending.settled).toBe(false);
  } finally { held.release(); }
  await expectComplete(request, cluster, await pending.finish(), operation);
  await expectEmptyLocalInventories(request, cluster, indexes);
});

for (const phase of ['prepare', 'clear', 'activate'] as const) {
  test(`lost ${phase} acknowledgement resumes the same durable reset after coordinator restart`, async ({ request, cluster }) => {
    await seedEveryMember(request, cluster);
    const operation = await resetRequest(request, cluster);
    const command = `reset/${phase}` as const;
    const dropped = cluster.administrationProxy(2).dropResponsesUntilReleased(command);
    let result: ClusterAdminResult;
    try { result = await reset(request, cluster, operation); }
    finally { dropped.release(); }
    expect(result.state).toBe('incomplete');
    expect(result.reset).toEqual(operation);
    expect(result.nodes.map(node => [node.nodeId, node.state])).toEqual([
      ['a', phaseState[phase]], ['b', phaseState[phase]], ['c', 'unconfirmed'],
    ]);
    expect(result.nodes[2]!.error).toEqual(expect.stringContaining('retry the same operation'));
    const lostAcknowledgements = cluster.administrationProxy(2).exchanges.filter(exchange =>
      exchange.command === command && exchange.state === 'dropped');
    expect(lostAcknowledgements.length).toBeGreaterThan(0);
    for (const exchange of lostAcknowledgements) {
      expect(exchange.upstream?.status).toBe(200);
      expect(JSON.parse(exchange.upstream!.body)).toMatchObject({
        nodeId: 'c', state: phaseState[phase], epoch: phase === 'activate' ? '1' : '0',
      });
    }
    expect((await control(request, cluster, 2)).state).toBe(phaseState[phase]);
    expect((await control(request, cluster, 0)).clusterReset).toMatchObject({ request: operation, phase });
    const resumedAt = cluster.administrationProxy(2).exchanges.length;
    const preservedKey = 'after-activation';
    const preserved = Buffer.from('acknowledgement loss must not delete this value');
    const etag = phase === 'activate' ? await write(request, cluster, 0, preservedKey, preserved) : undefined;
    await restart(request, cluster, 0);
    expect((await control(request, cluster, 0)).clusterReset).toMatchObject({ request: operation, phase });
    await expectComplete(request, cluster, await reset(request, cluster, operation), operation);
    const remaining: Record<ResetPhase, ParticipantCommand[]> = {
      prepare: ['reset/prepare', 'reset/clear', 'reset/activate'],
      clear: ['reset/clear', 'reset/activate'],
      activate: ['reset/activate'],
    };
    expect(cluster.administrationProxy(2).exchanges.slice(resumedAt).map(exchange => exchange.command)).toEqual(remaining[phase]);
    if (phase === 'activate') {
      for (const node of indexes) await expectValue(request, cluster, node, preservedKey, preserved, etag);
    } else await expectEmptyLocalInventories(request, cluster, indexes);
  });
}

test('partial activation preserves new-generation quorum writes through fenced restart and completed replay', async ({ request, cluster }) => {
  // This case tests a majority while c is fenced, not adaptive fast-path fallback.
  // Select classic explicitly so the write does not depend on a fast quorum.
  expect((await apply(request, cluster, { ...selected, mode: 'classic' })).state).toBe('complete');
  await seedEveryMember(request, cluster);
  const operation = await resetRequest(request, cluster);
  cluster.administrationProxy(2).failNext('reset/activate', 'node c activation deliberately withheld');
  const partial = await reset(request, cluster, operation);
  expect(partial.state).toBe('incomplete');
  expect(partial.nodes.map(node => [node.nodeId, node.state])).toEqual([
    ['a', 'active'], ['b', 'active'], ['c', 'unconfirmed'],
  ]);
  expect(partial.nodes[2]!.error).toEqual(expect.stringContaining('node c activation deliberately withheld'));
  expect((await controls(request, cluster)).map(status => [status.state, status.epoch])).toEqual([
    ['active', '1'], ['active', '1'], ['cleared', '0'],
  ]);
  await expectEmptyLocalInventories(request, cluster, indexes);
  const key = 'new-generation/quorum-write';
  const bytes = Buffer.from([110, 101, 119, 0, 255]);
  const etag = await write(request, cluster, 0, key, bytes);
  for (const node of [0, 1])
    expect((await local(request, cluster, node, key)).entry?.register?.value?.base64).toBe(bytes.toString('base64'));
  expect((await request.get(registerUrl(cluster, 2, key))).status()).toBe(503);
  await restart(request, cluster, 2);
  expect(await control(request, cluster, 2)).toMatchObject({ state: 'cleared', epoch: '0', pendingReset: operation });
  expect((await request.get(registerUrl(cluster, 2, key))).status()).toBe(503);
  expect((await local(request, cluster, 2, key)).entry).toBeNull();
  await restart(request, cluster, 0);
  expect((await control(request, cluster, 0)).clusterReset).toMatchObject({ request: operation, phase: 'activate' });
  await expectValue(request, cluster, 1, key, bytes, etag);
  const phaseCounts = [1, 2].map(node => commands(cluster, node).length);
  await expectComplete(request, cluster, await reset(request, cluster, operation), operation);
  for (const [index, node] of [1, 2].entries())
    expect(commands(cluster, node).slice(phaseCounts[index]!)).toEqual(['reset/activate']);
  // Inspect before any logical read can repair an incorrectly re-cleared replica.
  for (const node of [0, 1])
    expect((await local(request, cluster, node, key)).entry?.register?.value?.base64).toBe(bytes.toString('base64'));
  for (const node of indexes) await expectValue(request, cluster, node, key, bytes, etag);
  const completedCounts = [1, 2].map(node => commands(cluster, node).length);
  await expectComplete(request, cluster, await reset(request, cluster, operation), operation);
  expect([1, 2].map(node => commands(cluster, node).length)).toEqual(completedCounts);
  // Lost acknowledgement replay through the actual participant HTTP route is also harmless.
  for (const node of indexes) {
    const replay = await request.post(`${cluster.origin(node)}/v1/admin/internal/reset/clear`, { data: operation, maxRetries: 0 });
    expect(replay.status(), await replay.text()).toBe(200);
    expect(await replay.json()).toMatchObject({ nodeId: cluster.ids[node], state: 'active', epoch: '1', pendingReset: null });
  }
  for (const node of [0, 1])
    expect((await local(request, cluster, node, key)).entry?.register?.value?.base64).toBe(bytes.toString('base64'));
  for (const node of indexes) await expectValue(request, cluster, node, key, bytes, etag);
  const changedIdentity = await request.post(`${cluster.origin(0)}/v1/admin/cluster/reset`, {
    data: { ...operation, expectedEpoch: '1' }, maxRetries: 0,
  });
  expect(changedIdentity.status()).toBe(409);
  await expectValue(request, cluster, 0, key, bytes, etag);
});

test('real cluster controls apply every field and require confirmation before clearing all nodes', async ({ page, context, request, cluster }, testInfo) => {
  const key = 'ui-existing';
  const bytes = Buffer.from('clear me');
  await write(request, cluster, 0, key, bytes);
  const pageErrors: string[] = [];
  page.on('pageerror', error => pageErrors.push(error.message));
  const resetBodies: ResetRequest[] = [];
  page.on('request', outgoing => {
    if (new URL(outgoing.url()).pathname === '/v1/admin/cluster/reset')
      resetBodies.push(outgoing.postDataJSON() as ResetRequest);
  });
  try {
    await page.goto(`${cluster.origin(2)}/admin/`);
    for (const id of cluster.ids)
      await expect(page.getByTestId(`node-${id}`).locator('.node-heading .badge')).toHaveText('connected');
    await page.getByText('Cluster controls', { exact: true }).click();
    await expect(page.getByRole('button', { name: 'Apply settings to all nodes' })).toBeEnabled();
    await page.getByLabel('Proposer mode').selectOption('leader');
    await page.getByLabel('Start adaptive mode fast').uncheck();
    await page.getByLabel('Enable adaptive leader forwarding').uncheck();
    for (const [label, value] of [
      ['Conflict debt increment', '7'], ['Conflict debt decay', '2'],
      ['Fast reopen threshold', '3'], ['Leader miss threshold', '5'],
    ]) await page.getByLabel(label!).fill(value!);
    const [applied] = await Promise.all([
      page.waitForResponse(response => new URL(response.url()).pathname === '/v1/admin/cluster/settings'),
      page.getByRole('button', { name: 'Apply settings to all nodes' }).click(),
    ]);
    expect(applied.url()).toBe(`${cluster.origin(0)}/v1/admin/cluster/settings`);
    expect(applied.status()).toBe(200);
    await expect(page.getByRole('status').filter({ hasText: 'Cluster command completed on every node.' })).toBeVisible();
    expect((await controls(request, cluster)).map(status => status.settings)).toEqual(indexes.map(() => ({ ...selected, mode: 'leader' })));
    await page.getByRole('button', { name: key, exact: true }).click();
    await expect(page.getByLabel('Current value')).toHaveText('clear me');
    let confirmation = '';
    page.once('dialog', async dialog => { confirmation = dialog.message(); await dialog.dismiss(); });
    await page.getByRole('button', { name: 'Clear entire cluster' }).click();
    expect(confirmation).toBe('Clear all register data and protocol state on every cluster node? Selected proposer settings are retained.');
    expect(resetBodies).toEqual([]);
    expect((await control(request, cluster, 0)).clusterReset).toBeNull();
    await expectValue(request, cluster, 0, key, bytes);
    page.once('dialog', dialog => dialog.accept());
    const [response] = await Promise.all([
      page.waitForResponse(response => new URL(response.url()).pathname === '/v1/admin/cluster/reset'),
      page.getByRole('button', { name: 'Clear entire cluster' }).click(),
    ]);
    expect(response.url()).toBe(`${cluster.origin(0)}/v1/admin/cluster/reset`);
    expect(response.status()).toBe(200);
    expect(resetBodies).toHaveLength(1);
    expect(resetBodies[0]!.operationId).toMatch(/^[0-9a-f]{32}$/);
    expect(resetBodies[0]!.expectedEpoch).toBe('0');
    await expectComplete(request, cluster, await response.json() as ClusterAdminResult, resetBodies[0]!);
    expect((await controls(request, cluster)).map(status => status.settings)).toEqual(indexes.map(() => ({ ...selected, mode: 'leader' })));
    for (const node of indexes) {
      expect((await local(request, cluster, node, key)).entry?.register?.isPresent ?? false).toBe(false);
      expect((await request.get(registerUrl(cluster, node, key))).status()).toBe(404);
    }
    await expect(page.getByRole('button', { name: 'Compare and swap', exact: true })).toHaveCount(0);
    await expect(page.getByRole('status').filter({ hasText: 'Cluster command completed on every node.' })).toBeVisible();
    expect(pageErrors).toEqual([]);
    await testInfo.attach('Real cluster controls after confirmed reset', { body: await page.screenshot({ fullPage: true }), contentType: 'image/png' });
  } finally { await context.close(); }
});
