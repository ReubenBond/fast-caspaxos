import { test, expect } from '@playwright/test';
import { Cluster } from './cluster';

test('real hosts stream changes, preserve conditional bytes, and recover one disconnected node', async ({ page, context, request }, testInfo) => {
  const cluster = new Cluster();
  await cluster.start();
  const failures: string[] = [];
  page.on('pageerror', (error) => failures.push(error.message));
  try {
    for (const [key, value] of [
      ['demo/config/feature-flags', '{"source":"isolated demo database","enabled":true}'],
      ['demo/config/worker-limit', '24'],
      ['demo/jobs/checkpoint', 'batch-0042'],
      ['demo/leases/processor-a', 'node-a'],
      ['demo/routing/region', 'west'],
    ]) {
      const result = await request.put(`${cluster.origin(0)}/v1/registers/value?key=${encodeURIComponent(key!)}`, {
        headers: { 'Content-Type': 'application/octet-stream', 'If-None-Match': '*' }, data: value,
      });

      expect(result.status()).toBe(201);
    }
    const streams = new Set<string>();
    page.on('request', (request) => {
      if (new URL(request.url()).pathname === '/v1/admin/events') streams.add(new URL(request.url()).origin);
    });
    const pageResponse = await page.goto(`${cluster.origin(0)}/admin/`);
    expect(pageResponse?.status()).toBe(200);
    expect(pageResponse?.headers()['cache-control']).toBe('no-store');
    const redirect = await request.get(`${cluster.origin(0)}/admin`, { maxRedirects: 0 });
    expect(redirect.status()).toBe(302);
    expect(redirect.headers().location).toBe('/admin/');
    expect((await request.get(`${cluster.origin(0)}/admin/missing-route`)).status()).toBe(404);
    await expect(page.getByRole('heading', { name: 'Register inspector' })).toBeVisible();
    for (const id of cluster.ids) await expect(page.getByTestId(`node-${id}`).locator('.node-heading .badge')).toHaveText('connected');
    await expect.poll(() => streams.size).toBe(3);
    expect([...streams].sort()).toEqual([0, 1, 2].map((index) => cluster.origin(index)).sort());
    await page.getByRole('button', { name: 'demo/config/feature-flags', exact: false }).click();
    await expect(page.getByLabel('Current value')).toContainText('isolated demo database');
    const before = await request.get(`${cluster.origin(0)}/v1/registers/value?key=demo%2Fconfig%2Ffeature-flags`);
    await page.getByLabel('New complete value').fill('{"source":"isolated demo database","enabled":false}');
    await page.getByRole('button', { name: 'Compare and swap', exact: true }).click();
    await expect(page.getByRole('status').filter({ hasText: 'Write applied.' })).toBeVisible();
    await expect(page.getByLabel('Current value')).toContainText('"enabled":false');
    const after = await request.get(`${cluster.origin(1)}/v1/registers/value?key=demo%2Fconfig%2Ffeature-flags`);
    expect(after.headers().etag).not.toBe(before.headers().etag);
    expect(await after.text()).toBe('{"source":"isolated demo database","enabled":false}');
    const timeline = page.getByRole('region', { name: 'Cluster timeline' });
    await expect(timeline.getByRole('columnheader', { name: 'Node a', exact: true })).toBeVisible();
    await expect(timeline.getByRole('columnheader', { name: 'Node b', exact: true })).toBeVisible();
    await expect(timeline.getByRole('img', { name: /Node a to Node [bc]: accept request-initiated/ }).first()).toBeAttached();
    await expect(timeline.getByText('Committed acceptor state', { exact: true }).first()).toBeAttached();
    await expect(timeline.getByText(/ms host elapsed/).first()).toBeAttached();
    await page.getByLabel('Event', { exact: true }).selectOption('protocol');
    await expect(page.locator('.event-table tbody')).toContainText('accept');

    // Native EventSource reconnects to the same endpoint with its previous cursor.
    const oldBoot = await request.get(`${cluster.origin(2)}/v1/admin/node`).then((response) => response.json() as Promise<{ bootId: string }>);
    await cluster.stopNode(2);
    await expect(page.getByTestId('node-c').locator('.node-heading .badge')).toHaveText(/reconnecting|unreachable/);
    await expect(page.getByTestId('node-a').locator('.node-heading .badge')).toHaveText('connected');
    await cluster.startNode(2);
    await expect(page.getByTestId('node-c').locator('.node-heading .badge')).toHaveText('connected');
    const newBoot = await request.get(`${cluster.origin(2)}/v1/admin/node`).then((response) => response.json() as Promise<{ bootId: string }>);
    expect(newBoot.bootId).not.toBe(oldBoot.bootId);
    await page.getByLabel('Event', { exact: true }).selectOption('reset');
    await expect(page.locator('.event-table')).toContainText('restarted');
    await page.getByLabel('Event', { exact: true }).selectOption('protocol');
    // Produce current message evidence after recovery, then freeze only the console.
    await page.getByRole('button', { name: 'Read current value' }).click();
    await expect(page.getByLabel('Current value')).toContainText('"enabled":false');
    await expect(page.locator('.local-table')).toContainText('Present');
    await page.getByRole('button', { name: 'Pause timeline' }).click();

    const mobile = await context.newPage();
    await mobile.setViewportSize({ width: 390, height: 844 });
    await mobile.goto(`${cluster.origin(0)}/admin/`);
    await mobile.getByRole('button', { name: 'demo/config/feature-flags', exact: false }).click();
    await expect(mobile.getByLabel('Current value')).toContainText('"enabled":false');
    await mobile.getByRole('button', { name: 'Pause timeline' }).click();
    // Paused node lanes remain horizontally navigable on a narrow viewport.
    const mobileTimeline = mobile.getByRole('region', { name: 'Scrollable event timeline' });
    await mobileTimeline.evaluate((element) => { element.scrollLeft = element.scrollWidth; });
    await expect(mobile.getByRole('columnheader', { name: 'Message / state change' })).toBeVisible();
    for (const target of [page, mobile]) {
      expect(await target.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    }
    const desktopPath = testInfo.outputPath('desktop.png');
    const mobilePath = testInfo.outputPath('mobile.png');
    const timelinePath = testInfo.outputPath('timeline.png');
    const mobileTimelinePath = testInfo.outputPath('timeline-mobile.png');
    await Promise.all([
      page.screenshot({ path: desktopPath, fullPage: true }),
      mobile.screenshot({ path: mobilePath, fullPage: true }),
    ]);
    await Promise.all([
      timeline.screenshot({ path: timelinePath }),
      mobile.getByRole('region', { name: 'Cluster timeline' }).screenshot({ path: mobileTimelinePath }),
    ]);
    await testInfo.attach('Desktop workbench, real isolated demo cluster', { path: desktopPath, contentType: 'image/png' });
    await testInfo.attach('Mobile workbench, real isolated demo cluster', { path: mobilePath, contentType: 'image/png' });
    await testInfo.attach('Node message lanes, real isolated demo cluster', { path: timelinePath, contentType: 'image/png' });
    await testInfo.attach('Mobile timeline, real isolated demo cluster', { path: mobileTimelinePath, contentType: 'image/png' });
    expect(failures).toEqual([]);
  } finally {
    await context.close();
    await cluster.dispose();
  }
});

test('creates an empty binary key and reconciles a real competing-host conflict without overwriting the draft', async ({ page, context, request }) => {
  const cluster = new Cluster();
  await cluster.start();
  try {
    await page.goto(`${cluster.origin(0)}/admin/`);
    await expect(page.getByTestId('node-a').locator('.node-heading .badge')).toHaveText('connected');
    await page.keyboard.press('Tab');
    await expect(page.getByRole('link', { name: 'Skip to registers' })).toBeFocused();
    await page.getByRole('button', { name: 'Create key', exact: true }).click();
    await page.getByRole('button', { name: 'Set key', exact: true }).click();
    await page.getByLabel('Encoding', { exact: true }).selectOption('base64');
    await page.getByLabel('New complete value').fill('//8ACg==');
    await page.getByRole('button', { name: 'Create register', exact: true }).click();
    await expect(page.getByLabel('Current value')).toHaveText('//8ACg==');
    const created = await request.get(`${cluster.origin(1)}/v1/registers/value?key=`);
    expect([...await created.body()]).toEqual([255, 255, 0, 10]);
    const updated = await request.put(`${cluster.origin(1)}/v1/registers/value?key=`, {
      headers: { 'Content-Type': 'application/octet-stream', 'If-Match': created.headers().etag! },
      data: 'remote change',
    });
    expect(updated.status()).toBe(204);
    await expect(page.getByLabel('Current value')).toHaveText('remote change');
    await expect(page.getByLabel('New complete value')).toHaveValue('//8ACg==');
    await page.getByRole('button', { name: 'Compare and swap', exact: true }).click();
    await expect(page.getByRole('status').filter({ hasText: 'Version conflict.' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Compare and swap', exact: true })).toBeDisabled();
    expect(await request.get(`${cluster.origin(2)}/v1/registers/value?key=`).then((response) => response.text())).toBe('remote change');
    await page.getByRole('button', { name: 'Refresh to reconcile' }).click();
    await page.getByRole('button', { name: 'Use observed version for this draft' }).click();
    await expect(page.getByLabel('New complete value')).toHaveValue('//8ACg==');
    await page.getByRole('button', { name: 'Compare and swap', exact: true }).click();
    await expect(page.getByRole('status').filter({ hasText: 'Write applied.' })).toBeVisible();
    const reconciled = await request.get(`${cluster.origin(1)}/v1/registers/value?key=`);
    expect([...await reconciled.body()]).toEqual([255, 255, 0, 10]);
    expect(reconciled.headers().etag).not.toBe(updated.headers().etag);
  } finally {
    await context.close();
    await cluster.dispose();
  }
});
