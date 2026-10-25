import { describe, expect, it } from 'vitest';
import type { NodeEvent, Protocol } from './contracts';
import { buildTimeline } from './timeline-model';
import { event, key } from './test-fixtures';

function protocol(sequence: string, changes: Partial<Protocol> = {}, nodeId = 'a', bootId = `${nodeId}-boot`) {
  return event('protocol', sequence, {
    operation: 'accept', direction: 'outbound', phase: 'request-initiated', peerId: 'b',
    callId: 'call', operationId: 'op', key: key(),
    ballot: { round: 1, proposer: 1 }, nextBallot: { round: 2, proposer: 1 }, ...changes,
  }, nodeId, bootId);
}

function state(sequence: string, round: number, nodeId = 'a', bootId = `${nodeId}-boot`) {
  return event('acceptor-state', sequence, {
    key: key(), promisedBallot: { round, proposer: 1 }, acceptedBallot: { round, proposer: 1 },
    acceptedValue: { base64: 'QQ==', length: 1, truncated: false },
    register: { format: 'register', isPresent: true, version: `v${round}`, value: { base64: 'QQ==', length: 1, truncated: false } },
  }, nodeId, bootId);
}

describe('timeline evidence', () => {
  it.each([
    { name: 'outbound initiation', direction: 'outbound', phase: 'request-initiated', from: 'a', to: 'b', shape: 'request' },
    { name: 'inbound receipt', direction: 'inbound', phase: 'request-received', from: 'b', to: 'a', shape: 'request' },
    { name: 'outbound completion', direction: 'outbound', phase: 'response-observed', from: 'b', to: 'a', shape: 'response' },
    { name: 'inbound completion', direction: 'inbound', phase: 'response-produced', from: 'a', to: 'b', shape: 'response' },
    { name: 'local invocation', direction: 'local', phase: 'request-initiated', from: 'a', to: 'a', shape: 'local' },
    { name: 'local completion', direction: 'local', phase: 'response-observed', from: 'a', to: 'a', shape: 'local' },
    { name: 'outbound fault', direction: 'outbound', phase: 'fault-observed', from: 'a', to: 'a', shape: 'fault' },
    { name: 'inbound fault', direction: 'inbound', phase: 'fault-observed', from: 'a', to: 'a', shape: 'fault' },
  ])('draws $name at its observed boundary', ({ direction, phase, from, to, shape }) => {
    const [entry] = buildTimeline([protocol('1', { direction, phase })]);
    expect(entry).toMatchObject({ from, to, shape, summary: `accept ${phase}` });
  });

  it('keeps an unidentified incoming caller separate from a local invocation', () => {
    const [entry] = buildTimeline([protocol('1', { direction: 'inbound', phase: 'request-received', peerId: null })]);
    expect(entry).toMatchObject({ from: null, to: 'a', shape: 'request' });
  });

  it('preserves arrival order under clock skew and computes only same-host call durations', () => {
    const request = protocol('1');
    const received = { ...protocol('1', { direction: 'inbound', phase: 'request-received', peerId: 'a' }, 'b'),
      timestamp: '2026-10-23T19:00:00.000Z' };
    const produced = { ...protocol('2', { direction: 'inbound', phase: 'response-produced', peerId: 'a' }, 'b'),
      timestamp: '2026-10-23T19:00:00.007Z' };
    const response = { ...protocol('2', { phase: 'response-observed' }), timestamp: '2026-10-23T20:00:00.023Z' };
    const input = [request, received, produced, response];
    const entries = buildTimeline(input);
    expect(entries.map((entry) => entry.event)).toEqual(input);
    expect(entries.map((entry) => entry.elapsedMs)).toEqual([undefined, undefined, 7, 23]);
  });

  it.each([
    { name: 'different call', change: { callId: 'other' } },
    { name: 'different operation', change: { operationId: 'other' } },
    { name: 'different peer', change: { peerId: 'c' } },
    { name: 'different method', change: { operation: 'prepare' } },
    { name: 'different direction', change: { direction: 'inbound' } },
  ])('keeps a $name completion unpaired', ({ change }) => {
    const entries = buildTimeline([protocol('1'), protocol('2', { phase: 'response-observed', ...change })]);
    expect(entries[1]!.elapsedMs).toBeUndefined();
  });

  it('shows committed promise, accepted ballot and version transitions per exact key and node', () => {
    const entries = buildTimeline([state('1', 1), state('1', 9, 'b'), state('2', 2)]);
    expect(entries[2]!.facts).toContain('Promised: (1, 1) -> (2, 1)');
    expect(entries[2]!.facts).toContain('Accepted: (1, 1) -> (2, 1)');
    expect(entries[2]!.facts).toContain('Version: v1 -> v2');
    expect(entries[2]!.facts).toContain('Register present');
    expect(entries[1]!.facts).toContain('First retained state for this key and node generation');
  });

  it.each([
    { name: 'sequence gap', barrier: [] as NodeEvent[], next: '4', boot: 'a-boot', gap: true },
    { name: 'stream reset', barrier: [event('reset', '2', { reason: 'history-unavailable' })], next: '3', boot: 'a-boot', gap: false },
    { name: 'node restart', barrier: [], next: '1', boot: 'new-boot', gap: false },
  ])('fences state and call comparisons across a $name', ({ barrier, next, boot, gap }) => {
    const entries = buildTimeline([state('1', 1), protocol('2'), ...barrier, state(next, 2, 'a', boot),
      protocol(String(BigInt(next) + 1n), { phase: 'response-observed' }, 'a', boot)]);
    const changed = entries.at(-2)!;
    expect(changed.historyGap).toBe(gap);
    expect(changed.facts).toContain('First retained state for this key and node generation');
    expect(changed.facts).not.toContain('Promised: (1, 1) -> (2, 1)');
    expect(entries.at(-1)!.elapsedMs).toBeUndefined();
  });

  it.each([
    { name: 'truncated', changes: { truncated: true } },
    { name: 'lossy', changes: { lossy: true } },
  ])('fences state comparisons around a $name key preview', ({ changes }) => {
    const ambiguous = state('2', 2);
    ambiguous.data.key = key('chosen', changes);
    const entries = buildTimeline([state('1', 1), ambiguous, state('3', 3)]);
    expect(entries[1]!.facts).toContain('First retained state for this key and node generation');
    expect(entries[2]!.facts).toContain('First retained state for this key and node generation');
  });

  it('reports an unmatched completion after retention eviction and preserves negative clock evidence', () => {
    expect(buildTimeline([protocol('3', { phase: 'response-observed' })])[0]!.elapsedMs).toBeUndefined();
    const backwards = { ...protocol('2', { phase: 'fault-observed' }), timestamp: '2026-10-23T19:59:59.999Z' };
    expect(buildTimeline([protocol('1'), backwards])[1]!.elapsedMs).toBe(-1);
  });

  it('keeps sequence gap detection exact beyond JavaScript number precision', () => {
    const entries = buildTimeline([state('9007199254740992', 1), state('9007199254740993', 2), state('9007199254740995', 3)]);
    expect(entries.map((entry) => entry.historyGap)).toEqual([false, false, true]);
  });
});
