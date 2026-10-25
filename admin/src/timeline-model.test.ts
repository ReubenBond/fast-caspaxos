import { describe, expect, it } from 'vitest';
import type { NodeEvent, Protocol } from './contracts';
import { buildTimeline, formatMessage } from './timeline-model';
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
  it('reports notification failure separately from the committed write outcome', () => {
    const notification = event('notification', '1', {
      operation: 'commit', key: key('dog'), operationId: 'op', error: 'ObjectDisposedException',
    });
    expect(formatMessage(notification).full).toBe('notification:commit("dog", error=ObjectDisposedException)');
    expect(buildTimeline([notification])[0]).toMatchObject({
      shape: 'fault', summary: 'commit notification failed', facts: ['ObjectDisposedException'],
    });
  });
  it('prints short request and response parameters on message labels', () => {
    expect(formatMessage(protocol('1', { key: key('dog') }))).toEqual({
      text: 'accept("dog", b=(1,1), next=(2,1))',
      full: 'accept("dog", b=(1,1), next=(2,1))',
    });
    expect(formatMessage(protocol('2', {
      key: key('dog'), phase: 'response-observed', outcome: 'accepted', promisedBallot: { round: 2, proposer: 1 },
      conflictingBallot: { round: 0, proposer: 0 },
    })).text).toBe('accept:accepted("dog", b=(1,1), p=(2,1))');
    expect(formatMessage(protocol('3', { key: key(''), operation: 'read' })).text).toBe('read("")');
  });

  it('abbreviates long parameters with ellipses while retaining full tooltip evidence', () => {
    const data = protocol('1', {
      key: key('abcdefghijklmnopqrstuvwxyz'),
      value: { base64: 'A'.repeat(80), length: 300, truncated: true },
    });
    const label = formatMessage(data);
    expect(label.text).toBe('accept("abcdefghijklmnopqrst..., b=(1,1), next=(2,1), value=300 B b64:AAAAAAAAAAA...)');
    expect(label.full).toBe(`accept("abcdefghijklmnopqrstuvwxyz", b=(1,1), next=(2,1), value=300 B b64:${'A'.repeat(80)} [truncated])`);
  });

  it('keeps parameters at the display limit and escapes controls without splitting surrogate pairs', () => {
    expect(formatMessage(protocol('1', { key: key('a'.repeat(22)), operation: 'read' })).text)
      .toBe(`read("${'a'.repeat(22)}")`);
    const label = formatMessage(protocol('1', { key: key('😀'.repeat(25) + '\n'), operation: 'read' }));
    expect(label.text).toBe(`read("${'😀'.repeat(20)}...)`);
    expect(label.full).toBe(`read("${'😀'.repeat(25)}\\n")`);
  });

  it('labels state changes, faults and empty payloads with their available parameters', () => {
    expect(formatMessage(state('1', 2)).text).toBe('state("chosen", p=(2,1), a=(2,1), v=v2)');
    expect(formatMessage(protocol('1', { phase: 'fault-observed', error: 'IOException' })).text)
      .toBe('accept:fault("chosen", b=(1,1), error=IOException)');
    expect(formatMessage(protocol('1', { value: { length: 0, base64: '', truncated: false } })).text)
      .toBe('accept("chosen", b=(1,1), next=(2,1), value=0 B)');
  });

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
