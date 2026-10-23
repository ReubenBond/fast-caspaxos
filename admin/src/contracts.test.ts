import { describe, expect, it } from 'vitest';
import { parseEvent } from './contracts';
import { event, key } from './test-fixtures';

describe('event wire boundary', () => {
  it('preserves sequence strings beyond Number precision', () => {
    const item = event('storage', '18446744073709551615', { status: 'ready' });
    expect(parseEvent(JSON.stringify(item), 'storage', 'a')).toEqual(item);
  });

  it.each([
    { name: 'numeric sequence', changes: { sequence: 1 } },
    { name: 'negative sequence', changes: { sequence: '-1', id: 'a-boot:-1' } },
    { name: 'fractional sequence', changes: { sequence: '1.5', id: 'a-boot:1.5' } },
    { name: 'mismatched cursor', changes: { id: 'a-boot:2' } },
    { name: 'wrong node', changes: { nodeId: 'b' } },
    { name: 'wrong kind', changes: { kind: 'heartbeat' } },
    { name: 'unsupported schema', changes: { schemaVersion: 2 } },
  ])('rejects $name instead of corrupting per-node order', ({ changes }) => {
    const item = { ...event('storage', '1', { status: 'ready' }), ...changes };
    expect(() => parseEvent(JSON.stringify(item), 'storage', 'a')).toThrow('Unsupported or inconsistent node event');
  });

  it.each([
    { name: 'present', isPresent: true },
    { name: 'absent', isPresent: false },
    { name: 'unavailable', isPresent: null },
    { name: 'omitted', isPresent: undefined },
  ])('preserves $name operation presence evidence', ({ isPresent }) => {
    const item = event('operation', '1', {
      operation: 'compare-exchange', operationId: 'op', outcome: 'applied', key: key(), isPresent,
    });
    const result = parseEvent(JSON.stringify(item), 'operation', 'a');
    expect(result.kind).toBe('operation');
    if (result.kind !== 'operation') throw new Error('Expected operation');
    expect(result.data.isPresent).toBe(isPresent);
    expect(result.data.key).toEqual(key());
  });
});
