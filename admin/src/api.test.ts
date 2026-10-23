import { describe, expect, it, vi } from 'vitest';
import { Api, endpoint, fromBase64, textBytes, toBase64, utf8 } from './api';
import { status } from './test-fixtures';

describe('API requests', () => {
  it('calls native fetch without binding the Api instance as its receiver', async () => {
    const request = vi.fn<typeof fetch>().mockImplementation(function (this: unknown) {
      if (this instanceof Api) throw new TypeError('Illegal invocation');
      return Promise.resolve(new Response(null, { status: 404 }));
    });
    vi.stubGlobal('fetch', request);
    const api = new Api();
    expect(await api.read('http://a.test', 'key', new AbortController().signal)).toEqual({ present: false });
    expect(request).toHaveBeenCalledTimes(1);
    expect(request.mock.contexts[0]).not.toBe(api);
  });

  it('preserves an empty continuation and exact reserved and Unicode key text', async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ keys: [''], continuation: '' }));
    const api = new Api(request);
    const signal = new AbortController().signal;
    expect(await api.keys('http://a.test', 'a +/&?é', '', signal)).toEqual({ keys: [''], continuation: '' });
    const url = new URL(request.mock.calls[0]![0] as string);
    expect(url.pathname).toBe('/v1/registers');
    expect([...url.searchParams]).toEqual([['prefix', 'a +/&?é'], ['after', ''], ['limit', '50']]);
    expect(request.mock.calls[0]![1]).toEqual({ signal, cache: 'no-store' });
    expect(new URL(endpoint('http://a.test', '/v1/registers', { after: null })).searchParams.has('after')).toBe(false);
  });

  it('reads complete bytes and an opaque ETag without decoding or normalizing', async () => {
    const bytes = new Uint8Array([0xef, 0xbb, 0xbf, 0x41, 0x00, 0xff, 0x0d, 0x0a]);
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(bytes, { headers: { ETag: '"opaque+/="' } }));
    const signal = new AbortController().signal;
    expect(await new Api(request).read('http://a.test', '', signal)).toEqual({ present: true, bytes, etag: '"opaque+/="' });
    expect(new URL(request.mock.calls[0]![0] as string).searchParams.get('key')).toBe('');
    expect(request.mock.calls[0]![1]).toEqual({ signal, cache: 'no-store' });
  });

  it('distinguishes an absent register from an empty present register', async () => {
    const request = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(new Response(new Uint8Array(), { headers: { ETag: '"empty"' } }));
    const api = new Api(request);
    const signal = new AbortController().signal;
    expect(await api.read('http://a.test', 'key', signal)).toEqual({ present: false });
    expect(await api.read('http://a.test', 'key', signal)).toEqual({ present: true, bytes: new Uint8Array(), etag: '"empty"' });
  });

  it('rejects successful reads with an inaccessible ETag and surfaces read failures', async () => {
    const request = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(new Response('value'))
      .mockResolvedValueOnce(new Response('unavailable', { status: 503 }));
    const api = new Api(request);
    const signal = new AbortController().signal;
    await expect(api.read('http://a.test', 'key', signal)).rejects.toThrow('ETag');
    await expect(api.read('http://a.test', 'key', signal)).rejects.toThrow('Register read failed (503): unavailable');
  });

  it.each([
    { name: 'unsupported schema', value: { ...status(), schemaVersion: 2 } },
    { name: 'zero heartbeat', value: status('a', { heartbeatMilliseconds: 0 }) },
    { name: 'empty peer configuration', value: status('a', { peers: [] }) },
  ])('rejects $name during bootstrap', async ({ value }) => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(Response.json(value));
    await expect(new Api(request).status('http://a.test', new AbortController().signal)).rejects.toThrow('Unsupported node configuration');
  });

  it.each([
    { name: 'create', expected: null, status: 201, header: 'If-None-Match', value: '*' },
    { name: 'compare and swap', expected: '"opaque+/="', status: 204, header: 'If-Match', value: '"opaque+/="' },
  ])('sends exact $name preconditions and payload once', async (test) => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: test.status, headers: { ETag: '"next"' } }));
    const signal = new AbortController().signal;
    const bytes = new Uint8Array([0x00, 0xff, 0xef, 0xbb, 0xbf]);
    const result = await new Api(request).write('http://a.test', 'a +/&?é', bytes, test.expected, signal);
    expect(result).toEqual({ kind: 'applied', etag: '"next"' });
    expect(request).toHaveBeenCalledTimes(1);
    const [url, options] = request.mock.calls[0]!;
    expect(new URL(url as string).searchParams.get('key')).toBe('a +/&?é');
    expect(options).toEqual({
      method: 'PUT', signal, cache: 'no-store',
      headers: { 'Content-Type': 'application/octet-stream', [test.header]: test.value }, body: bytes,
    });
    expect(options!.body).not.toBe(bytes);
  });

  it.each([
    { name: 'precondition conflict', response: () => new Response(null, { status: 412 }), kind: 'conflict' },
    { name: 'acknowledgement without ETag', response: () => new Response(null, { status: 204 }), kind: 'unknown' },
    { name: 'explicit unknown with later ETag', response: () => Response.json({ outcome: 'unknown', currentEtag: '"observed"', detail: 'ambiguous' }, { status: 503 }), kind: 'unknown' },
    { name: 'unclassified server failure', response: () => Response.json({ detail: 'failed after dispatch' }, { status: 500 }), kind: 'unknown' },
    { name: 'proxy non-JSON failure', response: () => new Response('<html>lost response</html>', { status: 502 }), kind: 'unknown' },
    { name: 'known not applied', response: () => Response.json({ outcome: 'not-applied', detail: 'not dispatched' }, { status: 500 }), kind: 'rejected' },
    { name: 'busy host', response: () => Response.json({ code: 'busy' }, { status: 503 }), kind: 'rejected' },
  ])('classifies $name without a retry', async ({ response, kind, name }) => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(response());
    const result = await new Api(request).write('http://a.test', 'key', new Uint8Array(), '"v1"', new AbortController().signal);
    expect(result.kind).toBe(kind);
    if (name === 'explicit unknown with later ETag') expect(result).toEqual({ kind: 'unknown', currentEtag: '"observed"', message: 'ambiguous' });
    expect(request).toHaveBeenCalledTimes(1);
  });

  it('treats transport loss after dispatch as unknown and never retries', async () => {
    const request = vi.fn<typeof fetch>().mockRejectedValue(new TypeError('connection lost'));
    const result = await new Api(request).write('http://a.test', 'key', new Uint8Array([42]), '"v1"', new AbortController().signal);
    expect(result.kind).toBe('unknown');
    expect('message' in result && result.message).toContain('The write may have applied');
    expect(request).toHaveBeenCalledTimes(1);
  });

  it('passes cancellation to reads and classifies an aborted write as unknown', async () => {
    const request = vi.fn<typeof fetch>().mockImplementation((_url, options) => new Promise((_resolve, reject) => {
      options!.signal!.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true });
    }));
    const api = new Api(request);
    const readController = new AbortController();
    const reading = expect(api.read('http://a.test', 'key', readController.signal)).rejects.toMatchObject({ name: 'AbortError' });
    readController.abort();
    await reading;
    const writeController = new AbortController();
    const writing = api.write('http://a.test', 'key', new Uint8Array(), null, writeController.signal);
    writeController.abort();
    expect((await writing).kind).toBe('unknown');
    expect(request).toHaveBeenCalledTimes(2);
  });
});

describe('lossless value representations', () => {
  it.each([
    { name: 'empty payload', bytes: new Uint8Array(), text: '' },
    { name: 'UTF-8 BOM and CRLF', bytes: new Uint8Array([0xef, 0xbb, 0xbf, 65, 13, 10]), text: '\uFEFFA\r\n' },
    { name: 'NUL and supplementary Unicode', bytes: new Uint8Array([0, 0xf0, 0x9f, 0x8c, 0x8d]), text: '\0🌍' },
  ])('preserves $name through UTF-8 and Base64', ({ bytes, text }) => {
    expect(utf8(bytes)).toBe(text);
    // Node's TextEncoder and jsdom use different typed-array realms.
    expect([...textBytes(text)]).toEqual([...bytes]);
    expect(fromBase64(toBase64(bytes))).toEqual(bytes);
  });

  it('preserves binary data across the Base64 chunk boundary and rejects lossy UTF-8', () => {
    const bytes = Uint8Array.from({ length: 16_385 }, (_, index) => index % 256);
    expect(utf8(bytes)).toBeNull();
    expect(fromBase64(toBase64(bytes))).toEqual(bytes);
    expect(utf8(new Uint8Array([0xc3, 0x28]))).toBeNull();
    expect(utf8(new Uint8Array([0xf0, 0x9f]))).toBeNull();
    expect(fromBase64(' /w==\r\n')).toEqual(new Uint8Array([255]));
  });

  it.each([
    { name: 'unpadded Base64', value: '/w' },
    { name: 'noncanonical padding bits', value: '/x==' },
    { name: 'invalid alphabet', value: '*w==' },
  ])('rejects $name instead of silently changing bytes', ({ value }) => {
    expect(() => fromBase64(value)).toThrow();
  });

  it.each([
    { name: 'unpaired high surrogate', value: '\uD800' },
    { name: 'unpaired low surrogate', value: '\uDC00' },
  ])('rejects $name instead of emitting replacement bytes', ({ value }) => {
    expect(() => textBytes(value)).toThrow('unpaired Unicode surrogate');
  });
});
