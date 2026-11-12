import { test } from 'node:test';
import assert from 'node:assert/strict';
import { listAll } from '../src/pgmanager/httpclient.ts';

test('listAll', async () => {
    // Every page is read, from the offset of the items so far, as the server
    // may return fewer than the limit, until there are as many as the count
    const items = Array.from({ length: 45 }, (_, i) => i);
    const offsets: number[] = [];
    const list = await listAll(async (offset, limit) => {
        offsets.push(offset);
        return { count: items.length, body: items.slice(offset, offset + Math.min(limit, 20)) };
    }, 50);
    assert.deepEqual(list, { count: 45, body: items });
    assert.deepEqual(offsets, [0, 20, 40]);

    // An empty page ends it, rather than reading forever
    const empty = await listAll(async () => ({ count: 10 }), 50);
    assert.deepEqual(empty, { count: 10, body: [] });
});
