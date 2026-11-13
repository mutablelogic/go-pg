import { test } from 'node:test';
import assert from 'node:assert/strict';
import { settingEffect, settingForm, settingFormErrors, settingReadOnly, settingRequest } from '../src/pgmanager/settingform.ts';
import type { Setting } from '../src/pgmanager/schema.ts';

const setting = (s: Partial<Setting>): Setting => ({ name: 'x', value: '', category: 'c', context: 'user', type: 'string', source: 'default', ...s });

test('settingFormErrors', async (t) => {
    await t.test('bool', () => {
        const s = setting({ type: 'bool', value: 'on' });
        assert.deepEqual(settingFormErrors(s, { value: 'off' }), {});
        assert.ok(settingFormErrors(s, { value: 'yes' }).value);
    });

    await t.test('enum', () => {
        const s = setting({ type: 'enum', values: ['minimal', 'replica', 'logical'] });
        assert.deepEqual(settingFormErrors(s, { value: 'replica' }), {});
        assert.ok(settingFormErrors(s, { value: 'archive' }).value);
    });

    await t.test('integer', () => {
        // Within its limits, or with a unit, which the server checks
        const s = setting({ type: 'integer', min: '64', max: '2147483647', unit: 'kB' });
        assert.deepEqual(settingFormErrors(s, { value: '8192' }), {});
        assert.deepEqual(settingFormErrors(s, { value: ' 8MB ' }), {});
        assert.match(settingFormErrors(s, { value: '1' }).value ?? '', /from 64 to 2147483647/);
        assert.ok(settingFormErrors(s, { value: '1.5' }).value);
        assert.ok(settingFormErrors(s, { value: '' }).value);
        assert.ok(settingFormErrors(s, { value: 'lots' }).value);

        // Without a unit, a unit isn't taken
        assert.ok(settingFormErrors(setting({ type: 'integer', min: '1', max: '100' }), { value: '8MB' }).value);
    });

    await t.test('real', () => {
        const s = setting({ type: 'real', min: '0', max: '100' });
        assert.deepEqual(settingFormErrors(s, { value: '0.2' }), {});
        assert.ok(settingFormErrors(s, { value: '101' }).value);
    });

    await t.test('string', () => {
        assert.deepEqual(settingFormErrors(setting({}), { value: '' }), {});
    });
});

test('settingRequest', () => {
    // A number is trimmed, a string isn't, and without a form, it's removed
    const memory = setting({ type: 'integer', value: ' 8MB ' });
    assert.deepEqual(settingRequest(memory, settingForm(memory)), { value: '8MB' });
    const prefix = setting({ name: 'log_line_prefix', value: '%m [%p] ' });
    assert.deepEqual(settingRequest(prefix, settingForm(prefix)), { value: '%m [%p] ' });
    assert.deepEqual(settingRequest(prefix), { value: null });
});

test('settingReadOnly', () => {
    assert.ok(settingReadOnly(setting({ context: 'internal' })));
    assert.ok(settingReadOnly(setting({ context: 'postmaster' })));
    assert.ok(settingReadOnly(setting({ context: 'sighup' }), false));
    assert.equal(settingReadOnly(setting({ context: 'sighup' })), undefined);

    // ALTER SYSTEM can't change allow_alter_system, even when it's on
    assert.ok(settingReadOnly(setting({ name: 'allow_alter_system', context: 'sighup', value: 'on' })));
});

test('settingEffect', () => {
    assert.match(settingEffect('postmaster'), /restarts/);
    assert.match(settingEffect('sighup'), /reloaded/);
    assert.match(settingEffect('user'), /new sessions/);
});
