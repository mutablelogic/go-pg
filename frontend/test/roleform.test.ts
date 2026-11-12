import { test } from 'node:test';
import assert from 'node:assert/strict';
import { roleForm, roleRequest } from '../src/pgmanager/roleform.ts';
import type { Role } from '../src/pgmanager/schema.ts';

test('roleRequest', async (t) => {
    await t.test('create', () => {
        // A new role has its name, permissions, and what's set, but not an
        // empty description, limit, expiry or memberships
        const form = roleForm();
        form.name = ' reporter ';
        form.password = 'secret';
        form.conlimit = '3';
        assert.deepEqual(roleRequest(form), {
            name: 'reporter', login: true, inherit: true, createdb: false, createrole: false, replication: false, bypassrls: false, super: false,
            password: 'secret', conlimit: 3,
        });
    });

    await t.test('change', () => {
        // A role which is changed has only what's changed, and a limit, or
        // expiry, which is removed is -1, or the zero time
        const role: Role = { oid: 1, name: 'reporter', login: true, inherit: true, conlimit: 3, expires: '2030-01-02T00:00:00Z', memberof: ['pg_monitor'] };
        const form = roleForm(role);
        assert.deepEqual(roleRequest(form, role), {});
        form.createdb = true;
        form.conlimit = '';
        form.expires = '';
        form.memberof = [];
        form.comment = 'Reads reports';
        assert.deepEqual(roleRequest(form, role), { createdb: true, conlimit: -1, expires: '0001-01-01T00:00:00Z', memberof: [], comment: 'Reads reports' });
    });

    await t.test('password', () => {
        // A password is left, unless one's typed, or it's removed
        const role: Role = { oid: 1, name: 'reporter' };
        const form = roleForm(role);
        form.password = 'new';
        assert.deepEqual(roleRequest(form, role).password, 'new');
        form.removePassword = true;
        assert.deepEqual(roleRequest(form, role).password, '');
    });

    await t.test('expiry', () => {
        // An expiry is a date, at the start of its day, in UTC
        const form = roleForm();
        form.name = 'x';
        form.expires = '2031-05-06';
        assert.equal(roleRequest(form).expires, '2031-05-06T00:00:00.000Z');
    });
});
