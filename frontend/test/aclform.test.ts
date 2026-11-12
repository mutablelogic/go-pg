import { test } from 'node:test';
import assert from 'node:assert/strict';
import { aclChanged, aclRequest, aclRows } from '../src/pgmanager/aclform.ts';
import { databaseCreateAcl, databaseForm, databaseFormErrors, databaseRequest } from '../src/pgmanager/databaseform.ts';
import type { Database } from '../src/pgmanager/schema.ts';

test('aclRows', () => {
    // Rows are a role, which is quoted when it needs it, or PUBLIC, and its
    // privileges, without the grant option, which isn't taken back
    assert.deepEqual(aclRows(['reporter:CONNECT,CREATE', 'PUBLIC:TEMPORARY', '"a, role":CONNECT WITH GRANT OPTION,TEMPORARY']), [
        { role: 'reporter', priv: ['CONNECT', 'CREATE'] },
        { role: 'PUBLIC', priv: ['TEMPORARY'] },
        { role: 'a, role', priv: ['CONNECT', 'TEMPORARY'] },
    ]);
    assert.deepEqual(aclRows(undefined), []);
});

test('aclRequest', () => {
    // A row without privileges is left out, and a role is once
    assert.deepEqual(aclRequest([{ role: 'a', priv: ['CONNECT'] }, { role: 'b', priv: [] }, { role: 'a', priv: ['CREATE'] }]), [
        { role: 'a', priv: ['CONNECT', 'CREATE'] },
    ]);
    assert.equal(aclChanged([{ role: 'a', priv: ['CREATE', 'CONNECT'] }], ['a:CONNECT,CREATE']), false);
    assert.equal(aclChanged([{ role: 'a', priv: ['CONNECT'] }], ['a:CONNECT,CREATE']), true);
});

test('databaseRequest', async (t) => {
    await t.test('create', () => {
        // A new database has its name, and what's set, and its access is
        // set once it's created, when it isn't PostgreSQL's defaults, with
        // its owner's, which the form doesn't have
        const form = databaseForm();
        assert.deepEqual(form.acl, [{ role: 'PUBLIC', priv: ['CONNECT', 'TEMPORARY'] }]);
        assert.deepEqual(databaseFormErrors(form), { name: 'A database needs a name' });
        form.name = ' shop ';
        form.owner = 'app';
        assert.deepEqual(databaseRequest(form), { name: 'shop', owner: 'app' });
        const created: Database = { oid: 1, name: 'shop', owner: 'app', acl: ['PUBLIC:TEMPORARY,CONNECT', 'app:CREATE,TEMPORARY,CONNECT'] };
        assert.equal(databaseCreateAcl(form, created), undefined);
        form.acl = [{ role: 'reporter', priv: ['CONNECT'] }];
        assert.deepEqual(databaseCreateAcl(form, created), [
            { role: 'reporter', priv: ['CONNECT'] },
            { role: 'app', priv: ['CREATE', 'TEMPORARY', 'CONNECT'] },
        ]);
    });

    await t.test('change', () => {
        // A database which is changed has only what's changed, and access
        // which is all removed is none
        const db: Database = { oid: 1, name: 'shop', owner: 'postgres', comment: 'The shop', acl: ['PUBLIC:CONNECT,TEMPORARY', 'postgres:CONNECT,CREATE,TEMPORARY'] };
        const form = databaseForm(db);
        assert.deepEqual(databaseRequest(form, db), {});
        form.name = 'store';
        form.comment = '';
        form.acl = form.acl.filter((row) => row.role !== 'PUBLIC');
        assert.deepEqual(databaseRequest(form, db), { name: 'store', comment: '', acl: [{ role: 'postgres', priv: ['CONNECT', 'CREATE', 'TEMPORARY'] }] });
    });
});
