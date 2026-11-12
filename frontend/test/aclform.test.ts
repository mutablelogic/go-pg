import { test } from 'node:test';
import assert from 'node:assert/strict';
import { aclChanged, aclRequest, aclRows } from '../src/pgmanager/aclform.ts';
import { databaseCreateAcl, databaseForm, databaseFormErrors, databaseRequest } from '../src/pgmanager/databaseform.ts';
import type { Database, Schema, Tablespace } from '../src/pgmanager/schema.ts';
import { tablespaceForm, tablespaceFormErrors, tablespaceLocationError, tablespaceRequest } from '../src/pgmanager/tablespaceform.ts';
import { schemaCreateAcl, schemaForm, schemaFormErrors, schemaRequest } from '../src/pgmanager/schemaform.ts';

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

test('schemaRequest', () => {
    // A new schema is named schema, not name, isn't PostgreSQL's, and a
    // changed one has only what's changed
    const form = schemaForm();
    assert.equal(form.acl.length, 0);
    assert.deepEqual(schemaFormErrors(form), { name: 'A schema needs a name' });
    form.name = 'pg_mine';
    assert.deepEqual(schemaFormErrors(form), { name: "A schema's name can't start with pg_" });
    form.name = 'sales';
    assert.deepEqual(schemaRequest(form), { schema: 'sales' });
    const created: Schema = { oid: 1, schema: 'sales', owner: 'app', acl: ['app:USAGE,CREATE'] };
    assert.equal(schemaCreateAcl(form, created), undefined);
    form.acl.push({ role: 'reporter', priv: ['USAGE'] });
    assert.deepEqual(schemaCreateAcl(form, created), [{ role: 'reporter', priv: ['USAGE'] }, { role: 'app', priv: ['USAGE', 'CREATE'] }]);
    const changed = schemaForm(created);
    changed.comment = 'Orders';
    assert.deepEqual(schemaRequest(changed, created), { comment: 'Orders' });
});

test('tablespaceRequest', () => {
    // A new tablespace has a location, from the server's root, which an
    // existing one doesn't send, as it can't be moved
    const form = tablespaceForm();
    form.name = 'reports';
    assert.deepEqual(tablespaceFormErrors(form, true), { location: 'A tablespace needs a directory on the server, such as /var/lib/postgresql/reports' });
    form.location = ' /data/reports ';
    assert.deepEqual(tablespaceFormErrors(form, true), {});
    assert.deepEqual(tablespaceRequest(form), { name: 'reports', location: '/data/reports' });
    const space: Tablespace = { oid: 1, name: 'reports', owner: 'app', location: '/data/reports', acl: ['app:CREATE'] };
    const changed = tablespaceForm(space);
    assert.deepEqual(tablespaceFormErrors(changed, false), {});
    changed.acl.push({ role: 'reporter', priv: ['CREATE'] });
    assert.deepEqual(tablespaceRequest(changed, space), { acl: [{ role: 'app', priv: ['CREATE'] }, { role: 'reporter', priv: ['CREATE'] }] });
});

test('tablespaceLocationError', () => {
    // PostgreSQL's reasons, in plain words, or none, for another reason
    assert.match(tablespaceLocationError(404, 'Not Found: directory "/data/x" does not exist') ?? '', /doesn't exist/);
    assert.match(tablespaceLocationError(400, 'Bad Request: could not set permissions on directory "/data/x": Operation not permitted') ?? '', /owned by the user PostgreSQL runs as/);
    assert.match(tablespaceLocationError(400, 'directory "/data/x/PG_17_202406281" already in use as a tablespace') ?? '', /already in use/);
    assert.equal(tablespaceLocationError(409, 'tablespace "x" already exists'), undefined);
});
