// pgmanager's HTTP client, of the resources the UI shows

import type { Client } from '@mutablelogic/go-server/api';
import { request } from '@mutablelogic/go-server/api';
import type { DatabaseRequest } from './databaseform.ts';
import type { SchemaRequest } from './schemaform.ts';
import type { TablespaceRequest } from './tablespaceform.ts';
import type { RoleRequest } from './roleform.ts';
import { ListLimits, type Connection, type Database, type Extension, type List, type PgObject, type ReplicationSlot, type Role, type Schema, type Setting, type Statement, type StatementSort, type Tablespace } from './schema.ts';

// listAll returns every page of a list, a page at a time, of at most the
// limit, from the offset of the items it has, until it has them all
export async function listAll<T>(page: (offset: number, limit: number) => Promise<List<T>>, limit: number): Promise<List<T>> {
    const body: T[] = [];
    for (;;) {
        const list = await page(body.length, limit);
        body.push(...(list.body ?? []));
        if (!list.body?.length || body.length >= list.count) {
            return { count: list.count, body };
        }
    }
}

// listDatabases returns a page of the databases
export function listDatabases(client: Client, offset: number, limit: number): Promise<List<Database>> {
    return request<List<Database>>(client, 'GET', 'database', { query: { offset, limit } });
}

// createDatabase creates a database, and returns it
export function createDatabase(client: Client, db: DatabaseRequest): Promise<Database> {
    return request<Database>(client, 'POST', 'database', { body: db });
}

// updateDatabase changes a database, by its name, and returns it
export function updateDatabase(client: Client, name: string, db: DatabaseRequest): Promise<Database> {
    return request<Database>(client, 'PATCH', `database/${encodeURIComponent(name)}`, { body: db });
}

// deleteDatabase deletes a database, by its name, and everything in it, and
// returns it as it was. It fails while it's connected to.
export function deleteDatabase(client: Client, name: string): Promise<Database> {
    return request<Database>(client, 'DELETE', `database/${encodeURIComponent(name)}`);
}

// getDatabase returns a database, by its name
export function getDatabase(client: Client, name: string): Promise<Database> {
    return request<Database>(client, 'GET', `database/${encodeURIComponent(name)}`);
}

// listRoles returns a page of the roles: PostgreSQL's own, such as
// pg_monitor, when predefined is true, or else the others
export function listRoles(client: Client, predefined: boolean, offset: number, limit: number): Promise<List<Role>> {
    return request<List<Role>>(client, 'GET', 'role', { query: { predefined, offset, limit } });
}

// createRole creates a role, and returns it
export function createRole(client: Client, role: RoleRequest): Promise<Role> {
    return request<Role>(client, 'POST', 'role', { body: role });
}

// updateRole changes a role, by its name, and returns it
export function updateRole(client: Client, name: string, role: RoleRequest): Promise<Role> {
    return request<Role>(client, 'PATCH', `role/${encodeURIComponent(name)}`, { body: role });
}

// deleteRole deletes a role, by its name, and returns it as it was
export function deleteRole(client: Client, name: string): Promise<Role> {
    return request<Role>(client, 'DELETE', `role/${encodeURIComponent(name)}`);
}

// getRole returns a role, by its name
export function getRole(client: Client, name: string): Promise<Role> {
    return request<Role>(client, 'GET', `role/${encodeURIComponent(name)}`);
}

// ObjectFilter is which objects of a database are listed: those in a
// schema, or of a type, or all of them
export interface ObjectFilter {
    database: string;
    schema?: string;
    type?: string;
}

// listObjects returns a page of the objects of a database, of the filter
export function listObjects(client: Client, filter: ObjectFilter, offset: number, limit: number): Promise<List<PgObject>> {
    return request<List<PgObject>>(client, 'GET', 'object', { query: { ...filter, offset, limit } });
}

// listAllDatabases returns every database
export function listAllDatabases(client: Client): Promise<List<Database>> {
    return listAll((offset, limit) => listDatabases(client, offset, limit), ListLimits.database);
}

// listAllSchemas returns every schema of a database
export function listAllSchemas(client: Client, database: string): Promise<List<Schema>> {
    return listAll((offset, limit) => listSchemas(client, database, offset, limit), ListLimits.schema);
}

// createSchema creates a schema in a database, and returns it
export function createSchema(client: Client, database: string, schema: SchemaRequest): Promise<Schema> {
    return request<Schema>(client, 'POST', `schema/${encodeURIComponent(database)}`, { body: schema });
}

// updateSchema changes a schema in a database, by its name, and returns it
export function updateSchema(client: Client, database: string, name: string, schema: SchemaRequest): Promise<Schema> {
    return request<Schema>(client, 'PATCH', `schema/${encodeURIComponent(database)}/${encodeURIComponent(name)}`, { body: schema });
}

// deleteSchema deletes a schema in a database, by its name, and returns it as
// it was. It fails while it has objects, unless force, which deletes them.
export function deleteSchema(client: Client, database: string, name: string, force = false): Promise<Schema> {
    return request<Schema>(client, 'DELETE', `schema/${encodeURIComponent(database)}/${encodeURIComponent(name)}`, { query: { force: force || undefined } });
}

// listSchemas returns a page of the schemas of a database
export function listSchemas(client: Client, database: string, offset: number, limit: number): Promise<List<Schema>> {
    return request<List<Schema>>(client, 'GET', `schema/${encodeURIComponent(database)}`, { query: { offset, limit } });
}

// getObject returns an object, by its database, schema and name
export function getObject(client: Client, object: PgObject): Promise<PgObject> {
    const path = [object.database ?? '', object.schema ?? '', object.name ?? ''].map(encodeURIComponent).join('/');
    return request<PgObject>(client, 'GET', `object/${path}`);
}

// ConnectionFilter is the database whose connections are listed, or all
// of them, and what they're sorted by, in which order
export interface ConnectionFilter {
    database?: string;
    sort?: string;
    order?: 'asc' | 'desc';
}

// listConnections returns a page of the connections to the server, of the
// filter
export function listConnections(client: Client, filter: ConnectionFilter, offset: number, limit: number): Promise<List<Connection>> {
    return request<List<Connection>>(client, 'GET', 'connection', { query: { ...filter, offset, limit } });
}

// getConnection returns a connection, by its process
export function getConnection(client: Client, pid: number): Promise<Connection> {
    return request<Connection>(client, 'GET', `connection/${pid}`);
}

// listSettings returns a page of the settings, of a category, or of all
export function listSettings(client: Client, category: string | undefined, offset: number, limit: number): Promise<List<Setting>> {
    return request<List<Setting>>(client, 'GET', 'setting', { query: { category, offset, limit } });
}

// listSettingCategories returns the categories of the settings
export function listSettingCategories(client: Client): Promise<List<string>> {
    return request<List<string>>(client, 'GET', 'setting/category');
}

// getSetting returns a setting, by its name
export function getSetting(client: Client, name: string): Promise<Setting> {
    return request<Setting>(client, 'GET', `setting/${encodeURIComponent(name)}`);
}

// listTablespaces returns a page of the tablespaces
export function listTablespaces(client: Client, offset: number, limit: number): Promise<List<Tablespace>> {
    return request<List<Tablespace>>(client, 'GET', 'tablespace', { query: { offset, limit } });
}

// createTablespace creates a tablespace, in its location, and returns it
export function createTablespace(client: Client, space: TablespaceRequest): Promise<Tablespace> {
    return request<Tablespace>(client, 'POST', 'tablespace', { body: space });
}

// updateTablespace changes a tablespace, by its name, and returns it
export function updateTablespace(client: Client, name: string, space: TablespaceRequest): Promise<Tablespace> {
    return request<Tablespace>(client, 'PATCH', `tablespace/${encodeURIComponent(name)}`, { body: space });
}

// deleteTablespace deletes a tablespace, by its name, and returns it as it
// was. It fails while anything is in it.
export function deleteTablespace(client: Client, name: string): Promise<Tablespace> {
    return request<Tablespace>(client, 'DELETE', `tablespace/${encodeURIComponent(name)}`);
}

// getTablespace returns a tablespace, by its name
export function getTablespace(client: Client, name: string): Promise<Tablespace> {
    return request<Tablespace>(client, 'GET', `tablespace/${encodeURIComponent(name)}`);
}

// listReplicationSlots returns a page of the replication slots
export function listReplicationSlots(client: Client, offset: number, limit: number): Promise<List<ReplicationSlot>> {
    return request<List<ReplicationSlot>>(client, 'GET', 'replication-slot', { query: { offset, limit } });
}

// getReplicationSlot returns a replication slot, by its name
export function getReplicationSlot(client: Client, name: string): Promise<ReplicationSlot> {
    return request<ReplicationSlot>(client, 'GET', `replication-slot/${encodeURIComponent(name)}`);
}

// StatementFilter is how the statements are sorted, in which order, or
// the sort's own, and the database whose statements are listed, or all of
// them
export interface StatementFilter {
    sort: StatementSort;
    order?: 'asc' | 'desc';
    database?: string;
}

// listStatements returns a page of the statements' statistics, of the filter
export function listStatements(client: Client, filter: StatementFilter, offset: number, limit: number): Promise<List<Statement>> {
    return request<List<Statement>>(client, 'GET', 'statement', { query: { ...filter, offset, limit } });
}

// resetStatements resets the statements' statistics
export async function resetStatements(client: Client): Promise<void> {
    await request<void>(client, 'DELETE', 'statement');
}

// ExtensionFilter is the database whose extensions are listed, or none, for
// those the server has available, and whether only those installed are
export interface ExtensionFilter {
    database?: string;
    installed?: boolean;
}

// listExtensions returns a page of the extensions, of the filter
export function listExtensions(client: Client, filter: ExtensionFilter, offset: number, limit: number): Promise<List<Extension>> {
    return request<List<Extension>>(client, 'GET', 'extension', { query: { ...filter, offset, limit } });
}
