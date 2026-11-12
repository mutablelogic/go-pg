// pgmanager's views: the columns of the tables of the resources, and the
// bodies of the panels which show one

import { html, type TemplateResult } from 'lit';
import type { Column } from '../core/table.ts';
import type { Tile } from '../core/tiles.ts';
import type { Connection, PgObject, ReplicationSlot, Role, Schema, Setting, Statement, Tablespace } from './schema.ts';

// Carbon components
import '@carbon/web-components/es/components/button/index.js';
import '@carbon/web-components/es/components/content-switcher/index.js';
import '@carbon/web-components/es/components/dropdown/index.js';
import '@carbon/web-components/es/components/tag/tag.js';

// formatBytes returns a size, such as 7.4 MB
export function formatBytes(bytes?: number): string {
    if (bytes === undefined) {
        return '';
    }
    const units = ['B', 'kB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unit = 0;
    while (size >= 1000 && unit < units.length - 1) {
        size /= 1000;
        unit++;
    }
    return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

// schemaTile is a schema's tile, with its owner, and its size
export function schemaTile(schema: Schema): Tile {
    return { value: schema.schema, label: schema.schema, badge: schema.owner, text: formatBytes(schema.bytes), title: `${schema.schema}, owned by ${schema.owner ?? 'nobody'}` };
}

// The types of objects which can be chosen, after all of them, as some,
// such as partitioned indexes, are rare
const objectTypeLabels: [string, string][] = [
    ['', 'All'], ['TABLE', 'Tables'], ['PARTITIONED TABLE', 'Partitioned tables'], ['VIEW', 'Views'],
    ['MATERIALIZED VIEW', 'Materialized views'], ['INDEX', 'Indexes'], ['SEQUENCE', 'Sequences'], ['FOREIGN TABLE', 'Foreign tables'],
];

// objectTypes are the buttons of the types of objects, of which the type,
// or all, is chosen, which call the function with a type which is chosen
export function objectTypes(type: string | undefined, onType: (type?: string) => void): TemplateResult {
    return html`
        <cds-content-switcher size="sm" value=${type ?? ''} @cds-content-switcher-selected=${(event: CustomEvent<{ item: { value: string } }>) => onType(event.detail.item.value || undefined)}>
            ${objectTypeLabels.map(([value, label]) => html`<cds-content-switcher-item value=${value} ?selected=${value === (type ?? '')}>${label}</cds-content-switcher-item>`)}
        </cds-content-switcher>
    `;
}

export const objectColumns: Column<PgObject>[] = [
    { header: 'Schema', cell: (object) => object.schema ?? '' },
    { header: 'Name', cell: (object) => object.name ?? '' },
    { header: 'Type', cell: (object) => object.type ? html`<cds-tag size="sm" type="gray">${object.type.toLowerCase()}</cds-tag>` : '' },
    { header: 'Owner', cell: (object) => object.owner ?? '' },
    { header: 'Rows', cell: (object) => object.table?.live_tuples?.toLocaleString() ?? '' },
    { header: 'Size', cell: (object) => formatBytes(object.bytes) },
];

// roleFlags returns the role's permissions, which are true
function roleFlags(role: Role): string[] {
    const flags: [string, boolean | undefined][] = [
        ['superuser', role.super], ['login', role.login], ['create roles', role.createrole],
        ['create databases', role.createdb], ['replication', role.replication], ['bypass RLS', role.bypassrls],
    ];
    return flags.filter(([, on]) => on).map(([name]) => name);
}

export const roleColumns: Column<Role>[] = [
    { header: 'Role', cell: (role) => role.name },
    { header: 'Permissions', cell: (role) => roleFlags(role).map((flag) => html`<cds-tag size="sm" type="gray">${flag}</cds-tag>`) },
    { header: 'Member of', cell: (role) => (role.memberof ?? []).join(', ') },
];

// fields returns the fields, which have values, as a list
function fields(entries: [string, unknown][]): TemplateResult {
    return html`<dl>${entries.filter(([, value]) => value !== undefined && value !== '').map(([name, value]) => html`<dt>${name}</dt><dd>${value}</dd>`)}</dl>`;
}

export function objectBody(object: PgObject): TemplateResult {
    return fields([
        ['Type', object.type?.toLowerCase()],
        ['Database', object.database],
        ['Schema', object.schema],
        ['Owner', object.owner],
        ['Size', formatBytes(object.bytes)],
        ['Rows', object.table?.live_tuples?.toLocaleString()],
        ['Dead rows', object.table?.dead_tuples?.toLocaleString()],
        ['Tablespace', object.tablespace],
        ['OID', object.oid],
        ['Access', object.acl?.length ? JSON.stringify(object.acl) : undefined],
    ]);
}

export function roleBody(role: Role): TemplateResult {
    return fields([
        ['Permissions', roleFlags(role).join(', ') || 'none'],
        ['Member of', role.memberof?.join(', ')],
        ['Connection limit', role.conlimit],
        ['Expires', role.expires],
        ['OID', role.oid],
    ]);
}


// formatTime returns a time, such as 12 Nov 2026, 17:56:37, or nothing,
// when it's the zero time, as Go's is
function formatTime(time?: string): string {
    if (!time || time.startsWith('0001-')) {
        return '';
    }
    return new Date(time).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
}

// tag returns a tag, such as of a state, or nothing, when there isn't one
function tag(text?: string, type = 'gray') {
    return text ? html`<cds-tag size="sm" type=${type}>${text}</cds-tag>` : '';
}

// The connections' states, and their tags' colours
const stateColours: Record<string, string> = { active: 'green', idle: 'gray', 'idle in transaction': 'warm-gray', 'idle in transaction (aborted)': 'red' };

export const connectionColumns: Column<Connection>[] = [
    { header: 'PID', cell: (conn) => conn.pid },
    { header: 'Database', cell: (conn) => conn.database, sort: 'database', sortFirst: 'ascending' },
    { header: 'Role', cell: (conn) => conn.role, sort: 'role', sortFirst: 'ascending' },
    { header: 'Application', cell: (conn) => conn.application ?? '', sort: 'application', sortFirst: 'ascending' },
    { header: 'Client', cell: (conn) => conn.client_addr ? `${conn.client_addr}:${conn.client_port ?? ''}` : 'local', sort: 'client', sortFirst: 'ascending' },
    { header: 'State', cell: (conn) => tag(conn.state, stateColours[conn.state ?? ''] ?? 'gray'), sort: 'state', sortFirst: 'ascending' },
    { header: 'Query', cell: (conn) => html`<span style="display: block; max-width: 16rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap" title=${conn.query ?? ''}>${conn.query ?? ''}</span>` },
];

export function connectionBody(conn: Connection): TemplateResult {
    return fields([
        ['State', conn.state],
        ['Database', conn.database],
        ['Role', conn.role],
        ['Application', conn.application],
        ['Client', conn.client_addr ? `${conn.client_addr}:${conn.client_port ?? ''}` : 'local'],
        ['Connected', formatTime(conn.conn_start)],
        ['Query started', formatTime(conn.query_start)],
        ['Query', conn.query ? html`<pre>${conn.query}</pre>` : undefined],
    ]);
}

// settingValue returns a setting's value, with its unit
function settingValue(setting: Setting): string {
    return setting.value === null ? '' : setting.unit ? `${setting.value} ${setting.unit}` : setting.value;
}

export const settingColumns: Column<Setting>[] = [
    { header: 'Setting', cell: (setting) => setting.name },
    { header: 'Value', cell: (setting) => settingValue(setting) },
    { header: 'Category', cell: (setting) => setting.category },
    { header: 'Context', cell: (setting) => tag(setting.context) },
];

export function settingBody(setting: Setting): TemplateResult {
    return html`
        ${setting.description ? html`<p class="description">${setting.description}</p>` : ''}
        ${setting.extra_desc ? html`<p class="description">${setting.extra_desc}</p>` : ''}
        ${fields([['Value', settingValue(setting)], ['Category', setting.category], ['Context', setting.context]])}
    `;
}

// settingCategories is the dropdown of the settings' categories, of which
// the category, or all, is chosen, which calls the function with the
// category which is chosen
export function settingCategories(categories: readonly string[], category: string | undefined, onCategory: (category?: string) => void): TemplateResult {
    return html`
        <cds-dropdown type="inline" title-text="Category" value=${category ?? ''} @cds-dropdown-selected=${(event: CustomEvent<{ item: { value: string } }>) => onCategory(event.detail.item.value || undefined)}>
            <cds-dropdown-item value="">All</cds-dropdown-item>
            ${categories.map((name) => html`<cds-dropdown-item value=${name}>${name}</cds-dropdown-item>`)}
        </cds-dropdown>
    `;
}

export const tablespaceColumns: Column<Tablespace>[] = [
    { header: 'Tablespace', cell: (space) => space.name },
    { header: 'Owner', cell: (space) => space.owner ?? '' },
    { header: 'Location', cell: (space) => space.location || 'default' },
    { header: 'Size', cell: (space) => formatBytes(space.bytes) },
];

export function tablespaceBody(space: Tablespace): TemplateResult {
    return fields([
        ['Owner', space.owner],
        ['Location', space.location || 'default'],
        ['Size', formatBytes(space.bytes)],
        ['Options', space.options?.join(', ')],
        ['OID', space.oid],
        ['Access', space.acl?.length ? JSON.stringify(space.acl) : undefined],
    ]);
}

// formatLag returns how far a slot's client lags, in bytes, and time
function formatLag(slot: ReplicationSlot): string {
    return [slot.lag_bytes === undefined ? '' : formatBytes(slot.lag_bytes), slot.lag_ms === undefined ? '' : `${(slot.lag_ms / 1000).toFixed(1)} s`].filter(Boolean).join(', ');
}

export const replicationSlotColumns: Column<ReplicationSlot>[] = [
    { header: 'Slot', cell: (slot) => slot.name },
    { header: 'Type', cell: (slot) => tag(slot.type) },
    { header: 'Database', cell: (slot) => slot.database ?? '' },
    { header: 'Plugin', cell: (slot) => slot.plugin ?? '' },
    { header: 'Status', cell: (slot) => tag(slot.status, slot.status === 'active' || slot.status === 'streaming' ? 'green' : 'gray') },
    { header: 'Lag', cell: (slot) => formatLag(slot) },
];

export function replicationSlotBody(slot: ReplicationSlot): TemplateResult {
    return fields([
        ['Type', slot.type],
        ['Status', slot.status],
        ['Database', slot.database],
        ['Plugin', slot.plugin],
        ['Client', slot.client_addr],
        ['Lag', formatLag(slot)],
        ['Temporary', slot.temporary ? 'yes' : undefined],
        ['Two-phase', slot.two_phase ? 'yes' : undefined],
    ]);
}

// formatMs returns a time, in milliseconds, such as 1.2 ms, or 3.4 s
function formatMs(ms: number): string {
    if (ms >= 60_000) {
        return `${(ms / 60_000).toFixed(1)} min`;
    }
    if (ms >= 1000) {
        return `${(ms / 1000).toFixed(1)} s`;
    }
    return `${ms < 10 ? ms.toFixed(2) : ms.toFixed(0)} ms`;
}

export const statementColumns: Column<Statement>[] = [
    { header: 'Query', cell: (stmt) => html`<span style="display: block; max-width: 14rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap" title=${stmt.query}>${stmt.query}</span>` },
    { header: 'Database', cell: (stmt) => stmt.database ?? '' },
    { header: 'Role', cell: (stmt) => stmt.role ?? '' },
    { header: 'Calls', cell: (stmt) => stmt.calls.toLocaleString(), sort: 'calls' },
    { header: 'Rows', cell: (stmt) => stmt.rows.toLocaleString(), sort: 'rows' },
    { header: 'Total', cell: (stmt) => formatMs(stmt.total_ms), sort: 'total_ms' },
    { header: 'Mean', cell: (stmt) => formatMs(stmt.mean_ms), sort: 'mean_ms' },
    { header: 'Min', cell: (stmt) => formatMs(stmt.min_ms), sort: 'min_ms', sortFirst: 'ascending' },
    { header: 'Max', cell: (stmt) => formatMs(stmt.max_ms), sort: 'max_ms' },
];

export function statementBody(stmt: Statement): TemplateResult {
    return html`
        <pre class="statement">${stmt.query}</pre>
        ${fields([
            ['Database', stmt.database],
            ['Role', stmt.role],
            ['Calls', stmt.calls.toLocaleString()],
            ['Rows', stmt.rows.toLocaleString()],
            ['Total', formatMs(stmt.total_ms)],
            ['Mean', formatMs(stmt.mean_ms)],
            ['Min', formatMs(stmt.min_ms)],
            ['Max', formatMs(stmt.max_ms)],
            ['Query ID', String(stmt.query_id)],
        ])}
    `;
}
