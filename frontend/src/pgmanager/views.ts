// pgmanager's views: the columns of the tables of the resources, and the
// bodies of the panels which show one

import { html, type TemplateResult } from 'lit';
import { you } from './current.ts';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import UserIcon from '@carbon/icons/es/user/16.js';
import TrashIcon from '@carbon/icons/es/trash-can/16.js';
import EditIcon from '@carbon/icons/es/edit/16.js';
import RestartIcon from '@carbon/icons/es/restart/16.js';
import type { Column, Legend } from '@mutablelogic/go-server/list/view';
import type { Tile } from '@mutablelogic/go-server/list/tiles';
import type { Connection, Extension, PgObject, ReplicationSlot, Role, Schema, Setting, Statement, Tablespace } from './schema.ts';

// Carbon components
import '@carbon/web-components/es/components/button/index.js';
import '@carbon/web-components/es/components/content-switcher/index.js';
import '@carbon/web-components/es/components/dropdown/index.js';
import '@carbon/web-components/es/components/icon-button/index.js';
import '@carbon/web-components/es/components/link/index.js';
import '@carbon/web-components/es/components/tag/tag.js';
import '@carbon/web-components/es/components/notification/index.js';

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

// description is a comment, in a table's cell, which is cut short, at the
// width, and shown in full when it's hovered. It's wider when it's the last
// column, as there are no columns after it to push out of the table.
function description(comment?: string, width = '20rem') {
    return comment ? html`<span style="display: block; max-width: ${width}; overflow: hidden; text-overflow: ellipsis" title=${comment}>${comment}</span>` : '';
}

// roleName is a role's name, with a person before it, so it's seen to be a
// role, in a table, whose cells are styled here, as the page's styles don't
// reach into the table
export function roleName(name?: string) {
    if (!name) {
        return '';
    }
    return html`<span style="display: inline-flex; align-items: center; gap: 0.25rem">${iconLoader(UserIcon, { width: '12', height: '12', 'aria-hidden': 'true', style: 'flex: none; fill: currentColor' })}${name}<pgm-you name=${name}></pgm-you></span>`;
}

// you, and setCurrentRole, which badges the role pgmanager runs as,
// wherever it's shown, once it's known
export { setCurrentRole, you } from './current.ts';

// ownerBadge is a tile's badge of its owner, with a person, which says it's
// the owner, when it's hovered
export function ownerBadge(owner?: string): Pick<Tile, 'badge' | 'badgeIcon' | 'badgeTitle'> {
    return owner ? { badge: owner, badgeIcon: iconLoader(UserIcon), badgeTitle: 'Owner' } : {};
}

// settingCategoryTile is a top level of the settings' categories, with how
// many settings it has, and a pencil, with how many of them are changed,
// unless none are, and its name, which may be cut short, when it's hovered
export function settingCategoryTile(name: string, count: number | undefined, changed: number): Tile {
    const badge = changed ? { badge: changed.toLocaleString(), badgeIcon: iconLoader(EditIcon), badgeTitle: `${changed.toLocaleString()} changed from their defaults` } : {};
    return { value: name, label: name, text: count?.toLocaleString(), title: name, ...badge };
}

// schemaTile is a schema's tile, with its owner, and its size
export function schemaTile(schema: Schema): Tile {
    return { value: schema.schema, label: schema.schema, ...ownerBadge(schema.owner), text: formatBytes(schema.bytes), title: schema.comment ?? `${schema.schema}, owned by ${schema.owner ?? 'nobody'}` };
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
    { header: 'Description', cell: (object) => description(object.comment) },
    { header: 'Type', cell: (object) => object.type ? html`<cds-tag size="sm" type="gray">${object.type.toLowerCase()}</cds-tag>` : '' },
    { header: 'Owner', cell: (object) => roleName(object.owner) },
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

// roleLinks are the names of roles, such as those a role is a member of,
// as links, which call the function with a role's name, to open it
function roleLinks(names: readonly string[] | undefined, onRole: (name: string) => void) {
    return html`<span style="display: inline-flex; flex-wrap: wrap; gap: 0 0.75rem">${(names ?? []).map((name) => html`<cds-link href="#" @click=${(event: Event) => {
        event.preventDefault();
        event.stopPropagation();
        onRole(name);
    }}>${roleName(name)}</cds-link>`)}</span>`;
}

// roleColumns are the roles' columns, whose roles they're members of open
// with the function. PostgreSQL's own roles have no permissions, so they've
// no column of them.
export const roleColumns = (onRole: (name: string) => void, predefined = false): Column<Role>[] => [
    { header: 'Role', cell: (role) => roleName(role.name), sort: 'name', sortFirst: 'ascending' },
    { header: 'Description', cell: (role) => description(role.comment) },
    ...(predefined ? [] : [{ header: 'Permissions', cell: (role: Role) => roleFlags(role).map((flag) => html`<cds-tag size="sm" type="gray">${flag}</cds-tag>`) }]),
    { header: 'Member of', cell: (role) => roleLinks(role.memberof, onRole) },
];

// fields returns the fields, which have values, as a list
function fields(entries: [string, unknown][]): TemplateResult {
    return html`<dl>${entries.filter(([, value]) => value !== undefined && value !== '').map(([name, value]) => html`<dt>${name}</dt><dd>${value}</dd>`)}</dl>`;
}

export function objectBody(object: PgObject): TemplateResult {
    return fields([
        ['Description', object.comment],
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

// removeButton is a small button, of a trash can, which removes a row of a
// table, and says what it removes, when it's hovered, and to assistive
// technology
export function removeButton(label: string, onRemove: () => void) {
    return html`
        <cds-icon-button kind="ghost" size="sm" align="left" @click=${onRemove}>
            ${iconLoader(TrashIcon, { slot: 'icon' })}
            <span slot="tooltip-content">${label}</span>
        </cds-icon-button>
    `;
}

// roleMembers is a table of roles, such as those a role is a member of,
// with their descriptions, whose names open them, with a button on each,
// which removes it, when there's onRemove
export function roleMembers(names: readonly string[], describe: (name: string) => string | undefined, onRole: (name: string) => void, onRemove?: (name: string) => void): TemplateResult {
    return html`
        <cds-table size="sm" class="members">
            <cds-table-head>
                <cds-table-header-row>
                    <cds-table-header-cell>Role</cds-table-header-cell>
                    <cds-table-header-cell>Description</cds-table-header-cell>
                    ${onRemove ? html`<cds-table-header-cell><span class="visually-hidden">Remove</span></cds-table-header-cell>` : ''}
                </cds-table-header-row>
            </cds-table-head>
            <cds-table-body>
                ${names.map((name) => html`
                    <cds-table-row>
                        <cds-table-cell>${roleLinks([name], onRole)}</cds-table-cell>
                        <cds-table-cell>${describe(name) ?? ''}</cds-table-cell>
                        ${onRemove ? html`
                            <cds-table-cell class="remove">
                                ${removeButton(`Remove ${name}`, () => onRemove(name))}
                            </cds-table-cell>
                        ` : ''}
                    </cds-table-row>
                `)}
            </cds-table-body>
        </cds-table>
    `;
}

export function roleBody(role: Role, onRole: (name: string) => void, describe: (name: string) => string | undefined = () => undefined): TemplateResult {
    return html`
        ${fields([
            ['Description', role.comment],
            ['Permissions', roleFlags(role).join(', ') || 'none'],
            ['Connection limit', role.conlimit],
            // An expiry is a day, which the form sets at its start, in UTC
        ['Expires', role.expires ? new Date(role.expires).toLocaleDateString(undefined, { dateStyle: 'medium', timeZone: 'UTC' }) : undefined],
            ['OID', role.oid],
        ])}
        ${role.memberof?.length ? html`
            <h4 class="section">Member of</h4>
            ${roleMembers(role.memberof, describe, onRole)}
        ` : ''}
    `;
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
    { header: 'PID', cell: (conn) => html`${conn.pid}${conn.current ? you : ''}` },
    { header: 'Database', cell: (conn) => conn.database, sort: 'database', sortFirst: 'ascending' },
    { header: 'Role', cell: (conn) => roleName(conn.role), sort: 'role', sortFirst: 'ascending' },
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

// settingValue returns a setting's value, or another of its values, such as
// its default, with its unit
function settingValue(setting: Setting): string {
    return setting.value === null ? '' : withUnit(setting, setting.value);
}

// withUnit is one of a setting's values, such as its default, with its unit
function withUnit(setting: Setting, value: string): string {
    return setting.unit ? `${value} ${setting.unit}` : value;
}

// settingDefault is a setting's default, with its unit, which is said to be
// empty, when it is, such as application_name's, or else undefined, when the
// setting doesn't have one
function settingDefault(setting: Setting): string | TemplateResult | undefined {
    if (setting.default === undefined) {
        return undefined;
    }
    return setting.default === '' ? html`<em>empty</em>` : withUnit(setting, setting.default);
}

// settingLimits are the limits of an integer or real, with its unit
function settingLimits(setting: Setting): string | undefined {
    return setting.min !== undefined && setting.max !== undefined ? `${setting.min} to ${withUnit(setting, setting.max)}` : undefined;
}

// settingIcon is a small icon, after a setting's value, which says what it
// means when it's hovered
function settingIcon(icon: typeof EditIcon, title: string) {
    return html`<span title=${title} style="display: inline-flex">${iconLoader(icon, { width: '12', height: '12', 'aria-label': title, style: 'flex: none; fill: currentColor' })}</span>`;
}

// settingValueCell is a setting's value, in a table, with a pencil after it
// when it's changed from its default, and a restart when it's changed, but
// waits for the server to restart, which say so when they're hovered
function settingValueCell(setting: Setting) {
    if (!setting.changed && !setting.pending_restart) {
        return settingValue(setting);
    }
    return html`<span style="display: inline-flex; align-items: center; gap: 0.25rem">${settingValue(setting)}${setting.changed ? settingIcon(EditIcon, 'Changed from its default') : ''}${setting.pending_restart ? settingIcon(RestartIcon, 'Changed, and waiting for the server to restart') : ''}</span>`;
}

// subcategory is a setting's category, under the top level, such as Locale
// and Formatting, under Client Connection Defaults, or else its category
function subcategory(category: string, top?: string): string {
    if (!top) {
        return category;
    }
    return category.startsWith(`${top} / `) ? category.slice(top.length + 3) : '';
}

// settingColumns are the settings' columns, of a top level of their
// categories, whose settings show the category under it, or of all
export const settingColumns = (top?: string): Column<Setting>[] => [
    { header: 'Setting', cell: (setting) => setting.name, sort: 'name', sortFirst: 'ascending' },
    { header: 'Value', cell: (setting) => settingValueCell(setting) },
    { header: 'Category', cell: (setting) => subcategory(setting.category, top), sort: 'category', sortFirst: 'ascending' },
    { header: 'Context', cell: (setting) => tag(setting.context), sort: 'context', sortFirst: 'ascending' },
];

// restartBanner says which settings wait for the server to restart, with a
// button which restarts it, when pgmanager can, or else nothing, when none
// wait
export function restartBanner(pending: readonly string[], canRestart: boolean, onRestart: () => void): TemplateResult {
    if (!pending.length) {
        return html``;
    }
    const title = pending.length === 1 ? '1 setting waits for the server to restart' : `${pending.length} settings wait for the server to restart`;
    return html`
        <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem">
            <cds-inline-notification kind="warning" low-contrast hide-close-button style="flex: 1; max-width: none; margin: 0"
                title=${title} subtitle=${`${pending.join(', ')}${canRestart ? '' : '. Restart PostgreSQL to apply them.'}`}></cds-inline-notification>
            ${canRestart ? html`<cds-button kind="danger" size="md" @click=${onRestart}>Restart</cds-button>` : ''}
        </div>
    `;
}

// settingLegend explains the settings' table: the icons after a value, and
// each context, from the least which can change it to the most, as it's
// tagged in the table
export const settingLegend: Legend = {
    title: 'Legend',
    items: [
        { term: settingIcon(EditIcon, 'Changed from its default'), description: 'Changed from its default, such as in the configuration file, or by ALTER SYSTEM' },
        { term: settingIcon(RestartIcon, 'Waiting for a restart'), description: 'Changed, but waiting for the server to restart' },
        { term: tag('internal'), description: "Set when PostgreSQL is built, or its data directory is created, and can't be changed" },
        { term: tag('postmaster'), description: 'Changed in the configuration, and takes effect when the server restarts' },
        { term: tag('sighup'), description: "Changed in the configuration, and takes effect in every session, once it's reloaded" },
        { term: tag('superuser-backend'), description: 'Changed in the configuration, or by a superuser when connecting, and takes effect in new sessions' },
        { term: tag('backend'), description: 'Changed in the configuration, or when connecting, and takes effect in new sessions' },
        { term: tag('superuser'), description: "Changed in the configuration, once it's reloaded, or by a superuser in their session, with SET" },
        { term: tag('user'), description: "Changed in the configuration, once it's reloaded, or by anyone in their session, with SET" },
    ],
};

// settingBody is a setting in the panel, with its values in the
// configuration files, and when a change takes effect, or why it can't be
// changed
export function settingBody(setting: Setting, effect?: string, readOnly?: string): TemplateResult {
    return html`
        ${setting.description ? html`<p class="description">${setting.description}</p>` : ''}
        ${setting.extra_desc ? html`<p class="description">${setting.extra_desc}</p>` : ''}
        ${setting.pending_restart ? html`<p class="helper">It's changed, but the server needs to restart before the change takes effect.</p>` : ''}
        ${readOnly ? html`<p class="helper">${readOnly}</p>` : ''}
        ${fields([
            ['Value', settingValue(setting)],
            ['Set by ALTER SYSTEM', setting.override],
            ['Configuration file', setting.config],
            ['Default', settingDefault(setting)],
            ['Source', setting.source],
            ['Type', setting.type],
            ['Limits', settingLimits(setting)],
            ['Values', setting.values?.join(', ')],
            ['Category', setting.category],
            ['Context', setting.context],
            ['Takes effect', effect],
        ])}
    `;
}

export const tablespaceColumns: Column<Tablespace>[] = [
    { header: 'Tablespace', cell: (space) => space.name, sort: 'name', sortFirst: 'ascending' },
    { header: 'Description', cell: (space) => description(space.comment) },
    { header: 'Owner', cell: (space) => roleName(space.owner), sort: 'owner', sortFirst: 'ascending' },
    { header: 'Location', cell: (space) => space.location || 'default', sort: 'location', sortFirst: 'ascending' },
    { header: 'Size', cell: (space) => formatBytes(space.bytes), sort: 'size' },
];

// tablespaceBody is a tablespace in the panel, with the databases which have
// files in it, as their default, or for some of their tables, or indexes,
// which link to their pages, unless they've none, such as template1, which
// it can't be deleted while it has
export function tablespaceBody(space: Tablespace, isPage: (database: string) => boolean = () => true): TemplateResult {
    const databases = space.databases ?? [];
    return html`
        ${fields([
            ['Description', space.comment],
            ['Owner', space.owner],
            ['Location', space.location || 'default'],
            ['Size', formatBytes(space.bytes)],
            ['Options', space.options?.join(', ')],
            ['OID', space.oid],
        ])}
        <h4 class="section">Used by</h4>
        ${databases.length ? html`
            <p class="helper">These databases have files in it, as their default, or for some of their tables, or indexes. It can't be deleted until they've none.</p>
            <ul class="links">
                ${databases.map((name) => html`<li>${isPage(name) ? html`<cds-link href="#database/${encodeURIComponent(name)}">${name}</cds-link>` : html`${name} <span class="helper">(a template)</span>`}</li>`)}
            </ul>
        ` : html`<p class="helper">Nothing is in it, so it can be deleted.</p>`}
    `;
}

// formatLag returns how far a slot's client lags, in bytes, and time
function formatLag(slot: ReplicationSlot): string {
    return [slot.lag_bytes === undefined ? '' : formatBytes(slot.lag_bytes), slot.lag_ms === undefined ? '' : `${(slot.lag_ms / 1000).toFixed(1)} s`].filter(Boolean).join(', ');
}

export const replicationSlotColumns: Column<ReplicationSlot>[] = [
    { header: 'Slot', cell: (slot) => slot.name, sort: 'name', sortFirst: 'ascending' },
    { header: 'Type', cell: (slot) => tag(slot.type), sort: 'type', sortFirst: 'ascending' },
    { header: 'Database', cell: (slot) => slot.database ?? '', sort: 'database', sortFirst: 'ascending' },
    { header: 'Plugin', cell: (slot) => slot.plugin ?? '', sort: 'plugin', sortFirst: 'ascending' },
    { header: 'Status', cell: (slot) => tag(slot.status, slot.status === 'active' || slot.status === 'streaming' ? 'green' : 'gray'), sort: 'status', sortFirst: 'ascending' },
    { header: 'Lag', cell: (slot) => formatLag(slot), sort: 'lag' },
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
    { header: 'Role', cell: (stmt) => roleName(stmt.role) },
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

// extensionVersion is an extension's version: the version which is
// installed, with the default, when it's another, which mayn't be newer, or
// else the default
function extensionVersion(ext: Extension): string {
    if (!ext.installed_version) {
        return ext.default_version ?? '';
    }
    if (ext.default_version && ext.default_version !== ext.installed_version) {
        return `${ext.installed_version} (default ${ext.default_version})`;
    }
    return ext.installed_version;
}

// extensionColumns are the extensions' columns: those installed in a
// database have a schema, and those the server has available don't
export const extensionColumns = (installed: boolean): Column<Extension>[] => [
    { header: 'Extension', cell: (ext) => ext.name, sort: 'name', sortFirst: 'ascending' },
    { header: 'Version', cell: (ext) => extensionVersion(ext), sort: 'version' },
    ...(installed ? [{ header: 'Schema', cell: (ext: Extension) => ext.schema ?? '', sort: 'schema', sortFirst: 'ascending' as const }] : []),
    { header: 'Description', cell: (ext) => description(ext.comment, 'min(40rem, 40vw)') },
];

export function extensionBody(ext: Extension): TemplateResult {
    return fields([
        ['Description', ext.comment],
        ['Installed version', ext.installed_version],
        ['Default version', ext.default_version],
        ['Database', ext.database],
        ['Schema', ext.schema],
        ['Owner', ext.owner],
        ['Requires', ext.requires?.join(', ')],
        ['Relocatable', ext.relocatable === undefined ? undefined : ext.relocatable ? 'yes' : 'no'],
    ]);
}
