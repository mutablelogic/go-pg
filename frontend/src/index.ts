// The web UI of pgmanager, which shows its databases, their schemas and
// objects, roles, and the server's connections, settings, tablespaces,
// replication slots and statements. The parts in
// core are candidates for go-server's @mutablelogic/go-server, and those in
// pgmanager are its own.

import '@carbon/styles/css/styles.min.css';
import './fonts.css';
import { render, type TemplateResult } from 'lit';

// go-server's components
import { ApiError } from '@mutablelogic/go-server/api';
import { ChangesController } from '@mutablelogic/go-server/broadcaster';
import { ConfigController, configClient, getConfig } from '@mutablelogic/go-server/config';
import '@mutablelogic/go-server/config/view';
import { NotificationController } from '@mutablelogic/go-server/notification';
import '@mutablelogic/go-server/notification/view';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';

// Candidates for go-server, and pgmanager's own
import { ListController } from '@mutablelogic/go-server/list';
import '@mutablelogic/go-server/list/view';
import type { TableElement } from '@mutablelogic/go-server/list/view';
import { TileSelectEvent, type TilesElement } from '@mutablelogic/go-server/list/tiles';
import { Router, type Route } from '@mutablelogic/go-server/router';
import { NavController } from './core/nav.ts';
import './core/shell.ts';
import './core/header.ts';
import type { PageHeaderElement } from './core/header.ts';
import * as api from './pgmanager/httpclient.ts';
import type { ObjectFilter } from './pgmanager/httpclient.ts';
import { ListLimits, type Connection, type Database, type Extension, type PgObject, type ReplicationSlot, type Role, type Schema, type Setting, type Statement, type StatementSort, type Tablespace } from './pgmanager/schema.ts';
import * as views from './pgmanager/views.ts';
import { roleForm, roleFormErrors, roleRequest, type RoleFormErrors } from './pgmanager/roleform.ts';
import { roleFormView } from './pgmanager/roleformview.ts';
import { databaseForm, databaseFormErrors, databaseRequest, type DatabaseFormErrors } from './pgmanager/databaseform.ts';
import { databaseFormView } from './pgmanager/databaseformview.ts';

// Carbon components, and icons
import '@carbon/web-components/es/components/ui-shell/index.js';
import ConnectIcon from '@carbon/icons/es/connect/32.js';
import DatabaseIcon from '@carbon/icons/es/data--base/32.js';
import GroupIcon from '@carbon/icons/es/group/32.js';
import PlugIcon from '@carbon/icons/es/plug/32.js';
import AddIcon from '@carbon/icons/es/add/16.js';
import EditIcon from '@carbon/icons/es/edit/16.js';
import TrashIcon from '@carbon/icons/es/trash-can/16.js';
import ReplicateIcon from '@carbon/icons/es/replicate/32.js';
import ResetIcon from '@carbon/icons/es/reset/16.js';
import SettingsIcon from '@carbon/icons/es/settings/32.js';
import SqlIcon from '@carbon/icons/es/SQL/32.js';
import StoragePoolIcon from '@carbon/icons/es/storage-pool/32.js';

// Errors, and the server's status, are shown as toasts
const notifications = new NotificationController();
const notificationsView = document.querySelector('gs-notifications');
if (notificationsView) {
    notificationsView.controller = notifications;
}

// The configuration says where the API is, and the changes, and the server
// it follows, which says when it's down, back, or upgraded
// The configuration, when it's read, which the server's controller is
// given, so it isn't read again, or else the API at /api, and the
// controller reads it once the server can be reached
const read = await getConfig().catch((error) => {
    notifications.error("The configuration couldn't be read", error);
    return undefined;
});
const client = configClient(read ?? { prefix: '/api' });
const changes = new ChangesController(client).onError((error) => notifications.error("Changes couldn't be watched", error));
const server = new ConfigController(changes, { config: read, notifications });
const status = document.querySelector('gs-server-status');
if (status) {
    status.controller = server;
    status.repository = 'https://github.com/mutablelogic/go-pg';
}

// The panel shows the item which is opened in a table, such as an object,
// or a role, and closes it in the table when it's closed
interface Item {
    kind: string;
    title: string;
    subtitle?: string;
    body: TemplateResult;
    actions?: PanelAction[];
}
const closeAction: PanelAction = { label: 'Close', kind: 'secondary' };
const panel = new PanelController<Item>({ actions: [closeAction] })
    .onError((error, action) => notifications.error(`${action.label} didn't work`, error));
const panelView = document.querySelector('gs-panel');
if (panelView) {
    panelView.controller = panel as PanelController<unknown>;
}

// The lists of the databases, which are the sidebar's pages, the schemas
// and the objects of the database which is shown, the roles, and the
// server's connections, settings, tablespaces and replication slots, which
// are loaded again when they change
const databases = new ListController<Database>({
    name: 'Databases',
    // Every database is listed, in the sidebar, and as tiles
    list: () => api.listAllDatabases(client),
    same: (a, b) => a.name === b.name,
    notifications, changes, filter: { table: 'database' },
});

// The database which is shown, and the filter of its objects, by schema,
// and type
let filter: ObjectFilter | undefined;
const schemas = new ListController<Schema>({
    name: 'Schemas',
    // Every schema is listed, as tiles
    list: () => filter === undefined ? Promise.resolve({ count: 0 }) : api.listAllSchemas(client, filter.database),
    same: (a, b) => a.schema === b.schema,
    notifications, changes, filter: { table: 'schema' },
});
const objects = new ListController<PgObject>({
    pageSize: 20,
    name: 'Objects',
    list: (offset, limit) => filter === undefined ? Promise.resolve({ count: 0 }) : api.listObjects(client, filter, offset, limit),
    get: (object) => api.getObject(client, object),
    same: (a, b) => a.database === b.database && a.schema === b.schema && a.name === b.name,
    notifications, changes,
});
// The roles are yours, or PostgreSQL's own, such as pg_monitor, which the
// tiles above them choose
let predefinedRoles = false;
const roles = new ListController<Role>({
    pageSize: 20,
    name: 'Roles',
    list: (offset, limit) => api.listRoles(client, predefinedRoles, offset, limit),
    get: (role) => api.getRole(client, role.name),
    same: (a, b) => a.name === b.name,
    notifications, changes, filter: { table: 'role' },
});
// The extensions the server has available, or, in a database, those
// installed there
let extensionDatabase: string | undefined;
const extensions = new ListController<Extension>({
    pageSize: 20,
    name: 'Extensions',
    // Without a database, the extensions are the server's, so only what's
    // the same in every database is kept, rather than what's installed in
    // the database pgmanager connects to
    list: async (offset, limit) => {
        if (extensionDatabase) {
            return api.listExtensions(client, { database: extensionDatabase, installed: true }, offset, limit);
        }
        const list = await api.listExtensions(client, {}, offset, limit);
        return { ...list, body: list.body?.map(({ name, default_version, comment }) => ({ name, default_version, comment })) };
    },
    same: (a, b) => a.name === b.name && a.database === b.database,
    notifications, changes, filter: { table: 'extension' },
});
let connectionDatabase: string | undefined;
const connections = new ListController<Connection>({
    pageSize: 20,
    name: 'Connections',
    list: (offset, limit, sort) => api.listConnections(client, {
        database: connectionDatabase,
        sort: sort?.key,
        order: sort && (sort.direction === 'descending' ? 'desc' : 'asc'),
    }, offset, limit),
    get: (conn) => api.getConnection(client, conn.pid),
    same: (a, b) => a.pid === b.pid,
    notifications, changes, filter: { table: 'connection' },
});
let category: string | undefined;
const settings = new ListController<Setting>({
    name: 'Settings',
    list: (offset, limit) => api.listSettings(client, category, offset, limit),
    get: (setting) => api.getSetting(client, setting.name),
    same: (a, b) => a.name === b.name,
    notifications, changes, filter: { table: 'setting' },
    pageSize: 50,
});
const tablespaces = new ListController<Tablespace>({
    pageSize: 20,
    name: 'Tablespaces',
    list: (offset, limit) => api.listTablespaces(client, offset, limit),
    get: (space) => api.getTablespace(client, space.name),
    same: (a, b) => a.name === b.name,
    notifications, changes, filter: { table: 'tablespace' },
});
const slots = new ListController<ReplicationSlot>({
    pageSize: 20,
    name: 'Replication slots',
    list: (offset, limit) => api.listReplicationSlots(client, offset, limit),
    get: (slot) => api.getReplicationSlot(client, slot.name),
    same: (a, b) => a.name === b.name,
    notifications, changes, filter: { table: 'replication_slot' },
});

// The statements, sorted, of a database, or all of them. Without
// pg_stat_statements, which the server says is unavailable, there are none.
let statementDatabase: string | undefined;
function listStatements(filter: api.StatementFilter, offset: number, limit: number) {
    return api.listStatements(client, filter, offset, limit).catch((error) => {
        if (error instanceof ApiError && error.status === 503) {
            return { count: 0 };
        }
        throw error;
    });
}
const statements = new ListController<Statement>({
    pageSize: 20,
    name: 'Statements',
    list: (offset, limit, sort) => listStatements({
        sort: (sort?.key ?? 'total_ms') as StatementSort,
        order: sort?.direction === 'ascending' ? 'asc' : 'desc',
        database: statementDatabase,
    }, offset, limit),
    sort: { key: 'total_ms', direction: 'descending' },
    same: (a, b) => a.query_id === b.query_id && a.database === b.database && a.role === b.role,
    notifications, changes, filter: { table: 'statement' },
});

// table shows the list in the table, with the sizes of a page which are at
// most its limit, and the item which is opened in it in the panel, as the
// function describes it, which closes when the item is gone, such as a
// connection which ended
const lists: ListController<unknown>[] = [];
function table<T>(id: string, controller: ListController<T>, limit: number, columns: TableElement<T>['columns'], describe: (item: T) => Omit<Item, 'kind'>) {
    const element = document.getElementById(id) as TableElement<T> | null;
    if (element) {
        element.controller = controller;
        element.columns = columns;
        element.pageSizes = [10, 20, 50, 100].filter((size) => size <= limit);
    }
    lists.push(controller as ListController<unknown>);
    controller.subscribe(({ selected }) => {
        if (selected) {
            open({ kind: id, ...describe(selected) });
        } else if (panel.current.open && panel.current.item?.kind === id) {
            panel.close();
        }
    });
}
table('object-table', objects, ListLimits.object, views.objectColumns, (object) => ({
    title: object.name ?? '', subtitle: [object.database, object.schema].filter(Boolean).join(' · '), body: views.objectBody(object),
}));
table('role-table', roles, ListLimits.role, views.roleColumns(openRole), roleView);

// roleView is a role in the panel, which, unless it's PostgreSQL's own, can
// be deleted, or changed
function roleView(role: Role): Omit<Item, 'kind'> {
    return {
        title: role.name,
        subtitle: role.predefined ? 'Predefined role' : 'Role',
        body: views.roleBody(role, openRole, describeRole),
        actions: role.predefined ? undefined : [
            { label: 'Delete', kind: 'danger', close: false, run: () => deleteRole(role) },
            { label: 'Edit', kind: 'secondary', close: false, run: () => editRole(role) },
            closeAction,
        ],
    };
}

// editRole shows the form of a role, or a new one, in the panel, which saves
// it, and shows it, or, when it's cancelled, shows the role again, or closes
// allRoles returns the name of every role, yours, and then PostgreSQL's
// own, which can own things, or be granted access to them
async function allRoles(): Promise<string[]> {
    const lists = await Promise.all([false, true].map((predefined) => api.listAll((offset, limit) => api.listRoles(client, predefined, offset, limit), ListLimits.role)));
    return lists.flatMap((list) => (list.body ?? []).map((role) => role.name));
}

// editDatabase shows the form of a database, or a new one, in the panel,
// which saves it, and shows its page
async function editDatabase(db?: Database) {
    const roleNames = await allRoles();
    const form = databaseForm(db);
    let errors: DatabaseFormErrors = {};
    const view = (): TemplateResult => databaseFormView(form, roleNames, rerender, errors);
    const rerender = () => {
        if (panelView) {
            render(view(), panelView);
        }
    };
    open({
        kind: 'database-form',
        title: db ? `Change ${db.name}` : 'Create a database',
        subtitle: 'Database',
        body: view(),
        actions: [
            { label: 'Cancel', kind: 'secondary' },
            {
                label: db ? 'Save' : 'Create',
                kind: 'primary',
                close: false,
                run: async () => {
                    errors = databaseFormErrors(form);
                    rerender();
                    if (Object.keys(errors).length) {
                        return;
                    }
                    const saved = db ? await api.updateDatabase(client, db.name, databaseRequest(form, db)) : await api.createDatabase(client, databaseRequest(form));
                    await databases.refresh();
                    panel.close();
                    location.hash = `#database/${encodeURIComponent(saved.name)}`;
                },
            },
        ],
    });
}

// deleteDatabase deletes a database, and everything in it, once its name is
// typed, which is asked for, as it can't be undone, and shows the first
async function deleteDatabase(db: Database) {
    const typed = prompt(`Delete the database ${db.name}, and everything in it? This can't be undone. Type its name to delete it.`);
    if (typed === null) {
        return;
    }
    if (typed.trim() !== db.name) {
        notifications.push({ kind: 'warning', title: `The database ${db.name} wasn't deleted`, subtitle: "The name you typed isn't its name" });
        return;
    }
    await api.deleteDatabase(client, db.name);
    notifications.push({ kind: 'success', title: `The database ${db.name} was deleted` });
    await databases.refresh();
    location.hash = '#database';
}

async function editRole(role?: Role) {
    const names = await allRoles();
    const form = roleForm(role);
    let errors: RoleFormErrors = {};
    const view = (): TemplateResult => roleFormView(form, names, !role, rerender, errors, describeRole, openRole);
    const rerender = () => {
        if (panelView) {
            render(view(), panelView);
        }
    };
    open({
        kind: 'role-form',
        title: role ? `Change ${role.name}` : 'Create a role',
        subtitle: 'Role',
        body: view(),
        actions: [
            role ? { label: 'Cancel', kind: 'secondary', close: false, run: () => open({ kind: 'role', ...roleView(role) }) } : { label: 'Cancel', kind: 'secondary' },
            {
                label: role ? 'Save' : 'Create',
                kind: 'primary',
                close: false,
                run: async () => {
                    // What's wrong is shown by its input, and isn't sent
                    errors = roleFormErrors(form);
                    rerender();
                    if (Object.keys(errors).length) {
                        return;
                    }
                    const saved = role ? await api.updateRole(client, role.name, roleRequest(form, role)) : await api.createRole(client, roleRequest(form));
                    void roles.refresh();
                    open({ kind: 'role', ...roleView(saved) });
                },
            },
        ],
    });
    // The roles table's selection is closed, once the form's open, so a
    // refresh doesn't show the role again in place of the form
    roles.close();
}

// deleteRole deletes a role, once it's confirmed, and closes the panel
async function deleteRole(role: Role) {
    if (!confirm(`Delete the role ${role.name}? This can't be undone.`)) {
        return;
    }
    await api.deleteRole(client, role.name);
    notifications.push({ kind: 'success', title: `The role ${role.name} was deleted` });
    roles.close();
    panel.close();
    void roles.refresh();
}

// openRole opens a role, by its name, such as one a role is a member of,
// which mayn't be on the page, so it isn't the table's. Only the role which
// was asked for last opens, so a slower response to an earlier one doesn't
// replace it.
let roleLookup = 0;
function openRole(name: string) {
    const lookup = ++roleLookup;
    api.getRole(client, name).then((role) => {
        if (lookup === roleLookup) {
            open({ kind: 'role', ...roleView(role) });
        }
    }).catch((error) => {
        if (lookup === roleLookup) {
            notifications.error(`The role ${name} couldn't be loaded`, error);
        }
    });
}
table('connection-table', connections, ListLimits.connection, views.connectionColumns, (conn) => ({
    title: `Connection ${conn.pid}`, subtitle: [conn.role, conn.database].filter(Boolean).join(' · '), body: views.connectionBody(conn),
}));
table('setting-table', settings, ListLimits.setting, views.settingColumns, (setting) => ({ title: setting.name, subtitle: setting.category, body: views.settingBody(setting) }));
table('tablespace-table', tablespaces, ListLimits.tablespace, views.tablespaceColumns, (space) => ({ title: space.name, subtitle: 'Tablespace', body: views.tablespaceBody(space) }));
table('extension-table', extensions, ListLimits.extension, views.extensionColumns, (ext) => ({
    title: ext.name, subtitle: ext.database ? `Extension · ${ext.database}` : 'Extension', body: views.extensionBody(ext),
}));
table('slot-table', slots, ListLimits.replicationSlot, views.replicationSlotColumns, (slot) => ({ title: slot.name, subtitle: `Replication slot · ${slot.type}`, body: views.replicationSlotBody(slot) }));
table('statement-table', statements, ListLimits.statement, views.statementColumns, (stmt) => ({
    title: 'Statement', subtitle: [stmt.role, stmt.database].filter(Boolean).join(' · '), body: views.statementBody(stmt),
}));
panel.onClose(() => lists.forEach((list) => list.close()));

// open shows the item in the panel
function open(item: Item) {
    panel.setHeader({ title: item.title, subtitle: item.subtitle });
    panel.setActions(item.actions ?? [closeAction]);
    if (panelView) {
        render(item.body, panelView);
    }
    panel.open(item);
}

// The tiles of the schemas, after one for all of them, with their sizes,
// choose the schema whose objects are shown
const tiles = document.getElementById('schema-tiles') as TilesElement<Schema> | null;
if (tiles) {
    tiles.controller = schemas;
    tiles.tile = views.schemaTile;
    tiles.addEventListener(TileSelectEvent, (event) => filter && filterObjects({ ...filter, schema: (event as CustomEvent<string>).detail || undefined }));
}
schemas.subscribe(({ items }) => {
    if (tiles) {
        tiles.all = items ? { value: '', label: 'All schemas', text: views.formatBytes(items.reduce((sum, schema) => sum + (schema.bytes ?? 0), 0)) } : undefined;
    }
    if (items && filter?.schema !== undefined && !items.some((schema) => schema.schema === filter?.schema)) {
        filterObjects({ ...filter, schema: undefined });
    }
});

// showDatabase shows the database's schemas, and all its objects, with its
// owner, and size, beneath its name
function showDatabase(name: string) {
    const db = databases.current.items?.find((db) => db.name === name);
    const header = pageHeader('database');
    if (header) {
        header.heading = name;
        header.subtitle = db ? [db.comment, db.owner && `Owned by ${db.owner}`, views.formatBytes(db.bytes)].filter(Boolean).join(' · ') : '';
        header.actions = [
            ...(db ? [
                { label: 'Delete', kind: 'danger' as const, icon: TrashIcon, run: () => void deleteDatabase(db).catch((error) => notifications.error(`The database ${db.name} couldn't be deleted`, error)) },
                { label: 'Change', kind: 'secondary' as const, icon: EditIcon, run: () => void editDatabase(db).catch((error) => notifications.error("The form couldn't be opened", error)) },
            ] : []),
            { label: 'Create a database', icon: AddIcon, run: () => void editDatabase().catch((error) => notifications.error("The form couldn't be opened", error)) },
        ];
    }
    if (filter?.database === name) {
        return;
    }
    filterObjects({ database: name });
    void schemas.reset();
}

// filterObjects shows the objects of the filter, from their first page
function filterObjects(next: ObjectFilter) {
    filter = next;
    if (tiles) {
        tiles.selected = next.schema ?? '';
    }
    if (panel.current.item?.kind === 'object-table') {
        panel.close();
    }
    renderTypes();
    void objects.reset();
}

// The buttons of the objects' types, which choose the type which is shown
const typesView = document.getElementById('object-types');
function renderTypes() {
    if (typesView && filter) {
        render(views.objectTypes(filter.type, (type) => filter && filterObjects({ ...filter, type })), typesView);
    }
}

// The dropdown of the settings' categories, which are loaded once, and
// choose the category of the settings which are shown
const categoriesView = document.getElementById('setting-categories');
let categories: string[] = [];
function renderCategories() {
    if (categoriesView) {
        render(views.settingCategories(categories, category, (next) => {
            category = next;
            if (panel.current.item?.kind === 'setting-table') {
                panel.close();
            }
            renderCategories();
            void settings.reset();
        }), categoriesView);
    }
}
api.listSettingCategories(client).then((list) => {
    categories = list.body ?? [];
    renderCategories();
}).catch((error) => notifications.error("The settings' categories couldn't be loaded", error));

// The pages' headers, with their icons, and the statements' action, which
// resets their statistics, once it's confirmed
function pageHeader(page: string) {
    return document.getElementById(`${page}-header`) as PageHeaderElement | null;
}
const icons = {
    database: DatabaseIcon, roles: GroupIcon, connections: ConnectIcon, settings: SettingsIcon,
    tablespaces: StoragePoolIcon, extensions: PlugIcon, 'replication-slots': ReplicateIcon, statements: SqlIcon,
};
for (const [page, icon] of Object.entries(icons)) {
    const header = pageHeader(page);
    if (header) {
        header.icon = icon;
    }
}
const rolesHeader = pageHeader('roles');
if (rolesHeader) {
    rolesHeader.actions = [{ label: 'Create a role', icon: AddIcon, run: () => void editRole().catch((error) => notifications.error("The form couldn't be opened", error)) }];
}
const statementsHeader = pageHeader('statements');
if (statementsHeader) {
    statementsHeader.actions = [{
        label: 'Reset statistics',
        kind: 'secondary',
        icon: ResetIcon,
        run: () => {
            if (!confirm("Reset the statements' statistics? This can't be undone.")) {
                return;
            }
            api.resetStatements(client).then(() => {
                notifications.push({ kind: 'success', title: "The statements' statistics were reset" });
                return statements.reset();
            }).catch((error) => notifications.error("The statements' statistics couldn't be reset", error));
        },
    }];
}

// databaseTiles shows the tiles of the databases, after one for all of
// them, with how many items of the list each has, which choose the
// database whose items the list shows, for the list's name, such as
// statement, whose tiles are statement-tiles, above statement-table. The
// counts are loaded again whenever the list is.
function databaseTiles<T>(name: string, list: ListController<T>, count: (database?: string) => Promise<number>, choose: (database?: string) => void, allLabel = 'All databases') {
    const tiles = document.getElementById(`${name}-tiles`) as TilesElement<Database> | null;
    if (!tiles) {
        return;
    }
    const counts = new Map<string, number>();
    let total: number | undefined;
    let selected = '';
    const text = (n?: number) => n?.toLocaleString();
    const renderTiles = () => {
        tiles.all = { value: '', label: allLabel, text: text(total) };
        tiles.tile = (db) => ({ value: db.name, label: db.name, ...views.ownerBadge(db.owner), text: text(counts.get(db.name)) });
        tiles.selected = selected;
    };
    tiles.controller = databases;
    tiles.addEventListener(TileSelectEvent, (event) => {
        selected = (event as CustomEvent<string>).detail;
        if (panel.current.item?.kind === `${name}-table`) {
            panel.close();
        }
        renderTiles();
        choose(selected || undefined);
        void list.reset();
    });
    renderTiles();

    let counted: unknown;
    list.subscribe(({ items }) => {
        if (!items || items === counted) {
            return;
        }
        counted = items;
        // Each is counted by itself, as a database can refuse its
        // connection, and the count of one which can't be counted is blank
        const names = (databases.current.items ?? []).map((db) => db.name);
        void Promise.allSettled([count(), ...names.map((name) => count(name))]).then(([all, ...each]) => {
            const value = (result: PromiseSettledResult<number>) => result.status === 'fulfilled' ? result.value : undefined;
            total = value(all);
            counts.clear();
            names.forEach((name, i) => {
                const n = value(each[i]);
                if (n !== undefined) {
                    counts.set(name, n);
                }
            });
            renderTiles();
        });
    });
}
// The tiles of the roles, yours, and PostgreSQL's own, with how many of
// each there are, which are counted again whenever the roles are loaded
interface RoleKind { value: string; label: string }
const roleKinds = new ListController<RoleKind>({
    name: 'Roles',
    list: async () => ({ count: 2, body: [{ value: '', label: 'Your roles' }, { value: 'predefined', label: 'Predefined' }] }),
    same: (a, b) => a.value === b.value,
});
const roleTiles = document.getElementById('role-tiles') as TilesElement<RoleKind> | null;
const roleCounts = new Map<string, number>();
function renderRoleTiles() {
    if (roleTiles) {
        roleTiles.tile = (kind) => ({ ...kind, text: roleCounts.get(kind.value)?.toLocaleString() });
        roleTiles.selected = predefinedRoles ? 'predefined' : '';
    }
}
if (roleTiles) {
    roleTiles.controller = roleKinds;
    roleTiles.addEventListener(TileSelectEvent, (event) => {
        predefinedRoles = (event as CustomEvent<string>).detail === 'predefined';
        if (panel.current.item?.kind === 'role-table') {
            panel.close();
        }
        const roleTable = document.getElementById('role-table') as TableElement<Role> | null;
        if (roleTable) {
            roleTable.columns = views.roleColumns(openRole, predefinedRoles);
        }
        renderRoleTiles();
        void roles.reset();
    });
    renderRoleTiles();
    void roleKinds.load();
}
// The descriptions of every role, yours and PostgreSQL's own, which a role's
// memberships show, and which are loaded again whenever the roles are
const roleDescriptions = new Map<string, string | undefined>();
function describeRole(name: string): string | undefined {
    return roleDescriptions.get(name);
}
function loadRoleDescriptions() {
    Promise.all([false, true].map((predefined) => api.listAll((offset, limit) => api.listRoles(client, predefined, offset, limit), ListLimits.role))).then((lists) => {
        roleDescriptions.clear();
        lists.flatMap((list) => list.body ?? []).forEach((role) => roleDescriptions.set(role.name, role.comment));
    }).catch(() => {
        // The descriptions are left out, as the roles say why they can't be
        // loaded
    });
}

let countedRoles: unknown;
roles.subscribe(({ items }) => {
    if (!items || items === countedRoles) {
        return;
    }
    countedRoles = items;
    loadRoleDescriptions();
    Promise.all([false, true].map((predefined) => api.listRoles(client, predefined, 0, 1))).then(([own, predefined]) => {
        roleCounts.set('', own.count);
        roleCounts.set('predefined', predefined.count);
        renderRoleTiles();
    }).catch(() => {
        // The counts aren't shown, as the roles say why they can't be loaded
    });
});

databaseTiles('statement', statements, async (database) => (await listStatements({ sort: 'calls', database }, 0, 1)).count, (database) => {
    statementDatabase = database;
});
databaseTiles('extension', extensions, async (database) => (await api.listExtensions(client, database ? { database, installed: true } : {}, 0, 1)).count, (database) => {
    extensionDatabase = database;
}, 'Available');
databaseTiles('connection', connections, async (database) => (await api.listConnections(client, { database }, 0, 1)).count, (database) => {
    connectionDatabase = database;
});

// The menu bar, of the sections, and the sidebar, of the pages: a page for
// each database, the roles, and the server's, of which the route's page is
// active
const otherPages = [
    { id: 'roles', label: 'Roles', href: '#roles', section: 'Access' },
    { id: 'connections', label: 'Connections', href: '#connections', section: 'Server' },
    { id: 'settings', label: 'Settings', href: '#settings', section: 'Server' },
    { id: 'extensions', label: 'Extensions', href: '#extensions', section: 'Server' },
    { id: 'tablespaces', label: 'Tablespaces', href: '#tablespaces', section: 'Server' },
    { id: 'replication-slots', label: 'Replication slots', href: '#replication-slots', section: 'Server' },
    { id: 'statements', label: 'Statements', href: '#statements', section: 'Server' },
];
const nav = new NavController({ name: 'manager', prefix: 'pg', href: '#', pages: otherPages });
const header = document.querySelector('gs-header');
const sideNav = document.querySelector('gs-side-nav');
if (header && sideNav) {
    header.controller = nav;
    sideNav.controller = nav;
}
databases.subscribe(({ items }) => {
    if (items) {
        nav.setPages([
            ...items.map((db) => ({ id: `database/${db.name}`, label: db.name, href: `#database/${encodeURIComponent(db.name)}`, section: 'Databases' })),
            ...otherPages,
        ]);
        show();
    }
});

// The pages, from the location's hash: a database, such as #database/shop,
// which is the first, when it isn't one, the roles, and the server's
let route: Route | undefined;
function show() {
    if (route?.page !== 'database') {
        nav.setActive(route?.page);
        return;
    }
    const items = databases.current.items;
    const name = items?.some((db) => db.name === route?.params[0]) ? route.params[0] : items?.[0]?.name;
    if (name !== undefined) {
        showDatabase(name);
    }
    nav.setActive(name === undefined ? undefined : `database/${name}`);
}
// A page's section is page-, and its name, such as page-roles, so the
// browser doesn't scroll to it, as it would to an element whose id is the
// hash
const router = new Router('database').add('database', document.getElementById('page-database'));
for (const page of otherPages) {
    router.add(page.id, document.getElementById(`page-${page.id}`));
}
router.onRoute((next) => {
    window.scrollTo(0, 0);
    route = next;
    show();
}).start();

// Follow the server. When the changes start, the lists are loaded, as are
// the server's configuration and status
changes.start();
