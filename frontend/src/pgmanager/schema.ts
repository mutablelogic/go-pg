// pgmanager's schema, which mirrors pgmanager/schema in Go, of the resources
// the UI shows

// Database is a database, with its owner, its access privileges, and its
// size, in bytes
export interface Database {
    oid: number;
    name: string;
    owner?: string;
    acl?: unknown[];
    bytes?: number;
}

// Role is a role, with its permissions, and the roles it's a member of
export interface Role {
    oid: number;
    name: string;
    super?: boolean;
    inherit?: boolean;
    createrole?: boolean;
    createdb?: boolean;
    replication?: boolean;
    login?: boolean;
    bypassrls?: boolean;
    conlimit?: number;
    expires?: string;
    memberof?: string[];
}

// PgObject is an object of a database, such as a table, or an index, in
// its schema, with its owner, and its size, in bytes, and a table's rows
export interface PgObject {
    oid: number;
    database?: string;
    schema?: string;
    type?: string;
    name?: string;
    owner?: string;
    acl?: unknown[];
    tablespace?: string;
    bytes?: number;
    table?: { live_tuples?: number; dead_tuples?: number };
}

// Schema is a schema of a database
export interface Schema {
    oid: number;
    database?: string;
    schema: string;
    owner?: string;
    bytes?: number;
}

// Connection is a connection to the server, by its process, with its
// client, and its query, when it has one
export interface Connection {
    pid: number;
    database: string;
    role: string;
    application?: string;
    client_addr?: string;
    client_port?: number;
    conn_start?: string;
    query_start?: string;
    query?: string;
    state?: string;
}

// Setting is a setting of the server, with its value, its unit, its
// category, and when it can be changed, its context
export interface Setting {
    name: string;
    value: string | null;
    unit?: string;
    category: string;
    context: string;
    description?: string;
    extra_desc?: string;
}

// Tablespace is a tablespace, with its location, and its size, in bytes
export interface Tablespace {
    oid: number;
    name: string;
    owner?: string;
    acl?: unknown[];
    location?: string;
    options?: string[];
    bytes?: number;
}

// ReplicationSlot is a replication slot, physical or logical, with its
// status, and how far its client lags
export interface ReplicationSlot {
    name: string;
    type: string;
    plugin?: string;
    database?: string;
    temporary?: boolean;
    two_phase?: boolean;
    status: string;
    client_addr?: string;
    lag_bytes?: number;
    lag_ms?: number;
}

// Statement is a normalized statement's statistics, from
// pg_stat_statements, with its times in milliseconds
export interface Statement {
    role?: string;
    database?: string;
    query_id: number;
    query: string;
    calls: number;
    rows: number;
    total_ms: number;
    min_ms: number;
    max_ms: number;
    mean_ms: number;
}

// StatementSort is what the statements are sorted by
export type StatementSort = 'total_ms' | 'mean_ms' | 'min_ms' | 'max_ms' | 'calls' | 'rows';

// List is a page of a list of the resources, with the number of them
export interface List<T> {
    count: number;
    body?: T[];
}
