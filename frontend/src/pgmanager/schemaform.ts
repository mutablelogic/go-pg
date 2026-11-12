// pgmanager's form of a schema, in a database, which creates one, or changes
// one: its name, owner, description, and access privileges. The request has
// only what's changed, when an existing schema is changed.

import { aclAfterCreate, aclChanged, aclRequest, aclRows, type AclItem, type AclRow } from './aclform.ts';
import type { Schema } from './schema.ts';

// SchemaForm is what the form's inputs set: its owner, which is empty for
// whoever creates it
export interface SchemaForm {
    name: string;
    owner: string;
    comment: string;
    acl: AclRow[];
}

// SchemaRequest is the body of a request which creates, or changes, a schema,
// whose name is schema
export interface SchemaRequest {
    schema?: string;
    owner?: string;
    comment?: string;
    acl?: AclItem[];
}

// SchemaFormErrors are what's wrong with the form, by its input
export type SchemaFormErrors = Partial<Record<'name', string>>;

// schemaForm returns the form of a schema, or of a new one, which only its
// owner can use, as PostgreSQL gives it
export function schemaForm(schema?: Schema): SchemaForm {
    return {
        name: schema?.schema ?? '',
        owner: schema?.owner ?? '',
        comment: schema?.comment ?? '',
        acl: aclRows(schema?.acl),
    };
}

// schemaFormErrors returns what's wrong with the form, which isn't sent until
// there's nothing: a name, which isn't PostgreSQL's
export function schemaFormErrors(form: SchemaForm): SchemaFormErrors {
    const name = form.name.trim();
    if (!name) {
        return { name: 'A schema needs a name' };
    }
    if (name.startsWith('pg_')) {
        return { name: "A schema's name can't start with pg_" };
    }
    return {};
}

// schemaRequest returns the request of the form, which, for a schema which
// is changed, has only what's changed. A new schema's access is set once
// it's created, with schemaCreateAcl.
export function schemaRequest(form: SchemaForm, schema?: Schema): SchemaRequest {
    const request: SchemaRequest = {};
    const name = form.name.trim();
    const owner = form.owner.trim();
    const comment = form.comment.trim();
    if (!schema || name !== schema.schema) {
        request.schema = name;
    }
    if (owner && owner !== (schema?.owner ?? '')) {
        request.owner = owner;
    }
    if (schema ? comment !== (schema.comment ?? '') : comment) {
        request.comment = comment;
    }
    if (schema && aclChanged(form.acl, schema.acl)) {
        request.acl = aclRequest(form.acl);
    }
    return request;
}

// schemaCreateAcl returns the access of a schema which was created, when
// the form's isn't what it was given, or else undefined
export function schemaCreateAcl(form: SchemaForm, created: Schema): AclItem[] | undefined {
    return aclAfterCreate(form.acl, created);
}
