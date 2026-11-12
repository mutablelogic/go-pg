// pgmanager's form of a database, which creates one, or changes one: its
// name, owner, description, and access privileges. The request has only
// what's changed, when an existing database is changed.

import { aclChanged, aclRequest, aclRows, type AclItem, type AclRow } from './aclform.ts';
import type { Database } from './schema.ts';

// DatabaseForm is what the form's inputs set: its owner, which is empty for
// whoever creates it
export interface DatabaseForm {
    name: string;
    owner: string;
    comment: string;
    acl: AclRow[];
}

// DatabaseRequest is the body of a request which creates, or changes, a
// database
export interface DatabaseRequest {
    name?: string;
    owner?: string;
    comment?: string;
    acl?: AclItem[];
}

// DatabaseFormErrors are what's wrong with the form, by its input
export type DatabaseFormErrors = Partial<Record<'name', string>>;

// databaseForm returns the form of a database, or of a new one
export function databaseForm(db?: Database): DatabaseForm {
    return {
        name: db?.name ?? '',
        owner: db?.owner ?? '',
        comment: db?.comment ?? '',
        acl: aclRows(db?.acl),
    };
}

// databaseFormErrors returns what's wrong with the form, which isn't sent
// until there's nothing
export function databaseFormErrors(form: DatabaseForm): DatabaseFormErrors {
    return form.name.trim() ? {} : { name: 'A database needs a name' };
}

// databaseRequest returns the request of the form, which, for a database
// which is changed, has only what's changed
export function databaseRequest(form: DatabaseForm, db?: Database): DatabaseRequest {
    const request: DatabaseRequest = {};
    const name = form.name.trim();
    const owner = form.owner.trim();
    const comment = form.comment.trim();
    if (!db || name !== db.name) {
        request.name = name;
    }
    if (owner && owner !== (db?.owner ?? '')) {
        request.owner = owner;
    }
    if (db ? comment !== (db.comment ?? '') : comment) {
        request.comment = comment;
    }
    if (db ? aclChanged(form.acl, db.acl) : aclRequest(form.acl).length) {
        request.acl = aclRequest(form.acl);
    }
    return request;
}
