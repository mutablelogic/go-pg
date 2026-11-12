// pgmanager's form of a tablespace, which creates one, or changes one: its
// name, location, which only a new one has, owner, description, and access
// privileges. The request has only what's changed, when an existing
// tablespace is changed.

import { aclAfterCreate, aclChanged, aclRequest, aclRows, type AclItem, type AclRow } from './aclform.ts';
import type { Tablespace } from './schema.ts';

// TablespaceForm is what the form's inputs set: its owner, which is empty
// for whoever creates it, and its location, a directory on the server
export interface TablespaceForm {
    name: string;
    location: string;
    owner: string;
    comment: string;
    acl: AclRow[];
}

// TablespaceRequest is the body of a request which creates, or changes, a
// tablespace, whose location is only a new one's
export interface TablespaceRequest {
    name?: string;
    location?: string;
    owner?: string;
    comment?: string;
    acl?: AclItem[];
}

// TablespaceFormErrors are what's wrong with the form, by its input
export type TablespaceFormErrors = Partial<Record<'name' | 'location', string>>;

// tablespaceForm returns the form of a tablespace, or of a new one, which
// only its owner can use, as PostgreSQL gives it
export function tablespaceForm(space?: Tablespace): TablespaceForm {
    return {
        name: space?.name ?? '',
        location: space?.location ?? '',
        owner: space?.owner ?? '',
        comment: space?.comment ?? '',
        acl: aclRows(space?.acl),
    };
}

// tablespaceFormErrors returns what's wrong with the form, which isn't sent
// until there's nothing: a name, which isn't PostgreSQL's, and, for a new
// one, a location, which is a directory, from the server's root
export function tablespaceFormErrors(form: TablespaceForm, isNew: boolean): TablespaceFormErrors {
    const errors: TablespaceFormErrors = {};
    const name = form.name.trim();
    if (!name) {
        errors.name = 'A tablespace needs a name';
    } else if (name.startsWith('pg_')) {
        errors.name = "A tablespace's name can't start with pg_";
    }
    if (isNew && !form.location.trim().startsWith('/')) {
        errors.location = 'A tablespace needs a directory on the server, such as /var/lib/postgresql/reports';
    }
    return errors;
}

// tablespaceLocationError returns what's wrong with a new tablespace's
// location, in plain words, from why PostgreSQL couldn't create it, by the
// response's status, and its message, or else undefined. PostgreSQL checks
// the directory itself, on its own server: that it exists, and that it can
// take it over, which it can only when it owns it.
export function tablespaceLocationError(status: number, message: string): string | undefined {
    if (status === 404 && /does not exist/i.test(message)) {
        return "That directory doesn't exist on the database server. Create it, empty, owned by the user PostgreSQL runs as, often postgres";
    }
    if (/could not set permissions|permission denied|operation not permitted/i.test(message)) {
        return "PostgreSQL can't use that directory: it must be owned by the user PostgreSQL runs as, often postgres";
    }
    if (/already in use as a tablespace|is not empty/i.test(message)) {
        return "That directory is already in use: it must be empty";
    }
    return undefined;
}

// tablespaceRequest returns the request of the form, which, for a tablespace
// which is changed, has only what's changed. A new tablespace's access is
// set once it's created, with tablespaceCreateAcl.
export function tablespaceRequest(form: TablespaceForm, space?: Tablespace): TablespaceRequest {
    const request: TablespaceRequest = {};
    const name = form.name.trim();
    const owner = form.owner.trim();
    const comment = form.comment.trim();
    if (!space || name !== space.name) {
        request.name = name;
    }
    if (!space) {
        request.location = form.location.trim();
    }
    if (owner && owner !== (space?.owner ?? '')) {
        request.owner = owner;
    }
    if (space ? comment !== (space.comment ?? '') : comment) {
        request.comment = comment;
    }
    if (space && aclChanged(form.acl, space.acl)) {
        request.acl = aclRequest(form.acl);
    }
    return request;
}

// tablespaceCreateAcl returns the access of a tablespace which was created,
// when the form's isn't what it was given, or else undefined
export function tablespaceCreateAcl(form: TablespaceForm, created: Tablespace): AclItem[] | undefined {
    return aclAfterCreate(form.acl, created);
}
