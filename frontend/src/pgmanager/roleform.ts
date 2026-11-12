// pgmanager's form of a role, which creates one, or changes one: its name,
// description, permissions, password, connection limit, expiry, and the
// roles it's a member of. The form is a plain object, which the inputs
// change, and which becomes the request, with only what's changed, when an
// existing role is changed.

import type { Role } from './schema.ts';

// RoleForm is what the form's inputs set: the connection limit, which is
// empty for none, the expiry, a date, which is empty for none, and the
// password, which is empty to leave it, unless it's removed
export interface RoleForm {
    name: string;
    comment: string;
    login: boolean;
    super: boolean;
    createdb: boolean;
    createrole: boolean;
    replication: boolean;
    bypassrls: boolean;
    inherit: boolean;
    password: string;
    confirm: string;
    removePassword: boolean;
    conlimit: string;
    expires: string;
    memberof: string[];
}

// RoleRequest is the body of a request which creates, or changes, a role
export type RoleRequest = Partial<Pick<Role, 'name' | 'comment' | 'login' | 'super' | 'createdb' | 'createrole' | 'replication' | 'bypassrls' | 'inherit' | 'memberof' | 'conlimit'>> & {
    password?: string;
    expires?: string;
};

// The permissions, which are checkboxes, as the role's fields
export const permissions: [keyof RoleForm & keyof Role, string][] = [
    ['login', 'Can sign in'],
    ['inherit', 'Inherits roles from group membership'],
    ['super', 'Superuser'],
    ['createdb', 'Can create databases'],
    ['createrole', 'Can create roles'],
    ['replication', 'Can replicate'],
    ['bypassrls', 'Bypasses row-level security'],
];

// impliedBySuperuser are the permissions a superuser has, whatever they're
// set to, as it bypasses every check, unlike signing in, or inheriting
export const impliedBySuperuser: readonly (keyof RoleForm)[] = ['createdb', 'createrole', 'replication', 'bypassrls'];

// The zero time, which is no expiry
const noExpiry = '0001-01-01T00:00:00Z';

// roleForm returns the form of a role, or of a new role, which can sign in,
// and inherits its roles' permissions
export function roleForm(role?: Role): RoleForm {
    return {
        name: role?.name ?? '',
        comment: role?.comment ?? '',
        login: role?.login ?? true,
        super: role?.super ?? false,
        createdb: role?.createdb ?? false,
        createrole: role?.createrole ?? false,
        replication: role?.replication ?? false,
        bypassrls: role?.bypassrls ?? false,
        inherit: role?.inherit ?? true,
        password: '',
        confirm: '',
        removePassword: false,
        conlimit: role?.conlimit === undefined ? '' : String(role.conlimit),
        // The day it expires, in UTC, which it's set at the start of
        expires: role?.expires ? new Date(role.expires).toISOString().slice(0, 10) : '',
        memberof: [...(role?.memberof ?? [])],
    };
}

// RoleFormErrors are what's wrong with the form, by its input
export type RoleFormErrors = Partial<Record<'name' | 'confirm', string>>;

// roleFormErrors returns what's wrong with the form, which isn't sent until
// there's nothing: a name, and a password which is confirmed
export function roleFormErrors(form: RoleForm): RoleFormErrors {
    const errors: RoleFormErrors = {};
    if (!form.name.trim()) {
        errors.name = 'A role needs a name';
    }
    if (!form.removePassword && form.password !== form.confirm) {
        errors.confirm = "The passwords don't match";
    }
    return errors;
}

// roleRequest returns the request of the form, which, for a role which is
// changed, has only what's changed. A connection limit, or expiry, which is
// removed is -1, or the zero time.
export function roleRequest(form: RoleForm, role?: Role): RoleRequest {
    const was = roleForm(role);
    const request: RoleRequest = {};
    const changed = <K extends keyof RoleForm>(key: K) => !role || JSON.stringify(form[key]) !== JSON.stringify(was[key]);
    if (changed('name')) {
        request.name = form.name.trim();
    }
    if (changed('comment') && (role || form.comment.trim())) {
        request.comment = form.comment.trim();
    }
    for (const [key] of permissions) {
        if (changed(key)) {
            (request as Record<string, unknown>)[key] = form[key];
        }
    }
    if (form.removePassword) {
        request.password = '';
    } else if (form.password) {
        request.password = form.password;
    }
    if (changed('conlimit') && (role || form.conlimit.trim())) {
        request.conlimit = form.conlimit.trim() === '' ? -1 : Number(form.conlimit);
    }
    if (changed('expires') && (role || form.expires)) {
        request.expires = form.expires ? new Date(`${form.expires}T00:00:00Z`).toISOString() : noExpiry;
    }
    if (changed('memberof') && (role || form.memberof.length)) {
        request.memberof = form.memberof;
    }
    return request;
}
