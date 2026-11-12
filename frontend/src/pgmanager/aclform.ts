// The access privileges of something, such as a database, as a form: a row
// for each role, including PUBLIC, which is everyone, with the privileges
// it has. pgmanager returns them as strings, such as reporter:CONNECT,CREATE,
// with a role which needs it quoted, and takes them as objects, which need
// no quoting.

// AclRow is a role, and the privileges it has
export interface AclRow {
    role: string;
    priv: string[];
}

// AclItem is a row as pgmanager takes it
export interface AclItem {
    role: string;
    priv: string[];
}

// Public is the role which is everyone
export const Public = 'PUBLIC';

// The privileges of each kind of thing, in the order they're shown
export const DatabasePrivileges = ['CONNECT', 'CREATE', 'TEMPORARY'] as const;
export const SchemaPrivileges = ['USAGE', 'CREATE'] as const;
export const TablespacePrivileges = ['CREATE'] as const;

// The option which lets a role grant a privilege it has, which pgmanager
// returns after it, such as CONNECT WITH GRANT OPTION, but doesn't take
// back, so it's left out
const withGrantOption = ' WITH GRANT OPTION';

// aclRows returns the rows of access privileges, as pgmanager returns them,
// such as "reporter:CONNECT,CREATE", or "\"a role\":CONNECT"
export function aclRows(acl: readonly string[] | undefined): AclRow[] {
    return (acl ?? []).flatMap((item) => {
        const match = /^("(?:[^"\\]|\\.)*"|[^:]*):(.*)$/.exec(item);
        if (!match) {
            return [];
        }
        const role = match[1].startsWith('"') ? JSON.parse(match[1]) as string : match[1];
        const priv = match[2].split(',').map((p) => p.trim().replace(withGrantOption, '')).filter(Boolean);
        return [{ role: role || Public, priv }];
    });
}

// aclRequest returns the rows as pgmanager takes them, without rows which
// have no privileges, which are removed, and with each role once
export function aclRequest(rows: readonly AclRow[]): AclItem[] {
    const items = new Map<string, Set<string>>();
    for (const row of rows) {
        const priv = items.get(row.role) ?? new Set<string>();
        row.priv.forEach((p) => priv.add(p));
        items.set(row.role, priv);
    }
    return [...items].filter(([, priv]) => priv.size).map(([role, priv]) => ({ role, priv: [...priv] }));
}

// aclChanged is true when the rows give other privileges than the access
// privileges, as pgmanager returns them
export function aclChanged(rows: readonly AclRow[], acl: readonly string[] | undefined): boolean {
    const key = (items: AclItem[]) => JSON.stringify(items.map(({ role, priv }) => [role, [...priv].sort()]).sort());
    return key(aclRequest(rows)) !== key(aclRequest(aclRows(acl)));
}
