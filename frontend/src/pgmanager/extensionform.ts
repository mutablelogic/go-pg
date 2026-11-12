// pgmanager's form which installs an extension in a database: the database,
// its schema, which is the extension's own, or public, when it's empty, its
// version, which is the default, when it's empty, and whether the
// extensions it requires are installed too

// ExtensionForm is what the form's inputs set
export interface ExtensionForm {
    name: string;
    database: string;
    schema: string;
    version: string;
    cascade: boolean;
}

// ExtensionRequest is the body of a request which installs an extension
export interface ExtensionRequest {
    name: string;
    database: string;
    schema?: string;
    version?: string;
}

// ExtensionFormErrors are what's wrong with the form, by its input
export type ExtensionFormErrors = Partial<Record<'database', string>>;

// extensionForm returns the form which installs the extension, in the
// database, when it's known, with what it requires
export function extensionForm(name: string, database = ''): ExtensionForm {
    return { name, database, schema: '', version: '', cascade: true };
}

// extensionFormErrors returns what's wrong with the form, which isn't sent
// until there's nothing: a database, which it isn't installed in
export function extensionFormErrors(form: ExtensionForm, installedIn: readonly string[] = []): ExtensionFormErrors {
    if (!form.database) {
        return { database: 'Choose the database to install it in' };
    }
    if (installedIn.includes(form.database)) {
        return { database: `It's already installed in ${form.database}` };
    }
    return {};
}

// extensionRequest returns the request of the form, without a schema, or
// version, which is empty
export function extensionRequest(form: ExtensionForm): ExtensionRequest {
    const request: ExtensionRequest = { name: form.name, database: form.database };
    if (form.schema.trim()) {
        request.schema = form.schema.trim();
    }
    if (form.version.trim()) {
        request.version = form.version.trim();
    }
    return request;
}
