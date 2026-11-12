// The inputs of pgmanager's form of a tablespace, which change the form

import { html, type TemplateResult } from 'lit';
import { TablespacePrivilegeHelp, TablespacePrivileges } from './aclform.ts';
import { aclGrid } from './aclview.ts';
import type { TablespaceForm, TablespaceFormErrors } from './tablespaceform.ts';

// Carbon components
import '@carbon/web-components/es/components/combo-box/index.js';
import '@carbon/web-components/es/components/text-input/index.js';
import '@carbon/web-components/es/components/textarea/index.js';

// tablespaceFormView is the form's inputs, with the roles which can own it,
// or be granted access, which is rendered again with rerender. A new one
// has its location, which, as PostgreSQL can't move it, an existing one
// shows, but can't change.
export function tablespaceFormView(form: TablespaceForm, roles: readonly string[], isNew: boolean, rerender: () => void, errors: TablespaceFormErrors = {}): TemplateResult {
    const text = (key: 'name' | 'location' | 'comment') => (event: Event) => {
        form[key] = (event.target as HTMLInputElement).value;
    };
    return html`
        <div class="form">
            <cds-text-input label="Name" required value=${form.name} ?invalid=${!!errors.name} invalid-text=${errors.name ?? ''}
                @input=${text('name')}></cds-text-input>
            <cds-text-input label="Location" value=${form.location} ?readonly=${!isNew} ?required=${isNew}
                helper-text=${isNew ? 'A directory on the server, which is empty, and which PostgreSQL owns' : "A tablespace can't be moved"}
                ?invalid=${!!errors.location} invalid-text=${errors.location ?? ''}
                @input=${text('location')}></cds-text-input>
            <cds-combo-box title-text="Owner" label="Choose a role" value=${form.owner}
                helper-text=${form.owner ? '' : 'Leave it empty for you'}
                @cds-combo-box-selected=${(event: CustomEvent<{ item?: { value: string } }>) => {
                    form.owner = event.detail.item?.value ?? '';
                }}>
                ${roles.map((role) => html`<cds-combo-box-item value=${role}>${role}</cds-combo-box-item>`)}
            </cds-combo-box>
            <cds-textarea label="Description" rows="2" value=${form.comment} @input=${text('comment')}></cds-textarea>
            ${aclGrid(form.acl, {
                privileges: TablespacePrivileges,
                help: TablespacePrivilegeHelp,
                roles,
                defaults: isNew ? 'Only its owner can use it, until a role is added.' : 'Nothing is granted, so only superusers can use it.',
                rerender,
            })}
        </div>
    `;
}
