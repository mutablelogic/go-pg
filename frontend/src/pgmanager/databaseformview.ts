// The inputs of pgmanager's form of a database, which change the form

import { html, type TemplateResult } from 'lit';
import { DatabasePrivilegeHelp, DatabasePrivileges } from './aclform.ts';
import { aclGrid } from './aclview.ts';
import type { DatabaseForm, DatabaseFormErrors } from './databaseform.ts';

// Carbon components
import '@carbon/web-components/es/components/combo-box/index.js';
import '@carbon/web-components/es/components/text-input/index.js';
import '@carbon/web-components/es/components/textarea/index.js';

// databaseFormView is the form's inputs, with the roles which can own it,
// or be granted access, which is rendered again with rerender, such as
// when a role is added to its access
export function databaseFormView(form: DatabaseForm, roles: readonly string[], rerender: () => void, errors: DatabaseFormErrors = {}): TemplateResult {
    const text = (key: 'name' | 'comment') => (event: Event) => {
        form[key] = (event.target as HTMLInputElement).value;
    };
    return html`
        <div class="form">
            <cds-text-input label="Name" required value=${form.name} ?invalid=${!!errors.name} invalid-text=${errors.name ?? ''}
                @input=${text('name')}></cds-text-input>
            <cds-combo-box title-text="Owner" label="Choose a role" value=${form.owner}
                helper-text=${form.owner ? '' : 'Leave it empty for you'}
                @cds-combo-box-selected=${(event: CustomEvent<{ item?: { value: string } }>) => {
                    form.owner = event.detail.item?.value ?? '';
                }}>
                ${roles.map((role) => html`<cds-combo-box-item value=${role}>${role}</cds-combo-box-item>`)}
            </cds-combo-box>
            <cds-textarea label="Description" rows="2" value=${form.comment} @input=${text('comment')}></cds-textarea>
            ${aclGrid(form.acl, {
                privileges: DatabasePrivileges,
                help: DatabasePrivilegeHelp,
                roles,
                defaults: 'Nothing is granted, so only superusers can connect.',
                rerender,
            })}
        </div>
    `;
}
