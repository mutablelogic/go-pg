// The inputs of pgmanager's form of a role, which change the form

import { html, type TemplateResult } from 'lit';
import { impliedBySuperuser, permissions, type RoleForm, type RoleFormErrors } from './roleform.ts';

// Carbon components
import '@carbon/web-components/es/components/checkbox/index.js';
import '@carbon/web-components/es/components/multi-select/index.js';
import '@carbon/web-components/es/components/number-input/index.js';
import '@carbon/web-components/es/components/password-input/index.js';
import '@carbon/web-components/es/components/text-input/index.js';
import '@carbon/web-components/es/components/textarea/index.js';

// roleFormView is the form's inputs, which change it, with the roles which
// it can be a member of, which are the others, and which, for a new role,
// has no password to remove. It's rendered again, with rerender, when what
// it shows depends on what's changed, such as a superuser's permissions,
// which are checked, and can't be changed, as a superuser has them.
export function roleFormView(form: RoleForm, roles: readonly string[], isNew: boolean, rerender: () => void, errors: RoleFormErrors = {}): TemplateResult {
    const text = (key: 'name' | 'comment' | 'password' | 'confirm' | 'conlimit' | 'expires') => (event: Event) => {
        form[key] = (event.target as HTMLInputElement).value;
    };
    const checked = (key: keyof RoleForm) => (event: Event) => {
        (form[key] as boolean) = (event.target as HTMLInputElement).checked;
    };
    return html`
        <div class="form">
            <cds-text-input label="Name" required value=${form.name} ?invalid=${!!errors.name} invalid-text=${errors.name ?? ''}
                @input=${text('name')}></cds-text-input>
            <cds-textarea label="Description" rows="2" value=${form.comment} @input=${text('comment')}></cds-textarea>
            <fieldset>
                <legend>Permissions</legend>
                ${permissions.map(([key, label]) => {
                    const implied = form.super && impliedBySuperuser.includes(key);
                    return html`
                        <cds-checkbox .checked=${implied || (form[key] as boolean)} ?disabled=${implied}
                            title=${implied ? 'A superuser has this permission' : ''}
                            @cds-checkbox-changed=${(event: Event) => {
                                checked(key)(event);
                                if (key === 'super') {
                                    rerender();
                                }
                            }}>${label}</cds-checkbox>
                    `;
                })}
                ${form.super ? html`<p class="helper">A superuser has every permission except signing in, and inheriting roles</p>` : ''}
            </fieldset>
            <cds-password-input label="Password" autocomplete="new-password"
                helper-text=${isNew ? 'Leave it empty for no password' : 'Leave it empty to keep the password'}
                @input=${text('password')}></cds-password-input>
            <cds-password-input label="Confirm password" autocomplete="new-password"
                ?invalid=${!!errors.confirm} invalid-text=${errors.confirm ?? ''}
                @input=${text('confirm')}></cds-password-input>
            ${isNew ? '' : html`<cds-checkbox ?checked=${form.removePassword} @cds-checkbox-changed=${checked('removePassword')}>Remove the password</cds-checkbox>`}
            <cds-text-input label="Connection limit" type="number" min="0" helper-text="Leave it empty for no limit"
                value=${form.conlimit} @input=${text('conlimit')}></cds-text-input>
            <cds-text-input label="Password expires" type="date" helper-text="Leave it empty for never"
                value=${form.expires} @input=${text('expires')}></cds-text-input>
            <cds-multi-select title-text="Member of" label="Choose roles" filterable
                @cds-multi-select-selected=${(event: Event) => {
                    const value = (event.target as HTMLElement & { value: string }).value;
                    form.memberof = value ? value.split(',') : [];
                }}>
                ${roles.filter((name) => name !== form.name).map((name) => html`
                    <cds-multi-select-item value=${name} ?selected=${form.memberof.includes(name)}>${name}</cds-multi-select-item>
                `)}
            </cds-multi-select>
        </div>
    `;
}
