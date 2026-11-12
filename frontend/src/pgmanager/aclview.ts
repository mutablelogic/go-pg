// The grid of access privileges: a row for each role, with a checkbox for
// each privilege, which changes the rows, a button which removes the row,
// and Add a role, beneath, which adds one, with none, to check

import { html, type TemplateResult } from 'lit';
import { keyed } from 'lit/directives/keyed.js';
import { Public, type AclRow } from './aclform.ts';
import { removeButton, roleName } from './views.ts';

// Carbon components
import '@carbon/web-components/es/components/button/index.js';
import '@carbon/web-components/es/components/checkbox/index.js';
import '@carbon/web-components/es/components/combo-box/index.js';
import '@carbon/web-components/es/components/data-table/index.js';

// AclGridOptions are the privileges of the kind of thing, the roles which
// can be added, what applies when no privileges are granted, and what's
// called when the rows change
export interface AclGridOptions {
    privileges: readonly string[];
    roles: readonly string[];
    defaults: string;
    rerender: () => void;
}

// aclGrid is the grid of the rows, which it changes
export function aclGrid(rows: AclRow[], options: AclGridOptions): TemplateResult {
    const addable = [Public, ...options.roles].filter((role) => !rows.some((row) => row.role === role));
    // Carbon's table takes a checkbox's change as choosing its row, so the
    // grid's checkboxes keep theirs
    const toggle = (row: AclRow, priv: string) => (event: Event) => {
        event.stopPropagation();
        const checked = (event.target as HTMLInputElement).checked;
        row.priv = checked ? [...row.priv, priv] : row.priv.filter((p) => p !== priv);
    };
    return html`
        <div class="members-field">
            <span class="label">Access</span>
            ${rows.length ? html`
                <cds-table size="sm" class="members">
                    <cds-table-head>
                        <cds-table-header-row>
                            <cds-table-header-cell>Role</cds-table-header-cell>
                            ${options.privileges.map((priv) => html`<cds-table-header-cell>${priv.toLowerCase()}</cds-table-header-cell>`)}
                            <cds-table-header-cell><span class="visually-hidden">Remove</span></cds-table-header-cell>
                        </cds-table-header-row>
                    </cds-table-head>
                    <cds-table-body>
                        ${rows.map((row) => html`
                            <cds-table-row>
                                <cds-table-cell>${row.role === Public ? html`<em>everyone</em>` : roleName(row.role)}</cds-table-cell>
                                ${options.privileges.map((priv) => html`
                                    <cds-table-cell>
                                        <cds-checkbox hide-label .checked=${row.priv.includes(priv)} @cds-checkbox-changed=${toggle(row, priv)}>${priv.toLowerCase()} for ${row.role}</cds-checkbox>
                                    </cds-table-cell>
                                `)}
                                <cds-table-cell class="remove">
                                    ${removeButton(`Remove ${row.role === Public ? 'everyone' : row.role}`, () => {
                                        rows.splice(rows.indexOf(row), 1);
                                        options.rerender();
                                    })}
                                </cds-table-cell>
                            </cds-table-row>
                        `)}
                    </cds-table-body>
                </cds-table>
            ` : html`<p class="helper">${options.defaults}</p>`}
            ${keyed(rows.map((row) => row.role).join(','), html`
                <cds-combo-box title-text="Add a role" label="Choose a role"
                    @cds-combo-box-selected=${(event: CustomEvent<{ item?: { value: string } }>) => {
                        const role = event.detail.item?.value;
                        if (role && !rows.some((row) => row.role === role)) {
                            rows.push({ role, priv: [] });
                            options.rerender();
                        }
                    }}>
                    ${addable.map((role) => html`<cds-combo-box-item value=${role}>${role === Public ? 'everyone (PUBLIC)' : role}</cds-combo-box-item>`)}
                </cds-combo-box>
            `)}
        </div>
    `;
}
