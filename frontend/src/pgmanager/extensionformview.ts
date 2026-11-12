// The inputs of pgmanager's form which installs an extension, which change
// the form

import { html, type TemplateResult } from 'lit';
import type { ExtensionForm, ExtensionFormErrors } from './extensionform.ts';

// Carbon components
import '@carbon/web-components/es/components/checkbox/index.js';
import '@carbon/web-components/es/components/combo-box/index.js';
import '@carbon/web-components/es/components/text-input/index.js';

// extensionFormView is the form's inputs, with the databases it can be
// installed in, and the extension's default version
export function extensionFormView(form: ExtensionForm, databases: readonly string[], defaultVersion: string | undefined, errors: ExtensionFormErrors = {}): TemplateResult {
    return html`
        <div class="form">
            <cds-combo-box title-text="Database" label="Choose a database" value=${form.database}
                ?invalid=${!!errors.database} invalid-text=${errors.database ?? ''}
                @cds-combo-box-selected=${(event: CustomEvent<{ item?: { value: string } }>) => {
                    form.database = event.detail.item?.value ?? '';
                }}>
                ${databases.map((name) => html`<cds-combo-box-item value=${name}>${name}</cds-combo-box-item>`)}
            </cds-combo-box>
            <cds-text-input label="Schema" value=${form.schema} helper-text="Leave it empty for the extension's own, or public"
                @input=${(event: Event) => { form.schema = (event.target as HTMLInputElement).value; }}></cds-text-input>
            <cds-text-input label="Version" value=${form.version} placeholder=${defaultVersion ?? ''}
                helper-text=${defaultVersion ? `Leave it empty for ${defaultVersion}, its default` : 'Leave it empty for its default'}
                @input=${(event: Event) => { form.version = (event.target as HTMLInputElement).value; }}></cds-text-input>
            <cds-checkbox .checked=${form.cascade} @cds-checkbox-changed=${(event: Event) => { form.cascade = (event.target as HTMLInputElement).checked; }}>
                Also install the extensions it requires
            </cds-checkbox>
        </div>
    `;
}
