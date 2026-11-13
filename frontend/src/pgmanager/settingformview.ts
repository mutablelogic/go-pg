// The input of pgmanager's form of a setting, which fits its type: a toggle
// for on or off, a dropdown of an enum's values, and text for a number, with
// its unit and limits, or a string

import { html, type TemplateResult } from 'lit';
import type { Setting } from './schema.ts';
import type { SettingForm, SettingFormErrors } from './settingform.ts';

// Carbon components
import '@carbon/web-components/es/components/dropdown/index.js';
import '@carbon/web-components/es/components/text-input/index.js';
import '@carbon/web-components/es/components/toggle/index.js';

// settingFormView is the form's input, of the setting, and what happens
// when it's changed
export function settingFormView(setting: Setting, form: SettingForm, effect: string, errors: SettingFormErrors = {}): TemplateResult {
    return html`
        <div class="form">
            ${input(setting, form, errors)}
            <p class="helper">
                It's written to postgresql.auto.conf, by ALTER SYSTEM, and used rather than the configuration file's value.
                It takes effect: ${effect.toLowerCase()}.
            </p>
        </div>
    `;
}

function input(setting: Setting, form: SettingForm, errors: SettingFormErrors): TemplateResult {
    const text = (event: Event) => {
        form.value = (event.target as HTMLInputElement).value;
    };
    switch (setting.type) {
        case 'bool':
            return html`
                <cds-toggle label-text=${setting.name} label-a="on" label-b="off" ?toggled=${form.value === 'on'}
                    @cds-toggle-changed=${(event: Event) => {
                        form.value = (event.target as HTMLElement & { toggled: boolean }).toggled ? 'on' : 'off';
                    }}></cds-toggle>
            `;
        case 'enum':
            return html`
                <cds-dropdown title-text=${setting.name} value=${form.value} ?invalid=${!!errors.value} invalid-text=${errors.value ?? ''}
                    @cds-dropdown-selected=${(event: CustomEvent<{ item: { value: string } }>) => {
                        form.value = event.detail.item.value;
                    }}>
                    ${(setting.values ?? []).map((value) => html`<cds-dropdown-item value=${value}>${value}</cds-dropdown-item>`)}
                </cds-dropdown>
            `;
        case 'integer':
        case 'real': {
            const unit = setting.unit ? ` (${setting.unit})` : '';
            const limits = setting.min !== undefined && setting.max !== undefined ? `From ${setting.min} to ${setting.max}${setting.unit ? ` ${setting.unit}` : ''}` : '';
            const helper = [limits, setting.unit ? 'or with a unit, such as 8MB, or 30s' : ''].filter(Boolean).join(', ');
            return html`
                <cds-text-input label=${`${setting.name}${unit}`} value=${form.value} helper-text=${helper}
                    ?invalid=${!!errors.value} invalid-text=${errors.value ?? ''} @input=${text}></cds-text-input>
            `;
        }
        default:
            return html`
                <cds-text-input label=${setting.name} value=${form.value} ?invalid=${!!errors.value} invalid-text=${errors.value ?? ''}
                    @input=${text}></cds-text-input>
            `;
    }
}
