// A setting as a form, which changes its value, with ALTER SYSTEM, which
// writes it to postgresql.auto.conf, rather than postgresql.conf, whose
// value it's used rather than, and which is reloaded, after it's changed

import type { Notification } from '@mutablelogic/go-server/notification';
import type { Setting, SettingReload } from './schema.ts';

// SettingForm is the setting's value, as it's typed, or chosen
export interface SettingForm {
    value: string;
}

// SettingFormErrors are what's wrong with the form
export interface SettingFormErrors {
    value?: string;
}

// The units a number can be written with, such as 8MB, or 30s, which the
// server converts to the setting's own
const withUnit = /^-?\d+(\.\d+)?\s*(B|kB|MB|GB|TB|us|ms|s|min|h|d)$/;

// settingForm is the form of a setting, with its value
export function settingForm(setting: Setting): SettingForm {
    return { value: setting.value ?? '' };
}

// settingFormErrors returns what's wrong with the form: a value which isn't
// one of the setting's type, or its values, or which is beyond its limits,
// unless it's written with a unit, which the server checks
export function settingFormErrors(setting: Setting, form: SettingForm): SettingFormErrors {
    const value = form.value.trim();
    switch (setting.type) {
        case 'bool':
            return value === 'on' || value === 'off' ? {} : { value: 'Choose on or off' };
        case 'enum':
            return setting.values?.includes(value) ? {} : { value: 'Choose one of its values' };
        case 'integer':
        case 'real': {
            if (setting.unit && withUnit.test(value)) {
                return {};
            }
            const number = Number(value);
            if (value === '' || !Number.isFinite(number) || (setting.type === 'integer' && !Number.isInteger(number))) {
                return { value: setting.type === 'integer' ? 'Type a whole number' : 'Type a number' };
            }
            const min = setting.min === undefined ? undefined : Number(setting.min);
            const max = setting.max === undefined ? undefined : Number(setting.max);
            if ((min !== undefined && number < min) || (max !== undefined && number > max)) {
                return { value: `Type a number from ${setting.min} to ${setting.max}` };
            }
            return {};
        }
        default:
            return {};
    }
}

// settingRequest is the setting's value, as pgmanager takes it, which is
// null to remove the value set by ALTER SYSTEM. A string is as it's typed,
// as its spaces can matter, such as log_line_prefix's last, and the others
// are trimmed.
export function settingRequest(setting: Setting, form?: SettingForm): { value: string | null } {
    if (!form) {
        return { value: null };
    }
    return { value: setting.type === 'string' ? form.value : form.value.trim() };
}

// settingReadOnly returns why a setting can't be changed, or else undefined:
// it's allow_alter_system, which ALTER SYSTEM can't change, it's
// PostgreSQL's own, or it needs a restart, which pgmanager can't do, or
// ALTER SYSTEM is turned off, with allow_alter_system
export function settingReadOnly(setting: Setting, alterSystem = true): string | undefined {
    if (setting.name === 'allow_alter_system') {
        return "ALTER SYSTEM can't change it, so it's changed in the configuration file, or on the command line.";
    }
    switch (setting.context) {
        case 'internal':
            return "It's set when PostgreSQL is built, or its data directory created, so it can't be changed.";
        case 'postmaster':
            return 'It needs the server to restart, which pgmanager can\'t do, so it\'s changed in the configuration file.';
    }
    if (!alterSystem) {
        return "ALTER SYSTEM is turned off, with allow_alter_system, so it's changed in the configuration file.";
    }
    return undefined;
}

// settingEffect says when a change of a setting, by its context, takes
// effect, once the configuration's reloaded, which pgmanager does
export function settingEffect(context: string): string {
    switch (context) {
        case 'internal':
            return "Can't be changed";
        case 'postmaster':
            return 'Only after the server restarts';
        case 'sighup':
            return 'Once the configuration is reloaded';
        case 'superuser-backend':
        case 'backend':
            return 'For new sessions, once the configuration is reloaded';
        default:
            // user, and superuser, which a session can also change for itself
            return 'For new sessions, once the configuration is reloaded, and in any session which sets it';
    }
}

// reloadNotification is what a reload says: that it's reloaded, with the
// settings which wait for a restart, or, when the configuration files have
// errors, whose settings are skipped, a warning of them
export function reloadNotification(result: SettingReload): Notification {
    const pending = result.pending_restart ?? [];
    const waiting = pending.length ? `${pending.length === 1 ? '1 setting waits' : `${pending.length} settings wait`} for the server to restart: ${pending.join(', ')}` : undefined;
    const errors = result.errors ?? [];
    if (errors.length) {
        return {
            kind: 'warning',
            title: `The configuration was reloaded, but ${errors.length === 1 ? 'a setting was' : `${errors.length} settings were`} skipped`,
            subtitle: errors.map((e) => `${e.file}:${e.line}${e.name ? ` ${e.name}` : ''}: ${e.error}`).join('; '),
            caption: waiting,
        };
    }
    return { kind: 'success', title: 'The configuration was reloaded', subtitle: waiting };
}
