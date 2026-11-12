// Candidate for go-server's @mutablelogic/go-server/page/view: the
// gs-page-header element is the dark header at the top of a page, across
// its width, with an icon, a title, a subtitle beneath it, and actions on
// the right, such as Create, which are Carbon buttons. Controls, such as a
// search, can be slotted beside the title. It's in Carbon's g90 theme, so
// the buttons, and what's slotted, are too.

import { LitElement, css, html, nothing } from 'lit';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import type { CarbonIconDescriptor } from '@carbon/web-components/es/globals/internal/icon-loader-utils.js';

// Carbon components
import '@carbon/web-components/es/components/button/index.js';

// PageAction is an action on the right of the header: its label, its kind,
// which is primary by default, its icon, a 16px Carbon icon, and what it
// runs, when it's clicked
export interface PageAction {
    label: string;
    kind?: 'primary' | 'secondary' | 'tertiary' | 'ghost' | 'danger';
    icon?: CarbonIconDescriptor;
    disabled?: boolean;
    run: () => void;
}

// PageHeaderTag is the element's tag
export const PageHeaderTag = 'gs-page-header';

export class PageHeaderElement extends LitElement {
    static override properties = {
        heading: { type: String },
        subtitle: { type: String },
        icon: { attribute: false },
        actions: { attribute: false },
    };

    // heading is the page's title, and subtitle what's beneath it
    declare heading: string;
    declare subtitle: string;

    // icon is a 32px Carbon icon, before the title
    declare icon?: CarbonIconDescriptor;

    // actions are the buttons on the right
    declare actions: PageAction[];

    static override styles = css`
        :host {
            display: block;
            margin: var(--gs-page-header-margin, 0 0 1.5rem);
            padding: 1.5rem 2rem;
            background: var(--cds-background, #262626);
            color: var(--cds-text-primary, #f4f4f4);
        }

        :host([hidden]) {
            display: none;
        }

        header {
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .icon {
            display: flex;
            flex: none;
        }

        .icon svg {
            fill: currentColor;
        }

        .text {
            min-width: 0;
        }

        h1 {
            margin: 0;
            font-size: 1.75rem;
            font-weight: 400;
            line-height: 1.28572;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        p {
            margin: 0.125rem 0 0;
            color: var(--cds-text-secondary, #c6c6c6);
            font-size: 0.875rem;
            line-height: 1.28572;
        }

        /* What's slotted beside the title fills the rest of the header */
        ::slotted(*) {
            flex: 1;
            min-width: 0;
        }

        .actions {
            display: flex;
            flex: none;
            gap: 1px;
            margin-left: auto;
        }
    `;

    constructor() {
        super();
        this.heading = '';
        this.subtitle = '';
        this.actions = [];
    }

    // The header is in Carbon's g90 theme, whose tokens the shadow root, and
    // what's slotted, inherit
    override connectedCallback() {
        super.connectedCallback();
        this.classList.add('cds--g90');
    }

    protected override render() {
        return html`
            <header>
                ${this.icon ? html`<span class="icon">${iconLoader(this.icon)}</span>` : nothing}
                <div class="text">
                    <h1>${this.heading}</h1>
                    ${this.subtitle ? html`<p>${this.subtitle}</p>` : nothing}
                </div>
                <slot></slot>
                ${this.actions.length ? html`
                    <div class="actions">
                        ${this.actions.map((action) => html`
                            <cds-button kind=${action.kind ?? 'primary'} size="md" ?disabled=${action.disabled} @click=${() => action.run()}>
                                ${action.label}
                                ${action.icon ? iconLoader(action.icon, { slot: 'icon' }) : nothing}
                            </cds-button>
                        `)}
                    </div>
                ` : nothing}
            </header>
        `;
    }
}

if (!customElements.get(PageHeaderTag)) {
    customElements.define(PageHeaderTag, PageHeaderElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [PageHeaderTag]: PageHeaderElement;
    }
}
