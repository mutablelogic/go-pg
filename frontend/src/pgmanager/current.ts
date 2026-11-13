// What's pgmanager's own, which is badged "you": the role it runs as, which
// is known once the roles are loaded, after what's shown may have been
// rendered, so its badges render themselves again when it's known

import { LitElement, html, nothing } from 'lit';

// Carbon components
import '@carbon/web-components/es/components/tag/tag.js';

// you is the badge of what's pgmanager's own: the database it's connected
// to, the role it runs as, and its connection, which can't be deleted
export const you = html`<cds-tag size="sm" type="blue" title="pgmanager's own" style="margin: 0 0 0 0.25rem">you</cds-tag>`;

// connected is the badge of the database pgmanager is connected to, which
// says so, rather than "you", beside its owner, who may be "you" too
export const connected = html`<cds-tag size="sm" type="blue" title="pgmanager is connected to this database, so it can't be deleted" style="margin: 0">connected</cds-tag>`;

// currentRole is the role pgmanager runs as, and badges are the elements
// which badge it, wherever they are, such as in a page's header, or a
// panel, which are rendered again when it changes
let currentRole: string | undefined;
const badges = new Set<YouElement>();

// setCurrentRole sets the role pgmanager runs as, and renders its badges
// again
export function setCurrentRole(name?: string) {
    if (name === currentRole) {
        return;
    }
    currentRole = name;
    badges.forEach((badge) => badge.requestUpdate());
}

// YouElement is pgm-you, which is the badge of the role it names, when it's
// the one
// pgmanager runs as, or else nothing. It renders in the light DOM, inline,
// beside the role's name.
export class YouElement extends LitElement {
    static override properties = {
        name: { type: String },
    };

    // name is the role's name
    declare name?: string;

    protected override createRenderRoot(): HTMLElement {
        return this;
    }

    override connectedCallback() {
        super.connectedCallback();
        badges.add(this);
    }

    override disconnectedCallback() {
        badges.delete(this);
        super.disconnectedCallback();
    }

    protected override render() {
        return this.name !== undefined && this.name === currentRole ? you : nothing;
    }
}

if (!customElements.get('pgm-you')) {
    customElements.define('pgm-you', YouElement);
}
