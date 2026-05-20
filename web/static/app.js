// SPA glue: Alpine stores + shared page helpers.
//
// Loaded at end of <body> (so the DOM is already parsed when this runs)
// and applies polish synchronously: price formatting and the
// #last-updated relative-time renderer.

const YMT_THEMES = [
    { id: 'light',    label: 'Light',    dark: false, swatch: '#f3f4f6' },
    { id: 'dark',     label: 'Dark',     dark: true,  swatch: '#111827' },
    { id: 'midnight', label: 'Midnight', dark: true,  swatch: '#020617' },
    { id: 'forest',   label: 'Forest',   dark: true,  swatch: '#0a1a14' },
    { id: 'rose',     label: 'Rose',     dark: true,  swatch: '#1c0a14' },
    { id: 'ocean',    label: 'Ocean',    dark: true,  swatch: '#032027' },
    { id: 'sunset',   label: 'Sunset',   dark: true,  swatch: '#1a0f0a' },
    { id: 'lavender', label: 'Lavender', dark: true,  swatch: '#14091f' },
    { id: 'coffee',   label: 'Coffee',   dark: true,  swatch: '#1c1410' },
    { id: 'nord',     label: 'Nord',     dark: true,  swatch: '#2e3440' },
    { id: 'dracula',  label: 'Dracula',  dark: true,  swatch: '#282a36' },
];

const YMT_FONT_SIZES = ['sm', 'md', 'lg', 'xl'];
const YMT_DEFAULT_FONT_SIZE = 'md';

document.addEventListener('alpine:init', () => {
    Alpine.store('theme', {
        current: document.documentElement.dataset.theme || 'light',
        themes: YMT_THEMES,
        set(id) {
            const t = YMT_THEMES.find(x => x.id === id);
            if (!t) return;
            this.current = id;
            const html = document.documentElement;
            html.dataset.theme = id;
            html.classList.toggle('dark', t.dark);
            try { localStorage.theme = id; } catch (e) { /* private mode */ }
        },
        get isDark() {
            const t = YMT_THEMES.find(x => x.id === this.current);
            return t ? t.dark : false;
        },
    });

    Alpine.store('fontSize', {
        current: document.documentElement.dataset.fontSize || YMT_DEFAULT_FONT_SIZE,
        sizes: YMT_FONT_SIZES,
        set(id) {
            if (!YMT_FONT_SIZES.includes(id)) return;
            this.current = id;
            document.documentElement.dataset.fontSize = id;
            try { localStorage.fontSize = id; } catch (e) { /* private mode */ }
        },
    });

    Alpine.store('tableFontSize', {
        current: document.documentElement.dataset.tableFontSize || YMT_DEFAULT_FONT_SIZE,
        sizes: YMT_FONT_SIZES,
        set(id) {
            if (!YMT_FONT_SIZES.includes(id)) return;
            this.current = id;
            document.documentElement.dataset.tableFontSize = id;
            try { localStorage.tableFontSize = id; } catch (e) { /* private mode */ }
        },
    });
});

function formatPrices(root) {
    (root || document).querySelectorAll('[data-price]').forEach(elem => {
        if (elem.dataset.formatted === '1') return;
        // Server-rendered fallback already present — don't clobber it.
        if (elem.textContent && elem.textContent.trim() !== '') {
            elem.dataset.formatted = '1';
            return;
        }
        const price = parseInt(elem.dataset.price, 10);
        if (!isNaN(price)) {
            elem.textContent = `${price.toLocaleString()}z`;
            elem.dataset.formatted = '1';
        }
    });
}

let lastUpdatedTimer = null;
function renderLastUpdated(root) {
    const el = (root || document).querySelector('#last-updated[data-timestamp]');
    if (!el) return;
    const lang = document.documentElement.lang || 'en';
    const ts = el.dataset.timestamp;

    const docRoot = document.documentElement.dataset;
    const neverLabel = el.dataset.labelNever || docRoot.labelNever || 'Updated: Never';
    const agoFmt = el.dataset.labelAgo || docRoot.labelAgo || 'Updated %s ago';

    if (!ts || ts === 'Never') {
        el.textContent = neverLabel;
        return;
    }

    const date = new Date(ts);
    const formattedTimestamp = date.toLocaleString([], {
        year: 'numeric', month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit',
    });

    const tick = () => {
        const secondsPast = Math.floor((Date.now() - date.getTime()) / 1000);
        let ago;
        if (lang === 'pt') {
            if (secondsPast < 5) ago = 'agora mesmo';
            else if (secondsPast < 60) ago = `${secondsPast}s`;
            else if (secondsPast < 3600) ago = `${Math.floor(secondsPast / 60)}m`;
            else if (secondsPast < 86400) ago = `${Math.floor(secondsPast / 3600)}h`;
            else ago = `${Math.floor(secondsPast / 86400)}d`;
        } else {
            if (secondsPast < 5) ago = 'just now';
            else if (secondsPast < 60) ago = `${secondsPast}s ago`;
            else if (secondsPast < 3600) ago = `${Math.floor(secondsPast / 60)}m ago`;
            else if (secondsPast < 86400) ago = `${Math.floor(secondsPast / 3600)}h ago`;
            else ago = `${Math.floor(secondsPast / 86400)}d ago`;
        }
        const main = agoFmt.replace('%s', ago);
        el.textContent = main + ' ';
        const sub = document.createElement('span');
        sub.className = 'text-gray-400 dark:text-gray-500 font-normal';
        sub.textContent = `(${formattedTimestamp})`;
        el.appendChild(sub);
    };

    tick();
    if (lastUpdatedTimer) clearInterval(lastUpdatedTimer);
    lastUpdatedTimer = setInterval(tick, 30000);
}

function annotateSortableHeaders(root) {
    (root || document).querySelectorAll('th > a[href*="sort_by="]').forEach(a => {
        const th = a.parentElement;
        if (!th || th.tagName !== 'TH') return;
        const arrow = a.querySelector('span');
        let sort = 'none';
        if (arrow) {
            const txt = (arrow.textContent || '').trim();
            if (txt === '▲') sort = 'ascending';
            else if (txt === '▼') sort = 'descending';
        }
        th.setAttribute('aria-sort', sort);
        if (!th.hasAttribute('scope')) th.setAttribute('scope', 'col');
    });
}

function applyPagePolish(root) {
    formatPrices(root);
    renderLastUpdated(root);
    annotateSortableHeaders(root);
}

applyPagePolish(document);

document.addEventListener('htmx:beforeSwap', (e) => {
    const xhr = e.detail && e.detail.xhr;
    if (!xhr || !xhr.responseText) return;
    try {
        const parsed = new DOMParser().parseFromString(xhr.responseText, 'text/html');
        if (parsed.title) document.title = parsed.title;
    } catch (err) { /* ignore */ }
});

// Wait for any external scripts injected into the swap target to finish
// loading before firing the synthetic DOMContentLoaded that page init
// scripts (e.g. chart pages) listen for. Without this wait, an inline
// init script that uses Chart.js fires before chart.umd.min.js has
// loaded on a partial swap and throws "Chart is not defined".
function awaitSwappedScripts(target) {
    const externals = target.querySelectorAll('script[src]');
    if (!externals.length) return Promise.resolve();
    const pending = [];
    externals.forEach(s => {
        if (s.dataset.ymtAwaited === '1') return;
        s.dataset.ymtAwaited = '1';
        pending.push(new Promise(resolve => {
            let done = false;
            const finish = () => { if (!done) { done = true; resolve(); } };
            s.addEventListener('load', finish, { once: true });
            s.addEventListener('error', finish, { once: true });
            // Fallback: if the script was already cached and fired its
            // load event before we attached the listener, this timer
            // unblocks DOMContentLoaded. 1500ms is well over a cache hit.
            setTimeout(finish, 1500);
        }));
    });
    return Promise.all(pending);
}

// The synthetic DOMContentLoaded below is intentional. Chart pages
// (history, players, chat, market_stats, …) wrap their canvas init in
// `document.addEventListener('DOMContentLoaded', …)`, which only fires
// once per real page load. On an htmx partial swap the new inline init
// script needs the listener it just registered to fire; dispatching the
// event manually after the swap (and after any new <script src> tags
// have loaded — see awaitSwappedScripts) is what unblocks them. If you
// change this, audit every chart template first.
document.addEventListener('htmx:afterSwap', (e) => {
    const target = (e.detail && e.detail.target) || document;
    applyPagePolish(target);
    if (window.Alpine && typeof Alpine.initTree === 'function') {
        try { Alpine.initTree(target); } catch (err) { /* ignore */ }
    }
    awaitSwappedScripts(target).then(() => {
        document.dispatchEvent(new Event('DOMContentLoaded'));
    });
});

document.addEventListener('htmx:sendError', (e) => {
    const path = e.detail && e.detail.pathInfo && e.detail.pathInfo.requestPath;
    if (path) window.location.href = path;
});
