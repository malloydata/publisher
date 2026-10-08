<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Drawers, trays and toasts in an embedded app

> How to place anything that floats over the page (an entity drawer, a compare tray, a toast) so it lands on the reader's screen and the page behind it holds still, standalone and inside an auto-sized frame. Referenced from `SKILL.md` and `reference/embedding.md`. Every rule below is a bug a real build shipped and a reader found.

## Why `position: fixed` breaks when embedded

A host that embeds the app (Credible, the Publisher console, a page using `Publisher.embed`) sizes the iframe to the app's full content height and scrolls its own container. So inside the frame:

- `window.innerHeight` is the whole document, thousands of pixels;
- `window.scrollY` is always 0, because the frame never scrolls; the host does;
- `position: fixed; top: 0` means **the top of the document**, not the top of the reader's screen.

A drawer built the standalone way therefore opens at the top of the app, far above where the reader is. Then focusing its close button scrolls the *host* up to meet it, and the page the reader was on jumps. Measured on a real build: the clicked row moved about 1,000px, the drawer was the height of the whole document so it could not scroll on its own, and closing it left the reader thousands of pixels from where they had been. None of this shows up when the page is opened directly, because standalone `fixed` is correct.

The host does not tell the frame where the reader is: the resize protocol goes one way, frame to host. The frame has to work it out.

## Find the visible band

An `IntersectionObserver` with the implicit root reports against the **top-level** viewport, clipped by the host's scroll container, for same-origin and cross-origin frames alike, and its `intersectionRect` comes back in the frame's own coordinates. Observe a column of thin strips down the page, keep the union of their visible parts current from page load, and read it synchronously when something opens.

```js
// view.js: which slice of our document is on the reader's screen.
export const EMBEDDED = (() => { try { return window.self !== window.top; } catch { return true; } })();
let VIEW = { top: 0, height: innerHeight, known: !EMBEDDED };
const listeners = new Set();
export const onView = (fn) => (listeners.add(fn), () => listeners.delete(fn));

if (EMBEDDED && "IntersectionObserver" in window) {
  document.documentElement.classList.add("is-embedded");
  const STEP = 100, holder = document.createElement("div"), strips = [], vis = {};
  holder.className = "view-strips";                        // height 0: see the rules below
  const io = new IntersectionObserver((entries) => {
    const sy = window.scrollY;                             // viewport -> document coordinates
    for (const e of entries) {
      const r = e.intersectionRect;
      if (e.isIntersecting && r.height > 0) vis[e.target.__i] = [r.top + sy, r.bottom + sy];
      else delete vis[e.target.__i];
    }
    const spans = Object.values(vis);
    if (!spans.length) return;
    const top = Math.min(...spans.map((s) => s[0])), bottom = Math.max(...spans.map((s) => s[1]));
    VIEW = { top: Math.round(top), height: Math.round(bottom - top), known: true };
    listeners.forEach((fn) => fn(VIEW));
  }, { threshold: Array.from({ length: 21 }, (_, i) => i / 20) });   // every 5% of a strip

  // Cover the content and STOP AT ITS BOTTOM EDGE, measured the way the SDK
  // measures the height it reports (the lowest bottom among body's children).
  const contentBottom = () => Math.ceil(Math.max(0, ...[...document.body.children]
    .filter((k) => k !== holder && !k.classList.contains("overlay"))
    .map((k) => k.getBoundingClientRect().bottom + window.scrollY)));
  const cover = () => {
    const end = contentBottom(), need = Math.max(1, Math.ceil(end / STEP));
    while (strips.length < need) { const s = document.createElement("div"); s.__i = strips.length; strips.push(s); holder.append(s); io.observe(s); }
    while (strips.length > need) { const s = strips.pop(); io.unobserve(s); s.remove(); delete vis[s.__i]; }
    strips.forEach((s, i) => { s.style.top = i * STEP + "px"; s.style.height = Math.max(1, Math.min(STEP, end - i * STEP)) + "px"; });
  };
  document.body.append(holder);
  cover();
  new ResizeObserver(cover).observe(document.body);        // tiles land; the page grows and shrinks
}

// The band an overlay should occupy, in document coordinates. The top is pinned
// to the top of what the reader sees; only the height gives way.
export function visibleBand() {
  if (!EMBEDDED) return { top: window.scrollY, height: innerHeight };
  if (!VIEW.known) return { top: 0, height: Math.min(innerHeight, 900) };
  const room = document.documentElement.scrollHeight - VIEW.top;
  return { top: VIEW.top, height: Math.min(Math.max(VIEW.height, 360), room) };
}
```

```css
.view-strips { position: absolute; top: 0; left: 0; width: 1px; height: 0; pointer-events: none; visibility: hidden; }
.view-strips > div { position: absolute; left: 0; width: 1px; }
/* Embedded, the host scrolls; our document never does. */
html.is-embedded { overflow: hidden; }
```

## Place one layer over the band

Put the scrim and the panel in one layer. Standalone it is `position: fixed; inset: 0`. Embedded it is `position: absolute` with its `top` and `height` set from `visibleBand()`, re-set whenever `onView` fires, so it follows the host if the reader scrolls it by other means. The layer clips its contents (`overflow: hidden`), so the panel's slide-in never widens the page.

```css
.overlay { position: fixed; inset: 0; z-index: 40; overflow: hidden; pointer-events: none; }
.overlay.is-open, .overlay.is-open > * { pointer-events: auto; }
.overlay.is-anchored { position: absolute; inset: auto 0 auto 0; }   /* top/height from JS */
.overlay .scrim { position: absolute; inset: 0; background: var(--scrim); opacity: 0; transition: opacity var(--dur-base); }
.overlay.is-open .scrim { opacity: 1; }
.overlay .panel {
  position: absolute; top: 0; right: 0; bottom: 0; width: min(760px, 94vw);
  display: flex; flex-direction: column;
  transform: translateX(calc(100% + 64px));         /* parked past its own shadow */
  transition: transform var(--dur-base) var(--ease-out);
}
.overlay.is-open .panel { transform: none; }
.overlay .panel-head { flex: none; }
.overlay .panel-body {                               /* the panel's only scroller */
  flex: 1 1 auto; min-height: 0; overflow-y: auto; overscroll-behavior: contain;
  display: grid; align-content: start; gap: var(--space-4);
}
```

A tray or toast pinned to the bottom of the screen follows the same rule: embedded, give it `position: absolute` and set its `top` to `band.top + band.height - itsHeight - gap` on every `onView`.

## The rules, and the bug each one prevents

- **Read the band synchronously; never wait on a fresh measurement to open.** An async probe with a timeout fallback raced the click, lost, and opened the drawer at the top of the document.
- **Pin the overlay's top to the top of the band; let only its height give way.** Pushing the top up to honour a minimum height puts the header above the screen when only the bottom of the app is visible.
- **Nothing you add may extend the content.** Sentinels, strips, an overlay layer: anything whose box reaches past the content's bottom edge is scrollable overflow. The document then becomes taller than the frame the host sized, the reader's first wheel turns scroll the app *inside* its frame, and everything placed by the band is offset by that much. Measured: strips rounded up to the next 100px made every drawer open 7 to 46px too high, header under the host's breadcrumb bar, bottom short of the window. Keep sentinels in a zero-height holder, end the last one at the content edge, and keep overlays inside the band.
- **The frame never scrolls.** `html { overflow: hidden }` when embedded, after the rule above makes sure nothing is clipped. Every wheel turn then belongs to the host, and there is no dead stretch of inner scrolling before the page moves.
- **Work in document coordinates.** `intersectionRect` is in viewport coordinates; an absolutely positioned layer is placed in document coordinates. Add `window.scrollY` at the moment you read it. They agree only while the frame is unscrolled, which is exactly the assumption the previous bug broke.
- **Focus without scrolling.** `el.focus({ preventScroll: true })`, both when focusing into the panel and when handing focus back on close. A plain `focus()` scrolls whatever ancestor (including the host) it takes to bring the element into view.
- **Consume scroll that is not the panel's.** A non-passive `wheel` and `touchmove` listener on the layer calls `preventDefault()` unless the event is inside the panel body and the body can still scroll that way. `overscroll-behavior: contain` on the body stops chaining at its ends. Without both, the wheel inside a drawer scrolled the page behind it by 2,000 to 3,000px.
- **Scroll keys scroll the panel.** With focus on the close button, PageDown and Space go to the page behind. Handle the scroll keys on the panel and scroll the body. Escape closes; Tab stays inside.
- **Nothing behind the panel moves.** No `overflow: hidden` toggled on the page (it removes a scrollbar and reflows everything), no `backdrop-filter: blur` on the scrim (it re-rasterises the page on every frame of the slide and reads as the page itself moving).
- **Exits keep their content.** Remove the open class, let the panel slide out with what it showed, then remove the layer. Under `prefers-reduced-motion: reduce`, remove it at once.
- **Page rules leak into the panel.** A panel section built from `<section>` inherits the page's `section { margin-top }`, and in a grid that margin does not collapse: measured as a 44px hole above every panel. Reset margins on panel content.

## Verify it embedded

None of this can be checked standalone. Load the app inside a real host (the Publisher console at `/<env>/<package>/data-apps/<path>`, or a test page that calls `Publisher.embed` inside a scrolling container), scroll the host with real wheel events, open a drawer from well down the page, and assert on screen coordinates. `reference/verification-harness.md` has the checks.
