/**
 * DOM environment for `bun test` in this package.
 *
 * Registered via `preload` in bunfig.toml, so it runs once before any spec and
 * every spec in the package gets the same globals. Lives outside `src/` on
 * purpose: `vite-plugin-dts` emits declarations for everything under `src/`, so
 * a setup file in there would ship a stray `.d.ts` into `dist/`.
 *
 * happy-dom rather than jsdom because `@happy-dom/global-registrator` installs
 * onto globalThis, which is what a Bun preload can do; jsdom expects to own the
 * test runner's environment and has no equivalent hook. See test/README.md.
 */
import { GlobalRegistrator } from "@happy-dom/global-registrator";
import { afterEach } from "bun:test";

GlobalRegistrator.register();

// Imported dynamically, AFTER register(). Testing Library binds to `document`
// at module scope, so a static import would be hoisted above the line that
// creates it and every query would resolve against an undefined document.
const { cleanup } = await import("@testing-library/react");

// Unmount between tests. Without this each render appends to the same <body>,
// so `getByLabelText` starts throwing "found multiple elements" on the second
// test that renders the same widget.
afterEach(cleanup);
