// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import { afterEach, setDefaultTimeout } from "bun:test";

GlobalRegistrator.register();

// Imported dynamically, AFTER register(). Testing Library binds to `document`
// at module scope, so a static import would be hoisted above the line that
// creates it and every query would resolve against an undefined document.
const { cleanup, configure } = await import("@testing-library/react");

// Unmount between tests. Without this each render appends to the same <body>,
// so `getByLabelText` starts throwing "found multiple elements" on the second
// test that renders the same widget.
//
// `cleanup` only removes what `render` mounted. A node a spec attaches by hand
// stays on the one <body> every later spec file shares, so the test that trips
// over it is in another file, and only in a file order that runs it later.
// Bun's file order is not fixed: two ubuntu runs of one commit used different
// orders. So the body is emptied here whatever the spec did, and the test that
// left something fails, naming it, in the same place on every run.
afterEach(() => {
   cleanup();
   const leaked = [...document.body.childNodes];
   if (leaked.length === 0) return;
   document.body.replaceChildren();
   throw new Error(
      `the test left ${leaked.length} node(s) on document.body, which every ` +
         `later spec file would query: ` +
         leaked.map(describeNode).join(", ") +
         `. Remove what the spec attaches by hand, or leave it detached.`,
   );
});

function describeNode(node: Node): string {
   const html =
      node instanceof Element ? node.outerHTML : String(node.textContent);
   return html.length > 120 ? `${html.slice(0, 120)}...` : html;
}

// How long `waitFor` and `findBy*` wait, and how long one test may run.
// Testing Library's 1s and Bun's 5s fit a laptop, not the CI runners: the
// ReportEditor specs that pass in about 1.1 to 1.8s on CI failed there with
// the element arriving just after the 1s wait gave up. The test limit stays
// well above the wait, so a wait that never resolves still fails with Testing
// Library's DOM dump rather than a bare timeout.
configure({ asyncUtilTimeout: 5_000 });
setDefaultTimeout(20_000);
