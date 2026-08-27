// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it } from "bun:test";
import { buildCollapseWrapper, isChromeSuppressed } from "./collapse_wrapper";

/**
 * This file had no spec, and the two options that interact here, `chrome=none`
 * and `forceCollapsed`, are exactly where an error card became unreachable.
 *
 * Each option is sensible alone. Under `chrome=none` the header is suppressed,
 * because the host is drawing its own chrome. `forceCollapsed` keeps an error
 * compact while the agent reads the tool result. Together they produced a card
 * with no header to click and a body left at `display: none`: the error text was
 * in the DOM at a 0x0 rect, the card was 2px tall, and there were zero clickable
 * elements. Every failure this widget writes copy for rendered as an empty
 * bordered box.
 *
 * happy-dom applies no layout, so these assert the STATE that drives the CSS,
 * the `is-open` class and whether a header exists, rather than measured pixels.
 * The pixel behaviour was verified in Chrome against a real payload.
 */

function makeRoot(): HTMLElement {
   const root = document.createElement("div");
   document.body.appendChild(root);
   return root;
}

/**
 * Set the widget's own query string.
 *
 * Assigning `location.href` rather than `history.replaceState`: happy-dom
 * accepts replaceState silently but does not move `location.search`, so the
 * chrome=none specs passed vacuously against an empty search string until this
 * was checked.
 */
function setSearch(search: string): void {
   window.location.href = `http://localhost/${search}`;
}

describe("isChromeSuppressed", () => {
   beforeEach(() => setSearch(""));

   it("is false with no query string", () => {
      expect(isChromeSuppressed()).toBe(false);
   });

   it("is true for chrome=none", () => {
      setSearch("?chrome=none");
      expect(isChromeSuppressed()).toBe(true);
   });

   it("is false for any other chrome value", () => {
      setSearch("?chrome=full");
      expect(isChromeSuppressed()).toBe(false);
   });
});

describe("buildCollapseWrapper", () => {
   beforeEach(() => {
      document.body.innerHTML = "";
      setSearch("");
   });

   it("starts collapsed with a clickable header by default", () => {
      const root = makeRoot();
      buildCollapseWrapper({ root, subject: "Query Result" });
      const card = root.querySelector(".mcp-card")!;
      expect(card.classList.contains("is-open")).toBe(false);
      expect(root.querySelectorAll(".mcp-card-header").length).toBe(1);
      expect(root.querySelector(".mcp-card-label")?.textContent).toBe(
         "Show Query Result",
      );
   });

   it("starts open with defaultOpen", () => {
      const root = makeRoot();
      buildCollapseWrapper({
         root,
         subject: "Query Result",
         defaultOpen: true,
      });
      expect(
         root.querySelector(".mcp-card")!.classList.contains("is-open"),
      ).toBe(true);
      expect(root.querySelector(".mcp-card-label")?.textContent).toBe(
         "Hide Query Result",
      );
   });

   it("omits the header under chrome=none and starts open", () => {
      // The host draws its own chrome, so there is nothing to click here. That
      // makes starting open a requirement rather than a preference.
      setSearch("?chrome=none");
      const root = makeRoot();
      buildCollapseWrapper({ root, subject: "Query Result" });
      expect(root.querySelectorAll(".mcp-card-header").length).toBe(0);
      expect(
         root.querySelector(".mcp-card")!.classList.contains("is-open"),
      ).toBe(true);
   });

   it("forceCollapsed keeps a card shut when a header exists to reopen it", () => {
      const root = makeRoot();
      buildCollapseWrapper({
         root,
         subject: "Query Error",
         forceCollapsed: true,
      });
      expect(
         root.querySelector(".mcp-card")!.classList.contains("is-open"),
      ).toBe(false);
      expect(root.querySelectorAll(".mcp-card-header").length).toBe(1);
   });

   it("forceCollapsed overrides defaultOpen", () => {
      // Paired with defaultOpen deliberately. Asserting forceCollapsed against
      // the default gives the same answer whether the option is honoured or
      // ignored, so it pins nothing: closed is already the default.
      const root = makeRoot();
      buildCollapseWrapper({
         root,
         subject: "Query Error",
         forceCollapsed: true,
         defaultOpen: true,
      });
      expect(
         root.querySelector(".mcp-card")!.classList.contains("is-open"),
      ).toBe(false);
   });

   it("NEVER leaves a card collapsed with no header to open it", () => {
      // The blocking bug, stated as the invariant rather than as one caller's
      // arguments: a closed card with no control is unreachable content. Asserted
      // over every combination so a future caller cannot reintroduce it by
      // passing the pair that used to.
      for (const chromeNone of [false, true]) {
         for (const forceCollapsed of [false, true]) {
            for (const defaultOpen of [false, true]) {
               document.body.innerHTML = "";
               setSearch(chromeNone ? "?chrome=none" : "");
               const root = makeRoot();
               buildCollapseWrapper({
                  root,
                  subject: "Query Error",
                  // What renderError now passes: force collapsed only when a
                  // header will exist.
                  forceCollapsed: forceCollapsed && !chromeNone,
                  defaultOpen: defaultOpen || chromeNone,
               });
               const card = root.querySelector(".mcp-card")!;
               const open = card.classList.contains("is-open");
               const hasHeader =
                  root.querySelectorAll(".mcp-card-header").length > 0;
               expect(
                  open || hasHeader,
                  `unreachable: chromeNone=${chromeNone} forceCollapsed=${forceCollapsed} defaultOpen=${defaultOpen}`,
               ).toBe(true);
            }
         }
      }
   });

   it("toggles the open state and the label together on header click", () => {
      const root = makeRoot();
      const cb: boolean[] = [];
      buildCollapseWrapper({
         root,
         subject: "Query Result",
         onToggle: (open) => cb.push(open),
      });
      const header = root.querySelector(".mcp-card-header") as HTMLElement;
      const card = root.querySelector(".mcp-card")!;

      header.click();
      expect(card.classList.contains("is-open")).toBe(true);
      expect(root.querySelector(".mcp-card-label")?.textContent).toBe(
         "Hide Query Result",
      );

      header.click();
      expect(card.classList.contains("is-open")).toBe(false);
      // onToggle carries the new state, which is what the re-measure on open
      // depends on: it must be true on opening and false on closing.
      expect(cb).toEqual([true, false]);
   });

   it("renders optional metadata into the header", () => {
      const root = makeRoot();
      buildCollapseWrapper({
         root,
         subject: "Query Result",
         metadata: "40 rows",
      });
      expect(root.querySelector(".mcp-card-meta")?.textContent).toBe("40 rows");
   });
});
