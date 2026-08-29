// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Result } from "@malloydata/malloy-interfaces";
import { MalloyRenderer } from "@malloydata/render";
import {
   buildCollapseWrapper,
   isChromeSuppressed,
} from "../shared/collapse_wrapper";
import { rehydrate, type ResultMeta } from "../shared/rehydrate";
import { highlightMalloy } from "./highlight_malloy";
import { horizontalScrollbarAllowance } from "./scrollbar_allowance";

// Size reporting is the ext-apps SDK's job, not ours.
//
// This used to postMessage `{ type: "ui-size-change" }` at the host directly.
// That string appears nowhere in @modelcontextprotocol/ext-apps: the protocol
// channel is `ui/notifications/size-changed`, so a spec host discarded it as a
// non-JSON-RPC message, which is exactly the log line mcp_app_init.ts keeps
// visible on purpose. It never showed up as a bug because `App` defaults
// `autoResize` to true and `connect()` installs its own ResizeObserver on
// documentElement and body, so the sizing that worked was always the SDK's.
//
// Measured against a host that honours ONLY `ui/notifications/size-changed` and
// ignores the raw message: a wide table sized to 36px collapsed and 1185px
// opened, identical to a host that accepted both. The eight manual calls, their
// double-rAF and the 420ms and 500ms follow-ups were inert.

/**
 * The card's timers and observer, so they can be cancelled when the card is
 * replaced. Module scope because `renderError` can be called from outside
 * `renderResult` (the payload path in app.ts) and has to be able to stop
 * whatever the previous render left running.
 */
let activeTeardown: (() => void) | null = null;

function stopActiveCard(): void {
   activeTeardown?.();
   activeTeardown = null;
}

/** The collapsed-header subject for a successful result. */
const CARD_SUBJECT = "Query Result";

/**
 * The subject for an error card, deliberately different from CARD_SUBJECT.
 * The two shared one string, so a failure was offered as "Show Query Result".
 */
const ERROR_CARD_SUBJECT = "Query Error";

function buildQueryExpando(query: string): {
   header: HTMLElement;
   body: HTMLElement;
} {
   const header = document.createElement("div");
   header.className = "query-expando-header";

   const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
   svg.setAttribute("width", "14");
   svg.setAttribute("height", "14");
   svg.setAttribute("viewBox", "0 0 24 24");
   svg.setAttribute("fill", "none");
   svg.setAttribute("stroke", "currentColor");
   svg.setAttribute("stroke-width", "2");
   svg.setAttribute("stroke-linecap", "round");
   svg.setAttribute("stroke-linejoin", "round");
   const left = document.createElementNS(
      "http://www.w3.org/2000/svg",
      "polyline",
   );
   left.setAttribute("points", "16 18 22 12 16 6");
   const right = document.createElementNS(
      "http://www.w3.org/2000/svg",
      "polyline",
   );
   right.setAttribute("points", "8 6 2 12 8 18");
   svg.append(left, right);
   header.appendChild(svg);

   const label = document.createElement("span");
   label.textContent = "Malloy query";
   header.appendChild(label);

   const arrow = document.createElement("span");
   arrow.className = "query-expando-arrow";
   arrow.textContent = "▾";
   header.appendChild(arrow);

   const body = document.createElement("div");
   body.className = "query-expando-body";
   const code = document.createElement("div");
   code.className = "query-expando-code";
   code.innerHTML = highlightMalloy(query);
   body.appendChild(code);

   header.addEventListener("click", () => {
      const isOpen = body.classList.toggle("open");
      arrow.classList.toggle("open", isOpen);
      // The max-height transition runs for 400ms; only make the body scrollable
      // once it has finished, or the scrollbar appears mid-animation.
      body.classList.remove("scrollable");
      if (isOpen) {
         // The max-height transition runs 400ms; only make the body scrollable
         // once it has finished, or the scrollbar appears mid-animation.
         setTimeout(() => body.classList.add("scrollable"), 420);
      }
   });

   return { header, body };
}

/**
 * Renders a query envelope.
 *
 * A render failure is shown in the card but NOT reported back to the agent.
 * That is deliberate: Publisher validates render tags server-side and returns
 * the problems as `renderLogErrors` on the envelope, which `malloy_executeQuery`
 * also states in the tool result's text. Reporting them from here too would say
 * the same thing twice, in a channel the server already owns.
 */
export function renderResult(
   root: HTMLElement,
   toolOutput: Record<string, unknown>,
   toolInput: Record<string, unknown> | null,
) {
   root.innerHTML = "";

   const rows = toolOutput.rows as Record<string, unknown>[] | undefined;
   const meta = toolOutput._meta as ResultMeta | undefined;

   if (!rows || !meta?.schema) {
      renderError(root, "Missing result data or schema metadata");
      return;
   }

   let result: Result;
   try {
      result = rehydrate(rows, meta);
   } catch (e) {
      renderError(root, `Rehydration failed: ${e}`);
      return;
   }

   // Agent-driven auto-expand: with `expanded=true` on the tool input the card
   // starts open instead of behind a "Show Query Result" header.
   const { body: collapseBody } = buildCollapseWrapper({
      root,
      subject: CARD_SUBJECT,
      defaultOpen: toolInput?.expanded === true,
      // Re-measure on open, not just on render: see measureAndSize below. Two
      // frames because the body has only just left `display: none` and a
      // measurement taken before layout means nothing.
      onToggle: (open) => {
         if (!open) return;
         requestAnimationFrame(() =>
            requestAnimationFrame(() => measureAndSize()),
         );
      },
   });

   const frame = document.createElement("div");
   frame.className = "result-frame";

   const query = toolInput?.query as string | undefined;
   if (query) {
      const { header, body } = buildQueryExpando(query);
      frame.appendChild(header);
      frame.appendChild(body);
   }

   // A capped or truncated result carries a warning; show it above the result so
   // nobody reads a partial answer as the whole one.
   const warning = toolOutput.warning as string | undefined;
   if (warning) {
      const warningBox = document.createElement("div");
      warningBox.className = "warning-box";
      warningBox.textContent = warning;
      frame.appendChild(warningBox);
   }

   const container = document.createElement("div");
   container.className = "malloy-result";
   container.style.width = "100%";
   container.style.height = "400px";
   frame.appendChild(container);

   collapseBody.appendChild(frame);

   const pollTimers: ReturnType<typeof setTimeout>[] = [];
   let teardown: () => void = () => {};

   let errorReported = false;
   const reportRenderError = (e: unknown, phase: string) => {
      if (errorReported) return;
      errorReported = true;
      // A SolidJS error boundary passes the thrown value directly, which may be
      // an Error, a string, or a wrapper like { error: Error }.
      const actual =
         e instanceof Error
            ? e
            : (e as Record<string, unknown>)?.error instanceof Error
              ? ((e as Record<string, unknown>).error as Error)
              : null;
      // Pass the renderer's own message through verbatim: it is already the most
      // accurate description of what it rejected (e.g. "row level views are too
      // large for the scorecard").
      renderError(
         root,
         `Malloy render error (${phase}): ${actual?.message ?? String(e)}`,
      );
   };

   try {
      const renderer = new MalloyRenderer({
         // Load-bearing, not a preference. MCP App hosts render the widget in a
         // sandboxed iframe under a CSP without 'unsafe-eval', which blocks
         // Vega's default expression compiler (it uses `new Function`). This
         // routes chart specs through vega-interpreter, the CSP-safe path that
         // is already a dependency of @malloydata/render. Publisher's own web UI
         // does not set this because a normal page has no such restriction.
         useVegaInterpreter: true,
         onError: (e) => reportRenderError(e, "component error boundary"),
      });
      const viz = renderer.createViz();
      viz.setResult(result);
      viz.render(container);
   } catch (e) {
      reportRenderError(e, "sync render");
      return;
   }

   // Plugin-level render errors are NOT caught by the SolidJS error boundary:
   // the renderer replaces the failed plugin with an inline
   // <div class="malloy-error-message"> instead.
   const checkForPluginErrors = (): boolean => {
      if (errorReported) return false;
      const errorEls = container.querySelectorAll(".malloy-error-message");
      if (errorEls.length === 0) return false;
      // Deduplicate: the same message often appears once per data row.
      const unique = [
         ...new Set(
            Array.from(errorEls)
               .map((el) => el.textContent?.trim())
               .filter(Boolean) as string[],
         ),
      ];
      reportRenderError(
         new Error(unique.join("; ") || "Unknown plugin render error"),
         "plugin",
      );
      return true;
   };

   // Measure the real rendered height once the Malloy web components settle,
   // then size the container to fit.
   //
   // COUPLED TO @malloydata/render's INTERNAL DOM. If you are bumping Malloy and
   // the card renders clipped, blank, or too tall, this block is the first place
   // to look. It depends on three things the renderer does not promise:
   //   - the class name `.malloy-dashboard` on the element below the render root,
   //   - the class name `.malloy-error-message` for a failed plugin,
   //   - the container > child > grandchild nesting depth walked below.
   // Publisher's SDK reaches for `.malloy-dashboard` too, in
   // sdk/src/components/RenderedResult/RenderedResult.tsx, but there it is CSS,
   // so a rename degrades into a style regression. Here it degrades into a card
   // the user cannot read, which is why this comment exists.
   //
   // Verified against @malloydata/render 0.0.432 by rendering the bundled
   // storefront `business_overview` dashboard: the grandchild does carry
   // `.malloy-dashboard`, and its own offsetHeight is clamped to the container
   // while the content below it is not, which is why the dashboard branch reads
   // the great-grandchild.
   //
   // The arithmetic that turns those measurements into a height is in
   // scrollbar_allowance.ts and IS tested. The DOM assumptions above are not.
   // Measuring is a named function because it has TWO triggers, and for a while
   // it only had one. The card starts collapsed unless the agent passes
   // `expanded=true`, so the first render happens while `.mcp-card-body` is
   // `display: none` and every measurement in the subtree is 0. `rendered > 0`
   // is then false, so nothing is assigned and the observer is never
   // disconnected: the container keeps the literal 400px it was created with.
   //
   // Opening the card cannot fix that through the observer. `setOpen` toggles
   // `is-open` on the WRAPPER, which is this container's ancestor, and a
   // MutationObserver sees the node it observes and its descendants, never its
   // ancestors. Measured: after the render settled while collapsed, zero
   // mutation records reached the observer.
   //
   // In practice the card did still correct itself, because @malloydata/render
   // re-renders when its subtree becomes visible and those mutations DO reach
   // the observer: 22 records within 50ms of the click, height right by 100ms.
   // That is the renderer's internal behaviour rescuing us, not this code
   // working, and it is the same unpromised coupling the block below documents.
   // So opening now re-measures explicitly and the accident is only a backstop.
   const measureAndSize = (): boolean => {
      if (checkForPluginErrors()) {
         teardown();
         return true;
      }

      const child = container.firstElementChild as HTMLElement | null;
      const grandchild = child?.firstElementChild as HTMLElement | null;
      if (!grandchild) return false;

      let rendered = grandchild.scrollHeight || grandchild.offsetHeight || 0;
      const greatGrandchild =
         grandchild.firstElementChild as HTMLElement | null;
      if (
         greatGrandchild &&
         grandchild.classList.contains("malloy-dashboard")
      ) {
         rendered =
            greatGrandchild.scrollHeight || greatGrandchild.offsetHeight || 0;
      }

      if (rendered <= 0) return false;
      // Reserve room for a horizontal scrollbar so a wide table's last row is
      // not cropped behind it (scrollHeight excludes the scrollbar).
      container.style.height = `${rendered + horizontalScrollbarAllowance(container)}px`;
      teardown();
      return true;
   };

   const observer = new MutationObserver(() => {
      pollTimers.push(setTimeout(measureAndSize, 100));
   });

   // Every timer and observer this card owns, torn down together. Previously
   // nothing was: `renderError` clears `root.innerHTML`, so the observer and the
   // polls below went on firing against a detached container for the life of the
   // widget.
   teardown = () => {
      observer.disconnect();
      for (const id of pollTimers) clearTimeout(id);
      pollTimers.length = 0;
   };
   activeTeardown = teardown;

   // The observer stops as soon as it measures a height, which can happen before
   // SolidJS has committed an error plugin's element. Poll a few times after
   // rendering stabilises to catch a late-arriving error element.
   for (const delay of [500, 1000, 2000]) {
      pollTimers.push(setTimeout(() => checkForPluginErrors(), delay));
   }
   observer.observe(container, {
      childList: true,
      subtree: true,
      attributes: true,
   });

   // Measure once now in case the card is already open (`expanded=true`).
   measureAndSize();
}

export function renderError(
   root: HTMLElement,
   message: string,
   query?: string,
) {
   // Clearing root detaches the previous card's container, so anything still
   // observing or polling it has to be stopped first. It was not, and those
   // timers went on measuring a detached node for the life of the widget.
   stopActiveCard();
   root.innerHTML = "";

   // Errors start collapsed so the card stays compact while the agent reads the
   // tool result and recovers. NOT under `chrome=none`: there the header is
   // suppressed entirely, so a collapsed card has no control to open it and the
   // error becomes unreachable. Measured: card 2px tall, the error text present
   // in the DOM at a 0x0 rect, and zero clickable elements, so every failure
   // this widget writes copy for rendered as an empty bordered box. A host that
   // suppressed chrome has already made the compactness call, so the error opens.
   const chromeSuppressed = isChromeSuppressed();
   const { body: collapseBody } = buildCollapseWrapper({
      root,
      // Labelled as an error, not as a result. "Show Query Result" on a card
      // containing an error reads as a result you have to click for.
      subject: ERROR_CARD_SUBJECT,
      forceCollapsed: !chromeSuppressed,
      defaultOpen: chromeSuppressed,
   });

   if (query) {
      const frame = document.createElement("div");
      frame.className = "result-frame";
      const { header, body } = buildQueryExpando(query);
      frame.appendChild(header);
      frame.appendChild(body);

      const errorBox = document.createElement("div");
      errorBox.className = "error-box error-box--in-frame";
      errorBox.textContent = message;
      frame.appendChild(errorBox);

      collapseBody.appendChild(frame);
   } else {
      const box = document.createElement("div");
      box.className = "error-box";
      box.textContent = message;
      collapseBody.appendChild(box);
   }
}
