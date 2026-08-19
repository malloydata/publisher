import type { Result } from "@malloydata/malloy-interfaces";
import { MalloyRenderer } from "@malloydata/render";
import { buildCollapseWrapper } from "../shared/collapse_wrapper";
import { rehydrate, type ResultMeta } from "../shared/rehydrate";
import { tokens } from "../shared/tokens";
import { highlightMalloy } from "./highlight_malloy";

// A wide table inside the Malloy render scrolls horizontally, and its scrollbar
// is laid out *inside* the element's box (border-box is global here).
// `scrollHeight` excludes that scrollbar, so sizing the container to
// scrollHeight alone clips the final row behind it. Walk the subtree for the
// element that overflows horizontally and return the vertical space its
// scrollbar occupies. Returns 0 for overlay scrollbars (macOS), which consume no
// layout, so there is nothing to add.
function horizontalScrollbarAllowance(el: HTMLElement): number {
   let allowance = 0;
   const nodes = [el, ...el.querySelectorAll<HTMLElement>("*")];
   for (const node of nodes) {
      if (node.scrollWidth > node.clientWidth + 1) {
         allowance = Math.max(allowance, node.offsetHeight - node.clientHeight);
      }
   }
   return allowance;
}

function notifySize() {
   // Double rAF: the browser has to reflow after a display change before the
   // measurement means anything. A single rAF can fire before layout settles.
   requestAnimationFrame(() => {
      requestAnimationFrame(() => {
         const root = document.getElementById("root");
         const height = root ? root.offsetHeight : document.body.offsetHeight;
         window.parent.postMessage(
            { type: "ui-size-change", payload: { height } },
            "*",
         );
      });
   });
}

/** The collapsed-header subject, shared by the result and error cards. */
const CARD_SUBJECT = "Query Result";

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
         setTimeout(() => {
            body.classList.add("scrollable");
            notifySize();
         }, 420);
      } else {
         setTimeout(notifySize, 420);
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
      onToggle: () => notifySize(),
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
   const observer = new MutationObserver(() => {
      setTimeout(() => {
         if (checkForPluginErrors()) {
            observer.disconnect();
            notifySize();
            return;
         }

         const child = container.firstElementChild as HTMLElement | null;
         const grandchild = child?.firstElementChild as HTMLElement | null;
         if (!grandchild) return;

         let rendered = grandchild.scrollHeight || grandchild.offsetHeight || 0;
         const greatGrandchild =
            grandchild.firstElementChild as HTMLElement | null;
         if (
            greatGrandchild &&
            grandchild.classList.contains("malloy-dashboard")
         ) {
            rendered =
               greatGrandchild.scrollHeight ||
               greatGrandchild.offsetHeight ||
               0;
         }

         if (rendered > 0) {
            // Reserve room for a horizontal scrollbar so a wide table's last row
            // is not cropped behind it (scrollHeight excludes the scrollbar).
            container.style.height = `${rendered + horizontalScrollbarAllowance(container)}px`;
            observer.disconnect();
            notifySize();
         }
      }, 100);
   });

   // The observer disconnects as soon as it measures a height, which can happen
   // before SolidJS has committed an error plugin's element. Poll a few times
   // after rendering stabilises to catch a late-arriving error element.
   [500, 1000, 2000].forEach((delay) => {
      setTimeout(() => checkForPluginErrors(), delay);
   });
   observer.observe(container, {
      childList: true,
      subtree: true,
      attributes: true,
   });

   notifySize();
   setTimeout(notifySize, 500);
}

export function renderError(
   root: HTMLElement,
   message: string,
   query?: string,
) {
   root.innerHTML = "";

   const { body: collapseBody } = buildCollapseWrapper({
      root,
      subject: CARD_SUBJECT,
      // Errors always start collapsed, even under `chrome=none`, so the card
      // stays compact while the agent reads the tool result and recovers.
      forceCollapsed: true,
      onToggle: () => notifySize(),
   });

   if (query) {
      const frame = document.createElement("div");
      frame.className = "result-frame";
      const { header, body } = buildQueryExpando(query);
      frame.appendChild(header);
      frame.appendChild(body);

      const errorBox = document.createElement("div");
      errorBox.className = "error-box";
      errorBox.style.borderRadius = "0";
      errorBox.style.border = "none";
      errorBox.style.borderTop = `1px solid ${tokens.border}`;
      errorBox.textContent = message;
      frame.appendChild(errorBox);

      collapseBody.appendChild(frame);
   } else {
      const box = document.createElement("div");
      box.className = "error-box";
      box.textContent = message;
      collapseBody.appendChild(box);
   }

   notifySize();
}
