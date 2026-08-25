// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Collapsible chrome around a widget card: the chevron, the "Show / Hide X"
// header, the border, and the toggle behaviour.
//
// Styled from the stylesheet in execute-query.html like everything else in the
// card. This used to build all of it with inline `element.style.*`, on the
// stated grounds that some hosts sandbox the widget iframe in a way that drops
// the <style> block. That was tested in Claude Desktop over an mcp-remote bridge
// and the block survives, so the claim was removed and the styles moved back
// where the rest of the card's styles live.

export interface CollapseWrapperOptions {
   root: HTMLElement;
   /** Composed into the label: "Show <subject>" / "Hide <subject>". */
   subject: string;
   /** Pass "flex" if the body's children rely on flex layout. */
   bodyDisplay?: "block" | "flex";
   /** Optional right-aligned mono text after the label, e.g. a row count. */
   metadata?: string;
   /** Called after the body opens or closes, for re-measuring the iframe. */
   onToggle?: (open: boolean) => void;
   /**
    * Start expanded. Used when the agent passes `expanded=true` on the tool
    * input. A host that suppresses chrome via `chrome=none` always starts open.
    */
   defaultOpen?: boolean;
   /**
    * Force collapsed even under `chrome=none` / `defaultOpen`. Used for error
    * cards: the agent recovers from the tool result, not from the iframe, so an
    * error stays compact instead of expanding into the conversation.
    */
   forceCollapsed?: boolean;
}

export interface CollapseWrapper {
   wrapper: HTMLElement;
   body: HTMLElement;
   setOpen: (open: boolean) => void;
}

export function isChromeSuppressed(): boolean {
   try {
      return (
         new URLSearchParams(window.location.search).get("chrome") === "none"
      );
   } catch {
      return false;
   }
}

export function buildCollapseWrapper(
   opts: CollapseWrapperOptions,
): CollapseWrapper {
   const {
      root,
      subject,
      bodyDisplay = "block",
      metadata,
      onToggle,
      defaultOpen = false,
      forceCollapsed = false,
   } = opts;
   const hostChromeSuppressed = isChromeSuppressed();
   const startsOpen = forceCollapsed
      ? false
      : hostChromeSuppressed || defaultOpen;

   const wrapper = document.createElement("div");
   wrapper.className = "mcp-card";

   const header = document.createElement("div");
   header.className = "mcp-card-header";

   const chevron = document.createElement("span");
   chevron.className = "mcp-card-chevron";
   chevron.textContent = "▸";
   header.appendChild(chevron);

   const labelText = (open: boolean) => `${open ? "Hide" : "Show"} ${subject}`;

   const label = document.createElement("span");
   label.className = "mcp-card-label";
   label.textContent = labelText(startsOpen);
   header.appendChild(label);

   if (metadata) {
      const meta = document.createElement("span");
      meta.className = "mcp-card-meta";
      meta.textContent = metadata;
      header.appendChild(meta);
   }

   if (!hostChromeSuppressed) {
      wrapper.appendChild(header);
   }

   const body = document.createElement("div");
   body.className =
      bodyDisplay === "flex"
         ? "mcp-card-body mcp-card-body--flex"
         : "mcp-card-body";
   wrapper.appendChild(body);
   // One state class drives the chevron, the header's border and the body's
   // visibility together, so they cannot get out of step the way three separate
   // inline assignments could.
   wrapper.classList.toggle("is-open", startsOpen);
   root.appendChild(wrapper);

   const setOpen = (open: boolean) => {
      wrapper.classList.toggle("is-open", open);
      label.textContent = labelText(open);
      onToggle?.(open);
   };

   header.addEventListener("click", () =>
      setOpen(!wrapper.classList.contains("is-open")),
   );

   return { wrapper, body, setOpen };
}
