// Collapsible chrome around a widget card: the chevron, the "Show / Hide X"
// header, the border, and the toggle behaviour.
//
// Every style here is applied INLINE, and that is load-bearing rather than a
// style preference. Some hosts (notably Claude Desktop) load the widget iframe
// in a sandbox that drops the <style> block, which would leave the chrome
// unstyled and the body permanently visible. `element.style.*` survives it.

import { FONT_MONO, FONT_SANS, tokens } from "./tokens";

export interface CollapseWrapperOptions {
   root: HTMLElement;
   /** Composed into the label: "Show <subject>" / "Hide <subject>". */
   subject: string;
   /** CSS display value when open. Pass "flex" if the body needs flex layout. */
   bodyDisplay?: string;
   /** Optional right-aligned mono text after the label, e.g. a row count. */
   metadata?: string;
   /** Called after the body's display flips, for re-measuring the iframe. */
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
   Object.assign(wrapper.style, {
      border: `1px solid ${tokens.border}`,
      borderRadius: "8px",
      overflow: "hidden",
      background: tokens.background,
   } as Partial<CSSStyleDeclaration>);

   const header = document.createElement("div");
   Object.assign(header.style, {
      display: "flex",
      alignItems: "center",
      gap: "8px",
      padding: "8px 12px",
      cursor: "pointer",
      userSelect: "none",
      background: tokens.background,
      borderBottom: startsOpen ? `1px solid ${tokens.border}` : "none",
   } as Partial<CSSStyleDeclaration>);

   const chevron = document.createElement("span");
   Object.assign(chevron.style, {
      display: "inline-block",
      fontFamily: FONT_SANS,
      fontSize: "9px",
      color: tokens.textFaint,
      width: "10px",
      transform: startsOpen ? "rotate(90deg)" : "rotate(0deg)",
      transition: "transform .12s",
   } as Partial<CSSStyleDeclaration>);
   chevron.textContent = "▸";
   header.appendChild(chevron);

   const labelText = (open: boolean) => `${open ? "Hide" : "Show"} ${subject}`;

   const label = document.createElement("span");
   Object.assign(label.style, {
      fontFamily: FONT_SANS,
      fontSize: "12px",
      fontWeight: "500",
      color: tokens.textMuted,
   } as Partial<CSSStyleDeclaration>);
   label.textContent = labelText(startsOpen);
   header.appendChild(label);

   if (metadata) {
      const spacer = document.createElement("span");
      spacer.style.flex = "1";
      header.appendChild(spacer);

      const meta = document.createElement("span");
      Object.assign(meta.style, {
         fontFamily: FONT_MONO,
         fontSize: "11px",
         color: tokens.textFaint,
      } as Partial<CSSStyleDeclaration>);
      meta.textContent = metadata;
      header.appendChild(meta);
   }

   if (!hostChromeSuppressed) {
      wrapper.appendChild(header);
   }

   const body = document.createElement("div");
   body.style.display = startsOpen ? bodyDisplay : "none";
   wrapper.appendChild(body);
   root.appendChild(wrapper);

   const setOpen = (open: boolean) => {
      body.style.display = open ? bodyDisplay : "none";
      header.style.borderBottom = open ? `1px solid ${tokens.border}` : "none";
      chevron.style.transform = open ? "rotate(90deg)" : "rotate(0deg)";
      label.textContent = labelText(open);
      onToggle?.(open);
   };

   header.addEventListener("click", () => {
      setOpen(body.style.display === "none");
   });

   return { wrapper, body, setOpen };
}
