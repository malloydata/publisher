// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Button semantics and a focus ring for a control rendered as a plain `Box`.
 *
 * A `Box` with only `onClick` is not in the tab order, is not exposed to
 * assistive tech as a control, and gets no focus indicator from MUI — and a
 * `Tooltip` around it surfaces its label on hover only, since a non-focusable
 * child never receives keyboard focus. Anywhere we reach for a clickable `Box`
 * instead of a `Button`/`ListItemButton` we owe it these, so they are defined
 * once here rather than hand-rolled per site.
 *
 * Prefer a real MUI button component when the styling allows it; use this when
 * it does not.
 */
import type { KeyboardEvent, MouseEvent, SyntheticEvent } from "react";

/**
 * `role`, `tabIndex`, accessible name, and Enter/Space activation for a
 * clickable `Box`. Pair with {@link focusRingSx} so the focus stop is visible.
 *
 * `onActivate` receives the originating event so a control that anchors a popup
 * can read `currentTarget`; handlers that don't need it can ignore it.
 *
 * Pass `undefined` for `label` ONLY when the element's own text content names it
 * — a `role="button"` falls back to its contents, so an explicit label there
 * would just be a second, drift-prone copy of the text on screen. A control
 * whose content is an icon, or arbitrary nodes that may not include text, needs
 * the explicit label.
 */
export function clickableBoxProps(
   label: string | undefined,
   onActivate?: (e: SyntheticEvent<HTMLElement>) => void,
) {
   return {
      role: "button",
      tabIndex: 0,
      ...(label === undefined ? {} : { "aria-label": label }),
      onClick: (e: MouseEvent<HTMLElement>) => onActivate?.(e),
      onKeyDown: (e: KeyboardEvent<HTMLElement>) => {
         // Enter/Space are what a real <button> would honor; preventDefault
         // stops Space from also scrolling the page.
         if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onActivate?.(e);
         }
      },
   } as const;
}

/** Focus ring for a clickable `Box` — MUI draws none, so it needs one explicitly. */
export const focusRingSx = {
   "&:focus-visible": {
      outline: "2px solid",
      outlineColor: "primary.main",
      outlineOffset: "-2px",
   },
} as const;
