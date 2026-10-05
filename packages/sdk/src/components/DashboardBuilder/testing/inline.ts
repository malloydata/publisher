// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, screen, within } from "@testing-library/react";

/** Edit a line of text where it is shown: click it, type, and commit (Enter) or leave another way. */
export function editInline(
   shown: string,
   label: string,
   next: string,
   key: "Enter" | "Escape" = "Enter",
   scope?: HTMLElement,
) {
   fireEvent.click((scope ? within(scope) : screen).getByText(shown));
   const field = screen.getByLabelText(label);
   fireEvent.change(field, { target: { value: next } });
   fireEvent.keyDown(field, { key });
}

/** Close an open tile menu: Escape inside it, now that no field in it holds focus. */
export const closeMenu = () =>
   fireEvent.keyDown(
      screen.getByRole("button", { name: "Remove tile", hidden: true }),
      { key: "Escape" },
   );
