// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useMediaQuery } from "@mui/material";

// MUI's `sm` breakpoint is 600px.
const NARROW = "(max-width:599.95px)";

/** Whether the viewport is below 600px, live; `noSsr` so the first render already knows. */
export function useNarrowScreen(): boolean {
   return useMediaQuery(NARROW, { noSsr: true });
}

/** The same question answered once, for a view that must not change under someone mid-task. */
export function isNarrowScreen(): boolean {
   return (
      typeof window !== "undefined" &&
      typeof window.matchMedia === "function" &&
      window.matchMedia(NARROW).matches
   );
}
