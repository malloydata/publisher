// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import type { SxProps, Theme } from "@mui/material/styles";
import { forwardRef, type ReactNode } from "react";
import { roles, fontSizes, fontWeights } from "./theme";
import { MONO_FONT_FAMILY, SANS_FONT_FAMILY } from "./theme";

/**
 * The one status/tag pill: fully rounded, tinted, borderless (the design
 * system has no outlines). Two voices, chosen by what the text IS:
 *
 * - `state` — a human state word ("Indexed", "In scope", "Materialized",
 *   "Published"): sans, sentence case supplied by the caller.
 * - `token` — a machine token that exists verbatim in code or config
 *   (`join_one`, `line_chart`): mono, rendered exactly as written.
 *
 * Every status the app shows is drawn by this component — a list row's
 * annotation and a page header's state alike — so a "Ready" reads the same
 * wherever it sits, at ONE size: a header's state is the same pill as a row's,
 * centered in the header's 28px row, so two states side by side never differ
 * in scale. A host's own states (`renderSourceStatus`, `renderFieldStatus`)
 * read best drawn with it too.
 */
/**
 * A pill reports a state, and there are four a reader acts on differently. The
 * first two are the labels: `neutral` names a thing, `positive` says it is good.
 *
 * `warning` and `negative` are for a state that is degraded — data that is out
 * of date, or something that failed outright — which the reader needs to see
 * WITHOUT opening the row. Reach for them only where the state is genuinely degraded — a join
 * relationship (`join_one`) is not a warning, so it takes no warm tone.
 */
/** `info`: work under way, with its spinner (`busy`). */
export type StatusPillTone =
   | "positive"
   | "neutral"
   | "warning"
   | "negative"
   | "quiet"
   | "info";

/**
 * Where the pill sits: a row's annotation (`inline`) or a page or panel header's state
 * (`header`). The same pill either way; the size names the place, not a scale.
 */
export type StatusPillSize = "inline" | "header";

const TONES: Record<StatusPillTone, { bg?: string; fg?: string }> = {
   positive: { bg: roles.accent.sage.tint, fg: roles.accent.sage.onTint },
   neutral: { bg: roles.background.strong, fg: roles.text.secondary },
   warning: { bg: roles.warning.tint, fg: roles.warning.onTint },
   negative: { bg: roles.error.tint, fg: roles.error.onTint },
   info: { bg: roles.info.tint, fg: roles.info.onTint },
   // A descriptor, not a state: a field's type, its "inherited", its display
   // tags. A step lighter than `neutral` so the states beside it (Indexed,
   // Given, Restricted) read first. Secondary text, not tertiary: tertiary on
   // this fill is 4.34:1 in light, under AA for micro text.
   quiet: { bg: roles.background.muted, fg: roles.text.secondary },
};

interface StatusPillProps {
   tone?: StatusPillTone;
   voice?: "state" | "token";
   size?: StatusPillSize;
   children: ReactNode;
   sx?: SxProps<Theme>;
   /**
    * Accessibility passthrough to the rendered span. Declared rather than left
    * to `...rest` because only one of the two was actually being type-checked:
    * TypeScript skips JSX attributes containing a hyphen, so `aria-label` was
    * always accepted while `role` was rejected — and an `aria-label` without a
    * role is exactly the combination screen readers ignore.
    */
   role?: string;
   "aria-label"?: string;
   /** A tab stop, for a pill whose tooltip must reach a keyboard (guide §7). */
   tabIndex?: number;
   /** Work in flight: a spinner before the label. */
   busy?: boolean;
}

export const StatusPill = forwardRef<HTMLSpanElement, StatusPillProps>(
   function StatusPill(
      {
         tone = "neutral",
         voice = "state",
         size = "inline",
         busy = false,
         children,
         sx,
         ...rest
      },
      ref,
   ) {
      const { bg, fg } = TONES[tone];
      const fontSize = fontSizes.micro;
      return (
         <Box
            ref={ref}
            component="span"
            // A hook for a container that tints on hover (a model row) to lift a
            // quiet pill a step, so it never sinks into the same grey.
            data-pill-tone={tone}
            // Where it sits (a row or a header), for a container or a test to select on;
            // it does not change the pill.
            data-pill-placement={size}
            {...rest}
            sx={[
               {
                  display: "inline-flex",
                  alignItems: "center",
                  gap: busy ? "4px" : 0,
                  padding: "2px 8px",
                  borderRadius: "999px",
                  bgcolor: bg,
                  color: fg,
                  flexShrink: 0,
                  whiteSpace: "nowrap",
                  ...(voice === "token"
                     ? { fontFamily: MONO_FONT_FAMILY, fontSize }
                     : {
                          fontFamily: SANS_FONT_FAMILY,
                          fontSize,
                          fontWeight: fontWeights.strong,
                       }),
               },
               ...(Array.isArray(sx) ? sx : [sx]),
            ]}
         >
            {busy && (
               <CircularProgress
                  size={9}
                  thickness={5}
                  sx={{ color: "info.main", flexShrink: 0 }}
                  aria-hidden
               />
            )}
            {children}
         </Box>
      );
   },
);
