// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, IconButton, Tooltip } from "@mui/material";
import type { ReactNode } from "react";
import { roles } from "./theme";

export interface SegmentIconOption<T extends string> {
   value: T;
   icon: ReactNode;
   /** The accessible name AND the tooltip — kept equal (ux-design-guide §4). */
   label: string;
   /**
    * Present but not selectable. For a representation this thing HAS but cannot show yet
    * (or at all) — the option keeps its place so the switch does not change width as the
    * data behind it arrives, which is the layout shift that omitting it causes.
    */
   disabled?: boolean;
   /**
    * Why it is disabled, as the tooltip, replacing `label`. A disabled control with no
    * explanation is the failure mode this exists to avoid: the reader can see the option
    * and cannot tell whether it is broken or merely not applicable here.
    */
   disabledReason?: string;
}

/**
 * An action icon's button: a switch option's (active when pressed) and an action
 * beside a switch (ActionIconButton) are one look, so a row of them reads as one set.
 * The pressed one sits in a faint fill so the active layer reads at a glance; the
 * rest are quiet.
 */
export function actionIconSx(active: boolean) {
   return {
      p: "3px",
      borderRadius: 1,
      color: active ? "text.primary" : "text.secondary",
      bgcolor: active ? roles.background.strong : "transparent",
      "&:hover": {
         bgcolor: active ? roles.background.strong : roles.background.muted,
         color: "text.primary",
      },
      "&.Mui-disabled": { color: "text.disabled" },
   } as const;
}

/**
 * A "which layer of this thing am I looking at" switch, drawn as **action
 * icons** rather than labelled chips — the app's one switch of record for
 * choosing a representation of something already named beside it: on an
 * open model row (its layers — Result, Code, Givens, Materialization — through
 * RowLayerSwitch, at the row's line) and over a result. (A whole file's views are not a switch but the first items of
 * its ⋯ — `viewActions`.) Any number of options, one pressed. The caller sizes
 * its own glyphs — 14 on a row.
 *
 * <p>Why icons, not labelled chips: these switches choose a REPRESENTATION of
 * something already named next to them, so words would restate the noun rather
 * than add to it — "Model | Code" under a heading that says `auto_recalls`.
 * The glyphs are
 * the app's own (ContentTypeIcon's mark for each kind of file, the code
 * brackets), so the switch reads as a lens over the named thing instead of
 * chrome beside it.
 *
 * <p>The label is not lost, only moved: every option's `label` is BOTH its
 * tooltip and its accessible name (ux-design-guide §4), so pointer users get
 * the word on hover and screen readers get it unconditionally — and the option
 * stays addressable by name, which is what keeps an icon switch testable. A
 * disabled option keeps its place and swaps both for its `disabledReason`.
 *
 * <p>Group semantics: `role="group"` named by the caller, each option a
 * `role="button"` with `aria-pressed`, so it is one control to a screen reader.
 */
export function SegmentIcons<T extends string>({
   value,
   options,
   onChange,
   "aria-label": ariaLabel,
}: {
   value: T;
   options: ReadonlyArray<SegmentIconOption<T>>;
   onChange: (value: T) => void;
   "aria-label": string;
}) {
   return (
      <Box
         role="group"
         aria-label={ariaLabel}
         sx={{ display: "inline-flex", gap: "2px" }}
      >
         {options.map((option) => {
            const active = option.value === value;
            const disabled = option.disabled === true;
            return (
               // `span` wrapper: MUI's Tooltip needs a mountable child that fires pointer
               // events, and a disabled IconButton fires none — without it the explanation
               // is unreachable on exactly the options that need one.
               <Tooltip
                  key={option.value}
                  title={
                     disabled
                        ? (option.disabledReason ?? option.label)
                        : option.label
                  }
               >
                  <span style={{ display: "inline-flex" }}>
                     <IconButton
                        size="small"
                        role="button"
                        aria-pressed={active}
                        // Matches the tooltip: a screen-reader user on a disabled option
                        // otherwise hears only its name and no reason.
                        aria-label={
                           disabled
                              ? (option.disabledReason ?? option.label)
                              : option.label
                        }
                        disabled={disabled}
                        onClick={() => onChange(option.value)}
                        sx={actionIconSx(active)}
                     >
                        {option.icon}
                     </IconButton>
                  </span>
               </Tooltip>
            );
         })}
      </Box>
   );
}
