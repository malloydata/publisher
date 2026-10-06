// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, InputBase } from "@mui/material";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { editableSx } from "./inlineEdit";
import { useRef, useState, type KeyboardEvent } from "react";
import { useReportOpenDraft } from "./openDraft";

/**
 * A single line of text that is edited where it is read: a click opens a field,
 * Enter or leaving it commits, Escape puts the old text back.
 *
 * The display is a real button, so it is a tab stop that Enter and Space open,
 * and the drag sensor refuses a press that starts on one: opening an editor can
 * never be the start of a move.
 */
export function InlineText({
   value,
   placeholder,
   ariaLabel,
   onCommit,
   faintWhenEmpty = false,
}: {
   value: string;
   /** Shown in place of an empty value. */
   placeholder: string;
   /** Names the field while it is open. */
   ariaLabel: string;
   /** Called once per edit, with the new text, only when it differs from `value`. */
   onCommit: (next: string) => void;
   /** Hides an empty value until its tile is hovered or focused. */
   faintWhenEmpty?: boolean;
}) {
   const { theme } = usePublisherTheme();
   const [draft, setDraft] = useState<string | undefined>(undefined);
   // A cancelled edit unmounts the field, and a browser may report that as a blur.
   const cancelled = useRef(false);

   const commit = () => {
      if (cancelled.current || draft === undefined) return;
      setDraft(undefined);
      if (draft !== value) onCommit(draft);
   };
   useReportOpenDraft(draft !== undefined && draft !== value, () => {
      commit();
      return true;
   });
   const open = () => {
      cancelled.current = false;
      setDraft(value);
   };

   if (draft === undefined) {
      const empty = value === "";
      const faint = empty && faintWhenEmpty;
      return (
         <Box
            component="button"
            type="button"
            className={faint ? "builder-affordance" : undefined}
            onClick={open}
            onKeyDown={(event: KeyboardEvent) => {
               if (event.key !== "Enter" && event.key !== " ") return;
               event.preventDefault();
               open();
            }}
            sx={[
               {
                  all: "unset",
                  boxSizing: "border-box",
                  font: "inherit",
                  color: "inherit",
                  display: "inline-block",
                  maxWidth: "100%",
                  // An empty slot says what it is for, faintly; a tile's
                  // own slot shows only while that tile is hovered.
                  ...(empty && {
                     fontStyle: "italic",
                     opacity: faint ? 0 : 0.6,
                  }),
               },
               editableSx(theme),
            ]}
         >
            {empty ? placeholder : value}
         </Box>
      );
   }
   return (
      <InputBase
         autoFocus
         fullWidth
         value={draft}
         placeholder={placeholder}
         inputProps={{ "aria-label": ariaLabel }}
         onFocus={(event) => event.target.select()}
         onChange={(event) => setDraft(event.target.value)}
         onKeyDown={(event) => {
            if (event.key === "Enter" && !event.nativeEvent.isComposing)
               commit();
            else if (event.key === "Escape") {
               cancelled.current = true;
               setDraft(undefined);
            }
         }}
         onBlur={commit}
         // The field takes its type from the line it replaces.
         sx={{ font: "inherit", color: "inherit", p: 0, "& input": { p: 0 } }}
      />
   );
}
