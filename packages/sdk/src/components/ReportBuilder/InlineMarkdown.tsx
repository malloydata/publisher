// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, InputBase, Typography } from "@mui/material";
import { editableSx } from "./inlineEdit";
import { PROSE_MAX_WIDTH, TYPE } from "../controlTokens";
import { MOD } from "./useBuilderShortcuts";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { Prose, type ProseVariant } from "../Prose";
import { useReportOpenDraft } from "./openDraft";
import { markdownProblem } from "./spliceDocument";
import { useDraft } from "./useDraft";

/**
 * Markdown that is edited where it is read: it draws as prose, a click on it
 * opens a textarea, and what was typed commits once, when the field closes.
 *
 * Escape and Cmd or Ctrl+Enter are Done, which holds on a draft the writer
 * would refuse and says why; leaving the field commits, so the closed prose
 * keeps saying what Save will refuse. Links stay inert: a click on one is not
 * a way out of the builder, and not an edit either.
 */
export function InlineMarkdown({
   markdown,
   placeholder,
   onCommit,
   variant = "document",
}: {
   markdown: string;
   /** Shown in place of empty prose. */
   placeholder: string;
   /** Called once per edit, with the new text, only when it differs from `markdown`. */
   onCommit: (next: string) => void;
   variant?: ProseVariant;
}) {
   const { theme } = usePublisherTheme();
   const [editing, setEditing] = useState(false);
   // One object per value, or the draft would reset on every render while open.
   const value = useMemo(
      () => (editing ? { text: markdown } : undefined),
      [editing, markdown],
   );
   const { draft, patch, close, discard } = useDraft<{ text: string }>(
      value,
      editing,
      (next) => onCommit(next.text),
      () => setEditing(false),
   );
   const draftDirty = editing && draft !== undefined && draft.text !== markdown;
   const refused =
      draft !== undefined && draft.text !== markdown
         ? markdownProblem(draft.text)
         : undefined;
   useReportOpenDraft(draftDirty, () => {
      if (refused !== undefined) return false;
      close();
      return true;
   });
   const caretPlaced = useRef(false);
   useEffect(() => {
      if (!editing) caretPlaced.current = false;
   }, [editing]);
   const placeCaret = useCallback((field: HTMLTextAreaElement | null) => {
      if (!field || caretPlaced.current) return;
      caretPlaced.current = true;
      field.setSelectionRange(field.value.length, field.value.length);
   }, []);

   if (!editing || draft === undefined) {
      // A blur commits an invalid draft, so the closed prose keeps saying what the writer will refuse at Save.
      const closedProblem = markdown.trim()
         ? markdownProblem(markdown)
         : undefined;
      return (
         <Box
            role="button"
            tabIndex={0}
            onClick={(event) => {
               if ((event.target as Element).closest("a"))
                  event.preventDefault();
               else setEditing(true);
            }}
            onKeyDown={(event) => {
               if (event.target !== event.currentTarget) return;
               if (event.key !== "Enter" && event.key !== " ") return;
               event.preventDefault();
               setEditing(true);
            }}
            sx={[{ minHeight: 24 }, editableSx(theme, "prose")]}
         >
            {markdown.trim() ? (
               <Prose variant={variant}>{markdown}</Prose>
            ) : (
               <Typography
                  variant="body2"
                  sx={{
                     color: theme.tileTitle,
                     opacity: 0.6,
                     fontStyle: "italic",
                  }}
               >
                  {placeholder}
               </Typography>
            )}
            {closedProblem && (
               <Typography variant="caption" color="error" role="alert">
                  {closedProblem}
               </Typography>
            )}
         </Box>
      );
   }

   // An untouched text is left as it was, so only a changed draft is held to the writer's rules.
   const problem =
      draft.text !== markdown ? markdownProblem(draft.text) : undefined;
   const finish = () => {
      if (problem === undefined) close();
   };
   return (
      // The text in place, in the page's own prose type: no box inside the
      // tile, no buttons under it, so the tile keeps its size as it opens. No
      // ring of its own either: it opens only on something already selected,
      // and a second ring inside the selection's read as a box in a box.
      // Leaving it or ⌘/Ctrl+Enter keeps the edit; Escape drops it, as a
      // title's Escape does.
      <Box
         sx={{
            position: "relative",
            mx: "-10px",
            px: "10px",
            my: "-6px",
            py: "6px",
         }}
      >
         <InputBase
            multiline
            autoFocus
            minRows={2}
            fullWidth
            value={draft.text}
            placeholder={placeholder}
            error={problem !== undefined}
            inputRef={placeCaret}
            inputProps={{
               "aria-label": "Markdown",
               ...(problem !== undefined ? { "aria-invalid": true } : {}),
            }}
            onChange={(event) =>
               patch((next) => {
                  next.text = event.target.value;
               })
            }
            onKeyDown={(event) => {
               if (event.key === "Escape") {
                  event.preventDefault();
                  discard();
                  setEditing(false);
               } else if (
                  event.key === "Enter" &&
                  (event.metaKey || event.ctrlKey)
               ) {
                  event.preventDefault();
                  finish();
               }
            }}
            onBlur={() => close()}
            sx={{
               p: 0,
               ...TYPE.prose,
               color: "inherit",
               maxWidth: PROSE_MAX_WIDTH,
               "& textarea": { p: 0 },
            }}
         />
         {problem !== undefined && (
            <Typography variant="caption" color="error" role="alert">
               {problem}
            </Typography>
         )}
         <Typography
            variant="caption"
            aria-hidden
            sx={{
               display: "block",
               textAlign: "right",
               mt: 0.5,
               color: "text.secondary",
               pointerEvents: "none",
            }}
         >
            Markdown · {MOD}↩ done · Esc cancels
         </Typography>
      </Box>
   );
}
