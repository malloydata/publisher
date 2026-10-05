// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Button, Stack, TextField, Typography } from "@mui/material";
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
   const actions = useRef<HTMLDivElement>(null);
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
            sx={{
               cursor: "text",
               minHeight: 24,
               borderRadius: "2px",
               "&:hover, &:focus-visible": {
                  outline: "1px dashed currentColor",
               },
            }}
         >
            {markdown.trim() ? (
               <Prose variant={variant}>{markdown}</Prose>
            ) : (
               <Typography
                  variant="body2"
                  sx={{ color: theme.tileTitle, opacity: 0.6 }}
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
      <Stack spacing={1}>
         <TextField
            multiline
            autoFocus
            minRows={3}
            maxRows={14}
            fullWidth
            value={draft.text}
            placeholder={placeholder}
            error={problem !== undefined}
            helperText={problem}
            inputRef={placeCaret}
            inputProps={{ "aria-label": "Markdown" }}
            onChange={(event) =>
               patch((next) => {
                  next.text = event.target.value;
               })
            }
            onKeyDown={(event) => {
               if (event.key === "Escape") finish();
               else if (
                  event.key === "Enter" &&
                  (event.metaKey || event.ctrlKey)
               ) {
                  event.preventDefault();
                  finish();
               }
            }}
            // Moving to Done or Cancel is not leaving the edit.
            onBlur={(event) => {
               if (
                  event.relatedTarget instanceof Node &&
                  actions.current?.contains(event.relatedTarget)
               )
                  return;
               close();
            }}
         />
         <Stack ref={actions} direction="row" spacing={1}>
            <Button
               size="small"
               variant="outlined"
               disabled={problem !== undefined}
               onClick={finish}
            >
               Done
            </Button>
            <Button
               size="small"
               // Keeps focus in the field, whose blur would otherwise commit the draft Cancel is dropping.
               onMouseDown={(event) => event.preventDefault()}
               onClick={() => {
                  discard();
                  setEditing(false);
               }}
            >
               Cancel
            </Button>
         </Stack>
      </Stack>
   );
}
