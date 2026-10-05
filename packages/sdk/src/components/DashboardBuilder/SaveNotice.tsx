// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Button } from "@mui/material";
import { DiffDialog } from "./DiffDialog";
import type { LastSave } from "./useDocumentEditor";

export interface SaveNoticeProps {
   /** The save the notice offers to take back; absent once that offer is withdrawn. */
   lastSave?: LastSave & { removedComments?: string[] };
   /** What the document's parts are called, singular: "tile" or "cell". */
   unit: string;
   /** How many parts the document had before the save and after it. */
   moved: { before: number; after: number } | undefined;
   canUndoSave: boolean;
   /** An Undo save just landed and the offer is gone. */
   undone: boolean;
   /** The read-only viewer of the saved change is open. */
   viewing: boolean;
   onView: (open: boolean) => void;
   onUndoSave: () => Promise<void> | void;
}

const plural = (count: number, unit: string) =>
   `${count} ${unit}${count === 1 ? "" : "s"}`;

/** What the save did to the file, as facts the author can check before moving on. */
function facts(
   { lastSave, unit, moved }: SaveNoticeProps,
   comments: string[],
): string[] {
   if (!lastSave) return [];
   const lines: string[] = [];
   const change = moved ? moved.after - moved.before : 0;
   if (change === 1) lines.push(`Added a ${unit}`);
   else if (change > 1) lines.push(`Added ${plural(change, unit)}`);
   else if (change < 0) lines.push(`Removed ${plural(-change, unit)}`);
   else if (lastSave.structural) lines.push(`Changed the ${unit}s in the file`);
   else lines.push("Saved your edits");
   if (comments.length > 0)
      lines.push(
         `Removed ${plural(comments.length, "comment")} with their ${unit}s`,
      );
   if (lastSave.clearsHistory) lines.push("Undo history was cleared");
   return lines;
}

/**
 * What a save just did, with the way back: View change shows the file's diff
 * read-only, Undo save writes the file back as it was. Stays until an edit
 * withdraws the offer or the next save replaces it; nothing else hides it.
 */
export function SaveNotice(props: SaveNoticeProps) {
   const { lastSave, canUndoSave, undone, viewing, onView, onUndoSave } = props;
   if (undone)
      return (
         <Alert severity="info" role="status">
            Save undone. Your edits are back and unsaved.
         </Alert>
      );
   if (!lastSave) return null;
   const comments = lastSave.removedComments ?? [];
   return (
      <>
         <Alert
            severity="success"
            role="status"
            action={
               <>
                  <Button
                     color="inherit"
                     size="small"
                     onClick={() => onView(true)}
                  >
                     View change
                  </Button>
                  <Button
                     color="inherit"
                     size="small"
                     disabled={!canUndoSave}
                     onClick={() => void onUndoSave()}
                  >
                     Undo save
                  </Button>
               </>
            }
         >
            {facts(props, comments).join(". ")}.
            {comments.length > 0 && (
               <Box
                  component="span"
                  aria-label="Comments removed with their cell"
                  sx={{
                     display: "block",
                     whiteSpace: "pre",
                     fontFamily: "monospace",
                     fontSize: 12,
                     mt: 0.5,
                  }}
               >
                  {comments.join("\n")}
               </Box>
            )}
         </Alert>
         <DiffDialog
            open={viewing}
            before={lastSave.before}
            after={lastSave.after}
            onClose={() => onView(false)}
         />
      </>
   );
}
