// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Button } from "@mui/material";
import { AppDialog } from "./AppDialog";

/** What to do with edits the record does not have yet, asked when someone leaves with them. */
export function UnsavedChangesDialog({
   open,
   canSave,
   onKeepEditing,
   onDiscard,
   onSaveAndExit,
}: {
   open: boolean;
   /** False where nothing can be written (read-only, a pinned version): the way out is discarding. */
   canSave: boolean;
   onKeepEditing: () => void;
   onDiscard: () => void;
   onSaveAndExit?: () => void;
}) {
   return (
      <AppDialog
         open={open}
         onClose={onKeepEditing}
         title="Leave with unsaved changes?"
         description={
            canSave && onSaveAndExit
               ? "Your edits have not been saved. Save them, or leave without them."
               : "Leaving now discards your unsaved edits."
         }
         actions={
            <>
               <Button onClick={onKeepEditing}>Keep editing</Button>
               <Button color="error" onClick={onDiscard}>
                  Discard changes
               </Button>
               {canSave && onSaveAndExit && (
                  <Button variant="contained" onClick={onSaveAndExit}>
                     Save and exit
                  </Button>
               )}
            </>
         }
      >
         {null}
      </AppDialog>
   );
}
