// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { TextField } from "@mui/material";
import { useEffect, useState } from "react";
import { captionProblem } from "./queryCell";

/** An added query's caption, committed once when the field is left so each keystroke is not an undo step. */
export function QueryCaptionField({
   caption,
   onCommit,
}: {
   caption: string;
   onCommit: (next: string) => void;
}) {
   const [draft, setDraft] = useState(caption);
   // An undo or redo changes the document's caption under the field.
   useEffect(() => setDraft(caption), [caption]);
   const problem = draft.trim() ? captionProblem(draft.trim()) : undefined;
   // A caption the writer would refuse stays in the field with its reason and is not committed.
   const commit = () => {
      if (draft !== caption && problem === undefined) onCommit(draft);
   };
   return (
      <TextField
         size="small"
         variant="standard"
         label="Caption"
         value={draft}
         onChange={(event) => setDraft(event.target.value)}
         error={problem !== undefined}
         helperText={problem}
         onBlur={commit}
         onKeyDown={(event) => {
            if (event.key === "Enter") commit();
         }}
         inputProps={{ "aria-label": "Query caption" }}
         sx={{ flex: 1 }}
      />
   );
}
