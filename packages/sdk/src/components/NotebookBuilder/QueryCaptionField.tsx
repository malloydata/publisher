// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { TextField } from "@mui/material";
import { useEffect, useState } from "react";

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
   const commit = () => {
      if (draft !== caption) onCommit(draft);
   };
   return (
      <TextField
         size="small"
         variant="standard"
         label="Caption"
         value={draft}
         onChange={(event) => setDraft(event.target.value)}
         onBlur={commit}
         onKeyDown={(event) => {
            if (event.key === "Enter") commit();
         }}
         inputProps={{ "aria-label": "Query caption" }}
         sx={{ flex: 1 }}
      />
   );
}
