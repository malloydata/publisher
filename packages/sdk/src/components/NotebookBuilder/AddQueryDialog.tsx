// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Button, Stack, TextField, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { AppDialog } from "../AppDialog";
import type { CatalogSource } from "../DashboardBuilder/catalog";
import { SourceViewPicker } from "../DashboardBuilder/SourceViewPicker";
import { captionProblem, type QueryRun } from "./queryCell";

export interface AddQueryDialogProps {
   open: boolean;
   /** The sources this notebook can read; undefined while its model loads or when that could not be read. */
   sources: CatalogSource[] | undefined;
   /** The notebook's model could not be read, so `sources` will not arrive. */
   failed?: boolean;
   /** Sources the notebook imports are still being read. */
   pending?: boolean;
   /** Package paths of imports that could not be read. */
   failedImports?: string[];
   onClose: () => void;
   onAdd: (run: QueryRun) => void;
}

/** What a new query cell is: a view of a source this notebook can read, and an optional caption. */
export function AddQueryDialog({
   open,
   sources,
   failed,
   pending,
   failedImports,
   onClose,
   onAdd,
}: AddQueryDialogProps) {
   const { theme } = usePublisherTheme();
   const [source, setSource] = useState("");
   const [view, setView] = useState("");
   const [caption, setCaption] = useState("");

   useEffect(() => {
      if (!open) return;
      setSource("");
      setView("");
      setCaption("");
   }, [open]);
   // Imported sources can arrive after the dialog opens; the first one is picked only if nothing is yet.
   const first = sources?.[0]?.name;
   useEffect(() => {
      if (open && source === "" && first) setSource(first);
   }, [open, source, first]);

   const unreadable = failedImports?.join(", ");
   const trimmed = caption.trim();
   const problem = trimmed ? captionProblem(trimmed) : undefined;
   const canAdd = source !== "" && view !== "" && problem === undefined;
   return (
      <AppDialog
         open={open}
         onClose={onClose}
         title="Add a query"
         description="A query cell runs one view of one source this notebook can read. It is not connected to the filter controls unless its source reads a given as $NAME. Pick a chart for it once it is added."
         actions={
            <>
               <Button onClick={onClose}>Cancel</Button>
               <Button
                  variant="contained"
                  disabled={!canAdd}
                  onClick={() =>
                     onAdd({
                        source,
                        view,
                        ...(trimmed ? { caption: trimmed } : {}),
                     })
                  }
               >
                  Add query
               </Button>
            </>
         }
      >
         <Stack sx={{ gap: 2, pt: 1 }}>
            {!sources || sources.length === 0 ? (
               <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                  {failed
                     ? "The notebook's sources could not be read, so a query cannot be added."
                     : sources === undefined
                       ? "The notebook's sources are still loading."
                       : pending
                         ? "Reading the sources this notebook imports…"
                         : unreadable
                           ? `Could not read ${unreadable}, which this notebook imports, so its sources are not offered.`
                           : "This notebook reads no source, so there is nothing to query."}
               </Typography>
            ) : (
               <>
                  <SourceViewPicker
                     sources={sources}
                     source={source}
                     view={view}
                     onSource={(name) => {
                        setSource(name);
                        setView("");
                     }}
                     onView={setView}
                  />
                  {unreadable && (
                     <Typography
                        variant="caption"
                        sx={{ color: theme.tileTitle }}
                     >
                        Could not read {unreadable}, which this notebook
                        imports, so its sources are not offered.
                     </Typography>
                  )}
                  {pending && (
                     <Typography
                        variant="caption"
                        sx={{ color: theme.tileTitle }}
                     >
                        Still reading the sources this notebook imports…
                     </Typography>
                  )}
                  <TextField
                     size="small"
                     label="Caption"
                     placeholder="Optional"
                     value={caption}
                     error={problem !== undefined}
                     helperText={problem}
                     onChange={(event) => setCaption(event.target.value)}
                     inputProps={{ "aria-label": "Query caption" }}
                  />
               </>
            )}
         </Stack>
      </AppDialog>
   );
}
