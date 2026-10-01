// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Button, Stack, TextField, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { AppDialog } from "../AppDialog";
import type { CatalogSource } from "../DashboardBuilder/catalog";
import { SourceViewPicker } from "../DashboardBuilder/SourceViewPicker";
import type { QueryRun } from "./queryCell";

export interface AddQueryDialogProps {
   open: boolean;
   /** The sources the notebook's own compiled model offers; undefined while that model loads. */
   sources: CatalogSource[] | undefined;
   onClose: () => void;
   onAdd: (run: QueryRun) => void;
}

/** What a new query cell is: a view of a source this notebook can read, and an optional caption. */
export function AddQueryDialog({
   open,
   sources,
   onClose,
   onAdd,
}: AddQueryDialogProps) {
   const { theme } = usePublisherTheme();
   const [source, setSource] = useState("");
   const [view, setView] = useState("");
   const [caption, setCaption] = useState("");

   useEffect(() => {
      if (!open) return;
      setSource(sources?.[0]?.name ?? "");
      setView("");
      setCaption("");
   }, [open, sources]);

   const canAdd = source !== "" && view !== "";
   return (
      <AppDialog
         open={open}
         onClose={onClose}
         title="Add a query"
         description="A query cell runs one view of one source this notebook can read. Pick a chart for it once it is added."
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
                        ...(caption.trim() ? { caption: caption.trim() } : {}),
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
                  {sources
                     ? "This notebook reads no source, so there is nothing to query."
                     : "The notebook's sources are still loading."}
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
                  <TextField
                     size="small"
                     label="Caption"
                     placeholder="Optional"
                     value={caption}
                     onChange={(event) => setCaption(event.target.value)}
                     inputProps={{ "aria-label": "Query caption" }}
                  />
                  <Typography variant="caption" sx={{ color: theme.tileTitle }}>
                     This chart will not follow the filters: an added query runs
                     as written.
                  </Typography>
               </>
            )}
         </Stack>
      </AppDialog>
   );
}
