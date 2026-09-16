// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Alert,
   Button,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   MenuItem,
   Stack,
   TextField,
   Typography,
} from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { newDashboardSource, slugFor } from "../DashboardBuilder/newDashboard";
import { useServer } from "../ServerProvider";

/**
 * A new dashboard, written into the package and opened in the builder: pick
 * the model, a source it declares, the view for the first tile, and a title.
 * The file is the one the builder would write (see `newDashboardSource`), so
 * everything after this is the builder's.
 */
export function NewDashboardDialog({
   open,
   environmentName,
   packageName,
   models,
   existing,
   onClose,
   onCreated,
}: {
   open: boolean;
   environmentName: string;
   packageName: string;
   /** The package's model files, relative to its root, to pick a source from. */
   models: string[];
   /** Slugs already in the package, which a new one may not repeat. */
   existing: string[];
   onClose: () => void;
   /** The new dashboard's slug, once its file is in the package. */
   onCreated: (slug: string) => void;
}) {
   const { apiClients } = useServer();
   const [modelPath, setModelPath] = useState("");
   const [source, setSource] = useState("");
   const [view, setView] = useState("");
   const [title, setTitle] = useState("");
   const [busy, setBusy] = useState(false);
   const [failure, setFailure] = useState<string | undefined>(undefined);

   useEffect(() => {
      if (!open) return;
      setModelPath(models[0] ?? "");
      setSource("");
      setView("");
      setTitle("");
      setFailure(undefined);
   }, [open, models]);

   const model = useQueryWithApiError({
      queryKey: [
         "new-dashboard-model",
         environmentName,
         packageName,
         modelPath,
      ],
      queryFn: () =>
         apiClients.models.getModel(environmentName, packageName, modelPath),
      enabled: open && modelPath !== "",
   });
   const sources = useMemo(
      () =>
         (model.data?.data.sources ?? []).filter(
            (s): s is typeof s & { name: string } => typeof s.name === "string",
         ),
      [model.data],
   );
   const views = useMemo(
      () =>
         (sources.find((s) => s.name === source)?.views ?? [])
            .map((v) => v.name)
            .filter((name): name is string => typeof name === "string"),
      [sources, source],
   );

   const slug = slugFor(title);
   const taken = existing.includes(slug);
   const canCreate =
      modelPath !== "" && source !== "" && view !== "" && slug !== "" && !taken;

   const create = async () => {
      if (!canCreate) return;
      setBusy(true);
      setFailure(undefined);
      try {
         await apiClients.models.putModelSource(
            environmentName,
            packageName,
            `dashboards/${slug}.malloy`,
            { source: newDashboardSource({ title, modelPath, source, view }) },
         );
         onCreated(slug);
      } catch (error) {
         const message = (
            error as { response?: { data?: { message?: string } } }
         ).response?.data?.message;
         setFailure(
            message ?? (error instanceof Error ? error.message : String(error)),
         );
      } finally {
         setBusy(false);
      }
   };

   return (
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
         <DialogTitle sx={{ pb: 0.5 }}>New dashboard</DialogTitle>
         <DialogContent>
            <Stack sx={{ gap: 2, pt: 1 }}>
               <TextField
                  select
                  size="small"
                  label="Model"
                  value={modelPath}
                  onChange={(event) => {
                     setModelPath(event.target.value);
                     setSource("");
                     setView("");
                  }}
                  inputProps={{ "aria-label": "Model" }}
               >
                  {models.map((path) => (
                     <MenuItem key={path} value={path}>
                        {path}
                     </MenuItem>
                  ))}
               </TextField>
               <TextField
                  select
                  size="small"
                  label="Source"
                  value={source}
                  disabled={sources.length === 0}
                  onChange={(event) => {
                     setSource(event.target.value);
                     setView("");
                  }}
                  inputProps={{ "aria-label": "Source" }}
                  helperText={
                     model.isError
                        ? "This model did not load."
                        : modelPath && !model.isSuccess
                          ? "Loading the model…"
                          : undefined
                  }
               >
                  {sources.map((s) => (
                     <MenuItem key={s.name} value={s.name}>
                        {s.name}
                     </MenuItem>
                  ))}
               </TextField>
               <TextField
                  select
                  size="small"
                  label="First tile"
                  value={view}
                  disabled={views.length === 0}
                  onChange={(event) => {
                     setView(event.target.value);
                     if (!title)
                        setTitle(event.target.value.replace(/_/g, " "));
                  }}
                  inputProps={{ "aria-label": "First tile" }}
                  helperText={
                     source && views.length === 0
                        ? "This source declares no views to put on a tile."
                        : "A view of the source; more tiles are added in the builder."
                  }
               >
                  {views.map((name) => (
                     <MenuItem key={name} value={name}>
                        {name}
                     </MenuItem>
                  ))}
               </TextField>
               <TextField
                  size="small"
                  label="Title"
                  value={title}
                  onChange={(event) => setTitle(event.target.value)}
                  inputProps={{ "aria-label": "Dashboard title" }}
                  error={taken}
                  helperText={
                     taken
                        ? `dashboards/${slug}.malloy already exists in this package.`
                        : slug
                          ? `Written as dashboards/${slug}.malloy`
                          : "Names the file too."
                  }
               />
               {failure && <Alert severity="error">{failure}</Alert>}
               <Typography variant="caption" sx={{ opacity: 0.7 }}>
                  The file is written into the package and opened in the
                  builder.
               </Typography>
            </Stack>
         </DialogContent>
         <DialogActions sx={{ px: 3, py: 1.5 }}>
            <Button onClick={onClose} disabled={busy}>
               Cancel
            </Button>
            <Button
               variant="contained"
               disabled={!canCreate || busy}
               onClick={() => void create()}
            >
               Create
            </Button>
         </DialogActions>
      </Dialog>
   );
}
