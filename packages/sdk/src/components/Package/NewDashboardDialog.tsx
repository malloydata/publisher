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
   /** The chosen tile, as `source::view` — one value, so the pair is always valid. */
   const [tile, setTile] = useState("");
   const [title, setTitle] = useState("");
   const [touchedTitle, setTouchedTitle] = useState(false);
   const [busy, setBusy] = useState(false);
   const [failure, setFailure] = useState<string | undefined>(undefined);

   // Every model at once rather than the chosen one: a model that declares no
   // source with a view cannot start a dashboard, and the only way to offer a
   // list without those is to know before the user picks. The package has a
   // handful of models and each is a cached GET the package page has usually
   // made already.
   const catalog = useQueryWithApiError({
      queryKey: ["new-dashboard-models", environmentName, packageName, models],
      queryFn: async () => {
         const loaded = await Promise.all(
            models.map(async (path) => {
               try {
                  const response = await apiClients.models.getModel(
                     environmentName,
                     packageName,
                     path,
                  );
                  return { path, sources: response.data.sources ?? [] };
               } catch {
                  // A model that will not load is a model that cannot start a
                  // dashboard; it drops out of the list rather than failing it.
                  return { path, sources: [] };
               }
            }),
         );
         return loaded;
      },
      enabled: open && models.length > 0,
   });

   /** Model → the (source, view) pairs it can start a dashboard from. */
   const choices = useMemo(() => {
      const out = new Map<string, Array<{ source: string; view: string }>>();
      for (const model of catalog.data ?? []) {
         const pairs: Array<{ source: string; view: string }> = [];
         for (const source of model.sources) {
            if (typeof source.name !== "string") continue;
            for (const view of source.views ?? []) {
               if (typeof view.name === "string")
                  pairs.push({ source: source.name, view: view.name });
            }
         }
         if (pairs.length > 0) out.set(model.path, pairs);
      }
      return out;
   }, [catalog.data]);

   const tiles = useMemo(
      () => choices.get(modelPath) ?? [],
      [choices, modelPath],
   );
   const chosen = tiles.find((t) => `${t.source}::${t.view}` === tile);

   // Everything with one answer answers itself: a package with one usable
   // model, a model with one view to put on a tile, and a title taken from
   // that view. What is left to fill in is what the author actually chooses.
   useEffect(() => {
      if (!open) return;
      const usable = [...choices.keys()];
      if (modelPath === "" && usable.length > 0) setModelPath(usable[0]);
   }, [open, choices, modelPath]);

   useEffect(() => {
      if (!open || tiles.length === 0) return;
      if (chosen === undefined) setTile(`${tiles[0].source}::${tiles[0].view}`);
   }, [open, tiles, chosen]);

   useEffect(() => {
      if (!open || touchedTitle || chosen === undefined) return;
      setTitle(chosen.view.replace(/_/g, " "));
   }, [open, touchedTitle, chosen]);

   useEffect(() => {
      if (open) return;
      // Reset on close, so the next open starts from the package again.
      setModelPath("");
      setTile("");
      setTitle("");
      setTouchedTitle(false);
      setFailure(undefined);
   }, [open]);

   const slug = slugFor(title);
   const taken = existing.includes(slug);
   const canCreate = chosen !== undefined && slug !== "" && !taken;

   const create = async () => {
      if (!canCreate || chosen === undefined) return;
      setBusy(true);
      setFailure(undefined);
      try {
         await apiClients.models.updateModelSource(
            environmentName,
            packageName,
            `dashboards/${slug}.malloy`,
            {
               source: newDashboardSource({
                  title,
                  modelPath,
                  source: chosen.source,
                  view: chosen.view,
               }),
            },
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
               {catalog.isSuccess && choices.size === 0 && (
                  <Alert severity="info">
                     A dashboard starts from a view of a source, and no model in
                     this package declares one yet. Add a view to a source and
                     this list fills in.
                  </Alert>
               )}
               <TextField
                  select
                  size="small"
                  label="Model"
                  value={modelPath}
                  disabled={choices.size === 0}
                  onChange={(event) => {
                     setModelPath(event.target.value);
                     setTile("");
                  }}
                  inputProps={{ "aria-label": "Model" }}
                  helperText={
                     catalog.isLoading
                        ? "Reading the package's models…"
                        : choices.size === 1
                          ? "The only model with a view to put on a tile."
                          : "Models with a view to put on a tile."
                  }
               >
                  {[...choices.keys()].map((path) => (
                     <MenuItem key={path} value={path}>
                        {path}
                     </MenuItem>
                  ))}
               </TextField>
               {/* One field, not two: the file needs a view OF a source, so
                   the pair is the thing being chosen. Picking them separately
                   let a reader land on a source with no views and a disabled
                   Create that said nothing about why. */}
               <TextField
                  select
                  size="small"
                  label="First tile"
                  value={chosen ? tile : ""}
                  disabled={tiles.length === 0}
                  onChange={(event) => setTile(event.target.value)}
                  inputProps={{ "aria-label": "First tile" }}
                  helperText={
                     tiles.length === 1
                        ? "The only view this model offers; more tiles are added in the builder."
                        : "A view of a source; more tiles are added in the builder."
                  }
               >
                  {tiles.map((option) => (
                     <MenuItem
                        key={`${option.source}::${option.view}`}
                        value={`${option.source}::${option.view}`}
                     >
                        {option.source} → {option.view}
                     </MenuItem>
                  ))}
               </TextField>
               <TextField
                  size="small"
                  label="Title"
                  value={title}
                  onChange={(event) => {
                     setTouchedTitle(true);
                     setTitle(event.target.value);
                  }}
                  inputProps={{ "aria-label": "Dashboard title" }}
                  error={taken}
                  helperText={
                     taken
                        ? `dashboards/${slug}.malloy already exists in this package.`
                        : slug
                          ? `Written as dashboards/${slug}.malloy, and opened in the builder.`
                          : "Names the file too."
                  }
               />
               {failure && <Alert severity="error">{failure}</Alert>}
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
