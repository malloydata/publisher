// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Button, MenuItem, Stack, TextField } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import {
   createDocument,
   documentPathForTitle,
   MAX_SLUG_ATTEMPTS,
   useDocumentChoices,
   type CreatedDocument,
   type CreateTarget,
   type DocumentCreatedEvent,
} from "../DocumentCreate";
import type { DocumentType } from "../DocumentStorage/DocumentStorage";
import { AppDialog } from "../AppDialog";

const KIND_LABEL: Record<DocumentType, string> = {
   dashboard: "Dashboard",
   notebook: "Notebook",
};

/**
 * A new dashboard or notebook: pick the model, the view its first tile or
 * query shows, and a title. The file is the one the builder would write, so
 * everything after this is the builder's; the host opens it from `onCreated`.
 */
export function NewDocumentDialog({
   open,
   kind: initialKind,
   environmentName,
   packageName,
   models,
   target,
   onClose,
   onCreated,
   onEvent,
}: {
   open: boolean;
   /** Which kind the dialog opens on; the reader can still change it. */
   kind: DocumentType;
   environmentName: string;
   packageName: string;
   /** The package's model files, relative to its root, to pick a source from. */
   models: string[];
   /** Where the file is written, and (on the package route) which names are taken. */
   target: CreateTarget;
   onClose: () => void;
   onCreated: (created: CreatedDocument) => void;
   onEvent?: (event: DocumentCreatedEvent) => void;
}) {
   const [kind, setKind] = useState<DocumentType>(initialKind);
   const [modelPath, setModelPath] = useState("");
   /** The chosen view, as `source::view` — one value, so the pair is always valid. */
   const [tile, setTile] = useState("");
   const [title, setTitle] = useState("");
   const [touchedTitle, setTouchedTitle] = useState(false);
   const [busy, setBusy] = useState(false);
   const [failure, setFailure] = useState<string | undefined>(undefined);

   // Every model at once rather than the chosen one: a model that declares no
   // source with a view cannot start a document, and the only way to offer a
   // list without those is to know before the reader picks.
   const { choices, isLoading, isSuccess } = useDocumentChoices({
      environmentName,
      packageName,
      models,
      enabled: open && models.length > 0,
   });

   const tiles = useMemo(
      () => choices.get(modelPath) ?? [],
      [choices, modelPath],
   );
   const chosen = tiles.find((t) => `${t.source}::${t.view}` === tile);

   useEffect(() => {
      if (open) setKind(initialKind);
   }, [open, initialKind]);

   // Everything with one answer answers itself: a package with one usable
   // model, a model with one view, and a title taken from that view.
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

   // The first name the package listing leaves free; the storage route checks
   // each name against the store itself when it writes.
   const path = useMemo(() => {
      if (title.trim() === "") return undefined;
      for (let n = 1; n <= MAX_SLUG_ATTEMPTS; n++) {
         const candidate = documentPathForTitle(kind, title, n);
         if (target.route !== "package" || !target.existing.includes(candidate))
            return candidate;
      }
      return undefined;
   }, [kind, title, target]);
   const canCreate = chosen !== undefined && path !== undefined;
   const label = KIND_LABEL[kind].toLowerCase();
   const first = kind === "dashboard" ? "First tile" : "First query";

   const create = async () => {
      if (!canCreate || chosen === undefined) return;
      setBusy(true);
      setFailure(undefined);
      try {
         const created = await createDocument({
            kind,
            document: {
               title,
               modelPath,
               source: chosen.source,
               view: chosen.view,
            },
            target,
            ...(onEvent ? { onEvent } : {}),
         });
         onCreated(created);
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
      <AppDialog
         open={open}
         onClose={onClose}
         title={`New ${label}`}
         description={
            kind === "dashboard"
               ? "A dashboard is a Malloy file in the package. Pick what its first tile shows; the rest is the builder."
               : "A notebook is a Malloy file in the package that mixes text and queries. Pick what its first query runs; the rest is the builder."
         }
         actions={
            <>
               <Button onClick={onClose} disabled={busy}>
                  Cancel
               </Button>
               <Button
                  variant="contained"
                  disabled={!canCreate || busy}
                  onClick={() => void create()}
               >
                  Create {label}
               </Button>
            </>
         }
      >
         <Stack sx={{ gap: 2, pt: 1 }}>
            {models.length === 0 ? (
               <Alert severity="info">
                  This package has no models yet. Add a .malloy file that
                  declares a source with a view, then create a {label} from it.
               </Alert>
            ) : (
               isSuccess &&
               choices.size === 0 && (
                  <Alert severity="info">
                     A {label} starts from a view of a source, and no model in
                     this package declares one yet. Add a view to a source in a
                     model file and this list fills in.
                  </Alert>
               )
            )}
            <TextField
               select
               size="small"
               label="Type"
               value={kind}
               onChange={(event) => setKind(event.target.value as DocumentType)}
               inputProps={{ "aria-label": "Type" }}
            >
               {(Object.keys(KIND_LABEL) as DocumentType[]).map((k) => (
                  <MenuItem key={k} value={k}>
                     {KIND_LABEL[k]}
                  </MenuItem>
               ))}
            </TextField>
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
                  isLoading
                     ? "Reading the package's models…"
                     : choices.size === 1
                       ? "The only model with a view to start from."
                       : "Models with a view to start from."
               }
            >
               {[...choices.keys()].map((modelFile) => (
                  <MenuItem key={modelFile} value={modelFile}>
                     {modelFile}
                  </MenuItem>
               ))}
            </TextField>
            {/* One field, not two: the file needs a view OF a source, so the
                pair is the thing being chosen. */}
            <TextField
               select
               size="small"
               label={first}
               value={chosen ? tile : ""}
               disabled={tiles.length === 0}
               onChange={(event) => setTile(event.target.value)}
               inputProps={{ "aria-label": first }}
               helperText={
                  tiles.length === 1
                     ? "The only view this model offers; more are added in the builder."
                     : "A view of a source; more are added in the builder."
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
               inputProps={{ "aria-label": `${KIND_LABEL[kind]} title` }}
               error={title.trim() !== "" && path === undefined}
               helperText={
                  path
                     ? `Written as ${path}, and opened in the builder.`
                     : title.trim() !== ""
                       ? "No free file name for this title; choose another."
                       : "Names the file too."
               }
            />
            {failure && <Alert severity="error">{failure}</Alert>}
         </Stack>
      </AppDialog>
   );
}
