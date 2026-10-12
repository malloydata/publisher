// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Alert,
   Button,
   MenuItem,
   Stack,
   TextField,
   Tooltip,
} from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import {
   createDocument,
   reportPathForTitle,
   MAX_SLUG_ATTEMPTS,
   newDocumentProblem,
   reportPathProblem,
   useDocumentChoices,
   type CreatedDocument,
   type CreateTarget,
   type DocumentCreatedEvent,
} from "../DocumentCreate";
import { canRetryRequest } from "../DocumentCreate/canRetry";
import type { DocumentType } from "../DocumentStorage/DocumentStorage";
import { AppDialog } from "../AppDialog";
import { layoutOfKind, type ReportLayout } from "./documentLocation";

const LAYOUT_LABEL: Record<ReportLayout, string> = {
   linear: "Linear: read top to bottom",
   grid: "Grid: tiles side by side",
};

export interface NewDocumentDialogProps {
   open: boolean;
   /** Which layout the dialog opens on. Default linear. */
   layout?: ReportLayout;
   /** @deprecated Pass `layout`: `dashboard` is grid, `notebook` linear. */
   kind?: DocumentType;
   /** Whether the reader may switch the layout. Default true. */
   allowLayoutChange?: boolean;
   /** @deprecated Pass `allowLayoutChange`. */
   allowKindChange?: boolean;
   environmentName: string;
   packageName: string;
   versionId?: string;
   /** The package's model files, relative to its root, to pick a source from. */
   models: readonly string[];
   /** The host is still listing `models`, so an empty list is not yet "no models". */
   modelsLoading?: boolean;
   /** The host's listing of `models` failed, so an empty list is not "no models". */
   modelsError?: unknown;
   /** Lists the models again; offered when `modelsError` is one a retry can clear. */
   onRetryModels?: () => void;
   /** Where the file is written, and (on the package route) which names are taken. */
   target: CreateTarget;
   /** The line under the title on the storage route, saying where the host keeps the new file. */
   savedAs?: string;
   onClose: () => void;
   onCreated: (created: CreatedDocument) => void;
   onEvent?: (event: DocumentCreatedEvent) => void;
}

/**
 * A new report: pick the model, the view its first query tile shows, a title
 * and a layout. The file is written as `<slug>.malloy` at the package root, or
 * at the location the author types instead. It is the file the builder would
 * write, so everything after this is the builder's; the host opens it from
 * `onCreated`.
 */
export function NewDocumentDialog({
   open,
   layout: layoutProp,
   kind: kindProp,
   allowLayoutChange,
   allowKindChange,
   environmentName,
   packageName,
   versionId,
   models,
   modelsLoading = false,
   modelsError,
   onRetryModels,
   target,
   savedAs,
   onClose,
   onCreated,
   onEvent,
}: NewDocumentDialogProps) {
   const initialLayout: ReportLayout =
      layoutProp ?? (kindProp ? layoutOfKind(kindProp) : "linear");
   const canChangeLayout = allowLayoutChange ?? allowKindChange ?? true;
   const [layout, setLayout] = useState<ReportLayout>(initialLayout);
   /** The location as typed; undefined until the author edits it, and the title then names the file. */
   const [location, setLocation] = useState<string | undefined>(undefined);
   const [modelPath, setModelPath] = useState("");
   /** The chosen view, as `source::view` — one value, so the pair is always valid. */
   const [tile, setTile] = useState("");
   const [title, setTitle] = useState("");
   const [touchedTitle, setTouchedTitle] = useState(false);
   const [attempted, setAttempted] = useState(false);
   const [busy, setBusy] = useState(false);
   const [failure, setFailure] = useState<string | undefined>(undefined);
   const modelsFailed = modelsError !== undefined && modelsError !== null;

   // Every model at once rather than the chosen one: a model that declares no
   // source with a view cannot start a document, and the only way to offer a
   // list without those is to know before the reader picks.
   const { choices, isLoading, isSuccess, failed, retry, canRetry } =
      useDocumentChoices({
         environmentName,
         packageName,
         ...(versionId !== undefined ? { versionId } : {}),
         models,
         enabled: open && models.length > 0,
      });

   const tiles = useMemo(
      () => choices.get(modelPath) ?? [],
      [choices, modelPath],
   );
   const chosen = tiles.find((t) => `${t.source}::${t.view}` === tile);

   useEffect(() => {
      if (open) setLayout(initialLayout);
   }, [open, initialLayout]);

   // Everything with one answer answers itself: a package with one usable
   // model, a model with one view, and a title taken from that view.
   useEffect(() => {
      if (!open) return;
      const usable = [...choices.keys()];
      if (modelPath === "" && usable.length > 0) setModelPath(usable[0]);
   }, [open, choices, modelPath]);

   // Only a pick not yet made is filled in: a picked view that leaves the list stays unpicked, never swapped.
   useEffect(() => {
      if (!open || tiles.length === 0 || tile !== "") return;
      setTile(`${tiles[0].source}::${tiles[0].view}`);
   }, [open, tiles, tile]);

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
      setLocation(undefined);
      setAttempted(false);
      setFailure(undefined);
   }, [open]);

   // The first name at the package root the listing leaves free; the storage
   // route checks each name against the store itself when it writes.
   const suggested = useMemo(() => {
      if (title.trim() === "") return undefined;
      for (let n = 1; n <= MAX_SLUG_ATTEMPTS; n++) {
         const candidate = reportPathForTitle(title, n);
         if (!target.existing?.includes(candidate)) return candidate;
      }
      return undefined;
   }, [title, target]);
   const typed = location?.trim();
   // A typed location is taken as written, `.malloy` added when it is left off.
   const typedPath =
      typed === undefined || typed === ""
         ? undefined
         : typed.endsWith(".malloy")
           ? typed
           : `${typed}.malloy`;
   const path = typedPath ?? suggested;
   const locationProblem =
      typedPath === undefined
         ? undefined
         : (reportPathProblem(typedPath) ??
           (target.existing?.includes(typedPath)
              ? `${typedPath} already exists; choose another location.`
              : undefined));
   const problem =
      chosen === undefined
         ? undefined
         : newDocumentProblem(layout === "grid" ? "dashboard" : "notebook", {
              title,
              modelPath,
              source: chosen.source,
              view: chosen.view,
           });
   const titleProblem =
      problem ??
      (title.trim() !== "" && path === undefined
         ? "No free file name for this title; choose another."
         : undefined);
   // A blank title is said only once Create is pressed; anything else as it is typed.
   const showTitleProblem =
      titleProblem !== undefined && (attempted || title.trim() !== "");

   const loading = modelsLoading || isLoading;
   const unread = choices.size === 0 && (modelsFailed || failed.length > 0);
   // Create is off only when nothing typed could make it succeed; the title is checked on press.
   const blocked = loading
      ? "Loading model views"
      : unread
        ? "Couldn't read the model views to start from"
        : choices.size === 0
          ? "No model view to start from yet"
          : chosen === undefined
            ? "Pick a view to start from"
            : undefined;
   const label = "report";
   const createLabel = busy ? `Creating ${label}…` : `Create ${label}`;
   const first = "First query";
   const picked = tile !== "" && chosen === undefined && tiles.length > 0;

   const create = async () => {
      if (blocked !== undefined || chosen === undefined || busy) return;
      if (titleProblem !== undefined || locationProblem !== undefined) {
         setAttempted(true);
         return;
      }
      setBusy(true);
      setFailure(undefined);
      try {
         const created = await createDocument({
            layout,
            ...(typedPath !== undefined ? { path: typedPath } : {}),
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

   const retryButton = (onRetry: () => void) => (
      <Button color="inherit" size="small" onClick={onRetry}>
         Retry
      </Button>
   );

   return (
      <AppDialog
         open={open}
         // A write in flight is not abandoned by Escape or a backdrop click.
         onClose={busy ? () => {} : onClose}
         title={`New ${label}`}
         description="A report is a Malloy file in the package that lays out text and queries, as a grid or top to bottom. Pick what its first query shows; the rest is the builder."
         actions={
            <>
               <Button onClick={onClose} disabled={busy}>
                  Cancel
               </Button>
               {/* A disabled button fires no events, so the tooltip hangs on a wrapper. */}
               <Tooltip title={busy ? "" : (blocked ?? "")}>
                  <span>
                     <Button
                        variant="contained"
                        disabled={busy || blocked !== undefined}
                        aria-label={
                           !busy && blocked !== undefined
                              ? `${createLabel}: ${blocked}`
                              : createLabel
                        }
                        onClick={() => void create()}
                     >
                        {createLabel}
                     </Button>
                  </span>
               </Tooltip>
            </>
         }
      >
         <Stack sx={{ gap: 2, pt: 1 }}>
            {modelsFailed ? (
               <Alert
                  severity="error"
                  action={
                     onRetryModels && canRetryRequest(modelsError)
                        ? retryButton(onRetryModels)
                        : undefined
                  }
               >
                  Couldn&apos;t read this package&apos;s models, so no view is
                  listed to start from.
               </Alert>
            ) : models.length === 0 ? (
               !modelsLoading && (
                  <Alert severity="info">
                     This package has no models yet. Add a .malloy file that
                     declares a source with a view, then create a {label} from
                     it.
                  </Alert>
               )
            ) : failed.length > 0 ? (
               <Alert
                  severity="warning"
                  action={canRetry ? retryButton(retry) : undefined}
               >
                  Couldn&apos;t read {failed.join(", ")}, so{" "}
                  {failed.length === 1 ? "its" : "their"} views aren&apos;t
                  listed.
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
            {canChangeLayout && (
               <TextField
                  select
                  size="small"
                  label="Layout"
                  value={layout}
                  disabled={busy}
                  onChange={(event) =>
                     setLayout(event.target.value as ReportLayout)
                  }
                  inputProps={{ "aria-label": "Layout" }}
                  helperText="Switch it any time in the builder."
               >
                  {(["linear", "grid"] as const).map((l) => (
                     <MenuItem key={l} value={l}>
                        {LAYOUT_LABEL[l]}
                     </MenuItem>
                  ))}
               </TextField>
            )}
            <TextField
               select
               size="small"
               label="Model"
               value={choices.has(modelPath) ? modelPath : ""}
               disabled={choices.size === 0 || busy}
               onChange={(event) => {
                  setModelPath(event.target.value);
                  setTile("");
               }}
               inputProps={{ "aria-label": "Model" }}
               helperText={
                  loading
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
               disabled={tiles.length === 0 || busy}
               onChange={(event) => setTile(event.target.value)}
               inputProps={{ "aria-label": first }}
               error={picked}
               helperText={
                  picked
                     ? "The view you picked is no longer in this model; pick another."
                     : tiles.length === 1
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
               disabled={busy}
               onChange={(event) => {
                  setTouchedTitle(true);
                  setTitle(event.target.value);
               }}
               inputProps={{ "aria-label": "Report title" }}
               error={showTitleProblem}
               helperText={
                  showTitleProblem
                     ? titleProblem
                     : target.route === "package"
                       ? "Names the file too, unless you type a location."
                       : (savedAs ??
                         `Saved as a new ${label} in the host's store, and opened in the builder.`)
               }
            />
            <TextField
               size="small"
               label="Location"
               value={location ?? suggested ?? ""}
               placeholder="report.malloy"
               disabled={busy}
               onChange={(event) => setLocation(event.target.value)}
               inputProps={{ "aria-label": "Location" }}
               error={locationProblem !== undefined}
               helperText={
                  locationProblem ??
                  (path
                     ? `Written as ${path} in the package, and opened in the builder.`
                     : "A path in the package; the package root by default.")
               }
            />
            {failure && <Alert severity="error">{failure}</Alert>}
         </Stack>
      </AppDialog>
   );
}
