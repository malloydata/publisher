// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Button, Stack, Typography } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { DashboardManifest, Given } from "../../client";
import { modelResultsKey } from "../../hooks/useQueryResult";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { DashboardTile, tileTitle } from "../Dashboard/DashboardTile";
import { tileFilterLabels } from "../Dashboard/TileFilterTag";
import type { BuilderEvent } from "./telemetry";
import { now } from "../../utils/clock";
import { encodeResourceUri } from "../../utils/formatting";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import {
   isDocumentNotFound,
   useOptionalDocumentStorage,
   type Workspace,
} from "../DocumentStorage";
import { GivensPanel } from "../given";
import { DashboardBar } from "../Dashboard/DashboardBar";
import type { TileHeadingSlots } from "../Dashboard/TileCard";
import { Loading } from "../Loading";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { useServer } from "../ServerProvider";
import {
   buildCatalog,
   isDashboardModel,
   visibleToDashboard,
   type PackageCatalog,
} from "./catalog";
import { locatorFor } from "../DocumentCreate/documentPath";
import { DashboardBuilder } from "./DashboardBuilder";
import type { DashboardDocument, DocumentKind, QueryTile } from "./document";
import { previewGivens, previewTileQuery } from "./preview";
import {
   chooseWorkspace,
   expectedHashFor,
   resolveEditorTarget,
   saveCaption,
   saveTarget,
   storageErrorMessage,
   withWorkspace,
   writePackageFile,
   type SavesTo,
} from "./documentSession";
import { readForEditor } from "./readForEditor";
import type { SaveContext, SaveHandler } from "./useDocumentEditor";

/**
 * The builder, opened on a package dashboard, with everything a host has to
 * supply already wired: the file's text and manifest from the server, a live
 * control row and live tiles that follow the document, a catalog for the
 * filter window's field search, and saving.
 *
 * SAVING goes wherever the record is. A {@link Workspace} the host marks
 * `authoritative` IS the record, so the editor opens that copy and writes back
 * to it, and the package file is a deploy of it. Otherwise the record is the
 * package: the editor writes there on a server that takes writes, and
 * otherwise keeps a copy beside it in the host's {@link DocumentStorage} — the
 * Console's default is this browser, where the copy is offered back on the
 * next visit. A host with neither still gets the editor, without Save.
 *
 * What that costs is stated in the toolbar: a control added here is live in the
 * editor (its value is written into each tile's query) but reaches the package
 * only when the file is saved into it.
 */
export type DashboardEditorProps = (
   | {
        /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
        resourceUri: string;
        /** The dashboard's slug, as listed by the dashboards endpoint. */
        dashboard: string;
     }
   | {
        /** @deprecated Pass `resourceUri` and `dashboard` instead. */
        environmentName: string;
        /** @deprecated Pass `resourceUri` and `dashboard` instead. */
        packageName: string;
        /**
         * @deprecated Pass `resourceUri` and `dashboard` instead. The
         * dashboard's slug: `overview`, not `dashboards/overview.malloy`.
         */
        dashboardName: string;
     }
) & {
   /** Leave the editor: its "Close", which asks first when edits are unsaved. Absent, no such button. */
   onExit?: () => void;
   /**
    * What the editor does — opened, saved, refused — for the host to log or
    * count; see `DashboardEvent`. A notebook reports `NotebookEvent`s instead.
    */
   onEvent?: (event: BuilderEvent) => void;
   /**
    * What the document is to the host: its storage locator and event names.
    * Default `dashboard`. A notebook is a dashboard with one column, and is
    * read with the authoring flag and served through the notebook endpoint.
    */
   kind?: DocumentKind;
   /**
    * The document's file within the package, when it is not where its kind
    * puts a new one (`dashboards/<slug>.malloy`, `notebooks/<slug>.malloy`):
    * the kind comes from the file's own tag, so a document may sit in either.
    */
   path?: string;
   /**
    * Whether there are edits the record does not have, on every change and
    * whenever the editor opens a document.
    *
    * For a host that owns the way out: a route change, a tab close, its own
    * "are you sure". The editor will not discard unsaved work on its own, so
    * without this a host cannot tell whether leaving costs anything.
    */
   onDirtyChange?: (dirty: boolean) => void;
};

const LEGACY_REFUSAL =
   "a .malloynb notebook is read, not edited. Fix: rewrite it as a `.malloy` notebook under notebooks/ to edit it here.";

const WITHHELD_REFUSAL =
   "the server did not send this notebook's text, so there is nothing here to edit. Fix: edit the file in the package.";

export function DashboardEditor(props: DashboardEditorProps) {
   const { onExit, onEvent, onDirtyChange, kind = "dashboard", path } = props;
   const notebook = kind === "notebook";
   // Degraded, not thrown, on a bad URI: a throw in a render body takes the host's whole tree down.
   const {
      environmentName,
      packageName,
      versionId,
      namesBoth: uriNamesBoth,
   } = resolveEditorTarget(props);
   const dashboardName =
      "resourceUri" in props ? props.dashboard : props.dashboardName;
   const noun = notebook ? "notebook" : "dashboard";
   const refusedEvent = notebook
      ? ("notebook.open_refused" as const)
      : ("dashboard.open_refused" as const);

   const { apiClients, mutable, isLoadingStatus } = useServer();
   const queryClient = useQueryClient();
   // When the editor was asked for — or the reader chose what to open — so
   // "opened" can say how long it took. Read through refs by the open effect,
   // so a host's handler changing identity does not re-open the document.
   const startedAt = useRef(now());
   const onEventRef = useRef(onEvent);
   onEventRef.current = onEvent;
   const onDirtyChangeRef = useRef(onDirtyChange);
   onDirtyChangeRef.current = onDirtyChange;
   const storage = useOptionalDocumentStorage()?.documentStorage;
   const legacyFormat = notebook && /\.malloynb$/i.test(dashboardName);
   const modelPath = path ?? `${kind}s/${dashboardName}.malloy`;

   // The file as the package has it. `sourceText` rides on the compiled model.
   const modelQuery = useQueryWithApiError({
      queryKey: [
         "dashboard-editor-model",
         environmentName,
         packageName,
         modelPath,
         versionId,
         notebook,
      ],
      // A curated package otherwise withholds the text of a notebook reading an off-surface source.
      queryFn: () =>
         apiClients.models.getModel(
            environmentName,
            packageName,
            modelPath,
            versionId,
            notebook ? true : undefined,
         ),
      enabled: uriNamesBoth && !legacyFormat,
   });
   const packageText = (
      modelQuery.data?.data as { sourceText?: string } | undefined
   )?.sourceText;

   // The host's copy, if one was saved earlier.
   const [workspace, setWorkspace] = useState<Workspace | undefined>(undefined);
   const [draft, setDraft] = useState<string | undefined>(undefined);
   const [draftChecked, setDraftChecked] = useState(storage === undefined);
   // Whether a copy was there when the editor opened: only that one is
   // offered. A copy this session saves is not "edits from an earlier visit".
   const [offered, setOffered] = useState(false);
   // A read the backend could not answer, which is not the same as no
   // document. Held rather than swallowed, because "there is nothing saved"
   // is what makes overwriting the record look safe.
   const [readFailure, setReadFailure] = useState<string | undefined>(
      undefined,
   );
   useEffect(() => {
      if (!storage) return;
      let stale = false;
      setReadFailure(undefined);
      (async () => {
         try {
            // Every workspace, not only the writeable ones: a reader who
            // cannot write to the record still has to be shown the record.
            // Asking for writeable only would hide it and fall back to the
            // package, which on a server that takes writes means publishing a
            // deploy of the record over the record.
            const chosen = chooseWorkspace(await storage.listWorkspaces(false));
            if (stale) return;
            setWorkspace(chosen);
            // Only a notebook's record is read: a copy beside the package is never offered back.
            if (chosen !== undefined && (!notebook || chosen.authoritative)) {
               const text = await storage
                  .getDocument(
                     locatorFor(
                        kind,
                        chosen.name,
                        environmentName,
                        packageName,
                        modelPath,
                     ),
                  )
                  // Absence is the ordinary first visit, not a failure.
                  .catch((error: unknown) => {
                     if (isDocumentNotFound(error)) return undefined;
                     throw error;
                  });
               if (stale) return;
               setDraft(text);
               setOffered(text !== undefined);
            }
         } catch (error) {
            if (stale) return;
            setReadFailure(storageErrorMessage(error));
         }
         if (!stale) setDraftChecked(true);
      })();
      return () => {
         stale = true;
      };
   }, [storage, kind, notebook, environmentName, packageName, modelPath]);

   // The host's copy is the record: the editor opens it, writes back to it,
   // and never offers to "resume" it, because a record is not a pending edit.
   const authoritative = workspace?.authoritative === true;
   // The package file is a deploy of the record, so opening it when the record
   // itself could not be read would put a reader on the wrong document.
   const blockedOnRecord = authoritative && readFailure !== undefined;

   // What the editor opens: the record when the host keeps one, the copy when
   // the reader chose to resume it, the package file otherwise. `generation`
   // remounts the builder for a fresh history when that choice changes.
   const [resume, setResume] = useState<boolean | undefined>(undefined);
   const fromDraft = authoritative
      ? draft !== undefined
      : resume === true && draft !== undefined;
   const [opened, setOpened] = useState<
      | {
           source: string;
           document: DashboardDocument;
           conversion?: { from: string; to: string };
           generation: number;
        }
      | undefined
   >(undefined);
   const [openError, setOpenError] = useState<string | undefined>(
      legacyFormat ? LEGACY_REFUSAL : undefined,
   );
   // The package's hash after this editor's last write, as the server computed
   // it: the base the next save is spliced against.
   const savedHashRef = useRef<string | undefined>(undefined);
   // The package file as it stood when the editor last opened a document. The
   // base a save is spliced against is the package's, which is NOT the text
   // the builder holds: a resumed copy was opened from the store, and a
   // version held back while the reader keeps editing has moved the fetch
   // past the file the reader is answering for.
   const packageBaseRef = useRef<string | undefined>(undefined);
   // What this editor last wrote into the package, and the fetch it was
   // written on top of. The write is in the file before the fetch behind
   // `packageText` catches up, so until a fetch actually lands `packageText`
   // is the text the save replaced rather than a version to open. Tied to the
   // fetch and not to the text, so the NEXT fetch speaks for the file whether
   // it carries this editor's write or someone else's.
   const [wrote, setWrote] = useState<
      { text: string; onFetch: number } | undefined
   >(undefined);
   const fetchedAt = modelQuery.dataUpdatedAt;
   const fetchedAtRef = useRef(fetchedAt);
   fetchedAtRef.current = fetchedAt;
   const packageNow =
      wrote !== undefined && wrote.onFetch === fetchedAt
         ? wrote.text
         : packageText;
   const packageNowRef = useRef(packageNow);
   packageNowRef.current = packageNow;
   const latest = fromDraft ? draft : packageNow;

   // Whether the builder holds edits the record does not have, and the version
   // being held back because of them.
   const [dirty, setDirty] = useState(false);
   // A save that can still be undone is held like an edit, until the next edit: remounting on a newer version would drop the offer silently.
   const [undoOffered, setUndoOffered] = useState(false);
   const [accepted, setAccepted] = useState<string | undefined>(undefined);
   // The text on this channel the editor has already reckoned with: what it
   // opened, and what it wrote. Compared against the CHANNEL rather than
   // against the builder, because the two diverge legitimately — a copy saved
   // beside the package leaves the builder ahead of a package that has not
   // moved, and reading that as an incoming version would offer a reader
   // their own work back forever.
   const [seen, setSeen] = useState<string | undefined>(undefined);
   const incoming = latest !== undefined && latest !== seen;
   // What the builder is on. A save does not remount it, so this follows the
   // save rather than the text the builder was opened with.
   const current = opened?.source;
   const holding =
      incoming &&
      (dirty || undoOffered) &&
      current !== undefined &&
      latest !== accepted;
   const opening = holding ? current : incoming ? latest : (current ?? latest);
   const held = holding ? latest : undefined;

   // Read by the open effect so it keeps its single dependency: the guard must
   // not re-run the effect when what it compares against changes.
   const openedSourceRef = useRef(current);
   openedSourceRef.current = current;
   const latestRef = useRef(latest);
   latestRef.current = latest;
   const fromRef = useRef<"package" | "draft" | "record">("package");
   fromRef.current = fromDraft
      ? authoritative
         ? "record"
         : "draft"
      : "package";
   useEffect(() => {
      // Not until the storage answer is in. Opening on the package file while
      // the editor still has no idea whether the host keeps the record would
      // open the wrong document, and report an open of it.
      if (opening === undefined || !draftChecked || blockedOnRecord) return;
      if (opening === openedSourceRef.current) return;
      let stale = false;
      const packageAtOpen = packageNowRef.current;
      const latestAtOpen = latestRef.current;
      void readForEditor(opening)
         .then((result) => {
            if (stale) return;
            if (result.ok === false) {
               setOpenError(result.reason);
               onEventRef.current?.({
                  type: refusedEvent,
                  reason: result.reason,
               });
               return;
            }
            setOpenError(undefined);
            // A different document is open, so what this editor wrote before is
            // no longer the base anything is spliced against; the package file
            // the reader is now answering for is the one current at this open.
            savedHashRef.current = undefined;
            packageBaseRef.current = packageAtOpen;
            setWrote(undefined);
            setAccepted(undefined);
            setSeen(latestAtOpen);
            setOpened((previous) => ({
               source: opening,
               document: result.document,
               ...(result.conversion ? { conversion: result.conversion } : {}),
               generation: (previous?.generation ?? 0) + 1,
            }));
            onEventRef.current?.(
               notebook || result.document.kind === "notebook"
                  ? {
                       type: "notebook.opened",
                       from:
                          fromRef.current === "package" ? "package" : "record",
                       cells: result.document.tiles.length,
                       durationMs: now() - startedAt.current,
                    }
                  : {
                       type: "dashboard.opened",
                       from: fromRef.current,
                       tiles: result.document.tiles.length,
                       durationMs: now() - startedAt.current,
                    },
            );
         })
         .catch((error: unknown) => {
            if (stale) return;
            const reason = `Could not read the ${noun}: ${error instanceof Error ? error.message : String(error)}`;
            setOpenError(reason);
            onEventRef.current?.({ type: refusedEvent, reason });
         });
      return () => {
         stale = true;
      };
   }, [opening, draftChecked, blockedOnRecord, notebook, noun, refusedEvent]);

   useEffect(() => {
      if (legacyFormat)
         onEventRef.current?.({
            type: "notebook.open_refused",
            reason: LEGACY_REFUSAL,
         });
   }, [legacyFormat]);

   const withheld =
      notebook &&
      !opened &&
      draftChecked &&
      !fromDraft &&
      modelQuery.isSuccess &&
      !modelQuery.isFetching &&
      packageText === undefined;
   useEffect(() => {
      if (!withheld) return;
      setOpenError(WITHHELD_REFUSAL);
      onEventRef.current?.({
         type: "notebook.open_refused",
         reason: WITHHELD_REFUSAL,
      });
   }, [withheld]);

   const locator =
      workspace === undefined
         ? undefined
         : locatorFor(
              kind,
              workspace.name,
              environmentName,
              packageName,
              modelPath,
           );
   // Into the host's store: the record when the host says so, and otherwise a
   // copy kept beside the package.
   const saveToStorage = useCallback(
      async (source: string) => {
         if (!storage || !locator)
            throw new Error(
               "This host keeps no documents, so there is nowhere to save.",
            );
         await storage.saveDocument(locator, source);
         // The builder keeps its history and its text; what it holds is now
         // this, not the text it was opened with.
         setOpened((previous) => previous && { ...previous, source });
         setDraft(source);
         // Whenever the copy IS the channel — the record, or a copy the reader
         // resumed — the write moved that channel, and its own text is not an
         // incoming version. A copy written while the package is what is open
         // leaves the package where it was, so nothing there has moved.
         if (authoritative || resume === true) setSeen(source);
         // Saving without choosing is choosing the package file.
         if (!authoritative) setResume((chosen) => chosen ?? false);
      },
      [storage, locator, authoritative, resume],
   );
   // A supersede that did not happen, left where a reader can see it: the copy
   // is still there and will be offered again on the next visit.
   const [supersedeFailure, setSupersedeFailure] = useState<string | undefined>(
      undefined,
   );
   // Into the package itself, when the server takes writes: compile-checked,
   // written atomically and reloaded there, refused if the file changed since
   // it was opened. A copy of the same file kept beside it is superseded by it.
   const saveToPackage = useCallback(
      async (source: string) => {
         // The hash of the package file this editor opened against, never of
         // the latest fetch. A version held back while the reader keeps
         // editing moves the fetch past that file, and a hash taken from it
         // would match, be accepted, and overwrite the change the reader was
         // just told about.
         const expectedHash = await expectedHashFor(
            savedHashRef.current,
            packageBaseRef.current,
         );
         if (expectedHash === undefined)
            throw new Error("The package file is still loading; try again.");
         const modelKey = [
            "dashboard-editor-model",
            environmentName,
            packageName,
            modelPath,
            versionId,
            notebook,
         ];
         await writePackageFile({
            apiClients,
            queryClient,
            environmentName,
            packageName,
            modelPath,
            source,
            expectedHash,
            // A refused write usually means the file moved: fetching it offers the reader that version instead of re-saving against a base the server keeps rejecting.
            invalidateOnError: [modelKey],
            afterWrite: async (contentHash) => {
               setOpened((previous) => previous && { ...previous, source });
               setWrote({ text: source, onFetch: fetchedAtRef.current });
               setSeen(source);
               savedHashRef.current = contentHash;
               setSupersedeFailure(undefined);
               if (storage && locator) {
                  try {
                     await storage.deleteDocument(locator);
                     setDraft(undefined);
                     setOffered(false);
                  } catch (error) {
                     // Only absence means the copy is gone; any other rejection leaves it there, so the state must keep saying so.
                     if (isDocumentNotFound(error)) {
                        setDraft(undefined);
                        setOffered(false);
                     } else setSupersedeFailure(storageErrorMessage(error));
                  }
               }
               // The package is what is open now even if the copy survived the supersede; reading the channel off a stale copy would put the builder back on the text this save replaced.
               setResume(false);
            },
            invalidate: [
               { queryKey: modelKey, wait: true },
               // A cached result is keyed on the query text, not the file, so a changed chart would otherwise be drawn from the old rows.
               {
                  queryKey: modelResultsKey({
                     environmentName,
                     packageName,
                     versionId,
                     modelPath,
                  }),
               },
               { queryKey: ["dashboard-editor-manifest"] },
               { queryKey: ["dashboards"] },
               { queryKey: ["dashboard"] },
               // The notebook viewer keys its read on the resource URI.
               ...(notebook
                  ? [
                       {
                          queryKey: [
                             encodeResourceUri({
                                environmentName,
                                packageName,
                                modelPath,
                             }),
                          ],
                       },
                    ]
                  : []),
            ],
         });
      },
      [
         apiClients,
         environmentName,
         packageName,
         modelPath,
         versionId,
         notebook,
         storage,
         locator,
         queryClient,
      ],
   );
   const canWriteWorkspace = workspace?.writeable === true;
   // Unknown while `/status` loads, and a package write needs a yes.
   const takesWrites = mutable === true;
   const { savesTo, pinnedPackageSave, writer } = saveTarget({
      authoritative,
      mutable: takesWrites,
      ...(versionId !== undefined ? { versionId } : {}),
      // A notebook reads back only the record, so a copy anywhere else would be a write nobody sees.
      canStore:
         !!storage &&
         !!locator &&
         canWriteWorkspace &&
         (!notebook || authoritative),
      readFailed: readFailure !== undefined,
   });
   const save =
      writer === "storage"
         ? saveToStorage
         : writer === "package"
           ? saveToPackage
           : undefined;
   const workspaceName = workspace?.name;
   const reportEvent = useCallback(
      (event: BuilderEvent) => {
         onEventRef.current?.(withWorkspace(event, workspaceName));
      },
      [workspaceName],
   );
   // Stable: the builder fires this from an effect that depends on it.
   const reportDirty = useCallback((value: boolean) => {
      setDirty(value);
      onDirtyChangeRef.current?.(value);
   }, []);
   // An explicit choice is a choice about the very text that would otherwise
   // be held back, so it is never held.
   const choose = (resumeDraft: boolean) => {
      startedAt.current = now();
      setAccepted(resumeDraft ? draft : packageNow);
      setResume(resumeDraft);
   };
   const acceptHeld = () => {
      startedAt.current = now();
      setAccepted(held);
   };

   // After every hook, so the hook order does not depend on the URI. Same
   // reasoning as `Dashboard`'s own check.
   if (!uriNamesBoth && "resourceUri" in props)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            A {noun} resource URI must name an environment and a package.
            Received: {props.resourceUri}
         </Alert>
      );
   if (openError)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            This {noun} cannot be opened in the builder: {openError}
         </Alert>
      );
   // Only before an open: after one, a failed refetch is a banner above the builder so unsaved edits survive it.
   if (modelQuery.isError && !opened)
      return (
         <ApiErrorDisplay
            error={modelQuery.error}
            context={`Opening the ${noun}`}
         />
      );
   if (!opened && (!packageText || !draftChecked))
      // The bar first, so the page it is opening into is already the right
      // shape: the reader's view had a bar in this spot, and a spinner where
      // the bar was made the switch look like a page reload.
      return (
         <Stack sx={{ gap: 2 }}>
            <DashboardBar />
            <Loading text={`Opening the ${noun}…`} />
         </Stack>
      );
   // Where the host's copy IS the document, a copy that could not be read
   // leaves nothing safe to edit: the package file is a deploy of the record,
   // so opening it and arming Save would publish it over the record.
   if (blockedOnRecord)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            This {noun} cannot be opened: {readFailure}
         </Alert>
      );
   const draftDiffers = draft !== undefined && draft !== packageText;
   return (
      <Stack sx={{ gap: 2 }}>
         {modelQuery.isError && (
            <Alert severity="warning">
               The {noun} could not be re-read from the server:{" "}
               {modelQuery.error?.message}. Your edits are still here.
            </Alert>
         )}
         {!notebook &&
            !authoritative &&
            offered &&
            draftDiffers &&
            resume === undefined && (
               <Alert
                  severity="info"
                  action={
                     <Stack direction="row" sx={{ gap: 1 }}>
                        <Button size="small" onClick={() => choose(true)}>
                           Resume
                        </Button>
                        <Button size="small" onClick={() => choose(false)}>
                           Start from the package
                        </Button>
                     </Stack>
                  }
               >
                  You have edits to this dashboard saved in this browser that
                  the package does not have.
               </Alert>
            )}
         {held !== undefined && (
            <Alert
               severity="warning"
               action={
                  <Button size="small" onClick={acceptHeld}>
                     Load it
                  </Button>
               }
            >
               {!dirty
                  ? "This dashboard changed since you opened it. Loading the new version drops Undo save."
                  : savesTo === "package"
                    ? "This dashboard changed since you opened it. Your edits are still here; loading the new version replaces them, and until you do, saving is refused."
                    : savesTo === "host"
                      ? "This dashboard changed since you opened it. Your edits are still here; loading the new version replaces them, and saving keeps yours and writes over it."
                      : "The package's copy of this dashboard changed since you opened it. Your edits are still here; loading the new version replaces them."}
            </Alert>
         )}
         {supersedeFailure !== undefined && (
            <Alert severity="warning">
               The file was saved into the package, but the copy kept beside it
               could not be cleared, so it will be offered again:{" "}
               {supersedeFailure}
            </Alert>
         )}
         {opened && (
            <Surface
               key={opened.generation}
               kind={kind}
               environmentName={environmentName}
               packageName={packageName}
               modelPath={modelPath}
               slug={dashboardName}
               modelGivens={
                  (modelQuery.data?.data as { givens?: Given[] } | undefined)
                     ?.givens
               }
               {...(versionId !== undefined ? { versionId } : {})}
               opened={opened}
               onSave={save}
               onDirtyChange={reportDirty}
               onSaveNoticeChange={setUndoOffered}
               savesTo={savesTo}
               {...(onEvent ? { onEvent: reportEvent } : {})}
               {...(onExit ? { onExit } : {})}
               note={
                  !authoritative && mutable === undefined
                     ? isLoadingStatus
                        ? "Checking whether this server takes writes."
                        : "This server did not say whether it takes writes, so Save is off."
                     : saveCaption({
                          authoritative,
                          mutable: takesWrites,
                          pinnedPackageSave,
                          ...(workspace ? { workspace } : {}),
                          ...(readFailure !== undefined ? { readFailure } : {}),
                          ...(versionId !== undefined ? { versionId } : {}),
                       })
               }
            />
         )}
      </Stack>
   );
}

/**
 * The builder under a real control row, with the controls driving the tiles,
 * both following the LIVE document rather than the saved file.
 *
 * Owns the given values for the same reason the reader's `Dashboard` does: the
 * bar and the tiles have to read one set of values, so whatever holds them has
 * to sit above both. The manifest supplies what the file cannot say about the
 * model's own givens, and the set of givens the server can be sent; a control
 * declared here and not yet in the package is written into each tile's query as
 * a literal, so it works the moment it is added.
 */
function Surface({
   kind,
   modelGivens,
   environmentName,
   packageName,
   modelPath,
   slug,
   versionId,
   opened,
   onSave,
   onDirtyChange,
   onSaveNoticeChange,
   onEvent,
   savesTo,
   onExit,
   note,
}: {
   kind: DocumentKind;
   /** The model's own givens, for a cell-format notebook whose manifest the server cannot build until it is saved. */
   modelGivens?: Given[];
   environmentName: string;
   packageName: string;
   modelPath: string;
   slug: string;
   versionId?: string;
   opened: {
      source: string;
      document: DashboardDocument;
      conversion?: { from: string; to: string };
      generation: number;
   };
   onSave?: (source: string) => Promise<void>;
   onDirtyChange: (dirty: boolean) => void;
   onSaveNoticeChange: (showing: boolean) => void;
   onEvent?: (event: BuilderEvent) => void;
   savesTo: SavesTo;
   onExit?: () => void;
   note: string;
}) {
   const { apiClients } = useServer();
   const notebook = kind === "notebook";

   // The server serves a dashboard only once the package file has a tile, so
   // an empty start becomes servable when a save puts the first one there.
   const [served, setServed] = useState(opened.document.tiles.length > 0);
   useEffect(
      () => setServed(opened.document.tiles.length > 0),
      [opened.generation, opened.document],
   );

   const { data, isSuccess, isError } = useQueryWithApiError({
      queryKey: [
         "dashboard-editor-manifest",
         environmentName,
         packageName,
         slug,
         versionId,
         kind,
      ],
      // A layout notebook's manifest rides on its notebook read; one in the cell format has only the tag's starting values until it is saved.
      queryFn: async (): Promise<DashboardManifest> => {
         if (!notebook)
            return (
               await apiClients.dashboards.getDashboard(
                  environmentName,
                  packageName,
                  slug,
                  versionId,
               )
            ).data;
         const read = (
            await apiClients.notebooks.getNotebook(
               environmentName,
               packageName,
               modelPath,
               versionId,
            )
         ).data;
         return (
            read.dashboard ?? {
               ...(read.startingGivens
                  ? { startingGivens: read.startingGivens }
                  : {}),
               ...(read.autorun === undefined ? {} : { autorun: read.autorun }),
            }
         );
      },
      // The server does not serve a dashboard with no tiles, so asking would 404.
      enabled: served,
   });
   const manifest = data;

   // The package's other dashboards, by slug: where a clicked cell can go.
   const { data: dashboardList } = useQueryWithApiError({
      // Shares the listing the page's location lookup already fetches.
      queryKey: ["dashboards", environmentName, packageName, versionId],
      staleTime: 60_000,
      queryFn: () =>
         apiClients.dashboards.listDashboards(
            environmentName,
            packageName,
            versionId,
         ),
      enabled: !notebook,
   });
   const otherDashboards = useMemo(
      () =>
         (dashboardList?.data ?? [])
            .map((d) => d.name)
            .filter((name): name is string => !!name && name !== slug),
      [dashboardList, slug],
   );

   // The catalog: what the package publishes, limited to what this file can
   // see. A tile runs against this file and may read only the package surface,
   // so offering a source from any other file would offer a tile that 404s.
   // Files off the surface are not readable anyway (their model GET is 404).
   const imports = useMemo(() => {
      const dir = modelPath.slice(0, modelPath.lastIndexOf("/") + 1);
      return opened.document.imports.map((imported) => ({
         ...imported,
         path: new URL(
            imported.from,
            `https://malloy.invalid/${dir}`,
         ).pathname.slice(1),
      }));
   }, [opened.document.imports, modelPath]);
   const { data: catalog } = useQueryWithApiError<PackageCatalog>({
      queryKey: [
         "dashboard-editor-catalog",
         environmentName,
         packageName,
         ...imports.map((i) => i.path),
         versionId,
      ],
      queryFn: async () => {
         const listed = (
            await apiClients.models.listModels(
               environmentName,
               packageName,
               versionId,
            )
         ).data.filter(
            // The catalog leaves dashboards out, and this file's own text is already fetched above.
            (m) =>
               m.path &&
               !m.error &&
               !isDashboardModel(m.path) &&
               !m.path.startsWith("notebooks/"),
         );
         // One model that fails to load (a reload racing this fetch, say)
         // costs the catalog that model's sources, not every suggestion.
         const settled = await Promise.allSettled(
            listed.map((m) =>
               apiClients.models
                  .getModel(environmentName, packageName, m.path!, versionId)
                  .then((response) => response.data),
            ),
         );
         const models = settled.flatMap((result) =>
            result.status === "fulfilled" ? [result.value] : [],
         );
         return visibleToDashboard(buildCatalog(models), imports, models);
      },
      enabled: imports.length > 0,
   });

   const [doc, setDoc] = useState(opened.document);
   useEffect(() => setDoc(opened.document), [opened.document]);
   // Only a package write can make the package serve it; a copy in the host's store does not.
   const saveThenServe = useMemo<SaveHandler<DashboardDocument> | undefined>(
      () =>
         onSave && savesTo === "package"
            ? async (
                 source: string,
                 context: SaveContext<DashboardDocument>,
              ) => {
                 await onSave(source);
                 // An undone first save writes back a file with no tile, which the server stops serving.
                 setServed(context.document.tiles.length > 0);
              }
            : onSave,
      [onSave, savesTo],
   );
   const modelSpecs = useMemo(
      () => manifest?.givens ?? (opened.conversion ? (modelGivens ?? []) : []),
      [manifest, opened.conversion, modelGivens],
   );
   const runnable = useMemo(
      () =>
         new Set(
            modelSpecs
               .map((spec) => spec.name)
               .filter((name): name is string => name !== undefined),
         ),
      [modelSpecs],
   );
   const specs = useMemo(
      () => previewGivens(doc, modelSpecs),
      [doc, modelSpecs],
   );
   const { declaredTypes, applied, panel } = useDocumentControls({
      specs,
      loaded: isSuccess,
      startingValues: manifest?.startingGivens,
      documentKey: `${environmentName}/${packageName}/${versionId ?? ""}/${notebook ? "notebook/" : ""}${slug}/edit`,
      autorun: manifest?.autorun !== false,
      environmentName,
      packageName,
      modelPath: manifest?.path,
      versionId,
      documentName: slug,
   });

   const manifestSettled = !served || isSuccess || isError;
   const renderTile = useMemo(
      () =>
         function LiveTile(tile: QueryTile, heading?: TileHeadingSlots) {
            // The bindings a tile runs with come from the manifest; running before it lands queries every tile once unbound and again bound.
            if (!manifestSettled) return <Loading text="Running…" />;
            const query = previewTileQuery(doc, tile, runnable, applied);
            return (
               <DashboardTile
                  environmentName={environmentName}
                  packageName={packageName}
                  versionId={versionId}
                  modelPath={modelPath}
                  tile={query.expression}
                  {...(query.annotation
                     ? { annotation: query.annotation }
                     : {})}
                  label={
                     tile.label ?? tileTitle(`${tile.source} -> ${tile.name}`)
                  }
                  subtitle={tile.subtitle}
                  {...(heading ? { heading } : {})}
                  borderless={tile.borderless}
                  givens={applied}
                  declaredTypes={declaredTypes}
                  givenNames={query.givenNames}
                  filterLabels={tileFilterLabels(query.givenNames, specs)}
                  height={TILE_MAX_HEIGHT}
               />
            );
         },
      [
         doc,
         manifestSettled,
         runnable,
         environmentName,
         packageName,
         versionId,
         modelPath,
         applied,
         declaredTypes,
         specs,
      ],
   );

   return (
      <Stack sx={{ gap: 1 }}>
         <DashboardBuilder
            source={opened.source}
            document={opened.document}
            renderTile={renderTile}
            givens={specs
               .filter((spec) => spec.name !== undefined)
               .map((spec) => ({
                  name: spec.name as string,
                  ...(spec.label ? { label: spec.label } : {}),
                  ...(spec.type ? { type: spec.type } : {}),
                  ...(spec.suggest?.dimension
                     ? { field: spec.suggest.dimension }
                     : {}),
               }))}
            onChange={setDoc}
            onDirtyChange={onDirtyChange}
            onSaveNoticeChange={onSaveNoticeChange}
            {...(opened.conversion ? { conversion: opened.conversion } : {})}
            {...(catalog ? { catalog } : {})}
            dashboards={otherDashboards}
            {...(onEvent ? { onEvent } : {})}
            {...(onExit ? { onExit } : {})}
            controls={
               isSuccess ? <GivensPanel {...panel} layout="bar" /> : undefined
            }
            {...(saveThenServe ? { onSave: saveThenServe } : {})}
            savesTo={savesTo}
         />
         <Box sx={{ px: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
               {note}
            </Typography>
         </Box>
      </Stack>
   );
}
