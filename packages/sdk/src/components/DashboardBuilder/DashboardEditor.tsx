// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CheckIcon from "@mui/icons-material/Check";
import { Alert, Box, Button, Stack, Typography } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { DashboardTile, tileTitle } from "../Dashboard/DashboardTile";
import {
   now,
   type DashboardEvent,
   type DashboardEventHandler,
} from "../Dashboard/telemetry";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import {
   isDocumentNotFound,
   useOptionalDocumentStorage,
   type DocumentLocator,
   type Workspace,
} from "../DocumentStorage";
import { GivensPanel } from "../given";
import { SecondaryButton } from "../buttons";
import { DashboardBar } from "../Dashboard/DashboardBar";
import { Loading } from "../Loading";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { useServer } from "../ServerProvider";
import { buildCatalog, type PackageCatalog } from "./catalog";
import { DashboardBuilder } from "./DashboardBuilder";
import type { DashboardDocument } from "./document";
import { previewGivens, previewTileQuery } from "./preview";
import { readDashboardDocument, readFailed } from "./readDocument";
import { sha256Hex } from "../../utils/sha256";

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
export interface DashboardEditorProps {
   environmentName: string;
   packageName: string;
   /** The dashboard's slug: `overview`, not `dashboards/overview.malloy`. */
   dashboardName: string;
   /** Leave the editor: the host's "Done editing". Absent, no such button. */
   onExit?: () => void;
   /**
    * What the editor does — opened, saved, refused — for the host to log or
    * count; see `DashboardEvent`.
    */
   onEvent?: DashboardEventHandler;
   /**
    * Whether there are edits the record does not have, on every change and
    * whenever the editor opens a document.
    *
    * For a host that owns the way out: a route change, a tab close, its own
    * "are you sure". The editor will not discard unsaved work on its own, so
    * without this a host cannot tell whether leaving costs anything.
    */
   onDirtyChange?: (dirty: boolean) => void;
}

/** The Console's key for a dashboard's copy; see the storage seam's locator rule. */
const dashboardLocator = (
   workspace: string,
   environmentName: string,
   packageName: string,
   modelPath: string,
): DocumentLocator => ({
   workspace,
   type: "dashboard",
   path: `${environmentName}/${packageName}/${modelPath}`,
});

export function DashboardEditor({
   environmentName,
   packageName,
   dashboardName,
   onExit,
   onEvent,
   onDirtyChange,
}: DashboardEditorProps) {
   const { apiClients, mutable } = useServer();
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
   const modelPath = `dashboards/${dashboardName}.malloy`;

   // The file as the package has it. `sourceText` rides on the compiled model.
   const modelQuery = useQueryWithApiError({
      queryKey: [
         "dashboard-editor-model",
         environmentName,
         packageName,
         modelPath,
      ],
      queryFn: () =>
         apiClients.models.getModel(environmentName, packageName, modelPath),
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
            const all = await storage.listWorkspaces(false);
            // The record when one declares itself, and otherwise the first
            // writeable one, which is what the editor has always taken.
            const chosen =
               all.find((candidate) => candidate.authoritative) ??
               all.find((candidate) => candidate.writeable);
            if (stale) return;
            setWorkspace(chosen);
            if (chosen !== undefined) {
               const text = await storage
                  .getDocument(
                     dashboardLocator(
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
   }, [storage, environmentName, packageName, modelPath]);

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
      | { source: string; document: DashboardDocument; generation: number }
      | undefined
   >(undefined);
   const [openError, setOpenError] = useState<string | undefined>(undefined);
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
      incoming && dirty && current !== undefined && latest !== accepted;
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
      void readDashboardDocument(opening).then((result) => {
         if (stale) return;
         if (readFailed(result)) {
            const reason = result.line
               ? `${result.reason} (line ${result.line})`
               : result.reason;
            setOpenError(reason);
            onEventRef.current?.({ type: "dashboard.open_refused", reason });
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
            generation: (previous?.generation ?? 0) + 1,
         }));
         onEventRef.current?.({
            type: "dashboard.opened",
            from: fromRef.current,
            tiles: result.document.tiles.length,
            durationMs: now() - startedAt.current,
         });
      });
      return () => {
         stale = true;
      };
   }, [opening, draftChecked, blockedOnRecord]);

   const locator =
      workspace === undefined
         ? undefined
         : dashboardLocator(
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
         const base = packageBaseRef.current;
         const expectedHash =
            savedHashRef.current ??
            (base === undefined ? undefined : await sha256Hex(base));
         if (expectedHash === undefined)
            throw new Error("The package file is still loading; try again.");
         let result;
         try {
            result = await apiClients.models.updateModelSource(
               environmentName,
               packageName,
               modelPath,
               { source, expectedHash },
            );
         } catch (error) {
            // A refused write usually means the file moved. Fetch it, so the
            // reader is offered that version rather than left re-saving
            // against a base the server will go on rejecting.
            void queryClient.invalidateQueries({
               queryKey: [
                  "dashboard-editor-model",
                  environmentName,
                  packageName,
                  modelPath,
               ],
            });
            throw new Error(apiErrorMessage(error));
         }
         setOpened((previous) => previous && { ...previous, source });
         setWrote({ text: source, onFetch: fetchedAtRef.current });
         setSeen(source);
         savedHashRef.current = result.data.contentHash;
         setSupersedeFailure(undefined);
         if (storage && locator) {
            try {
               await storage.deleteDocument(locator);
               setDraft(undefined);
               setOffered(false);
            } catch (error) {
               // Only absence means the copy is gone. Any other rejection
               // leaves it there, so the state must keep saying so.
               if (isDocumentNotFound(error)) {
                  setDraft(undefined);
                  setOffered(false);
               } else setSupersedeFailure(storageErrorMessage(error));
            }
         }
         // The package is what is open now, even if the copy beside it
         // survived the supersede: reading the channel off a stale copy would
         // put the builder back on the text this save replaced.
         setResume(false);
         // The package changed: the file, the manifest the live view reads,
         // the package's dashboards list, and the dashboard the reader sees.
         await queryClient.invalidateQueries({
            queryKey: [
               "dashboard-editor-model",
               environmentName,
               packageName,
               modelPath,
            ],
         });
         for (const key of [
            "dashboard-editor-manifest",
            "dashboards",
            "dashboard",
         ])
            void queryClient.invalidateQueries({ queryKey: [key] });
      },
      [
         apiClients,
         environmentName,
         packageName,
         modelPath,
         storage,
         locator,
         queryClient,
      ],
   );
   // `authoritative` wins outright rather than breaking a tie: a host whose
   // store is the record may well sit on a server that reports itself
   // writable, and writing the package there would edit a deploy of the
   // record instead of the record.
   const savesTo = authoritative ? "host" : mutable ? "package" : "browser";
   const canWriteWorkspace = workspace?.writeable === true;
   const writer = authoritative
      ? storage && locator && canWriteWorkspace
         ? saveToStorage
         : undefined
      : mutable
        ? saveToPackage
        : storage && locator && canWriteWorkspace
          ? saveToStorage
          : undefined;
   // A copy that could not be read is not a copy that is not there. Saving on
   // that belief is what rewinds the record, so Save is off until a reader can
   // be told what actually happened.
   const save = readFailure === undefined ? writer : undefined;
   const workspaceName = workspace?.name;
   const reportEvent = useCallback(
      (event: DashboardEvent) => {
         // Only where a workspace actually took the write: a save into the
         // package was not taken by one, and naming it there would say the
         // record moved somewhere it did not.
         onEventRef.current?.(
            event.type === "dashboard.saved" &&
               event.where !== "package" &&
               workspaceName !== undefined
               ? { ...event, workspace: workspaceName }
               : event,
         );
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

   if (modelQuery.isError)
      return (
         <ApiErrorDisplay
            error={modelQuery.error}
            context="Opening the dashboard"
         />
      );
   if (!packageText || !draftChecked)
      // The bar first, so the page it is opening into is already the right
      // shape: the reader's view had a bar in this spot, and a spinner where
      // the bar was made the switch look like a page reload.
      return (
         <Stack sx={{ gap: 2 }}>
            <DashboardBar />
            <Loading text="Opening the dashboard…" />
         </Stack>
      );
   // Where the host's copy IS the document, a copy that could not be read
   // leaves nothing safe to edit: the package file is a deploy of the record,
   // so opening it and arming Save would publish it over the record.
   if (blockedOnRecord)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            This dashboard cannot be opened: {readFailure}
         </Alert>
      );
   if (openError)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            This dashboard cannot be opened in the builder: {openError}
         </Alert>
      );

   const draftDiffers = draft !== undefined && draft !== packageText;
   return (
      <Stack sx={{ gap: 2 }}>
         {!authoritative && offered && draftDiffers && resume === undefined && (
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
               You have edits to this dashboard saved in this browser that the
               package does not have.
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
               {savesTo === "package"
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
               environmentName={environmentName}
               packageName={packageName}
               modelPath={modelPath}
               slug={dashboardName}
               opened={opened}
               onSave={save}
               onDirtyChange={reportDirty}
               savesTo={savesTo}
               {...(onEvent ? { onEvent: reportEvent } : {})}
               toolbar={
                  onExit && (
                     <SecondaryButton
                        label="Done editing"
                        icon={<CheckIcon />}
                        onClick={onExit}
                     />
                  )
               }
               note={caption({
                  authoritative,
                  mutable,
                  ...(workspace ? { workspace } : {}),
                  ...(readFailure !== undefined ? { readFailure } : {}),
               })}
            />
         )}
      </Stack>
   );
}

/**
 * What the toolbar says about where Save goes, in the backend's own words
 * wherever it has any: a workspace carries a `description` precisely so the
 * editor does not have to guess, and "this browser" is one host's answer
 * rather than the interface's.
 */
function caption({
   authoritative,
   mutable,
   workspace,
   readFailure,
}: {
   authoritative: boolean;
   mutable: boolean;
   workspace?: Workspace;
   readFailure?: string;
}): string {
   if (readFailure !== undefined)
      return `The saved copy could not be read, so Save is off: ${readFailure}`;
   if (authoritative && workspace)
      return workspace.writeable
         ? workspace.description
         : `${workspace.description}: you cannot save into it.`;
   if (mutable) return "Save writes the file into the package.";
   if (workspace)
      return `${workspace.description}: this server does not take writes.`;
   return "This server does not take writes.";
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
   environmentName,
   packageName,
   modelPath,
   slug,
   opened,
   onSave,
   onDirtyChange,
   onEvent,
   savesTo,
   toolbar,
   note,
}: {
   environmentName: string;
   packageName: string;
   modelPath: string;
   slug: string;
   opened: { source: string; document: DashboardDocument; generation: number };
   onSave?: (source: string) => Promise<void>;
   onDirtyChange: (dirty: boolean) => void;
   onEvent?: DashboardEventHandler;
   savesTo: "package" | "browser" | "host";
   toolbar: React.ReactNode;
   note: string;
}) {
   const { apiClients } = useServer();

   const { data, isSuccess } = useQueryWithApiError({
      queryKey: [
         "dashboard-editor-manifest",
         environmentName,
         packageName,
         slug,
      ],
      queryFn: () =>
         apiClients.dashboards.getDashboard(
            environmentName,
            packageName,
            slug,
            undefined,
         ),
   });
   const manifest = data?.data;

   // The package's other dashboards, by slug: where a clicked cell can go.
   const { data: dashboardList } = useQueryWithApiError({
      queryKey: ["dashboard-editor-dashboards", environmentName, packageName],
      queryFn: () =>
         apiClients.dashboards.listDashboards(
            environmentName,
            packageName,
            undefined,
         ),
   });
   const otherDashboards = useMemo(
      () =>
         (dashboardList?.data ?? [])
            .map((d) => d.name)
            .filter((name): name is string => !!name && name !== slug),
      [dashboardList, slug],
   );

   // The catalog: the models this file imports, which is where its tiles'
   // sources and their fields are declared.
   const importPaths = useMemo(() => {
      const dir = modelPath.slice(0, modelPath.lastIndexOf("/") + 1);
      const resolved = new Set<string>();
      for (const imported of opened.document.imports) {
         const url = new URL(imported.from, `https://malloy.invalid/${dir}`);
         resolved.add(url.pathname.slice(1));
      }
      return [...resolved];
   }, [opened.document.imports, modelPath]);
   const { data: catalog } = useQueryWithApiError<PackageCatalog>({
      queryKey: [
         "dashboard-editor-catalog",
         environmentName,
         packageName,
         ...importPaths,
      ],
      queryFn: async () => {
         const models = await Promise.all(
            importPaths.map((path) =>
               apiClients.models
                  .getModel(environmentName, packageName, path)
                  .then((response) => response.data),
            ),
         );
         return buildCatalog(models);
      },
      enabled: importPaths.length > 0,
   });

   const [doc, setDoc] = useState(opened.document);
   useEffect(() => setDoc(opened.document), [opened.document]);
   const modelSpecs = useMemo(() => manifest?.givens ?? [], [manifest]);
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
      documentKey: `${environmentName}/${packageName}/${slug}/edit`,
      autorun: manifest?.autorun !== false,
      environmentName,
      packageName,
      modelPath: manifest?.path,
      documentName: slug,
   });

   const renderTile = useMemo(
      () =>
         function LiveTile(tile: DashboardDocument["tiles"][number]) {
            const query = previewTileQuery(doc, tile, runnable, applied);
            return (
               <DashboardTile
                  environmentName={environmentName}
                  packageName={packageName}
                  modelPath={modelPath}
                  tile={query.expression}
                  label={
                     tile.label ?? tileTitle(`${tile.source} -> ${tile.name}`)
                  }
                  subtitle={tile.subtitle}
                  borderless={tile.borderless}
                  givens={applied}
                  declaredTypes={declaredTypes}
                  givenNames={query.givenNames}
                  height={TILE_MAX_HEIGHT}
               />
            );
         },
      [
         doc,
         runnable,
         environmentName,
         packageName,
         modelPath,
         applied,
         declaredTypes,
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
            {...(catalog ? { catalog } : {})}
            dashboards={otherDashboards}
            {...(onEvent ? { onEvent } : {})}
            toolbar={toolbar}
            controls={
               isSuccess ? <GivensPanel {...panel} layout="bar" /> : undefined
            }
            {...(onSave ? { onSave } : {})}
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

/** What a storage backend said went wrong, for a reader who has to act on it. */
function storageErrorMessage(error: unknown): string {
   return error instanceof Error ? error.message : String(error);
}

/** The server's own reason for a refused write, when it gave one. */
function apiErrorMessage(error: unknown): string {
   const data = (error as { response?: { data?: { message?: string } } })
      .response?.data;
   if (data?.message) return data.message;
   return error instanceof Error ? error.message : String(error);
}
