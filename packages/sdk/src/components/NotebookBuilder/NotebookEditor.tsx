// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CheckIcon from "@mui/icons-material/Check";
import { Alert, Box, Stack, Typography } from "@mui/material";
import { useQueries, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CompiledModel, Given, RawNotebook } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { SecondaryButton } from "../buttons";
import { now } from "../Dashboard/telemetry";
import { buildCatalog } from "../DashboardBuilder/catalog";
import {
   apiErrorMessage,
   chooseWorkspace,
   expectedHashFor,
   saveCaption,
   saveTarget,
   storageErrorMessage,
} from "../DashboardBuilder/documentSession";
import {
   isDocumentNotFound,
   useOptionalDocumentStorage,
   type DocumentLocator,
   type Workspace,
} from "../DocumentStorage";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { importedCatalog, notebookImports } from "./imports";
import { NotebookBuilder } from "./NotebookBuilder";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "./readNotebookSource";
import type { NotebookEvent, NotebookEventHandler } from "./telemetry";

/**
 * The notebook builder opened on a package notebook, with the file's text and
 * its givens from the server, and saving wired to wherever the record is: the
 * host's `authoritative` workspace, else the package on a server that takes
 * writes. Where Save goes is fixed when the notebook opens.
 *
 * Narrower than `DashboardEditor` on purpose: only the record is read from the
 * host's {@link DocumentStorage}, so a workspace that is not the record takes
 * no Save, and a version that lands while editing is not held against the
 * edits. A refused save keeps the edits and says why.
 */
export type NotebookEditorProps = (
   | {
        /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
        resourceUri: string;
        /** The notebook's slug: `overview` for `notebooks/overview.malloy`. */
        notebook: string;
     }
   | {
        environmentName: string;
        packageName: string;
        /** The notebook's slug: `overview` for `notebooks/overview.malloy`. */
        notebookName: string;
     }
) & {
   /** Leave the editor: the host's "Done editing". Absent, no such button. */
   onExit?: () => void;
   /** What the editor does — opened, saved, refused — for the host to log. */
   onEvent?: NotebookEventHandler;
   /** Whether there are edits the record does not have, on every change and on open. */
   onDirtyChange?: (dirty: boolean) => void;
};

/** The storage key for a notebook's copy; see the storage seam's locator rule. */
const notebookLocator = (
   workspace: string,
   environmentName: string,
   packageName: string,
   modelPath: string,
): DocumentLocator => ({
   workspace,
   type: "notebook",
   path: `${environmentName}/${packageName}/${modelPath}`,
});

const LEGACY_REFUSAL =
   "a .malloynb notebook is read, not edited. Fix: rewrite it as a `.malloy` notebook under notebooks/ to edit it here.";

const WITHHELD_REFUSAL =
   "the server did not send this notebook's text, so there is nothing here to edit. Fix: edit the file in the package.";

/** Where an open document saves: the record in a named workspace, or the package. */
interface Route {
   authoritative: boolean;
   workspace?: string;
}

const sameRoute = (a: Route, b: Route) =>
   a.authoritative === b.authoritative &&
   (!a.authoritative || a.workspace === b.workspace);

/** What the host's store answered about this notebook. */
interface StorageAnswer {
   workspace?: Workspace;
   /** The record's text, when the workspace is the record and holds a copy. */
   record?: string;
   /** A read the backend could not answer, which is not the same as no document. */
   readFailure?: string;
}

export function NotebookEditor(props: NotebookEditorProps) {
   const { onExit, onEvent, onDirtyChange } = props;
   // A render body must not throw, and `parseResourceUri` does on a non-`publisher://` string.
   const parsed = (() => {
      if (!("resourceUri" in props)) return undefined;
      try {
         return parseResourceUri(props.resourceUri);
      } catch {
         return undefined;
      }
   })();
   const environmentName =
      "resourceUri" in props
         ? (parsed?.environmentName ?? "")
         : props.environmentName;
   const packageName =
      "resourceUri" in props ? (parsed?.packageName ?? "") : props.packageName;
   const slug = "resourceUri" in props ? props.notebook : props.notebookName;
   const versionId = parsed?.versionId;
   const uriNamesBoth =
      "resourceUri" in props
         ? !!parsed?.environmentName && !!parsed?.packageName
         : true;

   if (!uriNamesBoth && "resourceUri" in props)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            A notebook resource URI must name an environment and a package.
            Received: {props.resourceUri}
         </Alert>
      );
   // Keyed on the document, so a host that switches notebooks without its own key never saves one into the other.
   return (
      <NotebookSession
         key={JSON.stringify([
            environmentName,
            packageName,
            slug,
            versionId ?? "",
         ])}
         environmentName={environmentName}
         packageName={packageName}
         slug={slug}
         {...(versionId !== undefined ? { versionId } : {})}
         {...(onExit ? { onExit } : {})}
         {...(onEvent ? { onEvent } : {})}
         {...(onDirtyChange ? { onDirtyChange } : {})}
      />
   );
}

/** One notebook's session: its read, its storage answer and its save bookkeeping, for this document only. */
function NotebookSession({
   environmentName,
   packageName,
   slug,
   versionId,
   onExit,
   onEvent,
   onDirtyChange,
}: {
   environmentName: string;
   packageName: string;
   slug: string;
   versionId?: string;
   onExit?: () => void;
   onEvent?: NotebookEventHandler;
   onDirtyChange?: (dirty: boolean) => void;
}) {
   const legacy = /\.malloynb$/i.test(slug);
   const modelPath = `notebooks/${slug}.malloy`;

   const { apiClients, mutable, isLoadingStatus } = useServer();
   const queryClient = useQueryClient();
   const startedAt = useRef(now());
   const onEventRef = useRef(onEvent);
   onEventRef.current = onEvent;
   const onDirtyChangeRef = useRef(onDirtyChange);
   onDirtyChangeRef.current = onDirtyChange;
   const storage = useOptionalDocumentStorage()?.documentStorage;

   const modelKey = useMemo(
      () => [
         "notebook-editor-model",
         environmentName,
         packageName,
         modelPath,
         versionId,
      ],
      [environmentName, packageName, modelPath, versionId],
   );
   const modelQuery = useQueryWithApiError({
      queryKey: modelKey,
      // An authoring read: a curated package otherwise withholds the text of a notebook reading an off-surface source.
      queryFn: () =>
         apiClients.models.getModel(
            environmentName,
            packageName,
            modelPath,
            versionId,
            true,
         ),
      enabled: !legacy,
   });
   const model = modelQuery.data?.data as
      | { sourceText?: string; givens?: Given[] }
      | undefined;
   // The notebook's own compiled model, already fetched above. A curated package leaves what the notebook imports out of its sources, so those are read separately, and only once someone looks at the choices.
   const compiled = modelQuery.data?.data;
   const catalogSources = useMemo(
      () => (compiled ? buildCatalog([compiled]).sources : undefined),
      [compiled],
   );
   // Only a fetch since this mount that succeeded: a failed one keeps the cached data, which is the copy a remount must not open.
   const fetched = modelQuery.isFetchedAfterMount && modelQuery.isSuccess;
   const packageText = fetched ? model?.sourceText : undefined;

   // The viewer's read and cache key: the artifact tag's starting givens and autorun, as the server derives them.
   const notebookUri = environmentName
      ? encodeResourceUri({
           environmentName,
           packageName,
           modelPath,
           ...(versionId !== undefined ? { versionId } : {}),
        })
      : "";
   const settingsQuery = useQueryWithApiError<RawNotebook>({
      queryKey: [notebookUri],
      queryFn: async () =>
         (
            await apiClients.notebooks.getNotebook(
               environmentName,
               packageName,
               modelPath,
               versionId,
            )
         ).data,
      enabled: !legacy,
   });
   // Latched at the first answer, so a later re-read cannot reset the controls or unmount the builder; a failure opens with the defaults.
   const [settings, setSettings] = useState<
      { startingGivens?: Record<string, string>; autorun: boolean } | undefined
   >(undefined);
   const settingsSettled = settingsQuery.isSuccess || settingsQuery.isError;
   const settingsData = settingsQuery.data;
   useEffect(() => {
      if (settings !== undefined || !settingsSettled) return;
      setSettings({
         ...(settingsData?.startingGivens
            ? { startingGivens: settingsData.startingGivens }
            : {}),
         autorun: settingsData?.autorun !== false,
      });
   }, [settings, settingsSettled, settingsData]);

   const [answer, setAnswer] = useState<StorageAnswer | undefined>(
      storage === undefined ? {} : undefined,
   );
   // A re-read keeps the previous answer, so the save target cannot flip mid-read, and holds Save until it lands.
   const [reading, setReading] = useState(storage !== undefined);
   useEffect(() => {
      if (!storage || legacy) return;
      let stale = false;
      setReading(true);
      (async () => {
         let workspace: Workspace | undefined;
         let next: StorageAnswer;
         try {
            workspace = chooseWorkspace(await storage.listWorkspaces(false));
            // Only the record is read: a copy beside the package is never offered back here.
            const record = workspace?.authoritative
               ? await storage
                    .getDocument(
                       notebookLocator(
                          workspace.name,
                          environmentName,
                          packageName,
                          modelPath,
                       ),
                    )
                    .catch((error: unknown) => {
                       if (isDocumentNotFound(error)) return undefined;
                       throw error;
                    })
               : undefined;
            next = {
               ...(workspace ? { workspace } : {}),
               ...(record !== undefined ? { record } : {}),
            };
         } catch (error) {
            next = {
               ...(workspace ? { workspace } : {}),
               readFailure: storageErrorMessage(error),
            };
         }
         if (stale) return;
         setAnswer(next);
         setReading(false);
      })();
      return () => {
         stale = true;
      };
   }, [storage, legacy, environmentName, packageName, modelPath]);

   const workspace = answer?.workspace;
   const readFailure = answer?.readFailure;
   const authoritative = workspace?.authoritative === true;
   // The package file is a deploy of the record, so it must not stand in for a record that could not be read.
   const blockedOnRecord = authoritative && readFailure !== undefined;
   const fromRecord = authoritative && answer?.record !== undefined;

   // `generation` keys the builder and only an open moves it, the same place the save base is reset.
   const [opened, setOpened] = useState<
      | {
           source: string;
           notebook: NotebookSource;
           from: "package" | "record";
           route: Route;
           generation: number;
        }
      | undefined
   >(undefined);
   const [openError, setOpenError] = useState<string | undefined>(
      legacy ? LEGACY_REFUSAL : undefined,
   );
   // The package file this editor opened against, and the server's hash after its last write there.
   const packageBaseRef = useRef<string | undefined>(undefined);
   const savedHashRef = useRef<string | undefined>(undefined);
   const packageTextRef = useRef(packageText);
   packageTextRef.current = packageText;

   const answered = answer !== undefined && !reading;
   // A record open needs the model only for its givens, not its text, which is just a package write's base.
   const modelSettled = modelQuery.isSuccess || modelQuery.isError;
   const ready =
      answered && (fromRecord ? modelSettled : packageText !== undefined);
   const opening = opened
      ? undefined
      : fromRecord
        ? answer?.record
        : packageText;
   const route: Route = {
      authoritative,
      ...(workspace ? { workspace: workspace.name } : {}),
   };
   const importList = useMemo(
      () => (opened ? notebookImports(opened.notebook, modelPath) : []),
      [opened, modelPath],
   );
   const importPaths = useMemo(
      () => [...new Set(importList.map((i) => i.path))],
      [importList],
   );
   const [wantImports, setWantImports] = useState(false);
   const wantSources = useCallback(() => setWantImports(true), []);
   // One query per imported model: read when the add-query dialog or a chart picker opens, never on load, and cached after.
   const importModels = useQueries({
      queries: importPaths.map((path) => ({
         queryKey: [
            "notebook-editor-import-model",
            environmentName,
            packageName,
            path,
            versionId,
         ],
         queryFn: async () =>
            (
               await apiClients.models.getModel(
                  environmentName,
                  packageName,
                  path,
                  versionId,
                  true,
               )
            ).data,
         enabled: wantImports,
         retry: false,
         staleTime: 5 * 60 * 1000,
         refetchOnWindowFocus: false,
      })),
   });
   // `useQueries` returns a new array each render; its data changes when `dataUpdatedAt` does.
   const importsVersion = importModels.map((q) => q.dataUpdatedAt).join();
   const importedSources = useMemo(
      () =>
         importedCatalog(
            importList,
            new Map(
               importPaths.flatMap((path, i): [string, CompiledModel][] => {
                  const data = importModels[i]?.data;
                  return data ? [[path, data]] : [];
               }),
            ),
         ),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [importList, importPaths, importsVersion],
   );
   const importsPending = wantImports && importModels.some((q) => q.isFetching);
   const routeRef = useRef(route);
   routeRef.current = route;
   useEffect(() => {
      if (opening === undefined || !ready || blockedOnRecord) return;
      let stale = false;
      const packageAtOpen = packageTextRef.current;
      const routeAtOpen = routeRef.current;
      const from = fromRecord ? "record" : "package";
      void readNotebookSource(opening).then((result) => {
         if (stale) return;
         if (notebookSourceRefused(result)) {
            setOpenError(result.refused);
            onEventRef.current?.({
               type: "notebook.open_refused",
               reason: result.refused,
            });
            return;
         }
         setOpenError(undefined);
         packageBaseRef.current = packageAtOpen;
         savedHashRef.current = undefined;
         setOpened((previous) => ({
            source: opening,
            from,
            route: routeAtOpen,
            notebook: result.source,
            generation: (previous?.generation ?? 0) + 1,
         }));
         onEventRef.current?.({
            type: "notebook.opened",
            from,
            cells: result.source.cells.length,
            durationMs: now() - startedAt.current,
         });
      });
      return () => {
         stale = true;
      };
   }, [opening, ready, blockedOnRecord, fromRecord]);

   useEffect(() => {
      if (legacy)
         onEventRef.current?.({
            type: "notebook.open_refused",
            reason: LEGACY_REFUSAL,
         });
   }, [legacy]);

   const withheld =
      !opened &&
      answered &&
      !fromRecord &&
      !blockedOnRecord &&
      fetched &&
      model?.sourceText === undefined;
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
         : notebookLocator(
              workspace.name,
              environmentName,
              packageName,
              modelPath,
           );
   const saveToStorage = useCallback(
      async (source: string) => {
         if (!storage || !locator)
            throw new Error(
               "This host keeps no documents, so there is nowhere to save.",
            );
         await storage.saveDocument(locator, source);
      },
      [storage, locator],
   );
   const saveToPackage = useCallback(
      async (source: string) => {
         const expectedHash = await expectedHashFor(
            savedHashRef.current,
            packageBaseRef.current,
         );
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
            throw new Error(apiErrorMessage(error));
         }
         savedHashRef.current = result.data.contentHash;
         void queryClient.invalidateQueries({ queryKey: modelKey });
         // The viewer keys its notebook on the resource URI.
         void queryClient.invalidateQueries({
            queryKey: [
               encodeResourceUri({ environmentName, packageName, modelPath }),
            ],
         });
      },
      [
         apiClients,
         environmentName,
         packageName,
         modelPath,
         modelKey,
         queryClient,
      ],
   );
   // Unknown while `/status` loads, and a package write needs a yes.
   const takesWrites = mutable === true;
   const { savesTo, pinnedPackageSave, writer } = saveTarget({
      authoritative,
      mutable: takesWrites,
      ...(versionId !== undefined ? { versionId } : {}),
      // Only the record is ever read back, so a copy anywhere else would be a write nobody sees.
      canStore:
         authoritative &&
         !!storage &&
         !!locator &&
         workspace?.writeable === true,
      readFailed: answer === undefined || reading || readFailure !== undefined,
   });
   // A store that changed sides since the open would take text built for the other one.
   const routeMoved =
      opened !== undefined && answered && !sameRoute(opened.route, route);
   const save = routeMoved
      ? undefined
      : writer === "storage"
        ? saveToStorage
        : writer === "package"
          ? saveToPackage
          : undefined;

   const workspaceName = workspace?.name;
   const reportEvent = useCallback(
      (event: NotebookEvent) => {
         // A package save was taken by no workspace, so naming one would misplace the record.
         onEventRef.current?.(
            event.type === "notebook.saved" &&
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
      onDirtyChangeRef.current?.(value);
   }, []);

   const exit = onExit && (
      <SecondaryButton
         label="Done editing"
         icon={<CheckIcon />}
         onClick={onExit}
      />
   );

   if (openError)
      return (
         <Alert severity="info" sx={{ m: 2 }} action={exit}>
            This notebook is read-only here: {openError}
         </Alert>
      );
   // Before an open these stand in for the editor; after one they sit above it, so unsaved edits survive them.
   if (!opened || settings === undefined) {
      if (modelQuery.isError && !fromRecord)
         return (
            <ApiErrorDisplay
               error={modelQuery.error}
               context="Opening the notebook"
            />
         );
      if (blockedOnRecord)
         return (
            <Alert severity="error" sx={{ m: 2 }}>
               This notebook cannot be opened: {readFailure}
            </Alert>
         );
      return <Loading text="Opening the notebook…" />;
   }

   return (
      <Stack sx={{ gap: 1 }}>
         {modelQuery.isError && (
            <Alert severity="warning">
               {opened.from === "record"
                  ? "The package's model, which previews and parameters use, could not be re-read from the server: "
                  : "The notebook could not be re-read from the server: "}
               {modelQuery.error?.message}. Your edits are still here.
            </Alert>
         )}
         {blockedOnRecord && (
            <Alert severity="warning">
               The saved copy could not be re-read, so Save is off:{" "}
               {readFailure}. Your edits are still here.
            </Alert>
         )}
         {routeMoved && (
            <Alert severity="warning">
               Where this notebook saves changed while it was open (it opened
               from{" "}
               {opened.from === "record" ? "the saved copy" : "the package"}
               ), so Save is off. Your edits are still here; reopen the notebook
               to save.
            </Alert>
         )}
         <NotebookBuilder
            key={opened.generation}
            source={opened.source}
            notebook={opened.notebook}
            {...(catalogSources ? { sources: catalogSources } : {})}
            {...(modelQuery.isError && !catalogSources
               ? { sourcesFailed: true }
               : {})}
            importedSources={importedSources}
            {...(importsPending ? { importsPending: true } : {})}
            onSourcesWanted={wantSources}
            environmentName={environmentName}
            packageName={packageName}
            modelPath={modelPath}
            {...(versionId !== undefined ? { versionId } : {})}
            givens={model?.givens ?? []}
            {...(settings.startingGivens
               ? { startingGivens: settings.startingGivens }
               : {})}
            autorun={settings.autorun}
            {...(save ? { onSave: save } : {})}
            onDirtyChange={reportDirty}
            onEvent={reportEvent}
            savesTo={savesTo}
            {...(exit ? { toolbar: exit } : {})}
         />
         <Box sx={{ px: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
               {!authoritative && mutable === undefined
                  ? isLoadingStatus
                     ? "Checking whether this server takes writes."
                     : "This server did not say whether it takes writes, so Save is off."
                  : saveCaption({
                       authoritative,
                       mutable: takesWrites,
                       pinnedPackageSave,
                       // Only the record takes a Save here, so no other workspace is named as where it goes.
                       ...(authoritative && workspace ? { workspace } : {}),
                       ...(readFailure !== undefined ? { readFailure } : {}),
                       ...(versionId !== undefined ? { versionId } : {}),
                    })}
            </Typography>
         </Box>
      </Stack>
   );
}
