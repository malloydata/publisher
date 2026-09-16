// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Button, Stack, Typography } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { DashboardTile, tileTitle } from "../Dashboard/DashboardTile";
import { now, type DashboardEventHandler } from "../Dashboard/telemetry";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import {
   useOptionalDocumentStorage,
   type DocumentLocator,
} from "../DocumentStorage";
import { GivensPanel } from "../given";
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
 * SAVING goes to the package on a server that takes writes, and otherwise to
 * the host's {@link DocumentStorage} — the Console's default is this browser,
 * where the copy is offered back on the next visit. A host with neither still
 * gets the editor, without Save.
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
   /** Leave the editor: the host's "Done". Absent, no Done button. */
   onExit?: () => void;
   /**
    * What the editor does — opened, saved, refused — for the host to log or
    * count; see `DashboardEvent`.
    */
   onEvent?: DashboardEventHandler;
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
}: DashboardEditorProps) {
   const { apiClients, mutable } = useServer();
   const queryClient = useQueryClient();
   // When the editor was asked for — or the reader chose what to open — so
   // "opened" can say how long it took. Read through refs by the open effect,
   // so a host's handler changing identity does not re-open the document.
   const startedAt = useRef(now());
   const onEventRef = useRef(onEvent);
   onEventRef.current = onEvent;
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
   // The hash of the package file as opened: what a save into the package
   // hands back as `expectedHash`, so a copy someone else changed in the
   // meantime is refused rather than overwritten.
   const [packageHash, setPackageHash] = useState<string | undefined>(
      undefined,
   );
   useEffect(() => {
      if (packageText === undefined) return;
      let stale = false;
      void sha256Hex(packageText).then((hash) => {
         if (!stale) setPackageHash(hash);
      });
      return () => {
         stale = true;
      };
   }, [packageText]);

   // The host's copy, if one was saved earlier: offered, never assumed.
   const [workspace, setWorkspace] = useState<string | undefined>(undefined);
   const [draft, setDraft] = useState<string | undefined>(undefined);
   const [draftChecked, setDraftChecked] = useState(storage === undefined);
   // Whether a draft was there when the editor opened: only that one is
   // offered. A copy this session saves is not "edits from an earlier visit".
   const [offered, setOffered] = useState(false);
   useEffect(() => {
      if (!storage) return;
      let stale = false;
      (async () => {
         const writeable = await storage.listWorkspaces(true);
         const name = writeable[0]?.name;
         if (stale) return;
         setWorkspace(name);
         if (name === undefined) {
            setDraftChecked(true);
            return;
         }
         const text = await storage
            .getDocument(
               dashboardLocator(name, environmentName, packageName, modelPath),
            )
            .catch(() => undefined);
         if (!stale) {
            setDraft(text);
            setOffered(text !== undefined);
            setDraftChecked(true);
         }
      })();
      return () => {
         stale = true;
      };
   }, [storage, environmentName, packageName, modelPath]);

   // What the editor opens: the draft when the reader chose to resume it, the
   // package file otherwise. `generation` remounts the builder for a fresh
   // history when that choice changes.
   const [resume, setResume] = useState<boolean | undefined>(undefined);
   const fromDraft = resume === true && draft !== undefined;
   const opening = fromDraft ? draft : packageText;
   const fromRef = useRef<"package" | "draft">("package");
   fromRef.current = fromDraft ? "draft" : "package";
   const [opened, setOpened] = useState<
      | { source: string; document: DashboardDocument; generation: number }
      | undefined
   >(undefined);
   const [openError, setOpenError] = useState<string | undefined>(undefined);
   // The text the builder last saved into the package. When that text comes
   // back from the server it is not a new document to open — the builder
   // already holds it, with its history — so the open effect leaves it be.
   const savedRef = useRef<string | undefined>(undefined);
   useEffect(() => {
      if (opening === undefined || opening === savedRef.current) return;
      let stale = false;
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
   }, [opening]);

   const locator =
      workspace === undefined
         ? undefined
         : dashboardLocator(workspace, environmentName, packageName, modelPath);
   const saveToBrowser = useCallback(
      async (source: string) => {
         if (!storage || !locator)
            throw new Error(
               "This host keeps no documents, so there is nowhere to save.",
            );
         await storage.saveDocument(locator, source);
         setDraft(source);
         // Saving without choosing is choosing the package file.
         setResume((chosen) => chosen ?? false);
      },
      [storage, locator],
   );
   // Into the package itself, when the server takes writes: compile-checked,
   // written atomically and reloaded there, refused if the file changed since
   // it was opened. A browser draft of the same file is superseded by it.
   const saveToPackage = useCallback(
      async (source: string) => {
         if (packageHash === undefined)
            throw new Error("The package file is still loading; try again.");
         let result;
         try {
            result = await apiClients.models.updateModelSource(
               environmentName,
               packageName,
               modelPath,
               { source, expectedHash: packageHash },
            );
         } catch (error) {
            throw new Error(apiErrorMessage(error));
         }
         savedRef.current = source;
         setPackageHash(result.data.contentHash);
         if (storage && locator) {
            await storage.deleteDocument(locator).catch(() => undefined);
            setDraft(undefined);
            setOffered(false);
         }
         setResume((chosen) => chosen ?? false);
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
         packageHash,
         storage,
         locator,
         queryClient,
      ],
   );
   const save = mutable
      ? saveToPackage
      : storage && locator
        ? saveToBrowser
        : undefined;
   const choose = (resumeDraft: boolean) => {
      startedAt.current = now();
      setResume(resumeDraft);
   };

   if (modelQuery.isError)
      return (
         <ApiErrorDisplay
            error={modelQuery.error}
            context="Opening the dashboard"
         />
      );
   if (!packageText || !draftChecked)
      return <Loading text="Opening the dashboard…" />;
   if (openError)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            This dashboard cannot be opened in the builder: {openError}
         </Alert>
      );

   const draftDiffers = draft !== undefined && draft !== packageText;
   return (
      <Stack sx={{ gap: 2 }}>
         {offered && draftDiffers && resume === undefined && (
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
         {opened && (
            <Surface
               key={opened.generation}
               environmentName={environmentName}
               packageName={packageName}
               modelPath={modelPath}
               slug={dashboardName}
               opened={opened}
               onSave={save}
               {...(onEvent ? { onEvent } : {})}
               toolbar={
                  onExit && (
                     <Button size="small" onClick={onExit}>
                        Done
                     </Button>
                  )
               }
               note={
                  mutable
                     ? "Save writes the file into the package."
                     : storage
                       ? "Saved in this browser: this server does not take writes."
                       : "This server does not take writes."
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
   environmentName,
   packageName,
   modelPath,
   slug,
   opened,
   onSave,
   onEvent,
   toolbar,
   note,
}: {
   environmentName: string;
   packageName: string;
   modelPath: string;
   slug: string;
   opened: { source: string; document: DashboardDocument; generation: number };
   onSave?: (source: string) => Promise<void>;
   onEvent?: DashboardEventHandler;
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
            {...(catalog ? { catalog } : {})}
            dashboards={otherDashboards}
            {...(onEvent ? { onEvent } : {})}
            toolbar={toolbar}
            controls={
               isSuccess ? <GivensPanel {...panel} layout="bar" /> : undefined
            }
            {...(onSave ? { onSave } : {})}
         />
         <Box sx={{ px: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
               {note}
            </Typography>
         </Box>
      </Stack>
   );
}

/** The server's own reason for a refused write, when it gave one. */
function apiErrorMessage(error: unknown): string {
   const data = (error as { response?: { data?: { message?: string } } })
      .response?.data;
   if (data?.message) return data.message;
   return error instanceof Error ? error.message : String(error);
}
