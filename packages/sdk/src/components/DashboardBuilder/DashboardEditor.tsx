// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import DownloadIcon from "@mui/icons-material/Download";
import { Alert, Box, Button, Stack, Typography } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useGivensState } from "../../hooks/useGivensState";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useSuggestOptions } from "../../hooks/useSuggestOptions";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { DashboardTile, tileTitle } from "../Dashboard/DashboardTile";
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
import { spliceDashboardDocument, spliceFailed } from "./spliceDocument";

/**
 * The builder, opened on a package dashboard, with everything a host has to
 * supply already wired: the file's text and manifest from the server, a live
 * control row and live tiles that follow the document, a catalog for the
 * filter window's field search, and saving.
 *
 * SAVING is through the host's {@link DocumentStorage}, not to the package.
 * The package dashboard is a read-only origin: editing works on a copy, the
 * copy is saved where the host keeps documents (the Console's default is this
 * browser), and "Export" hands the file back so it can be put in the package.
 * That is the plan's copy-and-export model, and it needs no server write path.
 * A host with no storage still gets the editor, without Save.
 *
 * What that costs is stated in the toolbar: a control added here is live in the
 * editor (its value is written into each tile's query) but reaches the package
 * only when the exported file does.
 */
export interface DashboardEditorProps {
   environmentName: string;
   packageName: string;
   /** The dashboard's slug: `overview`, not `dashboards/overview.malloy`. */
   dashboardName: string;
   /** Leave the editor: the host's "Done". Absent, no Done button. */
   onExit?: () => void;
}

/** The Console's key for a dashboard's copy; see the storage seam's locator rule. */
export const dashboardLocator = (
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
}: DashboardEditorProps) {
   const { apiClients } = useServer();
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

   // The host's copy, if one was saved earlier: offered, never assumed.
   const [workspace, setWorkspace] = useState<string | undefined>(undefined);
   const [draft, setDraft] = useState<string | undefined>(undefined);
   const [draftChecked, setDraftChecked] = useState(storage === undefined);
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
   const opening = resume === true && draft !== undefined ? draft : packageText;
   const [opened, setOpened] = useState<
      | { source: string; document: DashboardDocument; generation: number }
      | undefined
   >(undefined);
   const [openError, setOpenError] = useState<string | undefined>(undefined);
   useEffect(() => {
      if (opening === undefined) return;
      let stale = false;
      void readDashboardDocument(opening).then((result) => {
         if (stale) return;
         if (readFailed(result)) {
            setOpenError(
               result.line
                  ? `${result.reason} (line ${result.line})`
                  : result.reason,
            );
            return;
         }
         setOpenError(undefined);
         setOpened((previous) => ({
            source: opening,
            document: result.document,
            generation: (previous?.generation ?? 0) + 1,
         }));
      });
      return () => {
         stale = true;
      };
   }, [opening]);

   // The text a save would write for the document as it stands: what Export
   // downloads, shown before any save.
   const [previewText, setPreviewText] = useState<string | undefined>(
      undefined,
   );

   const save = useCallback(
      async (source: string) => {
         if (!storage || workspace === undefined)
            throw new Error(
               "This host keeps no documents, so there is nowhere to save.",
            );
         await storage.saveDocument(
            dashboardLocator(
               workspace,
               environmentName,
               packageName,
               modelPath,
            ),
            source,
         );
         setDraft(source);
      },
      [storage, workspace, environmentName, packageName, modelPath],
   );

   const exportFile = useCallback(() => {
      const text = previewText ?? opened?.source;
      if (!text) return;
      const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `${dashboardName}.malloy`;
      anchor.click();
      URL.revokeObjectURL(url);
   }, [previewText, opened, dashboardName]);

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
         {draftDiffers && resume === undefined && (
            <Alert
               severity="info"
               action={
                  <Stack direction="row" sx={{ gap: 1 }}>
                     <Button size="small" onClick={() => setResume(true)}>
                        Resume
                     </Button>
                     <Button size="small" onClick={() => setResume(false)}>
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
               onSave={storage && workspace !== undefined ? save : undefined}
               onPreview={setPreviewText}
               toolbar={
                  <>
                     <Button
                        size="small"
                        startIcon={<DownloadIcon fontSize="small" />}
                        onClick={exportFile}
                     >
                        Export
                     </Button>
                     {onExit && (
                        <Button size="small" onClick={onExit}>
                           Done
                        </Button>
                     )}
                  </>
               }
               note={
                  storage
                     ? "Saved in this browser. Export puts the file in the package."
                     : "Export puts the file in the package."
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
   onPreview,
   toolbar,
   note,
}: {
   environmentName: string;
   packageName: string;
   modelPath: string;
   slug: string;
   opened: { source: string; document: DashboardDocument; generation: number };
   onSave?: (source: string) => Promise<void>;
   onPreview: (text: string) => void;
   toolbar: React.ReactNode;
   note: string;
}) {
   const { apiClients } = useServer();
   const queryClient = useQueryClient();

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
      const dir = modelPath.replace(/[^/]*$/, "");
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
   useEffect(() => {
      let stale = false;
      void spliceDashboardDocument(opened.source, doc).then((result) => {
         if (!stale && !spliceFailed(result)) onPreview(result.source);
      });
      return () => {
         stale = true;
      };
   }, [doc, opened, onPreview]);

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
   const declaredTypes = useMemo(
      () =>
         new Map(
            specs
               .filter((spec) => spec.name !== undefined)
               .map((spec) => [spec.name as string, spec.type]),
         ),
      [specs],
   );
   const { draft, applied, setGiven, reset, apply, pending } = useGivensState({
      declaredTypes,
      startingValues: manifest?.startingGivens,
      documentKey: `${environmentName}/${packageName}/${slug}/edit`,
      autorun: manifest?.autorun !== false,
   });
   const { options, isLoading, failed } = useSuggestOptions(
      environmentName,
      packageName,
      manifest?.path,
      specs,
      undefined,
      { values: applied, declaredTypes },
   );

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
            toolbar={toolbar}
            controls={
               isSuccess ? (
                  <GivensPanel
                     givens={specs}
                     values={draft}
                     onChange={setGiven}
                     onReset={reset}
                     layout="bar"
                     options={options}
                     optionsLoading={isLoading}
                     optionsFailed={failed}
                     apply={
                        manifest?.autorun === false
                           ? { onApply: apply, pending }
                           : undefined
                     }
                  />
               ) : undefined
            }
            {...(onSave
               ? {
                    onSave: async (source: string) => {
                       await onSave(source);
                       // A saved copy may declare controls the live row shows
                       // from the document already; nothing server-side moved.
                       void queryClient.invalidateQueries({
                          queryKey: ["dashboard-editor-manifest"],
                       });
                    },
                 }
               : {})}
         />
         <Box sx={{ px: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
               {note}
            </Typography>
         </Box>
      </Stack>
   );
}
