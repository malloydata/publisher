// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Stack, Typography } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import type { DashboardManifest, Given } from "../../client";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { DashboardTile } from "../Dashboard/DashboardTile";
import {
   TileCard,
   TileHeading,
   type TileChrome,
   type TileHeadingSlots,
} from "../Dashboard/TileCard";
import { tileIgnoredFilterLabels } from "../Dashboard/TileFilterTag";
import { GivensPanel } from "../given";
import { Loading } from "../Loading";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { useServer } from "../ServerProvider";
import { buildCatalog, isDashboardModel, type PackageCatalog } from "./catalog";
import { DashboardBuilder } from "./DashboardBuilder";
import type { DashboardDocument, DocumentKind, QueryTile } from "./document";
import type { SavesTo } from "./documentSession";
import { previewGivens, previewTileQuery, tileExpressionKey } from "./preview";
import type { BuilderEvent } from "./telemetry";
import { tileDisplayTitle } from "./tileDisplayTitle";
import type { SaveContext, SaveHandler } from "./useDocumentEditor";

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
export function EditorSurface({
   kind,
   modelGivens,
   environmentName,
   packageName,
   modelPath,
   slug,
   versionId,
   opened,
   replaces,
   onSave,
   onDirtyChange,
   onExit,
   onEvent,
   savesTo,
   saveLabel,
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
   replaces?: string;
   onSave?: (source: string) => Promise<void>;
   onDirtyChange: (dirty: boolean) => void;
   onExit?: () => void;
   onEvent?: (event: BuilderEvent) => void;
   savesTo: SavesTo;
   saveLabel?: string;
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

   // The catalog: every source the package publishes. A tile runs against
   // this file and may read only the package surface; files off it are not
   // readable anyway (their model GET is 404).
   const { data: catalog } = useQueryWithApiError<PackageCatalog>({
      queryKey: [
         "dashboard-editor-catalog",
         environmentName,
         packageName,
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
         // Every published source: a tile on one the file cannot see yet
         // imports it as it is added.
         return buildCatalog(models);
      },
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
   // What the saved file's compiled tiles read, keyed as the server keys a tile expression.
   const servedReads = useMemo(
      () =>
         new Map(
            (manifest?.tiles ?? []).flatMap((tile) =>
               tile.kind !== "text" && tile.query && tile.givenNames
                  ? [[tileExpressionKey(tile.query), tile.givenNames] as const]
                  : [],
            ),
         ),
      [manifest],
   );
   const renderTile = useMemo(
      () =>
         function LiveTile(
            tile: QueryTile,
            heading?: TileHeadingSlots,
            chrome: TileChrome = "card",
         ) {
            // The bindings a tile runs with come from the manifest; running before it lands queries every tile once unbound and again bound.
            // Until then the tile's own card and heading stand, so nothing pops in when it runs.
            if (!manifestSettled)
               return (
                  <TileCard chrome={chrome}>
                     <TileHeading
                        title={heading?.title ?? tileDisplayTitle(tile)}
                        subtitle={heading ? heading.subtitle : tile.subtitle}
                     />
                     <Loading text="Running…" />
                  </TileCard>
               );
            const query = previewTileQuery(
               doc,
               tile,
               runnable,
               applied,
               servedReads.get(
                  tileExpressionKey(`${tile.source} -> ${tile.name}`),
               ),
            );
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
                  label={tileDisplayTitle(tile)}
                  chrome={chrome}
                  subtitle={tile.subtitle}
                  {...(heading ? { heading } : {})}
                  borderless={tile.borderless}
                  givens={applied}
                  declaredTypes={declaredTypes}
                  givenNames={query.givenNames}
                  ignoredFilters={tileIgnoredFilterLabels(query.reads, specs)}
                  height={TILE_MAX_HEIGHT}
               />
            );
         },
      [
         doc,
         manifestSettled,
         runnable,
         servedReads,
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
            {...(onExit ? { onExit } : {})}
            {...(opened.conversion ? { conversion: opened.conversion } : {})}
            {...(catalog ? { catalog } : {})}
            dashboards={otherDashboards}
            {...(onEvent ? { onEvent } : {})}
            controls={
               isSuccess ? (
                  // The reader's control layout for the kind: a notebook's panel, a dashboard's bar.
                  <GivensPanel {...panel} layout={notebook ? "panel" : "bar"} />
               ) : undefined
            }
            {...(saveThenServe ? { onSave: saveThenServe } : {})}
            savesTo={savesTo}
            {...(saveLabel ? { saveLabel } : {})}
            {...(replaces !== undefined ? { replaces } : {})}
            modelPath={modelPath}
         />
         <Box sx={{ px: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
               {note}
            </Typography>
         </Box>
      </Stack>
   );
}
