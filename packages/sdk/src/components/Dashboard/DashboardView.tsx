// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Stack, Typography } from "@mui/material";
import { useCallback, useMemo, useState } from "react";
import type { DashboardManifest } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import {
   useDrill,
   type DrillBinding,
   type DrillClickPayload,
   type DrillNavigation,
   type DrillRowsRequest,
} from "../drill";
import { GivensPanel } from "../given";
import { givensToParams, givensToRequest } from "../given/paramCodec";
import { Prose } from "../Prose";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { DashboardGrid, DEFAULT_COLUMNS } from "./DashboardGrid";
import { DashboardTile } from "./DashboardTile";
import { ExploreDialog } from "./ExploreDialog";
import { RowsDialog, stepsOf, type RowsRequest } from "./RowsDialog";
import type { DashboardEventHandler } from "./telemetry";
import { TileCard, type TileChrome } from "./TileCard";
import { tileIgnoredFilterLabels } from "./TileFilterTag";

/** Narrowest a tile that sets no `colspan` is allowed to render. */
const MIN_TILE_PX = 240;

export interface DashboardViewProps {
   /** The dashboard, or the tile layout a layout notebook carries. */
   manifest: DashboardManifest;
   environmentName: string;
   packageName: string;
   versionId?: string;
   /** The slug or path the manifest was fetched under, which keys its control edits. */
   documentName: string;
   givens?: Record<string, string>;
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   onNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   height?: number;
   maxResultSize?: number;
   onEvent?: DashboardEventHandler;
   /** `none` renders tiles as a document, with no cards or title block. */
   chrome?: TileChrome;
}

/**
 * A loaded dashboard manifest, drawn: the control row and the tile grid.
 *
 * Split from the manifest fetch so a notebook written as a tile layout, whose
 * manifest arrives inside the notebook response, renders through the same code.
 */
export function DashboardView({
   manifest,
   environmentName,
   packageName,
   versionId,
   documentName,
   givens,
   onGivensChange,
   onNavigate,
   height,
   maxResultSize,
   onEvent,
   chrome = "card",
}: DashboardViewProps) {
   const specs = useMemo(() => manifest.givens ?? [], [manifest]);

   // The control row's state, options and `to=self` drill: the same hook the
   // notebook uses, so a control behaves identically on both surfaces.
   const controls = useDocumentControls({
      specs,
      loaded: true,
      startingValues: manifest.startingGivens,
      params: givens,
      onGivensChange,
      // Version included: two dashboards whose starting values coincide would
      // otherwise look like one document, and the one you came from would keep
      // filtering the one you drilled into.
      documentKey: `${environmentName}/${packageName}/${versionId ?? ""}/${documentName}`,
      // Absent means autorun; only an explicit `autorun=false` batches.
      autorun: manifest.autorun !== false,
      environmentName,
      packageName,
      modelPath: manifest.path,
      versionId,
      documentName,
   });
   const { applied, declaredTypes, canSelf, onSelf } = controls;

   // A given only a gate reads is in a tile's `givenNames` but not the row, so
   // the host's value for it is sent as given: the row would drop it as undeclared.
   const tileGivens = useMemo(() => {
      const named = new Set(
         (manifest.tiles ?? []).flatMap((tile) => tile.givenNames ?? []),
      );
      const hostOnly = Object.entries(givens ?? {}).filter(
         ([name]) => named.has(name) && !declaredTypes.has(name),
      );
      if (hostOnly.length === 0) return applied;
      return new Map<string, GivenValue>([...hostOnly, ...applied]);
   }, [manifest, givens, declaredTypes, applied]);

   // The rows behind a clicked value, and a tile's query in the explorer —
   // the two ways past a number. Composite tiles only: each names its
   // source, which is what the rows are of and what the explorer opens on.
   const [rows, setRows] = useState<RowsRequest | undefined>(undefined);
   const [exploring, setExploring] = useState<string | undefined>(undefined);
   const onRows = useCallback((request: DrillRowsRequest) => {
      const steps = stepsOf(request.context);
      if (steps === undefined) return;
      setRows({
         ...steps,
         field: request.field,
         rawValue: request.rawValue,
         label: request.label,
      });
   }, []);

   // The whole applied row and any gate givens: a source's own `where:` may read any of it, and a
   // given the rows query does not reference is ignored by the server.
   const rowsGivens = useMemo(
      () => givensToRequest(tileGivens, declaredTypes),
      [tileGivens, declaredTypes],
   );

   // The explorer's controls take the URL-string form, not the request form.
   const exploreGivens = useMemo(
      () => givensToParams(applied, declaredTypes),
      [applied, declaredTypes],
   );

   const { drill, drillMenu } = useDrill({
      onNavigate,
      onSelf,
      canSelf,
      selfLabel: "Filter this dashboard",
      onRows,
   });
   // Each tile's clicks carry the tile they came from, so the rows behind a
   // value know which source to run against.
   const drillFor = useCallback(
      (tile: string): DrillBinding => ({
         canDrill: drill.canDrill,
         onClick: (payload: DrillClickPayload) =>
            drill.onClick({ ...payload, context: tile }),
      }),
      [drill],
   );

   // Reachable on the in-process load path only: production aborts the package
   // on the first compile error, so an uncompilable file answers 424 instead.
   if (manifest.error) {
      return (
         <Stack spacing={2}>
            <DashboardHeader manifest={manifest} />
            <Alert severity="error">{manifest.error}</Alert>
         </Stack>
      );
   }

   const modelPath = manifest.path;
   const tiles = manifest.tiles ?? [];
   const columns = manifest.dashboardColumns ?? DEFAULT_COLUMNS;

   return (
      <Stack spacing={2}>
         {chrome === "card" && <DashboardHeader manifest={manifest} />}

         {/* Sticky so the controls stay in reach while the tiles scroll under. */}
         <Box
            sx={{
               position: "sticky",
               top: 0,
               zIndex: 2,
               bgcolor: "background.default",
            }}
         >
            <GivensPanel {...controls.panel} layout="bar" />
         </Box>

         {modelPath === undefined ? (
            <Alert severity="error">
               This dashboard has no model path, so there is nothing to run.
            </Alert>
         ) : manifest.query !== undefined ? (
            // Single-query form: one query whose result IS the dashboard. Its
            // `# dashboard {columns=N}` tag is the renderer's business, so no
            // grid is imposed here: doing so would nest a grid in a grid.
            <DashboardTile
               environmentName={environmentName}
               packageName={packageName}
               versionId={versionId}
               modelPath={modelPath}
               queryName={manifest.query}
               givens={applied}
               declaredTypes={declaredTypes}
               height={height}
               maxResultSize={maxResultSize}
               drill={drill}
               chrome={chrome}
            />
         ) : tiles.length > 0 ? (
            // Composite form: each tile runs on its own and the results are
            // combined into one grid here, since no single Malloy result spans
            // them.
            <DashboardGrid
               tiles={tiles}
               columns={columns}
               minTilePx={MIN_TILE_PX}
               // Position too, not the expression alone: `tiles=[…]` can repeat
               // one, which is a typo rather than a request for two identical
               // panels, and keying on the expression made the duplicate warn
               // and reconcile onto its twin.
               keyOf={(tile, index) =>
                  `${index}:${tile.kind === "text" ? `text:${tile.name}` : tile.query}`
               }
               renderTile={(tile) =>
                  // An absent `kind` is a query tile, as it was before text tiles.
                  tile.kind === "text" || tile.query === undefined ? (
                     <TextTile markdown={tile.markdown ?? ""} chrome={chrome} />
                  ) : (
                     <DashboardTile
                        environmentName={environmentName}
                        packageName={packageName}
                        versionId={versionId}
                        modelPath={modelPath}
                        tile={tile.query}
                        label={tile.label}
                        subtitle={tile.subtitle}
                        borderless={tile.borderless}
                        givens={tileGivens}
                        declaredTypes={declaredTypes}
                        givenNames={tile.givenNames}
                        height={height ?? TILE_MAX_HEIGHT}
                        maxResultSize={maxResultSize}
                        drill={drillFor(tile.query)}
                        chrome={chrome}
                        ignoredFilters={tileIgnoredFilterLabels(
                           tile.givenNames,
                           specs,
                        )}
                        onExplore={() => {
                           setExploring(tile.query);
                           onEvent?.({
                              type: "dashboard.explored",
                              tile: tile.query ?? "",
                           });
                        }}
                     />
                  )
               }
            />
         ) : (
            <Alert severity="warning">
               This dashboard names neither a query nor any tiles.
            </Alert>
         )}

         {drillMenu}
         {modelPath !== undefined && (
            <>
               <RowsDialog
                  request={rows}
                  environmentName={environmentName}
                  packageName={packageName}
                  {...(versionId === undefined ? {} : { versionId })}
                  modelPath={modelPath}
                  givens={rowsGivens}
                  onClose={() => setRows(undefined)}
                  onDone={(ok, durationMs) => {
                     if (rows)
                        onEvent?.({
                           type: "dashboard.rows_shown",
                           source: rows.source,
                           view: rows.view,
                           field: rows.field,
                           ok,
                           durationMs,
                        });
                  }}
               />
               <ExploreDialog
                  tile={exploring}
                  environmentName={environmentName}
                  packageName={packageName}
                  {...(versionId === undefined ? {} : { versionId })}
                  modelPath={modelPath}
                  givens={exploreGivens}
                  onClose={() => setExploring(undefined)}
               />
            </>
         )}
      </Stack>
   );
}

/** A text tile: its markdown and nothing else, no heading. */
function TextTile({
   markdown,
   chrome,
}: {
   markdown: string;
   chrome: TileChrome;
}) {
   return (
      <TileCard chrome={chrome}>
         <Prose variant="document">{markdown}</Prose>
      </TileCard>
   );
}

/**
 * The dashboard's prose header: its title, and the description as MARKDOWN
 * (Malloy carries a `##"` block through with its newlines intact).
 */
function DashboardHeader({ manifest }: { manifest: DashboardManifest }) {
   return (
      <DashboardProse
         title={manifest.title ?? manifest.name}
         {...(manifest.description
            ? { description: manifest.description }
            : {})}
      />
   );
}

/**
 * The prose header over just the two fields it needs, so the BUILDER can draw
 * the same header over a `DashboardDocument`, which is not a manifest.
 */
export function DashboardProse({
   title,
   description,
}: {
   title: string;
   description?: string;
}) {
   return (
      <Box>
         <Typography variant="h5" sx={{ fontWeight: 600 }}>
            {title}
         </Typography>
         {description && <Prose variant="caption">{description}</Prose>}
      </Box>
   );
}
