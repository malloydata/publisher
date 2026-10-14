// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert } from "@mui/material";
import { useCallback, useMemo, useState } from "react";
import type { DashboardManifest } from "../../client";
import type { HostGivenValue } from "../../hooks/givenValue";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import {
   useDrill,
   type DrillBinding,
   type DrillClickPayload,
   type DrillNavigation,
   type DrillRowsRequest,
} from "../drill";
import { GivensPanel, type GivensLayout } from "../given";
import { givensToRequest, withHostGivens } from "../given/paramCodec";
import { Prose } from "../Prose";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { DashboardGrid, GRID_GAP_PX } from "./DashboardGrid";
import { DashboardTile } from "./DashboardTile";
import { RowsDialog, stepsOf, type RowsRequest } from "./RowsDialog";
import { useReportEvents } from "../Report/telemetry";
import type { TileChrome } from "./TileCard";
import { tileIgnoredFilterLabels } from "./TileFilterTag";
import { layoutRules } from "../Report/layoutRules";
import {
   ReportHeader,
   ReportPage,
   StickyControls,
   TextTileFrame,
} from "../Report/ReportPage";

/** Narrowest a tile that sets no `colspan` is allowed to render. */
const MIN_TILE_PX = 240;

/** Re-exported for hosts that import it from here; defined with the page parts. */
export { STICKY_CONTROLS_Z } from "../Report/ReportPage";

export interface DashboardViewProps {
   /** The dashboard, or the tile layout a layout notebook carries. */
   manifest: DashboardManifest;
   environmentName: string;
   packageName: string;
   versionId?: string;
   /** The slug or path the manifest was fetched under, which keys its control edits. */
   documentName: string;
   /**
    * The host's values, typically its URL query parameters. A string drives a
    * control; a value no control declares is sent as given to the tiles that
    * read it, and only there may it be a list.
    */
   givens?: Record<string, HostGivenValue>;
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   onNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   height?: number;
   maxResultSize?: number;
   /**
    * Ignored: kept so existing callers compile. A linear report and a grid
    * draw the same header, cards and controls, and differ only in how tiles
    * are arranged, which the manifest says.
    */
   chrome?: TileChrome;
   /**
    * How the control row is drawn: `bar` for a dashboard, `panel` for a
    * notebook, so a layout notebook's controls match a cell notebook's.
    */
   controlsLayout?: GivensLayout;
   /**
    * Text-source mode: the document's definitions (see `documentPreamble`),
    * sent ahead of each tile's `run:` so every query runs as the viewer's own
    * text. Rows, which reopens a tile by name, is off in this mode.
    */
   preamble?: string;
   /** Text-source mode: the model the text runs on top of, in place of `manifest.path`. */
   runModelPath?: string;
   /** Givens the host sets itself: no control is shown for them. */
   hiddenGivens?: readonly string[];
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
   controlsLayout = "bar",
   preamble,
   runModelPath,
   hiddenGivens,
}: DashboardViewProps) {
   const specs = useMemo(
      () =>
         (manifest.givens ?? []).filter(
            // In text-source mode, a `#(secure)` given is the host's to set: no control for it.
            (spec) =>
               !(preamble !== undefined && spec.secure === true) &&
               !(spec.name !== undefined && hiddenGivens?.includes(spec.name)),
         ),
      [manifest, hiddenGivens, preamble],
   );

   // Only a string can be a control's value; a list has no URL form.
   const controlParams = useMemo(
      () =>
         givens === undefined
            ? undefined
            : (Object.fromEntries(
                 Object.entries(givens).filter(
                    ([, value]) => typeof value === "string",
                 ),
              ) as Record<string, string>),
      [givens],
   );

   // The control row's state, options and `to=self` drill: the same hook the
   // notebook uses, so a control behaves identically on both surfaces.
   const controls = useDocumentControls({
      specs,
      loaded: true,
      startingValues: manifest.startingGivens,
      params: controlParams,
      onGivensChange,
      // Version included: two dashboards whose starting values coincide would
      // otherwise look like one document, and the one you came from would keep
      // filtering the one you drilled into.
      documentKey: `${environmentName}/${packageName}/${versionId ?? ""}/${documentName}`,
      // Absent means autorun; only an explicit `autorun=false` batches.
      autorun: manifest.autorun !== false,
      environmentName,
      packageName,
      modelPath: runModelPath ?? manifest.path,
      versionId,
      documentName,
      ...(preamble !== undefined ? { preamble } : {}),
      ...(givens ? { hostGivens: givens } : {}),
   });
   const { applied, declaredTypes, canSelf, onSelf } = controls;
   // Set by the `Report` around this view; silent anywhere else.
   const events = useReportEvents();

   // A given only a gate reads is in a tile's `givenNames` but not the row, so
   // the host's value for it is sent as given: the row would drop it as undeclared.
   const tileHostGivens = useMemo(() => {
      const named = new Set(
         (manifest.tiles ?? []).flatMap((tile) => tile.givenNames ?? []),
      );
      return Object.fromEntries(
         Object.entries(givens ?? {}).filter(
            ([name]) => named.has(name) && !declaredTypes.has(name),
         ),
      );
   }, [manifest, givens, declaredTypes]);

   // The rows behind a clicked value: the way past a number. Composite tiles
   // only: each names its source, which is what the rows are of.
   const [rows, setRows] = useState<RowsRequest | undefined>(undefined);
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
      () =>
         withHostGivens(
            givensToRequest(applied, declaredTypes),
            tileHostGivens,
         ),
      [applied, declaredTypes, tileHostGivens],
   );

   const { drill, drillMenu } = useDrill({
      onNavigate,
      onSelf,
      canSelf,
      selfLabel: "Filter this report",
      ...(preamble === undefined ? { onRows } : {}),
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

   // The layout decides the arrangement only, as it does in the builder.
   const { columns, layout } = layoutRules(
      manifest.layout,
      manifest.dashboardColumns,
   );

   // Reachable on the in-process load path only: production aborts the package
   // on the first compile error, so an uncompilable file answers 424 instead.
   if (manifest.error) {
      return (
         <ReportPage
            layout={layout}
            header={<DashboardHeader manifest={manifest} />}
         >
            <Alert severity="error">{manifest.error}</Alert>
         </ReportPage>
      );
   }

   const modelPath = runModelPath ?? manifest.path;
   const tiles = manifest.tiles ?? [];

   return (
      <ReportPage
         layout={layout}
         header={<DashboardHeader manifest={manifest} />}
         // Only when there are controls: GivensPanel draws nothing without
         // them, and an empty bar would still cost the page a gap.
         controls={
            specs.length > 0 ? (
               <StickyControls>
                  <GivensPanel {...controls.panel} layout={controlsLayout} />
               </StickyControls>
            ) : undefined
         }
      >
         {modelPath === undefined ? (
            <Alert severity="error">
               This dashboard has no model path, so there is nothing to run.
            </Alert>
         ) : tiles.length > 0 ? (
            // Composite form: each tile runs on its own and the results are
            // combined into one grid here, since no single Malloy result spans
            // them.
            <DashboardGrid
               tiles={tiles}
               columns={columns}
               minTilePx={MIN_TILE_PX}
               rowGapPx={GRID_GAP_PX}
               // Position too, not the expression alone: `tiles=[…]` can repeat
               // one, which is a typo rather than a request for two identical
               // panels, and keying on the expression made the duplicate warn
               // and reconcile onto its twin.
               keyOf={(tile, index) =>
                  `${index}:${tile.kind === "text" ? `text:${tile.name}` : tile.query}`
               }
               renderTile={(tile, index) =>
                  // An absent `kind` is a query tile, as it was before text tiles.
                  tile.kind === "text" || tile.query === undefined ? (
                     // Prose, never a card, in either layout: a text tile is
                     // part of the page, not a result.
                     <TextTile markdown={tile.markdown ?? ""} />
                  ) : (
                     <DashboardTile
                        index={index}
                        environmentName={environmentName}
                        packageName={packageName}
                        versionId={versionId}
                        modelPath={modelPath}
                        tile={tile.query}
                        label={tile.label}
                        subtitle={tile.subtitle}
                        borderless={tile.borderless}
                        givens={applied}
                        hostGivens={tileHostGivens}
                        declaredTypes={declaredTypes}
                        givenNames={tile.givenNames}
                        height={height ?? TILE_MAX_HEIGHT}
                        maxResultSize={maxResultSize}
                        drill={drillFor(tile.query)}
                        ignoredFilters={tileIgnoredFilterLabels(
                           tile.givenNames,
                           specs,
                        )}
                        {...(preamble !== undefined
                           ? { preamble, restricted: tile.restricted }
                           : {})}
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
                     events.emit({
                        type: "report.rows_shown",
                        source: rows.source,
                        view: rows.view,
                        field: rows.field,
                        ok,
                        durationMs,
                     });
               }}
            />
         )}
      </ReportPage>
   );
}

/** A text tile: its markdown and nothing else, no heading. */
function TextTile({ markdown }: { markdown: string }) {
   return (
      <TextTileFrame>
         <Prose variant="document">{markdown}</Prose>
      </TextTileFrame>
   );
}

/**
 * The report's header as the reader draws it: the shared {@link ReportHeader}
 * with the title, and the description as markdown prose (Malloy carries a
 * `##"` block through with its newlines intact) in a text tile's frame.
 */
function DashboardHeader({ manifest }: { manifest: DashboardManifest }) {
   return (
      <ReportHeader
         title={manifest.title ?? manifest.name}
         description={
            manifest.description ? (
               <TextTileFrame>
                  <Prose variant="document">{manifest.description}</Prose>
               </TextTileFrame>
            ) : undefined
         }
      />
   );
}
