// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Stack, Typography } from "@mui/material";
import Markdown from "markdown-to-jsx";
import { useCallback, useMemo, useState } from "react";
import type { DashboardManifest } from "../../client";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import {
   useDrill,
   type DrillBinding,
   type DrillClickPayload,
   type DrillNavigation,
   type DrillRowsRequest,
} from "../drill";
import { GivensPanel } from "../given";
import { givensToRequest } from "../given/paramCodec";
import { Loading } from "../Loading";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { useServer } from "../ServerProvider";
import { DashboardGrid, DEFAULT_COLUMNS } from "./DashboardGrid";
import { DashboardTile } from "./DashboardTile";
import { ExploreDialog } from "./ExploreDialog";
import { RowsDialog, stepsOf, type RowsRequest } from "./RowsDialog";
import type { DashboardEventHandler } from "./telemetry";

// The grid rule moved to `DashboardGrid`, which the builder shares.
// Re-exported so existing importers of it are unaffected.
export { DEFAULT_COLUMNS, tileGridColumn } from "./DashboardGrid";

export interface DashboardProps {
   /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
   resourceUri: string;
   /** The dashboard's slug, as listed by the dashboards endpoint. */
   dashboard: string;
   /**
    * Control values from the host, typically its URL query parameters. These
    * beat the dashboard's own starting values, so a shared link shows what the
    * sender was looking at.
    */
   givens?: Record<string, string>;
   /**
    * Applied control values, for a host that wants them in its URL. Fires with
    * what the results reflect, not with every keystroke.
    *
    * `managed` is every given this dashboard declares, whether or not it
    * currently holds a value, and a host writing to a shared query string needs
    * it. `givens` alone says which parameters to write but not which to REMOVE,
    * so a host that guesses by deleting everything it did not just receive
    * deletes the unrelated parameters it has no business touching. Same contract
    * as `Notebook`, so a host can treat the two surfaces alike.
    */
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   /**
    * Where to go when a `# drill` cell is clicked. Without it, drilling to
    * another dashboard is inert: `to=self` still filters in place, since that
    * never leaves the component.
    */
   onNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   /**
    * Height cap for a result panel. Left unset, each form gets the cap that
    * suits its shape: {@link TILE_MAX_HEIGHT} per tile for the composite form,
    * and no cap at all for the single-query form, where the one result IS the
    * dashboard and a cap would clip the page rather than tidy it. Set it to
    * hold a dashboard to a fixed box, as an embedding host might.
    */
   height?: number;
   maxResultSize?: number;
   /** The rows shown and tiles explored, for the host to log or count. */
   onEvent?: DashboardEventHandler;
}

/**
 * A Malloyyo-style dashboard: a control row over one or more query results,
 * declared entirely by tags in a package's `dashboards/*.malloy`.
 *
 * Host-agnostic on purpose. It takes props rather than reading a router, and
 * hands navigation and URL state back to whoever mounted it, so the Publisher
 * Console and an external React app render the same component and differ only
 * in what they do with `onNavigate` and `onGivensChange`.
 */
export function Dashboard({
   resourceUri,
   dashboard,
   givens,
   onGivensChange,
   onNavigate,
   height,
   maxResultSize,
   onEvent,
}: DashboardProps) {
   const parsed = parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   // Degraded, not thrown. `Notebook`, `Model` and `Package` all let a bad URI
   // fail into an error display; a throw in the render body takes the host's
   // whole tree down with it instead, which is a white screen rather than a
   // message. The Console cannot reach this, because `DashboardPage` builds the
   // URI from params `ModelPage` has already guarded, so the only caller a throw
   // could punish is the embedding host the docs invite. Reported below rather
   // than here, so every hook still runs in the same order.
   const environmentName = parsed.environmentName ?? "";
   const packageName = parsed.packageName ?? "";
   const versionId = parsed.versionId;
   const uriNamesBoth = !!parsed.environmentName && !!parsed.packageName;

   const {
      data: manifestResponse,
      isSuccess,
      isError,
      error,
   } = useQueryWithApiError({
      // Every value the request is built from, so the key cannot drift out of
      // step with it. A version that reached the request alone would leave two
      // versions of one dashboard on a single cache entry, each able to serve
      // the other's manifest.
      queryKey: [
         "dashboard",
         environmentName,
         packageName,
         versionId,
         dashboard,
      ],
      queryFn: () =>
         apiClients.dashboards.getDashboard(
            environmentName,
            packageName,
            dashboard,
            versionId,
         ),
      // No point asking for a dashboard under a name the URI never carried.
      enabled: uriNamesBoth,
   });
   const manifest = manifestResponse?.data;

   const specs = useMemo(() => manifest?.givens ?? [], [manifest]);

   // The control row's state, options and `to=self` drill: the same hook the
   // notebook uses, so a control behaves identically on both surfaces.
   const controls = useDocumentControls({
      specs,
      loaded: isSuccess,
      startingValues: manifest?.startingGivens,
      params: givens,
      onGivensChange,
      // Which document these edits belong to, version included: two
      // dashboards whose starting values coincide (both empty, usually)
      // would otherwise look like one document, and the one you came from
      // would keep filtering the one you drilled into. Only the EDITS: a host
      // that round-trips givens through its own URL hands them straight back,
      // and they still apply across the swap.
      documentKey: `${environmentName}/${packageName}/${versionId ?? ""}/${dashboard}`,
      // Absent means autorun; only an explicit `autorun=false` batches.
      autorun: manifest?.autorun !== false,
      environmentName,
      packageName,
      modelPath: manifest?.path,
      versionId,
      documentName: dashboard,
   });
   const { applied, declaredTypes, canSelf, onSelf } = controls;

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

   // The whole applied row: a source's own `where:` may read any of it, and a
   // given the rows query does not reference is ignored by the server.
   const rowsGivens = useMemo(
      () => givensToRequest(applied, declaredTypes),
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

   // After every hook, so the hook order does not depend on the URI.
   if (!uriNamesBoth) {
      return (
         <Alert severity="error">
            A dashboard resource URI must name an environment and a package.
            Received: {resourceUri}
         </Alert>
      );
   }

   if (isError) {
      return (
         <ApiErrorDisplay
            context={`${environmentName} > ${packageName} > ${dashboard}`}
            error={error}
         />
      );
   }
   if (!isSuccess || !manifest) {
      return <Loading text="Loading dashboard…" />;
   }

   // Reachable on the in-process load path only. Production loads a package
   // through the worker pool, which aborts the package on the first compile
   // error, so an uncompilable file answers 424 everywhere rather than serving a
   // per-file error. Measured on this base with one bad file: the package
   // endpoint, the dashboards listing, and the manifest of a dashboard that was
   // FINE all answered 424. The earlier note here said such a dashboard "still
   // lists and still resolves", which contradicted this repo's own docs and was
   // the wrong half of the pair. Kept because the per-file error manifest still
   // exists and an empty frame would be worse than saying why.
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
         <DashboardHeader manifest={manifest} />

         <GivensPanel {...controls.panel} layout="bar" />

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
            />
         ) : tiles.length > 0 ? (
            // Composite form: each tile runs on its own and the results are
            // combined into one grid here, since no single Malloy result spans
            // them.
            <DashboardGrid
               tiles={tiles}
               columns={columns}
               // Position too, not the expression alone: `tiles=[…]` can repeat
               // one, which is a typo rather than a request for two identical
               // panels, and keying on the expression made the duplicate warn
               // and reconcile onto its twin.
               keyOf={(tile, index) => `${index}:${tile.query}`}
               renderTile={(tile) => (
                  <DashboardTile
                     environmentName={environmentName}
                     packageName={packageName}
                     versionId={versionId}
                     modelPath={modelPath}
                     tile={tile.query}
                     label={tile.label}
                     subtitle={tile.subtitle}
                     borderless={tile.borderless}
                     givens={applied}
                     declaredTypes={declaredTypes}
                     givenNames={tile.givenNames}
                     height={height ?? TILE_MAX_HEIGHT}
                     maxResultSize={maxResultSize}
                     drill={drillFor(tile.query)}
                     onExplore={() => {
                        setExploring(tile.query);
                        onEvent?.({
                           type: "dashboard.explored",
                           tile: tile.query,
                        });
                     }}
                  />
               )}
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
                  onClose={() => setExploring(undefined)}
               />
            </>
         )}
      </Stack>
   );
}

/**
 * The dashboard's prose header: its title, and the description as MARKDOWN.
 *
 * A description comes from the file's model-level doc comment (`##"` lines),
 * and Malloy carries a whole block of them through with its newlines and blank
 * lines intact — measured: `'## Why this page exists\nRevenue is up but margin
 * is flat.\n\nThe tile below says where it went.'` reaches the manifest exactly
 * like that. Rendering it as a plain `Typography` collapsed all of that onto
 * one unstyled line, so a dashboard could already carry a narrative header and
 * was throwing it away at the last step.
 *
 * Markdown here and not in a tile: prose BETWEEN tiles needs a tile kind the
 * format cannot express yet. This is the half that needs nothing new.
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
 * The prose header itself, over the two fields it actually needs.
 *
 * Separate from {@link DashboardHeader} so the BUILDER can render the same
 * header over a `DashboardDocument` — which carries `title` and `description`
 * but is not a manifest. Same argument as {@link tileGridColumn}: what the
 * builder shows a author is the thing a reader will see, and one component is
 * what stops the two drifting. A builder that restated this markdown block
 * would be one edit away from showing a different header than it writes.
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
         {description && (
            <Box
               sx={{
                  color: "text.secondary",
                  typography: "body2",
                  // The block starts flush under the title and ends flush
                  // against the controls, so a one-line description sits
                  // exactly where the old `Typography` put it and a longer one
                  // grows downward rather than pushing the title around.
                  "& > :first-of-type": { mt: 0 },
                  "& > :last-child": { mb: 0 },
                  // Headings in a description are section labels within the
                  // page, not competitors to its title, so they stay at body
                  // weight and size rather than MUI's h1..h6 scale.
                  "& h1, & h2, & h3, & h4, & h5, & h6": {
                     fontSize: "inherit",
                     fontWeight: 600,
                     m: "0.5em 0 0.25em",
                  },
                  "& p": { m: "0.5em 0" },
                  "& ul, & ol": { m: "0.5em 0", pl: 3 },
                  "& code": {
                     fontFamily: "monospace",
                     fontSize: "0.9em",
                  },
               }}
            >
               <Markdown>{description}</Markdown>
            </Box>
         )}
      </Box>
   );
}

export default Dashboard;
