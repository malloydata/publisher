// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Stack, Typography } from "@mui/material";
import { useCallback, useMemo } from "react";
import type { DashboardManifest } from "../../client";
import { useGivensState } from "../../hooks/useGivensState";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useSuggestOptions } from "../../hooks/useSuggestOptions";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { encodeDrillValue, useDrill, type DrillNavigation } from "../drill";
import { GivensPanel } from "../given";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { DashboardTile } from "./DashboardTile";

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
    * suits its shape, see {@link TILE_HEIGHT} and {@link WHOLE_PAGE_HEIGHT}.
    * Set it to hold a dashboard to a fixed box, as an embedding host might.
    */
   height?: number;
   maxResultSize?: number;
}

/**
 * Per-tile cap for the composite form. A tile is one panel among several, so
 * capping them keeps the grid even instead of letting one long table set the
 * height of its whole row.
 */
const TILE_HEIGHT = 400;

/**
 * Cap for the single-query form, where the one result *is* the dashboard and a
 * cap would clip the page rather than tidy it. High enough to be no cap in
 * practice, and still a guard against a pathological result.
 *
 * A result that REPORTS its own height, which a `# dashboard` grid does, renders
 * at that height and the page scrolls, which is what a reader expects. A result
 * that sizes to its CONTAINER instead, which a bare `# bar_chart` does, has no
 * height to report and keeps whatever first-paint height it was handed, so it
 * stretches: measured 1992px for a two-row bar chart against 227px for the same
 * query under a grid tag. `INITIAL_RENDER_HEIGHT` bounds that near 2000 rather
 * than removing it, and lowering the bound is NOT the fix, because the same seed
 * sizes a notebook's chart cells (measured 700px there). Telling the two kinds of
 * result apart needs something the renderer does not expose here.
 */
const WHOLE_PAGE_HEIGHT = 20000;

/** Grid width when the dashboard declares no `# dashboard { columns=N }`. */
const DEFAULT_COLUMNS = 2;

/**
 * The `grid-column` one tile occupies: its `# colspan`, and a `# break` forcing
 * it to start a fresh row.
 *
 * Clamped to the grid width the same way @malloydata/render clamps it, so one
 * view laid out as a composite tile and as a `nest:` under `# dashboard` lands
 * in the same place. A break is `1 / span N` — an explicit start line, which is
 * what pushes the tile down to the next row; the renderer's grid does the same.
 */
export function tileGridColumn(
   tile: { colspan?: number; break?: boolean },
   columns: number,
): string {
   const span = Math.min(tile.colspan ?? 1, columns);
   return tile.break ? `1 / span ${span}` : `span ${span}`;
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
   const declaredTypes = useMemo(
      () =>
         new Map(
            specs
               .filter((spec) => spec.name !== undefined)
               .map((spec) => [spec.name as string, spec.type]),
         ),
      [specs],
   );

   // Hands the host the names this dashboard MANAGES alongside the values, so it
   // can merge into a shared query string instead of replacing it. Guarded on
   // `isSuccess` as well as at the call site, since an empty declared set before
   // the manifest lands is the absence of an answer rather than the answer.
   const reportGivens = useCallback(
      (next: Record<string, string>) => {
         if (!isSuccess) return;
         onGivensChange?.(next, Array.from(declaredTypes.keys()));
      },
      [isSuccess, onGivensChange, declaredTypes],
   );

   const { draft, applied, setGiven, reset, apply, pending } = useGivensState({
      declaredTypes,
      startingValues: manifest?.startingGivens,
      params: givens,
      // Withheld until the manifest has loaded, for the same reason the notebook
      // withholds it. Changing `dashboard` changes the query key, so `data` is
      // undefined for one commit and `declaredTypes` is empty; `applied` prunes
      // to nothing and the hook reports "no values, and I manage nothing". This
      // component is reconciled rather than remounted on a dashboard-to-dashboard
      // drill, so the hook's record of what it last reported still holds the
      // PREVIOUS dashboard's values and does not suppress that report as a
      // repeat. The host reasonably clears its query string, which is exactly the
      // givens the drill just seeded for the dashboard now arriving.
      onParamsChange: isSuccess ? reportGivens : undefined,
      // Which document these edits belong to. Without it the edits are keyed by
      // their starting VALUES alone, so two dashboards whose starting values
      // coincide (the common case: both empty) look like one document, and the
      // one you came from keeps filtering the one you drilled into.
      // The version belongs in the identity too: a host that swaps versions
      // under this component would otherwise carry the values a reader applied
      // to one version into the other's queries.
      documentKey: `${environmentName}/${packageName}/${versionId ?? ""}/${dashboard}`,
      // Absent means autorun; only an explicit `autorun=false` batches.
      autorun: manifest?.autorun !== false,
   });

   const {
      options,
      isLoading: optionsLoading,
      failed: optionsFailed,
   } = useSuggestOptions(
      environmentName,
      packageName,
      manifest?.path,
      specs,
      versionId,
   );

   // A drill tag names its given as the model spells it, and `# drill` with no
   // `given=` falls back to the DIMENSION's spelling, which need not match. The
   // notebook folds case rather than picking a side, and this folds it the same
   // way so one tag behaves identically on both surfaces.
   const givenNamesByFold = useMemo(() => {
      const byFold = new Map<string, string>();
      for (const name of declaredTypes.keys()) {
         // First declaration wins, so a model with `REGION` and `region` keeps
         // the one it declared first rather than silently flipping.
         if (!byFold.has(name.toLowerCase()))
            byFold.set(name.toLowerCase(), name);
      }
      return byFold;
   }, [declaredTypes]);

   /** The declared given a drill tag's name refers to, or undefined. */
   const resolveGiven = useCallback(
      (given: string) =>
         declaredTypes.has(given)
            ? given
            : givenNamesByFold.get(given.toLowerCase()),
      [declaredTypes, givenNamesByFold],
   );

   // `to=self` filters in place, which only works for a given this dashboard
   // actually surfaces: sending one it cannot bind would fail every tile's
   // query. Asked here rather than checked after the click, so a cell that
   // cannot be honoured is never painted as clickable in the first place.
   const canSelf = useCallback(
      (given: string) => resolveGiven(given) !== undefined,
      [resolveGiven],
   );

   const onSelf = useCallback(
      (given: string, rawValue: unknown) => {
         const declared = resolveGiven(given);
         if (declared === undefined) return;
         // Encoded against the DECLARED type, which is knowable here and is not
         // knowable at the click: `useDrill` hands over the raw cell value for
         // exactly this reason. Passing it straight to `setGiven` skipped the
         // encoder, so a clicked date reached a `number` given as epoch
         // milliseconds and a filter value went unescaped. Set under the name
         // the MODEL declares, so the value reaches the URL and the request
         // under the one name the server knows.
         const declaredType = declaredTypes.get(declared);
         const value = encodeDrillValue(rawValue, declaredType);
         if (value === undefined) {
            // Say so, the way the notebook does. `canSelf` only checks that the
            // NAME resolves, so a `given=` naming a type the clicked value
            // cannot become still paints the whole column as clickable and then
            // drops every click. That is an authoring mistake with no other
            // symptom, and this is the likelier surface for it, because the
            // givens are the dashboard's own.
            console.warn(
               `Drill declined: ${JSON.stringify(rawValue)} cannot be a value for given "${declared}"` +
                  (declaredType ? ` of type ${declaredType}` : ""),
            );
            return;
         }
         setGiven(declared, value);
      },
      [declaredTypes, resolveGiven, setGiven],
   );

   const { drill, drillMenu } = useDrill({
      onNavigate,
      onSelf,
      canSelf,
      selfLabel: "Filter this dashboard",
   });

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

         <GivensPanel
            givens={specs}
            values={draft}
            onChange={setGiven}
            onReset={reset}
            layout="bar"
            options={options}
            optionsLoading={optionsLoading}
            optionsFailed={optionsFailed}
            apply={
               manifest.autorun === false
                  ? { onApply: apply, pending }
                  : undefined
            }
         />

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
               height={height ?? WHOLE_PAGE_HEIGHT}
               maxResultSize={maxResultSize}
               drill={drill}
            />
         ) : tiles.length > 0 ? (
            // Composite form: each tile runs on its own and the results are
            // combined into one grid here, since no single Malloy result spans
            // them.
            <Box
               sx={{
                  display: "grid",
                  gridTemplateColumns: {
                     xs: "1fr",
                     md: `repeat(${columns}, minmax(0, 1fr))`,
                  },
                  gap: 2,
               }}
            >
               {tiles.map((tile, index) => (
                  <Box
                     // Position too, not the expression alone: `tiles=[…]` can
                     // repeat one, which is a typo rather than a request for two
                     // identical panels, and keying on the expression made the
                     // duplicate warn and reconcile onto its twin.
                     key={`${index}:${tile.query}`}
                     sx={{
                        display: "grid",
                        // Only above `md`: the narrow breakpoint is one column,
                        // where a span would overflow the grid rather than widen
                        // anything.
                        gridColumn: { md: tileGridColumn(tile, columns) },
                     }}
                  >
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
                        height={height ?? TILE_HEIGHT}
                        maxResultSize={maxResultSize}
                        drill={drill}
                     />
                  </Box>
               ))}
            </Box>
         ) : (
            <Alert severity="warning">
               This dashboard names neither a query nor any tiles.
            </Alert>
         )}

         {drillMenu}
      </Stack>
   );
}

function DashboardHeader({ manifest }: { manifest: DashboardManifest }) {
   return (
      <Box>
         <Typography variant="h5" sx={{ fontWeight: 600 }}>
            {manifest.title ?? manifest.name}
         </Typography>
         {manifest.description && (
            <Typography variant="body2" color="text.secondary">
               {manifest.description}
            </Typography>
         )}
      </Box>
   );
}

export default Dashboard;
