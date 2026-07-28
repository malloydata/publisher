import { Alert, Box, Stack, Typography } from "@mui/material";
import { useCallback, useMemo } from "react";
import type { DashboardManifest } from "../../client";
import { useGivensState } from "../../hooks/useGivensState";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useSuggestOptions } from "../../hooks/useSuggestOptions";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { useDrill, type DrillNavigation } from "../drill";
import { GivensPanel } from "../given";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { DashboardTile } from "./DashboardTile";

export interface DashboardProps {
   /** `publisher://environments/{env}/packages/{pkg}` — env and package only. */
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
    */
   onGivensChange?: (givens: Record<string, string>) => void;
   /**
    * Where to go when a `# drill` cell is clicked. Without it, drilling to
    * another dashboard is inert — `to=self` still filters in place, since that
    * never leaves the component.
    */
   onNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   /**
    * Height cap for a result panel. Left unset, each form gets the cap that
    * suits its shape — see {@link TILE_HEIGHT} and {@link WHOLE_PAGE_HEIGHT}.
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
 * practice, and still a guard against a pathological result; the result renders
 * at its natural height and the page scrolls, which is what a reader expects of
 * a dashboard.
 */
const WHOLE_PAGE_HEIGHT = 20000;

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
   const { environmentName, packageName } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   if (!environmentName || !packageName) {
      throw new Error(
         "A Dashboard resource URI must name an environment and a package.",
      );
   }

   const {
      data: manifestResponse,
      isSuccess,
      isError,
      error,
   } = useQueryWithApiError({
      queryKey: ["dashboard", environmentName, packageName, dashboard],
      queryFn: () =>
         apiClients.dashboards.getDashboard(
            environmentName,
            packageName,
            dashboard,
         ),
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

   const { draft, applied, setGiven, clearAll, apply, pending } =
      useGivensState({
         declaredTypes,
         startingValues: manifest?.startingGivens,
         params: givens,
         onParamsChange: onGivensChange,
         // Absent means autorun; only an explicit `autorun=false` batches.
         autorun: manifest?.autorun !== false,
      });

   const { options, isLoading: optionsLoading } = useSuggestOptions(
      environmentName,
      packageName,
      manifest?.path,
      specs,
   );

   // `to=self` filters in place, which only works for a given this dashboard
   // actually surfaces: sending one it cannot bind would fail every tile's
   // query. A mismatch (a dimension whose upper-cased name is not the given,
   // say) is reported to the author rather than issued.
   const onSelf = useCallback(
      (given: string, value: string) => {
         if (!declaredTypes.has(given)) {
            console.warn(
               `# drill { to=self } tried to set '${given}', which '${dashboard}' ` +
                  `does not declare as a given. Name the given with 'given=' on ` +
                  `the drill tag.`,
            );
            return;
         }
         setGiven(given, value);
      },
      [dashboard, declaredTypes, setGiven],
   );
   const { drill, drillMenu } = useDrill({
      onNavigate,
      onSelf,
      selfLabel: "Filter this dashboard",
   });

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

   // A dashboard whose file failed to compile still lists and still resolves,
   // so say why rather than rendering an empty frame.
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

   return (
      <Stack spacing={2}>
         <DashboardHeader manifest={manifest} />

         <GivensPanel
            givens={specs}
            values={draft}
            onChange={setGiven}
            onClearAll={clearAll}
            layout="bar"
            options={options}
            optionsLoading={optionsLoading}
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
            // grid is imposed here — doing so would nest a grid in a grid.
            <DashboardTile
               environmentName={environmentName}
               packageName={packageName}
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
                     md: `repeat(${manifest.dashboardColumns ?? 2}, minmax(0, 1fr))`,
                  },
                  gap: 2,
               }}
            >
               {tiles.map((tile) => (
                  <DashboardTile
                     key={tile.query}
                     environmentName={environmentName}
                     packageName={packageName}
                     modelPath={modelPath}
                     tile={tile.query}
                     givens={applied}
                     declaredTypes={declaredTypes}
                     givenNames={tile.givenNames}
                     height={height ?? TILE_HEIGHT}
                     maxResultSize={maxResultSize}
                     drill={drill}
                  />
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
