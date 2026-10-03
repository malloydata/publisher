// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert } from "@mui/material";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { DrillNavigation } from "../drill";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { DashboardView } from "./DashboardView";
import type { DashboardEventHandler } from "./telemetry";
import type { TileChrome } from "./TileCard";

// The grid rule moved to `DashboardGrid`, which the builder shares.
// Re-exported so existing importers of it are unaffected.
export { DEFAULT_COLUMNS, tileGridColumn } from "./DashboardGrid";
export { DashboardProse } from "./DashboardView";

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
   /**
    * `card` (default) draws each tile as a panel; `none` draws them bare, a
    * document that reads top to bottom.
    */
   chrome?: TileChrome;
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
   chrome,
}: DashboardProps) {
   const parsed = parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   // Degraded, not thrown: a throw in the render body takes the embedding
   // host's whole tree down with it, which is a white screen rather than a
   // message.
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

   return (
      <DashboardView
         manifest={manifest}
         environmentName={environmentName}
         packageName={packageName}
         versionId={versionId}
         documentName={dashboard}
         givens={givens}
         onGivensChange={onGivensChange}
         onNavigate={onNavigate}
         height={height}
         maxResultSize={maxResultSize}
         onEvent={onEvent}
         chrome={chrome}
      />
   );
}

export default Dashboard;
