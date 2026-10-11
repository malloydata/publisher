// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert } from "@mui/material";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { NavigationClick } from "../click_helper";
import { DashboardView } from "../Dashboard/DashboardView";
import type { DashboardEventHandler } from "../Dashboard/telemetry";
import type { TileChrome } from "../Dashboard/TileCard";
import type { DrillNavigation } from "../drill";
import { Loading, LOADING_COPY } from "../Loading";
import { useServer } from "../ServerProvider";
import { CellReport } from "./CellReport";
import { LinearReport } from "./LinearReport";

export interface ReportProps {
   /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
   resourceUri: string;
   /**
    * The report's package path, including `.malloy`, exactly as
    * `GET …/reports` lists it: `sales/q3.malloy`. Its identity. A `.malloynb`
    * the listing also has is rendered by the deprecated `Notebook`, which runs
    * its cells through the only routes that can.
    */
   path: string;
   /**
    * Control values from the host, typically its URL query parameters. These
    * beat the report's own starting values, so a shared link shows what the
    * sender was looking at.
    */
   givens?: Record<string, string>;
   /**
    * Applied control values, for a host that wants them in its URL.
    *
    * `managed` is every given this report declares, set or not: `givens` alone
    * says which parameters to write but not which to REMOVE.
    */
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   /**
    * Where to go when a `# drill` cell is clicked. `target.dashboard` is the
    * tag's `to=` as written: a package path from the root, without `.malloy`,
    * for the host to resolve against `GET …/reports` (`resolveDrillTarget`).
    * Without it, drilling to another report is inert: `to=self` still filters
    * in place.
    */
   onNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   /**
    * An ordinary link inside a linear report's prose, for a host with its own
    * router. Omitted, links are plain anchors.
    */
   onLinkNavigate?: (to: string, event?: NavigationClick) => void;
   /** Height cap for a grid tile's result panel; see `Dashboard`. */
   height?: number;
   maxResultSize?: number;
   /** The rows shown and tiles explored on a grid report, for the host to log. */
   onEvent?: DashboardEventHandler;
   /** A grid report's tile chrome: `card` (default) or `none`. */
   chrome?: TileChrome;
}

/**
 * A report of either layout, from `GET …/reports/{path}`: a grid drawn as a
 * dashboard, a linear report top to bottom, and a linear report written as
 * `run:` cells cell by cell, each through the governed query endpoint.
 *
 * Host-agnostic, like `Dashboard`: it takes props rather than reading a
 * router, and hands navigation and URL state back to whoever mounted it.
 */
export function Report({
   resourceUri,
   path,
   givens,
   onGivensChange,
   onNavigate,
   onLinkNavigate,
   height,
   maxResultSize,
   onEvent,
   chrome,
}: ReportProps) {
   const parsed = parseResourceUri(resourceUri);
   const { apiClients } = useServer();
   const environmentName = parsed.environmentName ?? "";
   const packageName = parsed.packageName ?? "";
   const versionId = parsed.versionId;
   const uriNamesBoth = !!parsed.environmentName && !!parsed.packageName;

   const {
      data: manifest,
      isSuccess,
      isError,
      error,
   } = useQueryWithApiError({
      // Every value the request is built from, so two versions of one report
      // never share a cache entry.
      queryKey: ["report", environmentName, packageName, versionId, path],
      queryFn: async ({ signal }) =>
         (
            await apiClients.reports.getReport(
               environmentName,
               packageName,
               path,
               versionId,
               { signal },
            )
         ).data,
      enabled: uriNamesBoth && path !== "",
   });

   if (!uriNamesBoth) {
      return (
         <Alert severity="error">
            A report resource URI must name an environment and a package.
            Received: {resourceUri}
         </Alert>
      );
   }
   if (isError) {
      return (
         <ApiErrorDisplay
            context={`${environmentName} > ${packageName} > ${path}`}
            error={error}
         />
      );
   }
   if (!isSuccess || !manifest) {
      return <Loading text={LOADING_COPY.opening("report")} />;
   }

   if (manifest.notebook?.format === "malloynb") {
      return (
         <Alert severity="info">
            {path} is a .malloynb notebook, which Report does not run: render it
            with Notebook.
         </Alert>
      );
   }
   if (manifest.notebook) {
      return (
         <CellReport
            resourceUri={encodeResourceUri({
               environmentName,
               packageName,
               versionId,
               modelPath: path,
            })}
            notebook={manifest.notebook}
            maxResultSize={maxResultSize}
            givens={givens}
            onGivensChange={onGivensChange}
            onNavigate={onLinkNavigate}
            onDrillNavigate={onNavigate}
         />
      );
   }
   if (manifest.layout === "linear") {
      return (
         <LinearReport
            manifest={manifest}
            environmentName={environmentName}
            packageName={packageName}
            versionId={versionId}
            documentName={path}
            givens={givens}
            onGivensChange={onGivensChange}
            onDrillNavigate={onNavigate}
            maxResultSize={maxResultSize}
         />
      );
   }
   return (
      <DashboardView
         manifest={manifest}
         environmentName={environmentName}
         packageName={packageName}
         versionId={versionId}
         documentName={path}
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

export default Report;
