// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert } from "@mui/material";
import { useCallback, useEffect, useRef } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { now } from "../../utils/clock";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { NavigationClick } from "../click_helper";
import { DashboardView } from "../Dashboard/DashboardView";
import type { TileChrome } from "../Dashboard/TileCard";
import type { DrillNavigation } from "../drill";
import { Loading, LOADING_COPY } from "../Loading";
import { useServer } from "../ServerProvider";
import { reportQueryOptions } from "./reportQuery";
import { CellReport } from "./CellReport";
import {
   failureCategory,
   ReportEventsContext,
   useReportEmitter,
   type DrillOutcome,
   type ReportEventHandler,
} from "./telemetry";

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
    * Where to go when a `# drill` cell is clicked. `target.path` is the
    * tag's `to=` as written: a package path from the root, without `.malloy`,
    * for the host to resolve against `GET …/reports` (`resolveDrillTarget`).
    * Without it, drilling to another report is inert: `to=self` still filters
    * in place. Return what the target resolved to and the drill is reported
    * as a `report.drill` event; return nothing and it is not.
    */
   onNavigate?: (
      target: DrillNavigation,
      event?: MouseEvent,
   ) => DrillOutcome | void;
   /**
    * An ordinary link inside a linear report's prose, for a host with its own
    * router. Omitted, links are plain anchors.
    */
   onLinkNavigate?: (to: string, event?: NavigationClick) => void;
   /** Height cap for a grid tile's result panel; see `Dashboard`. */
   height?: number;
   maxResultSize?: number;
   /**
    * What the report does — opened, failed to load, a failed query, rows
    * shown, a drill — for the host to log or count; see `ReportEvent`.
    */
   onEvent?: ReportEventHandler;
   /** Ignored: kept so existing callers compile. Both layouts draw the same cards. */
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
      ...reportQueryOptions(apiClients, {
         environmentName,
         packageName,
         path,
         versionId,
      }),
      enabled: uriNamesBoth && path !== "",
   });

   // The layout once the report has said; a `run:`-cell report reads top to bottom.
   const layout = manifest
      ? manifest.notebook
         ? "linear"
         : manifest.layout
      : undefined;
   const events = useReportEmitter({ path, layout }, onEvent);
   // One open or one failure per report shown: a refetch is not another open,
   // and the guard survives a development double-run of the effect.
   const reportKey = `${resourceUri}|${path}`;
   const reported = useRef<string | undefined>(undefined);
   // Restarted for each report this mount shows, so "opened" times that one.
   const started = useRef({ key: reportKey, at: now() });
   if (started.current.key !== reportKey)
      started.current = { key: reportKey, at: now() };
   useEffect(() => {
      if (reported.current === reportKey) return;
      if (!uriNamesBoth) {
         reported.current = reportKey;
         events.emit({
            type: "report.load_failed",
            surface: "view",
            category: "invalid",
         });
      } else if (isError) {
         reported.current = reportKey;
         events.emit({
            type: "report.load_failed",
            surface: "view",
            category: failureCategory(error),
         });
      } else if (manifest?.notebook?.format === "malloynb") {
         // Report does not run one; the host renders it with `Notebook`.
         reported.current = reportKey;
         events.emit({
            type: "report.load_failed",
            surface: "view",
            category: "unsupported",
         });
      } else if (isSuccess && manifest) {
         reported.current = reportKey;
         events.emit({
            type: "report.opened",
            surface: "view",
            from: "package",
            tiles: manifest.notebook
               ? (manifest.notebook.notebookCells?.length ?? 0)
               : (manifest.tiles?.length ?? 0),
            durationMs: now() - started.current.at,
         });
      }
   }, [reportKey, uriNamesBoth, isError, error, isSuccess, manifest, events]);
   // The host resolves a drill to another report; it says what that came to.
   const navigate = useCallback(
      (target: DrillNavigation, event?: MouseEvent) => {
         const outcome = onNavigate?.(target, event);
         if (outcome)
            events.emit({ type: "report.drill", to: "report", outcome });
      },
      [onNavigate, events],
   );
   const drillNavigate = onNavigate ? navigate : undefined;

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
   let body;
   if (manifest.notebook) {
      body = (
         <CellReport
            {...(manifest.title ? { title: manifest.title } : {})}
            {...(manifest.description
               ? { description: manifest.description }
               : {})}
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
            onDrillNavigate={drillNavigate}
         />
      );
   } else {
      // Either layout: the same view, arranged as the manifest's layout says.
      body = (
         <DashboardView
            manifest={manifest}
            environmentName={environmentName}
            packageName={packageName}
            versionId={versionId}
            documentName={path}
            givens={givens}
            onGivensChange={onGivensChange}
            onNavigate={drillNavigate}
            height={height}
            maxResultSize={maxResultSize}
            chrome={chrome}
         />
      );
   }
   return (
      <ReportEventsContext.Provider value={events}>
         {body}
      </ReportEventsContext.Provider>
   );
}

export default Report;
