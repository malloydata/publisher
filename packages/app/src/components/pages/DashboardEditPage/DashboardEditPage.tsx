// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   type DashboardEvent,
   Loading,
   LOADING_COPY,
   NarrowEditGate,
   type ReportLayout,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import React, { Suspense, useMemo } from "react";
import type { NotebookEvent } from "@malloy-publisher/sdk/builder";
import {
   logDashboardEvent,
   logNotebookEvent,
} from "../../../utils/consoleTelemetry";
import { useLeaveGuard } from "../useLeaveGuard";

/**
 * The builder's entry is loaded here and nowhere else. It carries the Malloy
 * parser (440 KB gzipped), which most Console users never need, so it stays out
 * of every other page's chunk and downloads the first time someone opens a
 * report for editing.
 */
const DashboardEditor = React.lazy(() =>
   import("@malloy-publisher/sdk/builder").then((module) => ({
      default: module.DashboardEditor,
   })),
);

export interface DashboardEditPageProps {
   environmentName: string;
   packageName: string;
   /** The report's package path, `sales/q3.malloy`: what the editor is keyed by. */
   path: string;
   /** The layout the report opens with, from the listing or the host's draft; default grid. */
   layout?: ReportLayout;
}

/**
 * The Console's host for the SDK `DashboardEditor`: `/<env>/<pkg>/<path>?edit`,
 * either layout. View in the header returns to the report itself, the same
 * route without `?edit`.
 */
export default function DashboardEditPage({
   environmentName,
   packageName,
   path,
   layout = "grid",
}: DashboardEditPageProps) {
   const guard = useLeaveGuard();
   // Telemetry keys on the path: two reports may share a basename. Each event
   // goes to the sink its own name belongs to, so after a layout switch the
   // builder's events follow the report's new layout.
   const onEvent = useMemo(() => {
      const dashboard = logDashboardEvent({
         environmentName,
         packageName,
         dashboardName: path,
      });
      const notebook = logNotebookEvent({
         environmentName,
         packageName,
         notebookName: path,
      });
      return (event: DashboardEvent | NotebookEvent) =>
         event.type.startsWith("notebook.")
            ? notebook(event as NotebookEvent)
            : dashboard(event as DashboardEvent);
   }, [environmentName, packageName, path]);
   return (
      // The reader's page width and edges, the same for either layout, so the
      // margins do not move between modes.
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
         <NarrowEditGate>
            <Suspense
               fallback={<Loading text={LOADING_COPY.opening("builder")} />}
            >
               <DashboardEditor
                  // Remounts on a route change so another report starts from a fresh read.
                  key={`${environmentName}/${packageName}/${path}`}
                  resourceUri={encodeResourceUri({
                     environmentName,
                     packageName,
                  })}
                  path={path}
                  layout={layout}
                  onDirtyChange={guard.onDirtyChange}
                  onEvent={onEvent}
               />
            </Suspense>
         </NarrowEditGate>
         {guard.dialog}
      </Box>
   );
}
