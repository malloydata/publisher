// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   Loading,
   LOADING_COPY,
   NarrowEditGate,
   type ReportLayout,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import React, { Suspense, useMemo } from "react";
import { logReportEvent } from "../../../utils/consoleTelemetry";
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
   // Each event names the report by path and carries its layout as it stands.
   const onEvent = useMemo(
      () => logReportEvent({ environmentName, packageName }),
      [environmentName, packageName],
   );
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
