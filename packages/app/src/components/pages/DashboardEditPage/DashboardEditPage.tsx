// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   type DashboardEvent,
   Loading,
   LOADING_COPY,
   NarrowEditGate,
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
   /** The layout the report opened with, in the legacy vocabulary; default `dashboard` (grid). */
   kind?: "dashboard" | "notebook";
}

/**
 * The Console's host for the SDK `DashboardEditor`: `/<env>/<pkg>/<path>/edit`,
 * either layout. View in the header returns to the report itself, one segment
 * up.
 */
export default function DashboardEditPage({
   environmentName,
   packageName,
   path,
   kind = "dashboard",
}: DashboardEditPageProps) {
   const guard = useLeaveGuard();
   // Telemetry keys on the path: two reports may share a basename.
   const onEvent = useMemo(() => {
      if (kind === "dashboard")
         return logDashboardEvent({
            environmentName,
            packageName,
            dashboardName: path,
         }) as (event: DashboardEvent | NotebookEvent) => void;
      return logNotebookEvent({
         environmentName,
         packageName,
         notebookName: path,
      }) as (event: DashboardEvent | NotebookEvent) => void;
   }, [kind, environmentName, packageName, path]);
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
                  kind={kind}
                  onDirtyChange={guard.onDirtyChange}
                  onEvent={onEvent}
               />
            </Suspense>
         </NarrowEditGate>
         {guard.dialog}
      </Box>
   );
}
