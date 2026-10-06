// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   type DashboardEvent,
   Loading,
   NarrowEditGate,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import React, { Suspense, useMemo } from "react";
import { useLocation, useNavigate } from "react-router-dom";
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
 * dashboard for editing.
 */
const DashboardEditor = React.lazy(() =>
   import("@malloy-publisher/sdk/builder").then((module) => ({
      default: module.DashboardEditor,
   })),
);

export interface DashboardEditPageProps {
   environmentName: string;
   packageName: string;
   /** The document's slug: `overview`, not `dashboards/overview.malloy`. */
   dashboardName: string;
   /** A notebook is the same editor with one column; default `dashboard`. */
   kind?: "dashboard" | "notebook";
   /** The file within the package, when the route's folder is not the whole story. */
   path?: string;
}

/**
 * The Console's host for the SDK `DashboardEditor`: `/<env>/<pkg>/dashboards/<slug>/edit`,
 * and `/<env>/<pkg>/notebooks/<slug>/edit` with `kind="notebook"`.
 * Close returns to the document itself, one segment up.
 */
export default function DashboardEditPage({
   environmentName,
   packageName,
   dashboardName,
   kind = "dashboard",
   path,
}: DashboardEditPageProps) {
   const navigate = useNavigate();
   const guard = useLeaveGuard();
   const { pathname } = useLocation();
   const dashboardPath = pathname.replace(/\/edit\/?$/, "");
   const onEvent = useMemo(() => {
      if (kind === "dashboard")
         return logDashboardEvent({
            environmentName,
            packageName,
            dashboardName,
         }) as (event: DashboardEvent | NotebookEvent) => void;
      return logNotebookEvent({
         environmentName,
         packageName,
         notebookName: dashboardName,
      }) as (event: DashboardEvent | NotebookEvent) => void;
   }, [kind, environmentName, packageName, dashboardName]);
   return (
      // The reader's page width and edges, the same for a dashboard and a
      // notebook, so the margins do not move between modes or kinds.
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
         <NarrowEditGate>
            <Suspense fallback={<Loading text="Opening the builder…" />}>
               <DashboardEditor
                  // Remounts on a route change so another dashboard starts from a fresh read.
                  key={`${environmentName}/${packageName}/${kind}/${dashboardName}`}
                  resourceUri={encodeResourceUri({
                     environmentName,
                     packageName,
                  })}
                  dashboard={dashboardName}
                  kind={kind}
                  path={path}
                  onExit={() => {
                     guard.leaving();
                     navigate(dashboardPath);
                  }}
                  onDirtyChange={guard.onDirtyChange}
                  onEvent={onEvent}
               />
            </Suspense>
         </NarrowEditGate>
         {guard.dialog}
      </Box>
   );
}
