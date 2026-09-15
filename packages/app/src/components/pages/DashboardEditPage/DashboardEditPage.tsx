// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Loading } from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import React, { Suspense } from "react";
import { useLocation, useNavigate } from "react-router-dom";

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
   /** The dashboard's slug: `overview`, not `dashboards/overview.malloy`. */
   dashboardName: string;
}

/**
 * The Console's host for the SDK `DashboardEditor`: `/<env>/<pkg>/dashboards/<slug>/edit`.
 * Done returns to the dashboard itself, one segment up.
 */
export default function DashboardEditPage({
   environmentName,
   packageName,
   dashboardName,
}: DashboardEditPageProps) {
   const navigate = useNavigate();
   const { pathname } = useLocation();
   const dashboardPath = pathname.replace(/\/edit\/?$/, "");
   return (
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
         <Suspense fallback={<Loading text="Opening the builder…" />}>
            <DashboardEditor
               environmentName={environmentName}
               packageName={packageName}
               dashboardName={dashboardName}
               onExit={() => navigate(dashboardPath)}
            />
         </Suspense>
      </Box>
   );
}
