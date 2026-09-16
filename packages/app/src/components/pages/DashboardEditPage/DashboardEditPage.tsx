// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { BackLink, DashboardBar, Loading } from "@malloy-publisher/sdk";
import { Box, Stack } from "@mui/material";
import React, { Suspense, useMemo } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { logDashboardEvent } from "../../../utils/dashboardTelemetry";

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
   const onEvent = useMemo(
      () => logDashboardEvent({ environmentName, packageName, dashboardName }),
      [environmentName, packageName, dashboardName],
   );
   return (
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
         {/* The same way up the reader's view has, in the same place, so the
             bar below it sits at the same height in both modes. Leaving by it
             is leaving without saving, exactly as the browser's own Back is;
             Done is the way out that keeps the page you were on. */}
         <BackLink
            label={packageName}
            onClick={() => navigate(`/${environmentName}/${packageName}`)}
         />
         {/* The bar, at the height the reader's view had it, so the page does
             not collapse and refill while the builder's chunk arrives. */}
         <Suspense
            fallback={
               <Stack sx={{ gap: 2 }}>
                  <DashboardBar />
                  <Loading text="Opening the builder…" />
               </Stack>
            }
         >
            <DashboardEditor
               environmentName={environmentName}
               packageName={packageName}
               dashboardName={dashboardName}
               onExit={() => navigate(dashboardPath)}
               onEvent={onEvent}
            />
         </Suspense>
      </Box>
   );
}
