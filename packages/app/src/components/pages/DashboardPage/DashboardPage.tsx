// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BackLink,
   Dashboard,
   DashboardBar,
   encodeResourceUri,
   SecondaryButton,
   useGivenUrlParams,
} from "@malloy-publisher/sdk";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import { Box } from "@mui/material";
import { useEffect, useMemo } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { logDashboardEvent } from "../../../utils/consoleTelemetry";
import { useDrillNavigate } from "../../common/useDrillNavigate";

export interface DashboardPageProps {
   environmentName: string;
   packageName: string;
   /** The dashboard's slug: `overview`, not `dashboards/overview.malloy`. */
   dashboardName: string;
}

/**
 * The Console's host for the SDK `Dashboard`.
 *
 * Everything the component externalizes on purpose lands here: the URL sync that
 * makes a filtered dashboard a shareable link, and the drill navigation that
 * turns a slug and a seeded given into a route. The component itself reads
 * nothing from the router.
 */
export default function DashboardPage({
   environmentName,
   packageName,
   dashboardName,
}: DashboardPageProps) {
   // Control values ride in the query string, so a link reproduces the view;
   // a drill pushes a route. Both are shared with NotebookPage.
   const { params: givens, onGivensChange } = useGivenUrlParams();
   const onNavigate = useDrillNavigate(environmentName, packageName);
   // Into the builder, one segment down; the builder's Done comes back here.
   const navigate = useNavigate();
   const { pathname } = useLocation();
   const onEvent = useMemo(
      () => logDashboardEvent({ environmentName, packageName, dashboardName }),
      [environmentName, packageName, dashboardName],
   );

   // The builder is a lazy chunk carrying the Malloy parser, so the first Edit
   // used to sit on a spinner while it downloaded. Fetch it as soon as a
   // dashboard is on screen: by the time anyone reaches for Edit it is usually
   // already here, and the switch is then a re-render rather than a page that
   // empties and refills. Idle time, and the browser caches the module, so a
   // reader who never edits pays one background request.
   useEffect(() => {
      const warm = () => void import("@malloy-publisher/sdk/builder");
      const idle = window.requestIdleCallback;
      if (idle) {
         const handle = idle(warm);
         return () => window.cancelIdleCallback?.(handle);
      }
      const timer = setTimeout(warm, 1500);
      return () => clearTimeout(timer);
   }, []);

   return (
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
         <BackLink
            label={packageName}
            href={`/${environmentName}/${packageName}`}
            onClick={() => navigate(`/${environmentName}/${packageName}`)}
         />
         {/* The same bar the builder has, with the same button in the same
             place: Edit becomes Done and nothing else on the page moves. */}
         <DashboardBar>
            <SecondaryButton
               label="Edit"
               icon={<EditOutlinedIcon />}
               onClick={() => navigate(`${pathname.replace(/\/$/, "")}/edit`)}
            />
         </DashboardBar>
         <Dashboard
            resourceUri={encodeResourceUri({ environmentName, packageName })}
            dashboard={dashboardName}
            givens={givens}
            onGivensChange={onGivensChange}
            onNavigate={onNavigate}
            onEvent={onEvent}
            maxResultSize={1024 * 1024}
         />
      </Box>
   );
}
