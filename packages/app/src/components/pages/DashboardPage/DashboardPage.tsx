// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BackLink,
   Dashboard,
   encodeResourceUri,
   SecondaryButton,
   useGivenUrlParams,
} from "@malloy-publisher/sdk";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import { Box, Stack } from "@mui/material";
import { useMemo } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { logDashboardEvent } from "../../../utils/dashboardTelemetry";
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

   return (
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
         <Stack
            direction="row"
            sx={{ justifyContent: "space-between", alignItems: "center" }}
         >
            <BackLink
               label={packageName}
               onClick={() => navigate(`/${environmentName}/${packageName}`)}
            />
            <SecondaryButton
               label="Edit"
               icon={<EditOutlinedIcon />}
               onClick={() => navigate(`${pathname.replace(/\/$/, "")}/edit`)}
            />
         </Stack>
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
