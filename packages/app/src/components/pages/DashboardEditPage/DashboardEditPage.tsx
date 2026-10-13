// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   NarrowEditGate,
   Report,
   type ReportLayout,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import React, { Suspense, useMemo } from "react";
import { logReportEvent } from "../../../utils/consoleTelemetry";
import { useLeaveGuard } from "../useLeaveGuard";
import { loadBuilder } from "./loadBuilder";
import { TITLED_PAGE_FRAME_SX } from "../pageFrame";

/**
 * The builder's entry is loaded here and nowhere else. It carries the Malloy
 * parser (440 KB gzipped), which most Console users never need, so it stays out
 * of every other page's chunk and downloads the first time someone opens a
 * report for editing.
 */
const ReportEditor = React.lazy(() =>
   loadBuilder().then((module) => ({
      default: module.ReportEditor,
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
 * The Console's host for the SDK `ReportEditor`: `/<env>/<pkg>/<path>?edit`,
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
   // While the builder's code and data load, the report as the reader drew
   // it rather than a spinner: the builder then replaces it in place, drawing
   // the same page from the same cached results, so switching to edit does not
   // blank the page. No `onGivensChange`: this copy does not write the URL.
   const reading = (
      <Report
         resourceUri={encodeResourceUri({ environmentName, packageName })}
         path={path}
      />
   );
   return (
      // The reader's page width and edges, the same for either layout, so the
      // margins do not move between modes.
      <Box sx={TITLED_PAGE_FRAME_SX}>
         <NarrowEditGate>
            <Suspense fallback={reading}>
               <ReportEditor
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
                  loading={reading}
               />
            </Suspense>
         </NarrowEditGate>
         {guard.dialog}
      </Box>
   );
}
