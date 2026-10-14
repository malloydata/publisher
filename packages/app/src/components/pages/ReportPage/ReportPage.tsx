// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   Notebook,
   Report,
   useGivenUrlParams,
   useNarrowScreen,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import { useEffect, useMemo } from "react";
import { logReportEvent } from "../../../utils/consoleTelemetry";
import { useDrillNavigate } from "../../common/useDrillNavigate";
import { TITLED_PAGE_FRAME_SX } from "../pageFrame";

interface ReportPageProps {
   environmentName: string;
   packageName: string;
   /** The report's package path, `sales/q3.malloy` (or a legacy `.malloynb`). */
   path: string;
   /** Whether the builder opens it, so its chunk is worth warming. Default true for a `.malloy`. */
   editable?: boolean;
}

/**
 * The Console's host for the SDK `Report`, either layout.
 *
 * Everything the component externalizes on purpose lands here: the URL sync
 * that makes a filtered report a shareable link, the drill navigation that
 * turns a target and a seeded given into a route, and in-app routing of the
 * links in a linear report's prose. The component itself reads nothing from
 * the router.
 */
export default function ReportPage({
   environmentName,
   packageName,
   path,
   editable = path.endsWith(".malloy"),
}: ReportPageProps) {
   // Control values ride in the query string, so a link reproduces the view;
   // a drill pushes a route.
   const { params: givens, onGivensChange } = useGivenUrlParams();
   const onNavigate = useDrillNavigate(environmentName, packageName);
   const onLinkNavigate = useRouterClickHandler();
   // The event names the report by path; the sink adds where it is.
   const onEvent = useMemo(
      () => logReportEvent({ environmentName, packageName }),
      [environmentName, packageName],
   );

   // The builder is a lazy chunk carrying the Malloy parser. Fetch it while
   // idle as soon as an editable report is on screen, so Edit is a re-render
   // rather than a spinner. Below 600px the editor steps aside, so there is
   // nothing to warm.
   const narrow = useNarrowScreen();
   useEffect(() => {
      if (!editable || narrow) return;
      const warm = () => void import("@malloy-publisher/sdk/builder");
      const idle = window.requestIdleCallback;
      if (idle) {
         const handle = idle(warm);
         return () => window.cancelIdleCallback?.(handle);
      }
      const timer = setTimeout(warm, 1500);
      return () => clearTimeout(timer);
   }, [editable, narrow]);

   return (
      <Box sx={TITLED_PAGE_FRAME_SX}>
         {/* A legacy `.malloynb` runs only through the deprecated notebook routes, which `Notebook` reads. */}
         {path.endsWith(".malloynb") ? (
            <Notebook
               resourceUri={encodeResourceUri({
                  environmentName,
                  packageName,
                  modelPath: path,
               })}
               givens={givens}
               onGivensChange={onGivensChange}
               onNavigate={onLinkNavigate}
               onDrillNavigate={onNavigate}
               maxResultSize={1024 * 1024}
            />
         ) : (
            <Report
               resourceUri={encodeResourceUri({ environmentName, packageName })}
               path={path}
               givens={givens}
               onGivensChange={onGivensChange}
               onNavigate={onNavigate}
               onLinkNavigate={onLinkNavigate}
               onEvent={onEvent}
               maxResultSize={1024 * 1024}
            />
         )}
      </Box>
   );
}
