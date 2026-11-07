// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   isDrillTargetPath,
   Report,
   reportRoute,
   resolveDrillTarget,
   ServerProvider,
   useReportListing,
   type DrillNavigation,
   type DrillOutcome,
   type ThemeMode,
} from "@malloy-publisher/sdk";
import { Box, Button, Typography } from "@mui/material";
import { useCallback, useMemo, useState } from "react";
import { PublisherMuiThemeProvider } from "../theme/PublisherMuiThemeProvider";
import { reportApiAdapter, type CallReportApi } from "./reportApiAdapter";

/** Which report the panel shows, and with which filter values. */
export interface PanelTarget {
   environmentName: string;
   packageName: string;
   path: string;
   givens: Record<string, string>;
}

export interface ReportPanelProps {
   /** The report `show_report` named; undefined until the host sends it. */
   target?: PanelTarget;
   /** The tool's error text, when `show_report` refused. */
   error?: string;
   /** The REST origin the panel's script came from: the Console's address. */
   origin: string;
   /** The host's light/dark. */
   mode?: ThemeMode;
   callReportApi: CallReportApi;
   /** Opens a URL outside the panel, through the host. */
   openLink: (url: string) => void;
}

/** The Console address of a report, with its filter values. */
export function consoleUrl(origin: string, target: PanelTarget): string {
   const query = new URLSearchParams(target.givens).toString();
   return `${origin}${reportRoute(target.environmentName, target.packageName, target.path)}${query ? `?${query}` : ""}`;
}

/**
 * A report in an MCP App panel: the SDK's `Report`, its API calls carried
 * through the host as `report_api` tool calls. A drill to another report
 * opens it here, with Back to return; "Open in Publisher" takes the current
 * report and filters to the Console.
 */
export function ReportPanel({
   target,
   error,
   origin,
   mode,
   callReportApi,
   openLink,
}: ReportPanelProps) {
   const adapter = useMemo(
      () => reportApiAdapter(callReportApi),
      [callReportApi],
   );
   return (
      <ServerProvider
         baseURL={`${origin}/api/v0`}
         adapter={adapter}
         mode={mode}
         mutable={false}
      >
         <PublisherMuiThemeProvider>
            <Box sx={{ p: 1.5, bgcolor: "background.default", minHeight: 120 }}>
               {error ? (
                  <Typography color="error" sx={{ whiteSpace: "pre-wrap" }}>
                     {error}
                  </Typography>
               ) : target ? (
                  <PanelBody
                     key={`${target.environmentName}/${target.packageName}/${target.path}`}
                     initial={target}
                     origin={origin}
                     openLink={openLink}
                  />
               ) : (
                  <Typography color="text.secondary">
                     Opening the report…
                  </Typography>
               )}
            </Box>
         </PublisherMuiThemeProvider>
      </ServerProvider>
   );
}

function PanelBody({
   initial,
   origin,
   openLink,
}: {
   initial: PanelTarget;
   origin: string;
   openLink: (url: string) => void;
}) {
   // The report on screen is the last entry; a drill pushes, Back pops.
   const [stack, setStack] = useState<PanelTarget[]>([initial]);
   const current = stack[stack.length - 1];
   const { reports, settled, isError } = useReportListing(
      current.environmentName,
      current.packageName,
   );

   const onGivensChange = useCallback((givens: Record<string, string>) => {
      setStack((s) => [...s.slice(0, -1), { ...s[s.length - 1], givens }]);
   }, []);

   const onNavigate = useCallback(
      (drill: DrillNavigation): DrillOutcome => {
         if (!isDrillTargetPath(drill.path)) return "malformed";
         const resolved = resolveDrillTarget(drill.path, reports);
         if (resolved.kind !== "path") {
            return settled && !isError ? "unresolved" : "unverified";
         }
         setStack((s) => [
            ...s,
            {
               environmentName: current.environmentName,
               packageName: current.packageName,
               path: resolved.report.path,
               givens: drill.givens ?? {},
            },
         ]);
         return "resolved";
      },
      [reports, settled, isError, current.environmentName, current.packageName],
   );

   const actions = (
      <Box sx={{ display: "flex", gap: 1 }}>
         {stack.length > 1 && (
            <Button
               size="small"
               onClick={() => setStack((s) => s.slice(0, -1))}
            >
               Back
            </Button>
         )}
         <Button
            size="small"
            onClick={() => openLink(consoleUrl(origin, current))}
         >
            Open in Publisher
         </Button>
      </Box>
   );

   return (
      <Report
         key={current.path + "#" + stack.length}
         resourceUri={encodeResourceUri({
            environmentName: current.environmentName,
            packageName: current.packageName,
         })}
         path={current.path}
         givens={current.givens}
         onGivensChange={onGivensChange}
         onNavigate={onNavigate}
         onLinkNavigate={(to) => openLink(new URL(to, origin).toString())}
         actions={actions}
         // A sandboxed panel cannot start a download; the Console can.
         rowsActions={() => null}
      />
   );
}
