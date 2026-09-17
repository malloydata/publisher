// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Warning } from "@mui/icons-material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { Box, Button, Tooltip, Typography } from "@mui/material";
import { lazy, Suspense, useRef, useState } from "react";
import { LogMessage } from "../../client";
import type { DrillBinding } from "../drill/useDrill";
import { FloatingIconButton } from "../FloatingIconButton";
import { Loading } from "../Loading";
import { summarizeRenderLogs } from "./renderLogs";
import { resolveResultHeight, type ResultSizing } from "./resultSizing";

const RenderedResult = lazy(() => import("../RenderedResult/RenderedResult"));

interface ResultContainerProps {
   result: string | undefined;
   /**
    * Cap on the panel's height. A CAP, not a height request: a result shorter
    * than this paints at its own height. Leave it out for no cap, which is what
    * the single-query dashboard form wants — a result that IS the page should
    * not be clipped to tidy it.
    */
   maxHeight?: number;
   // if Results are larger than this size, show a warning and a button to proceed
   // this is to prevent performance issues with large results.
   // the default is 0, which means no warning will be shown.
   maxResultSize?: number;
   // Render tag findings from the query response. Callers whose result did not
   // come from a query response have none to pass.
   renderLogs?: LogMessage[];
   // `# drill` click handling and its affordance. Passed straight through: this
   // is the one render path dashboards and notebooks share, which is what lets
   // drill be implemented once for both.
   drill?: DrillBinding;
}

// ResultContainer is a component that renders a result, with a toggle button to expand/collapse the result.
// For fill-elements, the result is rendered at minHeight, and the toggle button is shown to scale up to maxHeight.
// For non-fill-elements, the result is rendered at explicitHeight, with a small window (minHeight) that can be expanded to maxHeight.
// Non-fill elements that are smaller than minHeight will be shrunk down to their natural height.
export default function ResultContainer({
   result,
   maxHeight,
   maxResultSize = 0,
   renderLogs,
   drill,
}: ResultContainerProps) {
   const containerRef = useRef<HTMLDivElement>(null);
   // Both start unknown and are filled in by the render: `sizing` as soon as
   // the renderer's metadata is read, `contentHeight` only if the root has one
   // to report. `resolveResultHeight` owns what to paint at each stage.
   const [sizing, setSizing] = useState<ResultSizing | undefined>(undefined);
   const [contentHeight, setContentHeight] = useState<number | undefined>(
      undefined,
   );
   const [userAcknowledged, setUserAcknowledged] = useState(false);
   const renderLogSummary = summarizeRenderLogs(renderLogs);

   if (!result) {
      return null;
   }

   // Check if result exceeds max size and user hasn't acknowledged yet
   const exceedsMaxSize = maxResultSize > 0 && result.length > maxResultSize;
   if (exceedsMaxSize && !userAcknowledged) {
      return (
         <Box
            sx={{
               // minHeight: `${minHeight}px`,
               display: "flex",
               flexDirection: "column",
               alignItems: "center",
               justifyContent: "center",
               gap: 2,
               p: 4,
               backgroundColor: "#fafafa",
               border: "1px solid",
               borderColor: "#e0e0e0",
               borderRadius: 1,
            }}
         >
            <Warning sx={{ fontSize: 48, color: "#757575" }} />
            <Typography variant="h6" color="text.secondary" align="center">
               Processing large results may cause browser performance issues.
               Proceed?
            </Typography>
            <Button
               variant="contained"
               color="primary"
               onClick={() => setUserAcknowledged(true)}
            >
               Proceed
            </Button>
         </Box>
      );
   }

   const loading = <Loading text="Loading..." centered={true} size={32} />;
   const renderedHeight = resolveResultHeight({
      sizing,
      contentHeight,
      maxHeight,
   });

   return (
      <Box
         ref={containerRef}
         sx={{
            position: "relative",
            height: "auto",
            border: "0px",
            borderRadius: 0,
            overflow: "hidden",
         }}
      >
         {result && (
            <Suspense fallback={loading}>
               <RenderedResult
                  result={result}
                  height={renderedHeight}
                  onSizeChange={setContentHeight}
                  onSizing={setSizing}
                  drill={drill}
               />
            </Suspense>
         )}
         {renderLogSummary && (
            // Overlaid rather than stacked, so the note cannot change the height
            // the container just measured. The button is what makes the message
            // reachable: an icon alone is aria-hidden and cannot take focus.
            <Box sx={{ position: "absolute", top: 4, right: 4, zIndex: 1 }}>
               <Tooltip
                  title={renderLogSummary.title}
                  slotProps={{ tooltip: { sx: { whiteSpace: "pre-line" } } }}
               >
                  <FloatingIconButton aria-label="Render tag warnings">
                     <InfoOutlinedIcon
                        fontSize="small"
                        color={
                           renderLogSummary.severity === "error"
                              ? "error"
                              : "warning"
                        }
                     />
                  </FloatingIconButton>
               </Tooltip>
            </Box>
         )}
      </Box>
   );
}
