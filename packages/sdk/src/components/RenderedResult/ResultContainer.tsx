// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Warning } from "@mui/icons-material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { Box, Button, IconButton, Tooltip, Typography } from "@mui/material";
import { lazy, Suspense, useRef, useState } from "react";
import { LogMessage } from "../../client";
import type { DrillBinding } from "../drill/useDrill";
import { Loading } from "../Loading";
import { summarizeRenderLogs } from "./renderLogs";

const RenderedResult = lazy(() => import("../RenderedResult/RenderedResult"));

interface ResultContainerProps {
   result: string | undefined;
   maxHeight: number;
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

/**
 * Height to paint at before the result has been measured.
 *
 * `maxHeight` is a CAP, but it was also the first paint's height, because the
 * measured height starts out equal to it. That was harmless while every caller
 * passed something viewport-sized (400 to 800). A caller that means "no cap"
 * passes a number that is not a height anyone wants to see: the dashboard
 * viewer's whole-page form passes 20000, so the panel painted twenty thousand
 * pixels tall until the measurement landed, and a result the renderer sizes to
 * its CONTAINER (a top-level chart, as opposed to a `# dashboard` grid, which
 * reports its own height) measured that back and kept it.
 *
 * Bounded here rather than by lowering the cap, so "no cap" stays expressible.
 * Above the cap it does nothing, which is every caller that passes a real
 * height, so this changes nothing for them.
 */
const INITIAL_RENDER_HEIGHT = 2000;

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
   const [measuredHeight, setMeasuredHeight] = useState(
      Math.min(maxHeight, INITIAL_RENDER_HEIGHT),
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
   // Fixed height for content - no resizing
   const renderedHeight = Math.min(maxHeight, measuredHeight);

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
                  onSizeChange={setMeasuredHeight}
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
                  <IconButton
                     size="small"
                     aria-label="Render tag warnings"
                     sx={{
                        backgroundColor: "rgba(255, 255, 255, 0.9)",
                        "&:hover": {
                           backgroundColor: "rgba(255, 255, 255, 1)",
                        },
                     }}
                  >
                     <InfoOutlinedIcon
                        fontSize="small"
                        color={
                           renderLogSummary.severity === "error"
                              ? "error"
                              : "warning"
                        }
                     />
                  </IconButton>
               </Tooltip>
            </Box>
         )}
      </Box>
   );
}
