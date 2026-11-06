// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Warning } from "@mui/icons-material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import { Box, Button, Tooltip, Typography } from "@mui/material";
import {
   lazy,
   Suspense,
   useCallback,
   useEffect,
   useRef,
   useState,
} from "react";
import { LogMessage } from "../../client";
import type { DrillBinding } from "../drill/useDrill";
import { FloatingIconButton } from "../FloatingIconButton";
import { Loading, LOADING_COPY } from "../Loading";
import { warmMalloyRenderer } from "./loadRenderer";
import { summarizeRenderLogs } from "./renderLogs";
import {
   RESULT_SCROLLER_CLASS,
   resolveResultHeight,
   resultCap,
   type ResultSizing,
   TIGHT_ATTR,
} from "./resultSizing";

export { RESULT_SCROLLER_CLASS };

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
   /**
    * A further cap for a result whose root is a table, and only then. A
    * linear report sets it: its tiles grow to fit, except a long table, which
    * stops here and scrolls inside itself under its pinned header row.
    */
   tableMaxHeight?: number;
   /** The height to paint at before the result is measured, in place of the default (see `initialResultHeight`). */
   firstPaintHeight?: number;
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
   /**
    * Stretch to the cell this sits in: a content-sized result is floored at
    * the cell's height, so a short table reaches the bottom of its card. The
    * parent has to be a flex column with a definite height, which a dashboard
    * tile's card is. Leave it off wherever the result sets the surface's height.
    */
   fill?: boolean;
   /**
    * The cell's height is set (a tile given rows), not its content's: only
    * then is a result taller than it drawn compact to fit. A cell that grows
    * with its content is briefly shorter than a result that just grew, and
    * compact there would be a mistake that never undoes itself.
    */
   compactToFit?: boolean;
}

// ResultContainer is a component that renders a result, with a toggle button to expand/collapse the result.
// For fill-elements, the result is rendered at minHeight, and the toggle button is shown to scale up to maxHeight.
// For non-fill-elements, the result is rendered at explicitHeight, with a small window (minHeight) that can be expanded to maxHeight.
// Non-fill elements that are smaller than minHeight will be shrunk down to their natural height.
export default function ResultContainer({
   result,
   maxHeight,
   tableMaxHeight,
   firstPaintHeight,
   maxResultSize = 0,
   renderLogs,
   drill,
   fill = false,
   compactToFit = false,
}: ResultContainerProps) {
   const containerRef = useRef<HTMLDivElement>(null);
   // A result is here: start the renderer download if no panel already has.
   useEffect(() => warmMalloyRenderer(), []);
   // Both start unknown and are filled in by the render: `sizing` as soon as
   // the renderer's metadata is read, `contentHeight` only if the root has one
   // to report. `resolveResultHeight` owns what to paint at each stage.
   const [sizing, setSizing] = useState<ResultSizing | undefined>(undefined);
   const [contentHeight, setContentHeight] = useState<number | undefined>(
      undefined,
   );
   // The root's render type, which decides whether `tableMaxHeight` applies.
   const [renderAs, setRenderAs] = useState<string | undefined>(undefined);
   const onSizing = useCallback(
      (next: ResultSizing, root: string | undefined) => {
         setSizing(next);
         setRenderAs(root);
      },
      [],
   );
   const [userAcknowledged, setUserAcknowledged] = useState(false);
   const [cellHeight, setCellHeight] = useState<number | undefined>(undefined);
   const renderLogSummary = summarizeRenderLogs(renderLogs);

   const hasResult = Boolean(result);
   useEffect(() => {
      const box = containerRef.current;
      if (!fill || !box) return;
      const observer = new ResizeObserver(([entry]) => {
         setCellHeight(Math.round(entry.contentRect.height));
         setCellWidth(Math.round(entry.contentRect.width));
      });
      observer.observe(box);
      return () => observer.disconnect();
   }, [fill, hasResult, userAcknowledged]);

   // Tight: a content-sized result taller than the cell it fills (value cards
   // in a tile set shorter than they are), drawn compact so it fits rather
   // than scrolls. Held at the height it needed at full size, and let go only
   // once the cell is that tall again: let go as soon as the compact result
   // fit, it would grow back past the cell and the two would flicker.
   const [tightFrom, setTightFrom] = useState<number | undefined>(undefined);
   const [cellWidth, setCellWidth] = useState<number | undefined>(undefined);
   const mayTighten = fill && compactToFit && sizing === "content";
   // A new result, or a cell of another width, may fit at full size where the
   // last one did not: let go and measure again, rather than stay compact on
   // the strength of a height measured for something else.
   useEffect(() => {
      setTightFrom(undefined);
   }, [result, cellWidth, mayTighten]);
   useEffect(() => {
      if (!mayTighten) return;
      if (cellHeight === undefined || contentHeight === undefined) return;
      if (tightFrom === undefined && contentHeight > cellHeight + 1)
         setTightFrom(contentHeight);
      else if (tightFrom !== undefined && cellHeight >= tightFrom)
         setTightFrom(undefined);
   }, [mayTighten, cellHeight, contentHeight, tightFrom]);

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

   const loading = (
      <Loading text={LOADING_COPY.loading} centered={true} size={32} />
   );
   const cap = resultCap({ renderAs, maxHeight, tableMaxHeight });
   const renderedHeight = resolveResultHeight({
      sizing,
      contentHeight,
      maxHeight: cap,
      fillHeight: fill ? cellHeight : undefined,
      firstPaintHeight,
   });
   const renderedResult = (
      <Suspense fallback={loading}>
         <RenderedResult
            result={result}
            height={renderedHeight}
            onSizeChange={setContentHeight}
            onSizing={onSizing}
            drill={drill}
            fill={fill}
         />
      </Suspense>
   );

   return (
      <Box
         ref={containerRef}
         sx={{
            position: "relative",
            height: "auto",
            border: "0px",
            borderRadius: 0,
            // Unfilled, the box is the result's own height, so a result that
            // outgrows its cap scrolls here rather than being cut off.
            overflowX: "hidden",
            overflowY: fill ? "hidden" : "auto",
            // Grows from the unfloored height, so a result that gets shorter
            // lets its row shrink; the result is out of flow for the same reason.
            ...(fill && {
               flex: `1 1 ${resolveResultHeight({ sizing, contentHeight, maxHeight: cap, firstPaintHeight })}px`,
               minHeight: 0,
            }),
         }}
      >
         {fill ? (
            // Scrolls rather than clips. A result taller than its cap (a row of
            // KPI cards wrapped one per line in a narrow tile, say) draws past
            // the box it was given; clipped, its last cards could not be reached
            // at all. A table never overflows here: it is held to the box and
            // scrolls inside itself, under its own pinned header.
            <Box
               className={RESULT_SCROLLER_CLASS}
               {...(mayTighten && tightFrom !== undefined
                  ? { [TIGHT_ATTR]: "" }
                  : {})}
               sx={{
                  position: "absolute",
                  inset: 0,
                  overflowX: "hidden",
                  overflowY: "auto",
               }}
            >
               {renderedResult}
            </Box>
         ) : (
            renderedResult
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
