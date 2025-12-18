import { Warning } from "@mui/icons-material";
import { Box, Button, Typography } from "@mui/material";
import { lazy, Suspense, useRef, useState } from "react";
import { Loading } from "../Loading";

const RenderedResult = lazy(() => import("../RenderedResult/RenderedResult"));

interface ResultContainerProps {
   result: string | undefined;
   minHeight: number;
   maxHeight: number;
   hideToggle?: boolean;
   // if Results are larger than this size, show a warning and a button to proceed
   // this is to prevent performance issues with large results.
   // the default is 0, which means no warning will be shown.
   maxResultSize?: number;
}

// ResultContainer is a component that renders a result, with a toggle button to expand/collapse the result.
// For fill-elements, the result is rendered at minHeight, and the toggle button is shown to scale up to maxHeight.
// For non-fill-elements, the result is rendered at explicitHeight, with a small window (minHeight) that can be expanded to maxHeight.
// Non-fill elements that are smaller than minHeight will be shrunk down to their natural height.
export default function ResultContainer({
   result,
   minHeight,
   maxHeight,
   hideToggle: _hideToggle = false,
   maxResultSize = 0,
}: ResultContainerProps) {
   const containerRef = useRef<HTMLDivElement>(null);
   const [measuredHeight, setMeasuredHeight] = useState(maxHeight);
   const [userAcknowledged, setUserAcknowledged] = useState(false);

   if (!result) {
      return null;
   }

   // Check if result exceeds max size and user hasn't acknowledged yet
   const exceedsMaxSize = maxResultSize > 0 && result.length > maxResultSize;
   if (exceedsMaxSize && !userAcknowledged) {
      return (
         <Box
            sx={{
               minHeight: `${minHeight}px`,
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
            height: `${renderedHeight}px`,
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
               />
            </Suspense>
         )}
      </Box>
   );
}
