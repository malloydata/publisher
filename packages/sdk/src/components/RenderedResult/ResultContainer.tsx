import { ExpandLess, ExpandMore, Warning } from "@mui/icons-material";
import { Box, Button, IconButton, Typography } from "@mui/material";
import {
   lazy,
   Suspense,
   useCallback,
   useEffect,
   useRef,
   useState,
} from "react";
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
   hideToggle = false,
   maxResultSize = 0,
}: ResultContainerProps) {
   const [isExpanded, setIsExpanded] = useState(false);
   const [contentHeight, setContentHeight] = useState<number>(0);
   const [shouldShowToggle, setShouldShowToggle] = useState(false);
   const contentRef = useRef<HTMLDivElement>(null);
   const containerRef = useRef<HTMLDivElement>(null);
   const [explicitHeight, setExplicitHeight] = useState<number>(undefined);
   const [isFillElement, setIsFillElement] = useState(false);
   const [userAcknowledged, setUserAcknowledged] = useState(false);
   const handleToggle = useCallback(() => {
      const wasExpanded = isExpanded;
      setIsExpanded(!isExpanded);

      // If we're collapsing (going from expanded to collapsed), scroll to top
      if (wasExpanded && containerRef.current) {
         setTimeout(() => {
            containerRef.current?.scrollIntoView({
               behavior: "smooth",
               block: "start",
            });
         }, 100); // Small delay to allow the collapse animation to start
      }
   }, [isExpanded]);

   // Handle size changes from RenderedResult
   const handleSizeChange = useCallback((height: number) => {
      setContentHeight(height);
   }, []);

   // Determine if toggle should be shown based on content height vs container height
   useEffect(() => {
      if (hideToggle) {
         setShouldShowToggle(false);
         return;
      }
      if (isFillElement) {
         setShouldShowToggle(true);
         return;
      }
      // Only proceed if we have a measured content height
      if (contentHeight === 0) {
         setShouldShowToggle(false);
         return;
      }

      // The available height should be the minHeight minus the padding
      // We don't subtract toggle button height here since we're deciding whether to show it
      const availableHeight = minHeight - 20; // Estimate padding
      const exceedsHeight = contentHeight > availableHeight;
      if (contentHeight < availableHeight) {
         setExplicitHeight(contentHeight + 20);
      }
      setShouldShowToggle(exceedsHeight);
   }, [contentHeight, isFillElement, minHeight, hideToggle]);

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
   const renderedHeight = isFillElement
      ? isExpanded
         ? maxHeight - 40
         : minHeight - 40
      : undefined;
   const height = explicitHeight
      ? {
           minHeight: `${explicitHeight}px`,
           height: `100%`,
        }
      : { height: `100%` };
   return (
      <>
         <Box
            ref={containerRef}
            sx={{
               position: "relative",
               minHeight: `${minHeight}px`,
               maxHeight: `${isExpanded ? maxHeight : minHeight}px`,
               border: "0px",
               borderRadius: 0,
               overflow: "hidden",
               display: "flex",
               flexDirection: "column",
               ...height,
            }}
         >
            {/* Content area */}
            <Box
               ref={contentRef}
               sx={{
                  flex: 1,
                  overflow: "hidden",
                  p: 0,
                  // Adjust bottom padding when toggle is shown to prevent content overlap
                  pb: shouldShowToggle ? "40px" : 1,
               }}
            >
               {(result && (
                  <Suspense fallback={loading}>
                     <RenderedResult
                        result={result}
                        height={renderedHeight}
                        isFillElement={(isFill) => {
                           setIsFillElement(isFill);
                        }}
                        onSizeChange={handleSizeChange}
                     />
                  </Suspense>
               )) ||
                  loading}
            </Box>

            {/* Toggle button - only show if content exceeds container height */}
            {shouldShowToggle && (
               <Box
                  sx={{
                     position: "absolute",
                     bottom: 0,
                     left: 0,
                     right: 0,
                     height: "32px",
                     backgroundColor: "rgba(255,255,255,0.75)",
                     display: "flex",
                     alignItems: "center",
                     justifyContent: "center",
                  }}
               >
                  <IconButton
                     size="small"
                     onClick={handleToggle}
                     sx={{
                        color: "text.secondary",
                        "&:hover": {
                           backgroundColor: "rgba(0, 0, 0, 0.04)",
                        },
                     }}
                     title={
                        isExpanded
                           ? "Collapse to original size"
                           : "Expand to full size"
                     }
                  >
                     {isExpanded ? (
                        <ExpandLess sx={{ fontSize: 30 }} />
                     ) : (
                        <ExpandMore sx={{ fontSize: 30 }} />
                     )}
                  </IconButton>
               </Box>
            )}
         </Box>
      </>
   );
}
