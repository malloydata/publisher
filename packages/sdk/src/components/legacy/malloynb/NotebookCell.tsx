// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @deprecated `.malloynb` only; deleted with the format. One cell of a
 * `.malloynb` notebook: prose, or code with its result in a report tile.
 */

import CodeIcon from "@mui/icons-material/Code";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import SearchIcon from "@mui/icons-material/Search";
import {
   Box,
   CircularProgress,
   IconButton,
   LinearProgress,
   Snackbar,
   Stack,
   Tooltip,
} from "@mui/material";
import React, { useEffect, useState } from "react";
import { usePublisherTheme } from "../../../theme/ThemeContext";
import { parseResourceUri } from "../../../utils/formatting";
import { ErrorDetailAlert } from "../../ApiErrorDisplay";
import { AppDialog } from "../../AppDialog";
import { highlight } from "../../highlighter";
import { FloatingIconButton } from "../../FloatingIconButton";
import { Prose } from "../../Prose";
import type { NavigationClick } from "../../click_helper";
import { useDrill, type DrillNavigation } from "../../drill";
import ResultContainer from "../../RenderedResult/ResultContainer";
import { NOTEBOOK_CELL_MAX_HEIGHT } from "../../RenderedResult/resultSizing";
import ResultsDialog from "../../ResultsDialog";
import { CleanMetricCard, MONO_FONT_FAMILY } from "../../styles";
import { stripProse } from "./cellKind";
import { TileCard, TileHeading } from "../../Dashboard/TileCard";
import { TextTileFrame } from "../../Report/ReportPage";
import { EnhancedNotebookCell } from "./types";

interface NotebookCellProps {
   cell: EnhancedNotebookCell;
   resourceUri: string;
   /** Whether this is the notebook's first markdown cell, which carries the copy-link icon. */
   showCopyLink?: boolean;
   maxResultSize?: number;
   isExecuting?: boolean;
   /**
    * A re-run is coming but has not started, i.e. the given-settle window.
    * Separate from `isExecuting` because the spinner below is gated on
    * `!cell.result`, so on a re-run (where a result IS present) it renders
    * nothing at all, which is exactly the case the settle window needs to
    * signal: the values on screen are already stale.
    */
   pendingRerun?: boolean;
   // Takes the modifier subset rather than a synthetic event, so a drill click
   // (which the Malloy renderer reports as a DOM event) can navigate without
   // being converted into a React one first.
   onNavigate?: (to: string, event?: NavigationClick) => void;
   /**
    * Applies a `# drill { to=self }` in place, by setting the named parameter
    * on the notebook that owns this cell. Omitted when the notebook declares no
    * givens, which makes a self-drill inert rather than an error.
    *
    * Takes the clicked value unencoded: only the notebook knows the declared
    * type of the given being set, and the encoding depends on it.
    */
   onDrillSelf?: (given: string, rawValue: unknown) => void;
   /**
    * Whether the owning notebook declares a given, so a `to=self` drill onto it
    * can be honoured. Without it every self-drill reads as clickable and the
    * refusal only shows up in the console.
    */
   canDrillSelf?: (given: string) => boolean;
   /**
    * Opens a `# drill { to=<dashboard> }` destination, seeded with the clicked
    * value. Distinct from {@link onNavigate}, which routes a markdown link and
    * takes a URL: this one takes a destination and the givens to seed, and only
    * the host knows how to turn those into a route.
    *
    * Omitted on a host with no dashboard route, which makes a cross-dashboard
    * drill inert rather than dead-ended, and removes the affordance with it.
    */
   onDrillNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
}

export function NotebookCell({
   cell,
   resourceUri,
   showCopyLink,
   maxResultSize,
   isExecuting,
   pendingRerun,
   onNavigate,
   onDrillSelf,
   canDrillSelf,
   onDrillNavigate,
}: NotebookCellProps) {
   const [codeDialogOpen, setCodeDialogOpen] = React.useState<boolean>(false);
   const [resultsDialogOpen, setResultsDialogOpen] =
      React.useState<boolean>(false);
   const [highlightedMalloyCode, setHighlightedMalloyCode] =
      React.useState<string>();

   const [copyMessage, setCopyMessage] = useState("");

   const { environmentName, packageName, modelPath } =
      parseResourceUri(resourceUri);
   // Links in a markdown cell are authored relative to the notebook file, and
   // route through the host when it gave us a way to.
   const links = {
      environmentName,
      packageName,
      sourcePath: modelPath,
      onNavigate,
   };

   // `# drill` is declared on a model dimension, so a notebook cell that groups
   // by that dimension is clickable for free: the same resolution, and the
   // same hook, the dashboard viewer uses.
   const { drill, drillMenu } = useDrill({
      // A `# drill { to=<report> }` from a cell is honoured: the host resolves
      // the destination (a package path from the root) and turns it and its
      // seeded givens into the report's route, the Console's being
      // `/{env}/{pkg}/<path>?GIVEN=value`. Passed through rather than built
      // here, because only the host knows its routing.
      //
      // Still optional, and the reason matters: a host without a dashboard
      // route omits it, which makes a named destination non-honorable and
      // removes the AFFORDANCE along with the navigation. That is
      // `markDrillableCells`'s principle, that a destination the surface cannot
      // reach never becomes a link, and it is why this is a prop rather than
      // something the cell assumes.
      onNavigate: onDrillNavigate,
      onSelf: onDrillSelf,
      // Only the notebook knows which givens it declares, and a self-drill onto
      // one it does not is refused. Asking here keeps the affordance honest
      // instead of painting a link whose click only logs a warning.
      canSelf: canDrillSelf,
   });
   // Regex to extract imported names from import statements
   const IMPORT_NAMES_REGEX = /import\s*\{([^}]+)\}\s*from\s*['"`][^'"`]+['"`]/;

   // Regex to extract model path from import statements
   const IMPORT_MODEL_PATH_REGEX =
      /import\s*(?:\{[^}]*\}\s*from\s*)?['"`]([^'"`]+)['"`]/;

   const hasValidImport =
      !!cell.text &&
      (IMPORT_NAMES_REGEX.test(cell.text) ||
         IMPORT_MODEL_PATH_REGEX.test(cell.text));

   const { mode } = usePublisherTheme();
   useEffect(() => {
      if (cell.type === "code")
         highlight(stripProse(cell), "malloy", mode).then((code) => {
            setHighlightedMalloyCode(code);
         });
   }, [cell, mode]);

   const copyToClipboard = () => {
      const url = window.location.href;
      navigator.clipboard
         .writeText(url)
         .then(() => setCopyMessage("URL copied to clipboard!"))
         .catch(() => setCopyMessage("Failed to copy URL"));
   };

   const codeDialog = (
      <AppDialog
         open={codeDialogOpen}
         onClose={() => setCodeDialogOpen(false)}
         title="Malloy code"
         maxWidth="lg"
         showClose
      >
         <Box
            sx={(theme) => ({
               border: `1px solid ${theme.palette.divider}`,
               borderRadius: "8px",
               padding: "16px",
               fontFamily: MONO_FONT_FAMILY,
               fontSize: "14px",
               lineHeight: "1.5",
               overflow: "auto",
               maxHeight: "70vh",
               backgroundColor: theme.palette.background.paper,
               color: theme.palette.text.primary,
            })}
         >
            <pre
               className="code-display"
               style={{
                  margin: 0,
               }}
               dangerouslySetInnerHTML={{
                  __html: highlightedMalloyCode,
               }}
            />
         </Box>
      </AppDialog>
   );

   return (
      (cell.type === "markdown" && (
         <Box>
            <TextTileFrame>
               {showCopyLink ? (
                  <Stack
                     direction="row"
                     alignItems="flex-start"
                     justifyContent="space-between"
                  >
                     <Prose variant="document" links={links}>
                        {cell.text}
                     </Prose>
                     <Tooltip title="Copy link">
                        <IconButton
                           aria-label="Copy link"
                           onClick={copyToClipboard}
                           sx={{ color: "text.secondary", mt: "18px" }}
                        >
                           <LinkOutlinedIcon sx={{ fontSize: "24px" }} />
                        </IconButton>
                     </Tooltip>
                  </Stack>
               ) : (
                  <Prose variant="document" links={links}>
                     {cell.text}
                  </Prose>
               )}
               <Snackbar
                  open={copyMessage !== ""}
                  autoHideDuration={6000}
                  onClose={() => setCopyMessage("")}
                  message={copyMessage}
               />
            </TextTileFrame>
         </Box>
      )) ||
      (cell.type === "code" && (
         <Box>
            {cell.newSources && cell.newSources.length > 0 && (
               <Stack
                  sx={{
                     flexDirection: "column",
                     gap: "8px",
                     marginBottom: "2px",
                  }}
               >
                  <CleanMetricCard
                     sx={{
                        position: "relative",
                        padding: "0",
                     }}
                  >
                     <Box
                        sx={{
                           display: "flex",
                           alignItems: "center",
                           justifyContent: "space-between",
                           paddingLeft: "24px",
                           paddingRight: "8px",
                        }}
                     >
                        {/* This shouldn't be needed but there's a compiler bug */}
                        {highlightedMalloyCode && (
                           <span
                              dangerouslySetInnerHTML={{
                                 __html: highlightedMalloyCode,
                              }}
                              style={{
                                 fontFamily: MONO_FONT_FAMILY,
                                 fontSize: "14px",
                                 flex: 1,
                                 marginRight: "8px",
                              }}
                           />
                        )}
                     </Box>
                  </CleanMetricCard>
               </Stack>
            )}

            {codeDialog}
            {/* Results Dialog */}
            <ResultsDialog
               open={resultsDialogOpen}
               onClose={() => setResultsDialogOpen(false)}
               result={cell.result || ""}
               title="Results"
               drill={drill}
            />

            {/* Loading state for executing code cells (not import cells) */}
            {isExecuting &&
               !cell.result &&
               /* A failed cell keeps `isExecuting` (it is notebook-wide, not
                  per cell), so without this the error card below renders under
                  a spinner and the cell reads as still loading. */
               !cell.error &&
               !hasValidImport &&
               !(cell.newSources && cell.newSources.length > 0) && (
                  <TileCard>
                     <Box
                        sx={{
                           display: "flex",
                           justifyContent: "center",
                           alignItems: "center",
                           minHeight: 200,
                        }}
                     >
                        <CircularProgress size={32} />
                     </Box>
                  </TileCard>
               )}

            {!cell.result && cell.error && (
               <ErrorDetailAlert
                  summary="This cell could not be run."
                  detail={cell.error}
               />
            )}

            {cell.result && pendingRerun && (
               <LinearProgress
                  aria-label="Re-run pending"
                  sx={{ mb: 1, height: 2, borderRadius: 1 }}
               />
            )}

            {cell.result && (
               // The tile a grid draws: the cell's own actions on the heading's
               // row, then the result.
               <TileCard>
                  <TileHeading
                     title=""
                     action={
                        <Stack direction="row" sx={{ gap: 0.5, mt: "-4px" }}>
                           <FloatingIconButton
                              aria-label="Malloy code"
                              onClick={(e) => {
                                 e.stopPropagation();
                                 setCodeDialogOpen(true);
                              }}
                           >
                              <CodeIcon />
                           </FloatingIconButton>
                           <FloatingIconButton
                              aria-label="Expand results"
                              onClick={() => setResultsDialogOpen(true)}
                           >
                              <SearchIcon />
                           </FloatingIconButton>
                        </Stack>
                     }
                  />
                  <ResultContainer
                     result={cell.result}
                     maxHeight={NOTEBOOK_CELL_MAX_HEIGHT}
                     maxResultSize={maxResultSize}
                     drill={drill}
                  />
                  {drillMenu}
               </TileCard>
            )}
         </Box>
      ))
   );
}
