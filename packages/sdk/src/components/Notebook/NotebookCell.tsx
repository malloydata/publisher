// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CloseIcon from "@mui/icons-material/Close";
import CodeIcon from "@mui/icons-material/Code";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import SearchIcon from "@mui/icons-material/Search";
import {
   Box,
   CircularProgress,
   Dialog,
   DialogContent,
   DialogTitle,
   IconButton,
   LinearProgress,
   Snackbar,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import React, { useEffect, useState } from "react";
import type { Given } from "../../client";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { parseResourceUri } from "../../utils/formatting";
import { highlight } from "../highlighter";
import { ModelExplorerDialog } from "../Model/ModelExplorerDialog";
import { FloatingIconButton } from "../FloatingIconButton";
import { Prose } from "../Prose";
import type { NavigationClick } from "../click_helper";
import { useDrill, type DrillNavigation } from "../drill";
import { createEmbeddedQueryResult } from "../QueryResult/QueryResult";
import ResultContainer from "../RenderedResult/ResultContainer";
import { NOTEBOOK_CELL_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import ResultsDialog from "../ResultsDialog";
import { CleanMetricCard } from "../styles";
import { EnhancedNotebookCell } from "./types";

interface NotebookCellProps {
   cell: EnhancedNotebookCell;
   expandCodeCell?: boolean;
   hideCodeCellIcon?: boolean;
   expandEmbedding?: boolean;
   hideEmbeddingIcon?: boolean;
   resourceUri: string;
   index: number;
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
   /**
    * The notebook's current control values, to seed the "Data Sources"
    * dialog's own so exploring from a cell starts from what the reader is
    * looking at rather than the model's bare defaults.
    */
   givens?: Record<string, string>;
   /** The notebook's declared givens, so that dialog renders their controls. */
   givenSpecs?: Given[];
}

export function NotebookCell({
   cell,
   hideCodeCellIcon,
   hideEmbeddingIcon,
   resourceUri,
   index,
   maxResultSize,
   isExecuting,
   pendingRerun,
   onNavigate,
   onDrillSelf,
   canDrillSelf,
   onDrillNavigate,
   givens,
   givenSpecs,
}: NotebookCellProps) {
   const [codeDialogOpen, setCodeDialogOpen] = React.useState<boolean>(false);
   const [embeddingDialogOpen, setEmbeddingDialogOpen] =
      React.useState<boolean>(false);
   const [resultsDialogOpen, setResultsDialogOpen] =
      React.useState<boolean>(false);
   const [highlightedMalloyCode, setHighlightedMalloyCode] =
      React.useState<string>();
   const [highlightedEmbedCode, setHighlightedEmbedCode] =
      React.useState<string>();
   const [sourcesDialogOpen, setSourcesDialogOpen] =
      React.useState<boolean>(false);

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
      // The dashboards route now exists, so a `# drill { to=<dashboard> }` from
      // a notebook cell is honoured: the host turns the destination and its
      // seeded givens into `/{env}/{pkg}/dashboards/<slug>?GIVEN=value`. Passed
      // through rather than built here, because only the host knows its routing.
      //
      // Still optional, and the reason matters: a host without a dashboard
      // route omits it, which makes a named destination non-honorable and
      // removes the AFFORDANCE along with the navigation. That is
      // `markDrillableCells`'s principle, that a destination the surface cannot
      // reach never becomes a link, and it is why this is a prop rather than
      // something the cell assumes.
      //
      // Wiring it also makes `drillMenu` reachable for the first time: a menu
      // opens only for two or more honorable destinations, so until there was a
      // second one every drillable cell applied its filter on the first click.
      onNavigate: onDrillNavigate,
      onSelf: onDrillSelf,
      // Only the notebook knows which givens it declares, and a self-drill onto
      // one it does not is refused. Asking here keeps the affordance honest
      // instead of painting a link whose click only logs a warning.
      canSelf: canDrillSelf,
      // The reader is in a notebook, so `to=self` says so: the drill tag is on
      // a shared model dimension and cannot know which document it fired in.
      selfLabel: "Filter this notebook",
   });
   // Regex to extract imported names from import statements
   const IMPORT_NAMES_REGEX = /import\s*\{([^}]+)\}\s*from\s*['"`][^'"`]+['"`]/;

   // Regex to extract model path from import statements
   const IMPORT_MODEL_PATH_REGEX =
      /import\s*(?:\{[^}]*\}\s*from\s*)?['"`]([^'"`]+)['"`]/;

   // Filter out lines starting with ## from Malloy code
   const filterMalloyCode = (code: string): string => {
      return code
         .split("\n")
         .filter((line) => !line.trimStart().startsWith("##"))
         .join("\n");
   };

   const hasValidImport =
      !!cell.text &&
      (IMPORT_NAMES_REGEX.test(cell.text) ||
         IMPORT_MODEL_PATH_REGEX.test(cell.text));
   const getInitialSourceIndex = () => {
      if (!cell.newSources || cell.newSources.length === 0) return 0;

      let importNames = [];
      let importPath = "";

      if (cell.text) {
         const namesMatch = cell.text.match(IMPORT_NAMES_REGEX);
         if (namesMatch) {
            importNames = namesMatch[1].split(",").map((name) => name.trim());
         }

         const pathMatch = cell.text.match(IMPORT_MODEL_PATH_REGEX);
         if (pathMatch) {
            importPath = pathMatch[1].trim();
         }
      }

      for (let i = 0; i < cell.newSources.length; i++) {
         try {
            const sourceInfo = JSON.parse(cell.newSources[i]);

            // Match either by imported name or by path
            if (
               (importNames.length > 0 &&
                  importNames.includes(sourceInfo.name)) ||
               (importPath && importPath === sourceInfo.path)
            ) {
               return i;
            }
         } catch (_e) {
            continue; // Skip invalid JSON
         }
      }

      return 0; // Default to the first source
   };

   // Memoized: the dialog's given controls key off this object's identity.
   const modelDataFromNewSources = React.useMemo(
      () =>
         cell.newSources && cell.newSources.length > 0
            ? {
                 sourceInfos: cell.newSources,
                 resource: resourceUri,
                 // A cell's sources carry no givens of their own; without the
                 // notebook's, the dialog has no controls and a gated source 403s.
                 givens: givenSpecs,
              }
            : undefined,
      [cell.newSources, resourceUri, givenSpecs],
   );

   const queryResultCodeSnippet = createEmbeddedQueryResult({
      query: cell.text,
      resourceUri: resourceUri,
   });

   const { mode } = usePublisherTheme();
   useEffect(() => {
      if (cell.type === "code")
         highlight(filterMalloyCode(cell.text), "malloy", mode).then((code) => {
            setHighlightedMalloyCode(code);
         });
   }, [cell, mode]);

   useEffect(() => {
      highlight(queryResultCodeSnippet, "typescript", mode).then((code) => {
         setHighlightedEmbedCode(code);
      });
   }, [queryResultCodeSnippet, mode]);

   const copyToClipboard = () => {
      const url = window.location.href;
      navigator.clipboard
         .writeText(url)
         .then(() => setCopyMessage("URL copied to clipboard!"))
         .catch(() => setCopyMessage("Failed to copy URL"));
   };

   return (
      (cell.type === "markdown" && (
         <Box>
            <Box>
               {index === 0 ? (
                  <Stack
                     direction="row"
                     alignItems="flex-start"
                     justifyContent="space-between"
                  >
                     <Prose variant="document" links={links}>
                        {cell.text}
                     </Prose>
                     <Tooltip title="Click to copy link">
                        <LinkOutlinedIcon
                           sx={{
                              fontSize: "24px",
                              color: "text.secondary",
                              cursor: "pointer",
                              marginTop: "26px",
                           }}
                           onClick={copyToClipboard}
                        />
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
            </Box>
         </Box>
      )) ||
      (cell.type === "code" && (
         <Box>
            {(!hideCodeCellIcon ||
               (!hideEmbeddingIcon && cell.result) ||
               (cell.newSources && cell.newSources.length > 0)) && (
               <Stack
                  sx={{
                     flexDirection: "column",
                     gap: "8px",
                     marginBottom: "2px",
                  }}
               >
                  {cell.newSources && cell.newSources.length > 0 && (
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
                                    fontFamily: "monospace",
                                    fontSize: "14px",
                                    flex: 1,
                                    marginRight: "8px",
                                 }}
                              />
                           )}
                           {hasValidImport && (
                              <FloatingIconButton
                                 aria-label="Data sources"
                                 sx={{ flexShrink: 0 }}
                                 onClick={() => setSourcesDialogOpen(true)}
                              >
                                 <SearchIcon />
                              </FloatingIconButton>
                           )}
                        </Box>
                     </CleanMetricCard>
                  )}
               </Stack>
            )}

            {/* Data Sources Dialog */}
            <ModelExplorerDialog
               open={sourcesDialogOpen}
               onClose={() => setSourcesDialogOpen(false)}
               title="Data Sources"
               hasValidImport={hasValidImport}
               resourceUri={resourceUri}
               data={modelDataFromNewSources}
               initialSelectedSourceIndex={getInitialSourceIndex()}
               startingGivens={givens}
            />

            {/* Code Dialog */}
            <Dialog
               open={codeDialogOpen}
               onClose={() => setCodeDialogOpen(false)}
               maxWidth="lg"
               fullWidth
            >
               <DialogTitle
                  sx={{
                     display: "flex",
                     justifyContent: "space-between",
                     alignItems: "center",
                  }}
               >
                  Malloy Code
                  <IconButton
                     onClick={() => setCodeDialogOpen(false)}
                     sx={{ color: "text.secondary" }}
                  >
                     <CloseIcon />
                  </IconButton>
               </DialogTitle>
               <DialogContent>
                  <Box
                     sx={(theme) => ({
                        border: `1px solid ${theme.palette.divider}`,
                        borderRadius: "8px",
                        padding: "16px",
                        fontFamily: "monospace",
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
               </DialogContent>
            </Dialog>

            {/* Embedding Dialog */}
            <Dialog
               open={embeddingDialogOpen}
               onClose={() => setEmbeddingDialogOpen(false)}
               maxWidth="lg"
               fullWidth
            >
               <DialogTitle
                  sx={{
                     display: "flex",
                     justifyContent: "space-between",
                     alignItems: "center",
                  }}
               >
                  Embeddable Code
                  <IconButton
                     onClick={() => setEmbeddingDialogOpen(false)}
                     sx={{ color: "text.secondary" }}
                  >
                     <CloseIcon />
                  </IconButton>
               </DialogTitle>
               <DialogContent>
                  <Stack
                     sx={{
                        flexDirection: "row",
                        justifyContent: "space-between",
                        alignItems: "flex-start",
                     }}
                  >
                     <Typography
                        component="div"
                        sx={{
                           fontSize: "12px",
                           fontFamily: "monospace",
                           "& .line": { textWrap: "wrap" },
                           flex: 1,
                        }}
                        dangerouslySetInnerHTML={{
                           __html: highlightedEmbedCode,
                        }}
                     />
                     <Tooltip title="Copy Embeddable Code">
                        <IconButton
                           sx={{
                              width: "24px",
                              height: "24px",
                              marginLeft: "8px",
                              color: "text.secondary",
                           }}
                           onClick={() => {
                              navigator.clipboard.writeText(
                                 queryResultCodeSnippet,
                              );
                           }}
                        >
                           <ContentCopyIcon />
                        </IconButton>
                     </Tooltip>
                  </Stack>
               </DialogContent>
            </Dialog>

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
                  <CleanMetricCard
                     sx={{
                        display: "flex",
                        justifyContent: "center",
                        alignItems: "center",
                        minHeight: 200,
                     }}
                  >
                     <CircularProgress size={32} />
                  </CleanMetricCard>
               )}

            {!cell.result && cell.error && (
               <CleanMetricCard sx={{ p: 2 }}>
                  <Typography variant="body2" sx={{ color: "error.main" }}>
                     This cell could not be run.
                  </Typography>
                  <Typography
                     variant="body2"
                     component="pre"
                     sx={{
                        color: "text.secondary",
                        whiteSpace: "pre-wrap",
                        m: 0,
                        mt: 1,
                     }}
                  >
                     {cell.error}
                  </Typography>
               </CleanMetricCard>
            )}

            {cell.result && pendingRerun && (
               <LinearProgress
                  aria-label="a re-run is pending"
                  sx={{ mb: 1, height: 2, borderRadius: 1 }}
               />
            )}

            {cell.result && (
               <CleanMetricCard
                  sx={{
                     position: "relative",
                  }}
               >
                  <Box
                     sx={{
                        paddingTop: "24px",
                     }}
                  >
                     <ResultContainer
                        result={cell.result}
                        maxHeight={NOTEBOOK_CELL_MAX_HEIGHT}
                        maxResultSize={maxResultSize}
                        drill={drill}
                     />
                  </Box>
                  {drillMenu}

                  {/* Top right corner controls.
                      `top: -12px` lifts the buttons above the
                      CleanMetricCard's inner ResultContainer so the
                      dashboard panel (which now paints slate in dark
                      mode) doesn't bisect them. The icons were already
                      visually overlapping the panel's top edge in
                      light mode too, but the white-on-white made it
                      invisible; in dark mode the contrast made the
                      clipping obvious. */}
                  <Stack
                     sx={{
                        position: "absolute",
                        top: "-12px",
                        right: "8px",
                        flexDirection: "row",
                        gap: "8px",
                        alignItems: "center",
                        zIndex: 2,
                     }}
                  >
                     {!hideCodeCellIcon && (
                        <FloatingIconButton
                           aria-label="Malloy code"
                           onClick={(e) => {
                              e.stopPropagation();
                              setCodeDialogOpen(true);
                           }}
                        >
                           <CodeIcon />
                        </FloatingIconButton>
                     )}
                     <FloatingIconButton
                        aria-label="Expand results"
                        onClick={() => setResultsDialogOpen(true)}
                     >
                        <SearchIcon />
                     </FloatingIconButton>
                  </Stack>
               </CleanMetricCard>
            )}
         </Box>
      ))
   );
}
