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
import Markdown from "markdown-to-jsx";
import React, { useEffect, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { parseResourceUri } from "../../utils/formatting";
import { highlight } from "../highlighter";
import { ModelExplorerDialog } from "../Model/ModelExplorerDialog";
import type { NavigationClick } from "../click_helper";
import { useDrill } from "../drill";
import { createEmbeddedQueryResult } from "../QueryResult/QueryResult";
import ResultContainer from "../RenderedResult/ResultContainer";
import ResultsDialog from "../ResultsDialog";
import { CleanMetricCard, CleanNotebookCell } from "../styles";
import { EnhancedNotebookCell } from "./types";

/**
 * Cap for a cell result that lays itself out: a table, mostly. Tall enough for
 * a screenful of rows, after which the cell scrolls rather than the page
 * turning into one long table.
 */
const CELL_MAX_HEIGHT = 700;

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
}

interface NotebookMarkdownLinkProps {
   href?: string;
   title?: string;
   children?: React.ReactNode;
   envName: string;
   pkgName: string;
   sourceDir: string;
   onNavigate?: (to: string, event?: NavigationClick) => void;
}

// Links inside a rendered notebook/README are authored relative to the source
// file (e.g. `spielberg.malloynb`). A plain browser anchor resolves them
// against the current page URL, which drops the package segment for a README
// shown at the package route. Resolve relative links against the source file's
// own directory instead and route them through the SPA. External, hash, and
// absolute links stay as normal anchors.
function NotebookMarkdownLink({
   href,
   title,
   children,
   envName,
   pkgName,
   sourceDir,
   onNavigate,
}: NotebookMarkdownLinkProps) {
   const hasScheme = !!href && /^[a-z][a-z0-9+.-]*:/i.test(href);
   const isInternalRelative =
      !!href && !hasScheme && !href.startsWith("/") && !href.startsWith("#");

   if (!isInternalRelative || !href) {
      // External, absolute, or hash link: render a normal anchor. Only allow
      // safe schemes so a crafted `javascript:`/`data:` href in a package
      // README (packages can come from untrusted git/S3 sources) cannot run.
      const isUnsafeScheme =
         hasScheme && !/^(https?|mailto|tel):/i.test(href ?? "");
      return (
         <a
            href={isUnsafeScheme ? undefined : href}
            title={title}
            {...(hasScheme
               ? { target: "_blank", rel: "noopener noreferrer" }
               : {})}
         >
            {children}
         </a>
      );
   }

   // Resolve the relative href against the source file's directory (empty for a
   // package-root README) via a dummy origin, which normalizes any `./`/`../`
   // and clamps escapes at the package root, then prefix the package path.
   const resolved = new URL(href, `https://malloy.invalid/${sourceDir}`);
   const packageRelative =
      resolved.pathname.slice(1) + resolved.search + resolved.hash;
   const to = `/${envName}/${pkgName}/${packageRelative}`;
   // With a host router (onNavigate) intercept and route through the SPA.
   // Without one (router-free SDK embedding) the absolute href navigates on
   // its own, so rendering a notebook needs no react-router context.
   return (
      <a
         href={to}
         title={title}
         onClick={
            onNavigate
               ? (event) => {
                    event.preventDefault();
                    onNavigate(to, event);
                 }
               : undefined
         }
      >
         {children}
      </a>
   );
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

   // `# drill` is declared on a model dimension, so a notebook cell that groups
   // by that dimension is clickable for free: the same resolution, and the
   // same hook, the dashboard viewer uses.
   const { drill, drillMenu } = useDrill({
      // No `onNavigate` yet, deliberately: a `# drill { to=<dashboard> }` would
      // route to `/{env}/{pkg}/dashboards/<slug>`, and no such route exists.
      // The Console's catch-all sends it to `ModelPage`, which answers "Nothing
      // to open at this path". Wiring it now would paint those cells as links to a
      // dead page, and `markDrillableCells` exists on the opposite principle:
      // "a destination the surface cannot reach never becomes a link: a dead
      // link is worse than none." Omitting it makes a named destination
      // non-honorable, which removes the affordance as well as the navigation,
      // so a `to=self` drill still works and a cross-dashboard one is simply
      // inert until the slice that adds the route also passes this handler.
      //
      // One consequence to know rather than rediscover: `drillMenu` below is
      // therefore unreachable for now. A menu only opens for two or more
      // honorable destinations, and `self` is the only honorable one, so a
      // drillable cell applies its filter on the first click. The menu comes
      // back with the navigation handler.
      onSelf: onDrillSelf,
      // Only the notebook knows which givens it declares, and a self-drill onto
      // one it does not is refused. Asking here keeps the affordance honest
      // instead of painting a link whose click only logs a warning.
      canSelf: canDrillSelf,
      // The reader is in a notebook, so `to=self` says so: the drill tag is on
      // a shared model dimension and cannot know which document it fired in.
      selfLabel: "Filter this notebook",
   });
   // Directory of the source file within the package (empty for a package-root
   // README), used to resolve relative links the author wrote against it.
   const sourceDir =
      modelPath && modelPath.includes("/")
         ? modelPath.slice(0, modelPath.lastIndexOf("/") + 1)
         : "";
   const markdownOptions = {
      overrides: {
         a: {
            component: NotebookMarkdownLink,
            props: {
               envName: environmentName,
               pkgName: packageName,
               sourceDir,
               onNavigate,
            },
         },
      },
   };

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

   const modelDataFromNewSources =
      cell.newSources && cell.newSources.length > 0
         ? {
              sourceInfos: cell.newSources,
              resource: resourceUri,
           }
         : undefined;

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
         <CleanNotebookCell>
            <Box
               sx={(theme) => ({
                  "& h1, & h2, & h3, & h4, & h5, & h6": {
                     fontWeight: "600",
                     color: theme.palette.text.primary,
                     marginBottom: "8px",
                     marginTop: "16px",
                  },
                  "& h1": { fontSize: "28px" },
                  "& h2": { fontSize: "24px" },
                  "& h3": { fontSize: "20px" },
                  "& p": {
                     color: theme.palette.text.primary,
                     lineHeight: "1.7",
                     marginBottom: "8px",
                     fontSize: "16px",
                  },
                  "& ul, & ol": {
                     color: theme.palette.text.primary,
                     lineHeight: "1.7",
                     marginBottom: "8px",
                     fontSize: "16px",
                  },
                  "& li": {
                     marginBottom: "4px",
                  },
               })}
            >
               {index === 0 ? (
                  <Stack
                     direction="row"
                     alignItems="flex-start"
                     justifyContent="space-between"
                  >
                     <Markdown options={markdownOptions}>{cell.text}</Markdown>
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
                  <Markdown options={markdownOptions}>{cell.text}</Markdown>
               )}
               <Snackbar
                  open={copyMessage !== ""}
                  autoHideDuration={6000}
                  onClose={() => setCopyMessage("")}
                  message={copyMessage}
               />
            </Box>
         </CleanNotebookCell>
      )) ||
      (cell.type === "code" && (
         <CleanNotebookCell>
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
                              <IconButton
                                 sx={{
                                    backgroundColor: "rgba(255, 255, 255, 0.9)",
                                    "&:hover": {
                                       backgroundColor:
                                          "rgba(255, 255, 255, 1)",
                                    },
                                    width: "32px",
                                    height: "32px",
                                    flexShrink: 0,
                                 }}
                                 onClick={() => setSourcesDialogOpen(true)}
                              >
                                 <SearchIcon
                                    sx={{
                                       fontSize: "18px",
                                       // grey.700 is a dark warm grey
                                       // that stays legible on the
                                       // white-ish IconButton in both
                                       // light and dark mode. The
                                       // button background is
                                       // hardcoded white, so the icon
                                       // must be hardcoded dark.
                                       color: "grey.700",
                                    }}
                                 />
                              </IconButton>
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
                        maxHeight={CELL_MAX_HEIGHT}
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
                        <IconButton
                           sx={{
                              backgroundColor: "rgba(255, 255, 255, 0.9)",
                              "&:hover": {
                                 backgroundColor: "rgba(255, 255, 255, 1)",
                              },
                              width: "32px",
                              height: "32px",
                           }}
                           onClick={(e) => {
                              e.stopPropagation();
                              setCodeDialogOpen(true);
                           }}
                        >
                           <CodeIcon
                              sx={{ fontSize: "18px", color: "grey.700" }}
                           />
                        </IconButton>
                     )}
                     <IconButton
                        sx={{
                           backgroundColor: "rgba(255, 255, 255, 0.9)",
                           "&:hover": {
                              backgroundColor: "rgba(255, 255, 255, 1)",
                           },
                           width: "32px",
                           height: "32px",
                        }}
                        onClick={() => setResultsDialogOpen(true)}
                     >
                        <SearchIcon
                           sx={{ fontSize: "18px", color: "grey.700" }}
                        />
                     </IconButton>
                  </Stack>
               </CleanMetricCard>
            )}
         </CleanNotebookCell>
      ))
   );
}
