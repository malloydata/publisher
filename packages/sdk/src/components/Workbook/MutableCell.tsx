import CheckIcon from "@mui/icons-material/Check";
import CodeIcon from "@mui/icons-material/Code";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import {
   Box,
   Button,
   Collapse,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   IconButton,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import MDEditor from "@uiw/react-md-editor/nohighlight";
import Markdown from "markdown-to-jsx";
import React, { useEffect, useState } from "react";
import { highlight } from "../highlighter";
import {
   emptyQueryExplorerResult,
   QueryExplorerResult,
   SourceAndPath,
} from "../Model/SourcesExplorer";
import ResultContainer from "../RenderedResult/ResultContainer";
import { StyledCard } from "../styles";
import { EditableMalloyCell } from "./EditableMalloyCell";
import { WorkbookCellValue } from "./WorkbookManager";

interface NotebookCellProps {
   cell: WorkbookCellValue;
   editingMalloy?: boolean;
   editingMarkdown?: boolean;
   sourceAndPaths: SourceAndPath[];
   resourceUri: string;
   onCellChange: (cell: WorkbookCellValue) => void;
   onClose: () => void;
   onEdit: () => void;
   onDelete: () => void;
   addButtonCallback: (isMarkdown: boolean) => React.ReactNode;
}

export function MutableCell({
   cell,
   editingMalloy,
   editingMarkdown,
   sourceAndPaths,
   resourceUri,
   onCellChange,
   onClose,
   onEdit,
   onDelete,
   addButtonCallback,
}: NotebookCellProps) {
   const [value, setValue] = useState(cell.value);
   const [codeExpanded, setCodeExpanded] = React.useState<boolean>(false);
   const [embeddingExpanded, setEmbeddingExpanded] =
      React.useState<boolean>(false);
   const [highlightedMalloyCode, setHighlightedMalloyCode] =
      React.useState<string>();
   const [highlightedEmbedCode] = React.useState<string>();
   const [query, setQuery] = React.useState<QueryExplorerResult>(
      emptyQueryExplorerResult(),
   );
   const [isHovered, setIsHovered] = React.useState<boolean>(false);
   const [selectedSourceIndex, setSelectedSourceIndex] = React.useState<number>(
      cell.sourceName
         ? sourceAndPaths.findIndex(
              (entry) => entry.sourceInfo.name === cell.sourceName,
           )
         : 0,
   );

   useEffect(() => {
      if (!cell.isMarkdown)
         highlight(cell.value, "malloy").then((code) => {
            setHighlightedMalloyCode(code);
         });
   }, [cell]);
   React.useEffect(() => {
      document.documentElement.setAttribute("data-color-mode", "light");
   });
   const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);

   const handleDeleteConfirm = () => {
      onDelete();
      setDeleteDialogOpen(false);
   };
   const noSources = sourceAndPaths.length === 0;

   const saveResult = React.useCallback(() => {
      // Get the current modelPath and sourceName from the selected source
      const currentSource = sourceAndPaths[selectedSourceIndex];
      const modelPath = currentSource?.modelPath || cell.modelPath || "";
      const sourceName =
         currentSource?.sourceInfo.name || cell.sourceName || "";

      // Convert the results of the Query Explorer into
      // the stringified JSON objects that are stored in the cell.
      onCellChange({
         ...cell,
         value: cell.isMarkdown ? value : query.query,
         result: query.malloyResult
            ? JSON.stringify(query.malloyResult)
            : undefined,
         queryInfo: query.malloyQuery
            ? JSON.stringify(query.malloyQuery)
            : undefined,
         sourceName,
         modelPath,
      });
   }, [cell, value, query, onCellChange, selectedSourceIndex, sourceAndPaths]);

   const deleteButton = (
      <Tooltip title="Delete Cell">
         <IconButton size="small" onClick={() => setDeleteDialogOpen(true)}>
            <svg
               width="20"
               height="20"
               viewBox="0 0 24 24"
               fill="none"
               xmlns="http://www.w3.org/2000/svg"
            >
               <path
                  d="M3 6h18M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2m2 0v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6h14zM10 11v6M14 11v6"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
               />
            </svg>
         </IconButton>
      </Tooltip>
   );

   const deleteDialog = (
      <Dialog
         open={deleteDialogOpen}
         onClose={() => setDeleteDialogOpen(false)}
      >
         <DialogTitle>Confirm Delete</DialogTitle>
         <DialogContent>
            <Typography>Are you sure you want to delete this cell?</Typography>
         </DialogContent>
         <DialogActions>
            <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>
            <Button onClick={handleDeleteConfirm} color="error">
               Delete
            </Button>
         </DialogActions>
      </Dialog>
   );
   const saveAndClose = () => {
      saveResult();
      onClose();
   };
   const buttons = cell.isMarkdown ? (
      <>
         {editingMarkdown ? (
            <Tooltip title="Save">
               <IconButton size="small" onClick={saveAndClose}>
                  <CheckIcon />
               </IconButton>
            </Tooltip>
         ) : (
            <Tooltip title="Edit">
               <IconButton size="small" onClick={onEdit}>
                  <EditOutlinedIcon />
               </IconButton>
            </Tooltip>
         )}
         {!editingMarkdown && deleteButton}
      </>
   ) : (
      <>
         {!editingMalloy && (
            <Tooltip title={codeExpanded ? "Hide Code" : "View Code"}>
               <IconButton
                  size="small"
                  onClick={() => {
                     setCodeExpanded(!codeExpanded);
                  }}
               >
                  <CodeIcon />
               </IconButton>
            </Tooltip>
         )}
         {!editingMalloy && cell.result && (
            <Tooltip
               title={embeddingExpanded ? "Hide Embedding" : "View Embedding"}
            >
               <IconButton
                  size="small"
                  onClick={() => {
                     setEmbeddingExpanded(!embeddingExpanded);
                  }}
               >
                  <LinkOutlinedIcon />
               </IconButton>
            </Tooltip>
         )}
         {editingMalloy && (
            <Tooltip title="Save">
               <IconButton size="small" onClick={saveAndClose}>
                  <CheckIcon />
               </IconButton>
            </Tooltip>
         )}
         {!editingMalloy && (
            <Tooltip title="Edit">
               <IconButton size="small" onClick={onEdit}>
                  <EditOutlinedIcon />
               </IconButton>
            </Tooltip>
         )}
         {!editingMalloy && deleteButton}
      </>
   );

   const isEditing = editingMalloy || editingMarkdown;
   const editingButtons = editingMarkdown ? (
      <Tooltip title="Save">
         <IconButton size="small" onClick={saveAndClose}>
            <CheckIcon />
         </IconButton>
      </Tooltip>
   ) : editingMalloy ? (
      <Tooltip title="Save">
         <IconButton size="small" onClick={saveAndClose}>
            <CheckIcon />
         </IconButton>
      </Tooltip>
   ) : null;

   const hoverButtonBox = isHovered && (
      <Box
         sx={{
            position: "absolute",
            top: "4px",
            right: "4px",
            // transform: "translateX(-50%)",
            display: "flex",
            gap: "8px",
            backgroundColor: "background.paper",
            border: "1px solid",
            borderColor: "divider",
            borderRadius: "8px",
            padding: "2px 4px",
            boxShadow: 1,
            zIndex: 10,
         }}
      >
         {(!isEditing && (
            <>
               {addButtonCallback(true)}
               {addButtonCallback(false)}
               {buttons}
            </>
         )) ||
            editingButtons}
      </Box>
   );

   return (
      <StyledCard
         sx={{
            position: "relative",
            marginTop: "5px",
            marginBottom: "5px",
            borderWidth: "1.5px",
            backgroundColor: "#fff",
            minHeight: "50px",
         }}
         onMouseEnter={() => setIsHovered(true)}
         onMouseLeave={() => {
            setIsHovered(false);
         }}
      >
         {hoverButtonBox}
         {cell.isMarkdown ? (
            <>
               {editingMarkdown ? (
                  <MDEditor
                     value={value}
                     preview="edit"
                     autoFocus
                     onChange={(newValue) => {
                        setValue(newValue);
                        onCellChange({ ...cell, value: newValue });
                     }}
                     onBlur={() => {
                        saveResult();
                        if (!isHovered) {
                           saveAndClose();
                        }
                     }}
                  />
               ) : (
                  <Box
                     sx={{
                        px: 0.5,
                        pt: 0,
                        mt: 0,
                        "& h1, & h2, & h3, & h4, & h5, & h6": {
                           mt: 0.5,
                           mb: 0.5,
                        },
                        "& p": { mt: 0.5, mb: 0.5 },
                        "& ul, & ol": { mt: 0.5, mb: 0.5 },
                        "& li": { mt: 0, mb: 0 },
                        "& pre, & code": { mt: 0.5, mb: 0.5 },
                        "& blockquote": { mt: 0.5, mb: 0.5 },
                     }}
                  >
                     {cell.value ? (
                        <Box onClick={onEdit} sx={{ cursor: "pointer" }}>
                           <Markdown>{cell.value}</Markdown>
                        </Box>
                     ) : (
                        <Box onClick={onEdit} sx={{ cursor: "pointer" }}>
                           <Typography
                              sx={{
                                 p: 2,
                                 textAlign: "center",
                                 variant: "subtitle2",
                                 fontWeight: "medium",
                              }}
                           >
                              Markdown is empty
                           </Typography>
                           <Typography
                              sx={{
                                 mb: 2,
                                 textAlign: "center",
                                 variant: "body2",
                              }}
                           >
                              Click to edit.
                           </Typography>
                        </Box>
                     )}
                  </Box>
               )}
            </>
         ) : (
            <>
               <Collapse in={embeddingExpanded} timeout="auto" unmountOnExit>
                  <Stack
                     sx={{
                        p: "10px",
                        borderRadius: 0,
                        flexDirection: "row",
                        justifyContent: "space-between",
                     }}
                  >
                     <Typography
                        component="div"
                        sx={{
                           fontSize: "12px",
                           "& .line": { textWrap: "wrap" },
                        }}
                        dangerouslySetInnerHTML={{
                           __html: highlightedEmbedCode,
                        }}
                     />
                  </Stack>
               </Collapse>
               <Collapse in={codeExpanded} timeout="auto" unmountOnExit>
                  <Stack
                     sx={{
                        p: "10px",
                        borderRadius: 0,
                        flexDirection: "row",
                        justifyContent: "space-between",
                     }}
                  >
                     <Typography
                        component="div"
                        className="content"
                        sx={{
                           fontSize: "12px",
                           "& .line": { textWrap: "wrap" },
                        }}
                        dangerouslySetInnerHTML={{
                           __html: highlightedMalloyCode,
                        }}
                     />
                  </Stack>
               </Collapse>
               {editingMalloy &&
                  (noSources ? (
                     <>
                        <Typography
                           sx={{
                              p: 2,
                              textAlign: "center",
                              variant: "subtitle2",
                              fontWeight: "medium",
                           }}
                        >
                           No Model Imports
                        </Typography>
                        <Typography
                           sx={{ mb: 2, textAlign: "center", variant: "body2" }}
                        >
                           Please add a model import above.
                        </Typography>
                     </>
                  ) : (
                     <EditableMalloyCell
                        sourceAndPaths={sourceAndPaths}
                        resourceUri={resourceUri}
                        onQueryChange={(query) => {
                           setQuery(query);
                        }}
                        onSourceChange={(index) => {
                           setSelectedSourceIndex(index);
                        }}
                        cell={cell}
                     />
                  ))}
               {!editingMalloy && cell.result && (
                  <StyledCard variant="outlined" sx={{ borderRadius: 0 }}>
                     <ResultContainer result={cell.result} maxHeight={800} />
                  </StyledCard>
               )}
               {!editingMalloy && !cell.result && (
                  <Box onClick={onEdit} sx={{ cursor: "pointer" }}>
                     <Typography
                        sx={{
                           p: 2,
                           textAlign: "center",
                           variant: "subtitle2",
                           fontWeight: "medium",
                        }}
                     >
                        Explore is empty
                     </Typography>
                     <Typography
                        sx={{ mb: 2, textAlign: "center", variant: "body2" }}
                     >
                        Click to edit.
                     </Typography>
                  </Box>
               )}
            </>
         )}
         {deleteDialogOpen && deleteDialog}
      </StyledCard>
   );
}
