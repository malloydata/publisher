import React from "react";
import { Dialog, DialogTitle, DialogContent, IconButton } from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";
import { ModelExplorer } from "./ModelExplorer";
import { QueryExplorerResult } from "./SourcesExplorer";
import { CompiledModel } from "../../client";

interface ModelExplorerDialogProps {
   open: boolean;
   onClose: () => void;
   resourceUri: string;
   data?: CompiledModel;
   title?: string;
   hasValidImport?: boolean;
   existingQuery?: QueryExplorerResult;
   initialSelectedSourceIndex?: number;
   onChange?: (query: QueryExplorerResult) => void;
   onSourceChange?: (index: number) => void;
}

export function ModelExplorerDialog({
   open,
   onClose,
   resourceUri,
   data,
   title = "Data Sources",
   hasValidImport = true,
   existingQuery,
   initialSelectedSourceIndex,
   onChange,
   onSourceChange,
}: ModelExplorerDialogProps) {
   return (
      <Dialog
         open={open}
         onClose={onClose}
         maxWidth={false}
         fullWidth
         sx={{
            "& .MuiDialog-paper": {
               width: "95vw",
               height: "95vh",
               maxWidth: "none",
            },
         }}
      >
         <DialogTitle
            sx={{
               display: "flex",
               justifyContent: "space-between",
               alignItems: "center",
            }}
         >
            {title}
            <IconButton onClick={onClose} sx={{ color: "#666666" }}>
               <CloseIcon />
            </IconButton>
         </DialogTitle>
         <DialogContent>
            {hasValidImport ? (
               <ModelExplorer
                  resourceUri={resourceUri}
                  data={data}
                  existingQuery={existingQuery}
                  initialSelectedSourceIndex={initialSelectedSourceIndex}
                  onChange={onChange}
                  onSourceChange={onSourceChange}
               />
            ) : (
               <div>No valid import statement found in cell</div>
            )}
         </DialogContent>
      </Dialog>
   );
}
