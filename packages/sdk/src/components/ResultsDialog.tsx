import CloseIcon from "@mui/icons-material/Close";
import { Dialog, DialogContent, DialogTitle, IconButton } from "@mui/material";
import { LogMessage } from "../client";
import ResultContainer from "./RenderedResult/ResultContainer";

interface ResultsDialogProps {
   open: boolean;
   onClose: () => void;
   result: string;
   title?: string;
   renderLogs?: LogMessage[];
}

export default function ResultsDialog({
   open,
   onClose,
   result,
   title = "Results",
   renderLogs,
}: ResultsDialogProps) {
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
            <IconButton onClick={onClose} sx={{ color: "text.secondary" }}>
               <CloseIcon />
            </IconButton>
         </DialogTitle>
         <DialogContent
            sx={{
               height: "calc(95vh - 120px)",
               overflow: "auto",
               padding: "0 16px",
            }}
         >
            <ResultContainer
               result={result}
               maxHeight={800}
               maxResultSize={1000000}
               renderLogs={renderLogs}
            />
         </DialogContent>
      </Dialog>
   );
}
