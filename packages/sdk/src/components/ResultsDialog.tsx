import CloseIcon from "@mui/icons-material/Close";
import { Dialog, DialogContent, DialogTitle, IconButton } from "@mui/material";
import ResultContainer from "./RenderedResult/ResultContainer";

interface ResultsDialogProps {
   open: boolean;
   onClose: () => void;
   result: string;
   title?: string;
}

export default function ResultsDialog({
   open,
   onClose,
   result,
   title = "Results",
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
            <IconButton onClick={onClose} sx={{ color: "#666666" }}>
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
            />
         </DialogContent>
      </Dialog>
   );
}
