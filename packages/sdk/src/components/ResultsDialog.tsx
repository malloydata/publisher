// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { LogMessage } from "../client";
import type { DrillBinding } from "./drill";
import ResultContainer from "./RenderedResult/ResultContainer";
import { RESULTS_DIALOG_MAX_HEIGHT } from "./RenderedResult/resultSizing";
import { AppDialog } from "./AppDialog";

interface ResultsDialogProps {
   open: boolean;
   onClose: () => void;
   result: string;
   title?: string;
   renderLogs?: LogMessage[];
   /** `# drill` wiring, so a result stays clickable when it is expanded. */
   drill?: DrillBinding;
}

export default function ResultsDialog({
   open,
   onClose,
   result,
   title = "Results",
   renderLogs,
   drill,
}: ResultsDialogProps) {
   return (
      <AppDialog
         open={open}
         onClose={onClose}
         title={title}
         maxWidth={false}
         showClose
      >
         <Box sx={{ height: "70vh", overflow: "auto" }}>
            <ResultContainer
               result={result}
               maxHeight={RESULTS_DIALOG_MAX_HEIGHT}
               maxResultSize={1000000}
               renderLogs={renderLogs}
               drill={drill}
            />
         </Box>
      </AppDialog>
   );
}
