// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Button,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   Typography,
} from "@mui/material";
import { useMemo } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { foldUnchanged, lineDiff } from "./diff";

/**
 * What a save will do to the file, before it does it.
 *
 * Shown for a STRUCTURAL save — a tile added or removed — because those move
 * declarations and the comments beside them, and the file is git-native: a
 * diff is the idiom its authors already read. A property edit never needs
 * this; it changes one tag on one line.
 */
export function DiffDialog({
   open,
   before,
   after,
   onConfirm,
   onClose,
}: {
   open: boolean;
   before: string;
   after: string;
   onConfirm: () => void;
   onClose: () => void;
}) {
   const { theme } = usePublisherTheme();
   const lines = useMemo(
      () => (open ? foldUnchanged(lineDiff(before, after)) : []),
      [open, before, after],
   );
   const added = lines.filter((l) => l.kind === "add").length;
   const removed = lines.filter((l) => l.kind === "del").length;
   return (
      <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
         <DialogTitle sx={{ pb: 0.5 }}>
            Review the change to the file
            <Typography
               component="div"
               variant="caption"
               sx={{ color: theme.tileTitle, mt: 0.25 }}
            >
               A tile was added or removed, which moves declarations. Lines the
               builder does not own — comments, other Malloy — are left where
               they were; check they still read right.{" "}
               <Box
                  component="span"
                  sx={{ fontVariantNumeric: "tabular-nums" }}
               >
                  +{added} −{removed}
               </Box>
            </Typography>
         </DialogTitle>
         <DialogContent dividers sx={{ p: 0 }}>
            <Box
               component="pre"
               aria-label="File changes"
               sx={{
                  m: 0,
                  px: 2,
                  py: 1,
                  fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
                  fontSize: 12,
                  lineHeight: 1.6,
                  overflowX: "auto",
                  bgcolor: theme.background,
                  color: theme.foreground,
               }}
            >
               {lines.map((line, index) =>
                  line.kind === "fold" ? (
                     <Box
                        key={index}
                        component="span"
                        sx={{ display: "block", opacity: 0.55, py: 0.25 }}
                     >
                        {`⋯ ${line.count} unchanged line${line.count === 1 ? "" : "s"}`}
                     </Box>
                  ) : (
                     <Box
                        key={index}
                        component="span"
                        sx={{
                           display: "block",
                           mx: -2,
                           px: 2,
                           whiteSpace: "pre",
                           bgcolor:
                              line.kind === "add"
                                 ? "rgba(46, 125, 91, 0.16)"
                                 : line.kind === "del"
                                   ? "rgba(168, 41, 31, 0.14)"
                                   : "transparent",
                           textDecoration:
                              line.kind === "del" ? "line-through" : "none",
                           opacity: line.kind === "del" ? 0.8 : 1,
                        }}
                     >
                        {line.kind === "add"
                           ? "+ "
                           : line.kind === "del"
                             ? "− "
                             : "  "}
                        {line.text}
                     </Box>
                  ),
               )}
            </Box>
         </DialogContent>
         <DialogActions sx={{ px: 3, py: 1.5 }}>
            <Button onClick={onClose}>Keep editing</Button>
            <Button variant="contained" onClick={onConfirm}>
               Save this
            </Button>
         </DialogActions>
      </Dialog>
   );
}
