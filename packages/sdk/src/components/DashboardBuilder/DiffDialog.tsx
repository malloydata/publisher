// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Button } from "@mui/material";
import { useMemo } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { foldUnchanged, lineDiff } from "./diff";
import { AppDialog } from "../AppDialog";

/** What the last save changed in the file, read-only: the file is git-native, and a diff is the idiom its authors already read. */
export function DiffDialog({
   open,
   before,
   after,
   onClose,
}: {
   open: boolean;
   before: string;
   after: string;
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
      <AppDialog
         open={open}
         onClose={onClose}
         maxWidth="md"
         title="The change to the file"
         description={
            <>
               What the last save wrote. Lines the builder does not own, such as
               comments and other Malloy, are left where they were.{" "}
               <Box
                  component="span"
                  sx={{ fontVariantNumeric: "tabular-nums" }}
               >
                  +{added} −{removed}
               </Box>
            </>
         }
         actions={<Button onClick={onClose}>Close</Button>}
      >
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
      </AppDialog>
   );
}
