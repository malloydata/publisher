// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CloseIcon from "@mui/icons-material/Close";
import {
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   IconButton,
   Stack,
   Typography,
} from "@mui/material";
import * as React from "react";
import { useId } from "react";

/**
 * Every dialog in the Console, in one shape.
 *
 * A dialog is a page that arrived in a box: it should be set like one. The
 * title is the page's heading at the same size, weight and tracking; a line
 * under it says what the dialog is for, where a page's description would be;
 * the body is one column with one gap; and the actions sit at the bottom right
 * with the same padding every time — a text Cancel and a filled confirm, the
 * two roles `buttons.tsx` names.
 *
 * Three dialogs stay off it deliberately, and each needs something this does
 * not have: `ModelExplorerDialog` is `fullScreen`, and `RowsDialog` and
 * `MaterializationDetailDialog` title themselves with a node rather than a
 * string. Widening this for three callers would cost more than it saves —
 * but a fourth exception is a reason to widen it, not to hand-roll a fourth.
 */
export function AppDialog({
   open,
   onClose,
   title,
   description,
   maxWidth = "sm",
   showClose = false,
   actions,
   children,
}: {
   open: boolean;
   onClose: () => void;
   title: string;
   /** What the dialog is for, in a line. The page's description, in a box. */
   description?: React.ReactNode;
   maxWidth?: "xs" | "sm" | "md" | "lg" | false;
   /** An X in the title, for a dialog whose actions do not include a way out. */
   showClose?: boolean;
   /** The buttons, in order. Cancel first, the thing it does last. */
   actions?: React.ReactNode;
   children: React.ReactNode;
}) {
   const titleId = useId();
   return (
      <Dialog
         open={open}
         onClose={onClose}
         maxWidth={maxWidth}
         fullWidth
         aria-labelledby={titleId}
      >
         <DialogTitle
            id={titleId}
            sx={{
               px: 3,
               pt: 3,
               pb: description ? 0.5 : 1,
               display: "flex",
               alignItems: "center",
               justifyContent: "space-between",
               gap: 1,
               // The page heading's type, so a dialog reads as part of the same
               // application rather than as a system alert.
               fontSize: "1.25rem",
               fontWeight: 600,
               letterSpacing: "-0.025em",
               lineHeight: 1.2,
            }}
         >
            {title}
            {showClose && (
               <IconButton
                  aria-label="Close"
                  onClick={onClose}
                  size="small"
                  sx={{ color: "text.secondary", mr: -1 }}
               >
                  <CloseIcon fontSize="small" />
               </IconButton>
            )}
         </DialogTitle>
         <DialogContent sx={{ px: 3, pb: 1, pt: 1 }}>
            {description && (
               <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ mb: 2 }}
               >
                  {description}
               </Typography>
            )}
            {/* One column, one gap: a dialog's fields are a list, and the list
                should not be spaced by whatever margin each field brought. */}
            <Stack sx={{ gap: 2, pt: description ? 0 : 1 }}>{children}</Stack>
         </DialogContent>
         {actions && (
            <DialogActions sx={{ px: 3, pb: 3, pt: 2, gap: 1 }}>
               {actions}
            </DialogActions>
         )}
      </Dialog>
   );
}
