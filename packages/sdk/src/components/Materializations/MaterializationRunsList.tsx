// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MoreVert } from "@mui/icons-material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import StopIcon from "@mui/icons-material/Stop";
import {
   Box,
   IconButton,
   ListItemIcon,
   ListItemText,
   Menu,
   MenuItem,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Tooltip,
   Typography,
} from "@mui/material";
import { useState } from "react";
import { Materialization } from "../../client";
import DeleteMaterializationDialog from "./DeleteMaterializationDialog";
import TriggerLabel from "./TriggerLabel";
import {
   formatDuration,
   formatRelativeTime,
   isActiveStatus,
   isTerminalStatus,
   parseMetadata,
   sourcesSummary,
   statusColor,
   statusLabel,
} from "./utils";

type MaterializationRunsListProps = {
   materializations: Materialization[];
   mutable: boolean;
   isMutating: boolean;
   onStop: (materialization: Materialization) => void;
   onDelete: (materialization: Materialization, dropTables: boolean) => void;
   onViewDetails: (materialization: Materialization) => void;
};

export default function MaterializationRunsList({
   materializations,
   mutable,
   isMutating,
   onStop,
   onDelete,
   onViewDetails,
}: MaterializationRunsListProps) {
   if (materializations.length === 0) {
      return (
         <Typography
            variant="body2"
            color="text.secondary"
            sx={{ py: 1, fontStyle: "italic" }}
         >
            No materializations yet.
         </Typography>
      );
   }

   return (
      <Table
         size="small"
         // The section's rows start at its left edge and end at its right, so
         // the table does too: MUI's own 16px on the outer cells put this one
         // table a thumb's width inside every list above it.
         sx={{
            "& td, & th": { borderColor: "divider" },
            "& td:first-of-type, & th:first-of-type": { pl: 0 },
            "& td:last-of-type, & th:last-of-type": { pr: 0 },
            "& th": { color: "text.secondary", fontWeight: 600 },
         }}
      >
         <TableHead>
            <TableRow>
               <TableCell>Status</TableCell>
               <TableCell>Trigger</TableCell>
               <TableCell>Started</TableCell>
               <TableCell>Duration</TableCell>
               <TableCell>Sources</TableCell>
               <TableCell align="right">Actions</TableCell>
            </TableRow>
         </TableHead>
         <TableBody>
            {materializations.map((materialization) => (
               <MaterializationRow
                  key={materialization.id}
                  materialization={materialization}
                  mutable={mutable}
                  isMutating={isMutating}
                  onStop={onStop}
                  onDelete={onDelete}
                  onViewDetails={onViewDetails}
               />
            ))}
         </TableBody>
      </Table>
   );
}

function MaterializationRow({
   materialization,
   mutable,
   isMutating,
   onStop,
   onDelete,
   onViewDetails,
}: {
   materialization: Materialization;
   mutable: boolean;
   isMutating: boolean;
   onStop: (materialization: Materialization) => void;
   onDelete: (materialization: Materialization, dropTables: boolean) => void;
   onViewDetails: (materialization: Materialization) => void;
}) {
   const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
   const menuOpen = Boolean(menuAnchorEl);
   const handleMenuClose = () => setMenuAnchorEl(null);

   const meta = parseMetadata(materialization);
   const sourcesLabel =
      meta.sourcesBuilt !== undefined || meta.sourcesReused !== undefined
         ? sourcesSummary(meta, ", ")
         : "-";
   const active = isActiveStatus(materialization.status);
   const terminal = isTerminalStatus(materialization.status);
   const error = materialization.error ?? undefined;
   const hasActions = mutable && (active || terminal);

   return (
      <TableRow
         hover
         sx={{
            cursor: "pointer",
            "&:focus-visible": {
               outline: "2px solid",
               outlineColor: "primary.main",
               outlineOffset: "-2px",
            },
         }}
         onClick={() => onViewDetails(materialization)}
         onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
               event.preventDefault();
               onViewDetails(materialization);
            }
         }}
         role="button"
         tabIndex={0}
         aria-label={`View materialization ${materialization.id ?? ""} details`.trim()}
      >
         <TableCell>
            <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
               {/* The word, in the colour the state already carries. A pill
                   around one word of a six-word row was the only chip on the
                   page, and it read as a control rather than as a value. */}
               <Typography
                  variant="body2"
                  sx={{
                     fontWeight: 500,
                     color:
                        statusColor(materialization.status) === "default"
                           ? "text.primary"
                           : `${statusColor(materialization.status)}.main`,
                  }}
               >
                  {statusLabel(materialization.status)}
               </Typography>
               {error && (
                  <Tooltip title={error}>
                     <InfoOutlinedIcon fontSize="small" color="error" />
                  </Tooltip>
               )}
            </Box>
         </TableCell>
         <TableCell>
            <TriggerLabel meta={meta} />
         </TableCell>
         <TableCell>
            {formatRelativeTime(
               materialization.startedAt ?? materialization.createdAt,
            )}
         </TableCell>
         <TableCell>
            {formatDuration(
               materialization.startedAt,
               materialization.completedAt,
            )}
         </TableCell>
         <TableCell>{sourcesLabel}</TableCell>
         <TableCell align="right" onClick={(event) => event.stopPropagation()}>
            {hasActions && (
               <>
                  <IconButton
                     size="small"
                     aria-label={`Materialization actions for ${materialization.id ?? ""}`.trim()}
                     onClick={(event) => setMenuAnchorEl(event.currentTarget)}
                  >
                     <MoreVert fontSize="small" />
                  </IconButton>
                  <Menu
                     anchorEl={menuAnchorEl}
                     open={menuOpen}
                     onClose={handleMenuClose}
                     anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
                     transformOrigin={{ vertical: "top", horizontal: "right" }}
                  >
                     {mutable && active && (
                        <MenuItem
                           aria-label={`Stop materialization ${materialization.id ?? ""}`.trim()}
                           disabled={isMutating}
                           onClick={() => {
                              handleMenuClose();
                              onStop(materialization);
                           }}
                        >
                           <ListItemIcon>
                              <StopIcon fontSize="small" />
                           </ListItemIcon>
                           <ListItemText>Stop</ListItemText>
                        </MenuItem>
                     )}
                     {mutable && terminal && (
                        <DeleteMaterializationDialog
                           materialization={materialization}
                           isMutating={isMutating}
                           onCloseDialog={handleMenuClose}
                           onDelete={(dropTables) =>
                              onDelete(materialization, dropTables)
                           }
                        />
                     )}
                  </Menu>
               </>
            )}
         </TableCell>
      </TableRow>
   );
}
