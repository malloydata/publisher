import { MoreVert } from "@mui/icons-material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import StopIcon from "@mui/icons-material/Stop";
import {
   Box,
   Chip,
   IconButton,
   Link,
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
import TriggerChip from "./TriggerChip";
import {
   formatDuration,
   formatRelativeTime,
   isActiveStatus,
   isTerminalStatus,
   parseMetadata,
   statusColor,
   statusLabel,
} from "./utils";

type MaterializationRunsListProps = {
   materializations: Materialization[];
   mutable: boolean;
   isMutating: boolean;
   /** Show a Package column — for the environment-scoped view spanning packages. */
   showPackage?: boolean;
   /** Navigate to a package (used by the Package column links). */
   onClickPackage?: (packageName: string) => void;
   onStop: (materialization: Materialization) => void;
   onDelete: (materialization: Materialization, dropTables: boolean) => void;
   onViewDetails: (materialization: Materialization) => void;
};

export default function MaterializationRunsList({
   materializations,
   mutable,
   isMutating,
   showPackage = false,
   onClickPackage,
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
      <Table size="small">
         <TableHead>
            <TableRow>
               {showPackage && <TableCell>Package</TableCell>}
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
                  showPackage={showPackage}
                  onClickPackage={onClickPackage}
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
   showPackage,
   onClickPackage,
   onStop,
   onDelete,
   onViewDetails,
}: {
   materialization: Materialization;
   mutable: boolean;
   isMutating: boolean;
   showPackage?: boolean;
   onClickPackage?: (packageName: string) => void;
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
         ? `${meta.sourcesBuilt ?? 0} built, ${meta.sourcesReused ?? 0} reused`
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
         {showPackage && (
            <TableCell onClick={(event) => event.stopPropagation()}>
               {onClickPackage ? (
                  <Link
                     component="button"
                     underline="hover"
                     onClick={() =>
                        onClickPackage(materialization.packageName ?? "")
                     }
                     sx={{ fontWeight: 500 }}
                  >
                     {materialization.packageName}
                  </Link>
               ) : (
                  materialization.packageName
               )}
            </TableCell>
         )}
         <TableCell>
            <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
               <Chip
                  size="small"
                  label={statusLabel(materialization.status)}
                  color={statusColor(materialization.status)}
                  variant={active ? "filled" : "outlined"}
               />
               {error && (
                  <Tooltip title={error}>
                     <InfoOutlinedIcon fontSize="small" color="error" />
                  </Tooltip>
               )}
            </Box>
         </TableCell>
         <TableCell>
            <TriggerChip meta={meta} />
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
