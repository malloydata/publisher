import CloseIcon from "@mui/icons-material/Close";
import {
   Box,
   Chip,
   Dialog,
   DialogContent,
   DialogTitle,
   IconButton,
   Stack,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Typography,
} from "@mui/material";
import { BuildPlan, Materialization } from "../../client";
import { MONO_FONT_FAMILY } from "../styles";
import ManifestView from "./ManifestView";
import SectionLabel from "./SectionLabel";
import TriggerChip from "./TriggerChip";
import {
   formatDuration,
   formatTimestamp,
   isActiveStatus,
   parseMetadata,
   statusColor,
   statusLabel,
} from "./utils";

type MaterializationDetailDialogProps = {
   materialization: Materialization | null;
   // The compiled package's current build plan (Package.buildPlan), shown for
   // context. It is a property of the package version, not the historical run.
   // Note it is null both in the environment-scoped view and for a package with
   // no persist sources, so it cannot itself gate the section — see
   // `showBuildPlan`.
   buildPlan: BuildPlan | null;
   // Whether to render the Build plan section. The package view shows it (with a
   // "no persist sources" empty state when the plan is empty); the
   // environment-scoped view, which spans packages and has no single plan, omits
   // it entirely.
   showBuildPlan?: boolean;
   onClose: () => void;
};

export default function MaterializationDetailDialog({
   materialization,
   buildPlan,
   showBuildPlan = true,
   onClose,
}: MaterializationDetailDialogProps) {
   const meta = materialization ? parseMetadata(materialization) : {};
   const planSources = Object.values(buildPlan?.sources ?? {});

   return (
      <Dialog
         open={materialization !== null}
         onClose={onClose}
         maxWidth="md"
         fullWidth
         aria-labelledby="materialization-detail-title"
      >
         {materialization && (
            <>
               <DialogTitle
                  sx={{ pr: 6, pb: 1.5 }}
                  id="materialization-detail-title"
               >
                  <Stack
                     direction="row"
                     alignItems="center"
                     spacing={1}
                     sx={{ mb: 0.75 }}
                  >
                     <Chip
                        size="small"
                        label={statusLabel(materialization.status)}
                        color={statusColor(materialization.status)}
                        variant={
                           isActiveStatus(materialization.status)
                              ? "filled"
                              : "outlined"
                        }
                     />
                     <TriggerChip meta={meta} />
                  </Stack>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                     {materialization.packageName ?? "Materialization"}
                  </Typography>
                  <Typography
                     variant="caption"
                     color="text.secondary"
                     sx={{
                        fontFamily: MONO_FONT_FAMILY,
                        wordBreak: "break-all",
                     }}
                  >
                     {materialization.id}
                  </Typography>
                  <IconButton
                     aria-label="close"
                     onClick={onClose}
                     sx={{ position: "absolute", right: 8, top: 8 }}
                  >
                     <CloseIcon fontSize="small" />
                  </IconButton>
               </DialogTitle>
               <DialogContent dividers>
                  <Box
                     sx={{
                        bgcolor: "action.hover",
                        borderRadius: 2,
                        p: 2,
                        mb: 3,
                        display: "grid",
                        gridTemplateColumns:
                           "repeat(auto-fit, minmax(140px, 1fr))",
                        gap: 2,
                     }}
                  >
                     <DetailField
                        label="Started"
                        value={formatTimestamp(
                           materialization.startedAt ??
                              materialization.createdAt,
                        )}
                     />
                     <DetailField
                        label="Completed"
                        value={formatTimestamp(materialization.completedAt)}
                     />
                     <DetailField
                        label="Duration"
                        value={formatDuration(
                           materialization.startedAt,
                           materialization.completedAt,
                        )}
                     />
                     <DetailField
                        label="Sources"
                        value={`${meta.sourcesBuilt ?? 0} built · ${meta.sourcesReused ?? 0} reused`}
                     />
                     <DetailField
                        label="Force refresh"
                        value={meta.forceRefresh ? "Yes" : "No"}
                     />
                  </Box>

                  {materialization.error && (
                     <Box
                        sx={{
                           borderRadius: 2,
                           p: 2,
                           mb: 3,
                           border: "1px solid",
                           borderColor: "error.main",
                        }}
                     >
                        <SectionLabel>
                           <Box component="span" sx={{ color: "error.main" }}>
                              Error
                           </Box>
                        </SectionLabel>
                        <Typography
                           variant="body2"
                           sx={{
                              fontFamily: MONO_FONT_FAMILY,
                              whiteSpace: "pre-wrap",
                           }}
                        >
                           {materialization.error}
                        </Typography>
                     </Box>
                  )}

                  {showBuildPlan && (
                     <Box sx={{ mb: 3 }}>
                        <SectionLabel>Build plan</SectionLabel>
                        {planSources.length === 0 ? (
                           <Typography
                              variant="body2"
                              color="text.secondary"
                              sx={{ fontStyle: "italic" }}
                           >
                              This package has no persist sources.
                           </Typography>
                        ) : (
                           <Table size="small">
                              <TableHead>
                                 <TableRow>
                                    <TableCell>Source</TableCell>
                                    <TableCell>Connection</TableCell>
                                    <TableCell>Dialect</TableCell>
                                    <TableCell align="right">Columns</TableCell>
                                    <TableCell>Source Entity ID</TableCell>
                                 </TableRow>
                              </TableHead>
                              <TableBody>
                                 {planSources.map((source) => (
                                    <TableRow key={source.sourceID}>
                                       <TableCell
                                          sx={{ fontFamily: MONO_FONT_FAMILY }}
                                       >
                                          {source.name}
                                       </TableCell>
                                       <TableCell
                                          sx={{ fontFamily: MONO_FONT_FAMILY }}
                                       >
                                          {source.connectionName}
                                       </TableCell>
                                       <TableCell>
                                          {source.dialect ?? "-"}
                                       </TableCell>
                                       <TableCell align="right">
                                          {source.columns?.length ?? 0}
                                       </TableCell>
                                       <TableCell
                                          sx={{
                                             fontFamily: MONO_FONT_FAMILY,
                                             fontSize: "0.75rem",
                                             wordBreak: "break-all",
                                             maxWidth: 220,
                                          }}
                                       >
                                          {source.sourceEntityId}
                                       </TableCell>
                                    </TableRow>
                                 ))}
                              </TableBody>
                           </Table>
                        )}
                     </Box>
                  )}

                  <ManifestView
                     entries={materialization.manifest?.entries}
                     builtAt={materialization.manifest?.builtAt}
                  />
               </DialogContent>
            </>
         )}
      </Dialog>
   );
}

function DetailField({ label, value }: { label: string; value: string }) {
   return (
      <Box>
         <Typography variant="caption" color="text.secondary" display="block">
            {label}
         </Typography>
         <Typography variant="body2">{value}</Typography>
      </Box>
   );
}
