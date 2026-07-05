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
import {
   formatDuration,
   formatTimestamp,
   parseMetadata,
   statusColor,
   statusLabel,
} from "./utils";

type MaterializationDetailDialogProps = {
   materialization: Materialization | null;
   // The compiled package's current build plan (Package.buildPlan), shown for
   // context. It is a property of the package version, not the historical run.
   buildPlan: BuildPlan | null;
   onClose: () => void;
};

export default function MaterializationDetailDialog({
   materialization,
   buildPlan,
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
               <DialogTitle sx={{ pr: 6 }} id="materialization-detail-title">
                  <Stack direction="row" alignItems="center" spacing={1.5}>
                     <Chip
                        size="small"
                        label={statusLabel(materialization.status)}
                        color={statusColor(materialization.status)}
                     />
                     <Typography
                        variant="subtitle1"
                        sx={{ fontFamily: MONO_FONT_FAMILY }}
                     >
                        {materialization.id}
                     </Typography>
                  </Stack>
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
                        display: "grid",
                        gridTemplateColumns:
                           "repeat(auto-fit, minmax(150px, 1fr))",
                        gap: 2,
                        mb: 3,
                     }}
                  >
                     <DetailField
                        label="Package"
                        value={materialization.packageName ?? "—"}
                     />
                     <DetailField
                        label="Force refresh"
                        value={meta.forceRefresh ? "Yes" : "No"}
                     />
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
                        label="Sources built"
                        value={`${meta.sourcesBuilt ?? 0}`}
                     />
                     <DetailField
                        label="Sources reused"
                        value={`${meta.sourcesReused ?? 0}`}
                     />
                     <DetailField
                        label="Created"
                        value={formatTimestamp(materialization.createdAt)}
                     />
                     <DetailField
                        label="Updated"
                        value={formatTimestamp(materialization.updatedAt)}
                     />
                  </Box>

                  {materialization.error && (
                     <Box sx={{ mb: 3 }}>
                        <Typography
                           variant="subtitle2"
                           color="error"
                           gutterBottom
                        >
                           Error
                        </Typography>
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

                  <Box sx={{ mb: 3 }}>
                     <Typography variant="subtitle2" gutterBottom>
                        Build plan
                     </Typography>
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
