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
import { Materialization } from "../../client";
import { MONO_FONT_FAMILY } from "../styles";
import ManifestView from "./ManifestView";
import {
   formatDuration,
   formatRelativeTime,
   parseMetadata,
   statusColor,
} from "./utils";

type MaterializationDetailDialogProps = {
   materialization: Materialization | null;
   onClose: () => void;
};

export default function MaterializationDetailDialog({
   materialization,
   onClose,
}: MaterializationDetailDialogProps) {
   const meta = materialization ? parseMetadata(materialization) : {};
   const planSources = Object.values(materialization?.buildPlan?.sources ?? {});

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
                        label={materialization.status ?? "UNKNOWN"}
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
                  <Stack direction="row" spacing={4} sx={{ mb: 3 }}>
                     <DetailField
                        label="Started"
                        value={formatRelativeTime(
                           materialization.startedAt ??
                              materialization.createdAt,
                        )}
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
                        label="Sources skipped"
                        value={`${meta.sourcesSkipped ?? 0}`}
                     />
                  </Stack>

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
                           No build plan yet.
                        </Typography>
                     ) : (
                        <Table size="small">
                           <TableHead>
                              <TableRow>
                                 <TableCell>Source</TableCell>
                                 <TableCell>Connection</TableCell>
                                 <TableCell>Dialect</TableCell>
                                 <TableCell>Build ID</TableCell>
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
                                    <TableCell>{source.dialect ?? "-"}</TableCell>
                                    <TableCell
                                       sx={{ fontFamily: MONO_FONT_FAMILY }}
                                    >
                                       {source.buildId}
                                    </TableCell>
                                 </TableRow>
                              ))}
                           </TableBody>
                        </Table>
                     )}
                  </Box>

                  <ManifestView entries={materialization.manifest?.entries} />
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
