import {
   Box,
   Stack,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Typography,
} from "@mui/material";
import { ManifestEntry } from "../../client";
import { MONO_FONT_FAMILY } from "../styles";
import { formatTimestamp } from "./utils";

type ManifestViewProps = {
   entries: { [buildId: string]: ManifestEntry } | undefined;
   builtAt?: string;
};

/**
 * Read-only view of a materialization's build manifest: the physical tables
 * produced from the build plan, carried on the materialization resource.
 */
export default function ManifestView({ entries, builtAt }: ManifestViewProps) {
   const rows = Object.entries(entries ?? {});

   return (
      <Box>
         <Stack
            direction="row"
            alignItems="baseline"
            justifyContent="space-between"
         >
            <Typography variant="subtitle2" gutterBottom>
               Build manifest
            </Typography>
            {builtAt && (
               <Typography variant="caption" color="text.secondary">
                  Built {formatTimestamp(builtAt)}
               </Typography>
            )}
         </Stack>
         {rows.length === 0 ? (
            <Typography
               variant="body2"
               color="text.secondary"
               sx={{ fontStyle: "italic" }}
            >
               No materialized tables yet.
            </Typography>
         ) : (
            <Table size="small">
               <TableHead>
                  <TableRow>
                     <TableCell>Source</TableCell>
                     <TableCell>Table name</TableCell>
                     <TableCell>Connection</TableCell>
                     <TableCell>Realization</TableCell>
                     <TableCell align="right">Rows</TableCell>
                  </TableRow>
               </TableHead>
               <TableBody>
                  {rows.map(([buildId, entry]) => (
                     <TableRow key={buildId}>
                        <TableCell sx={{ fontFamily: MONO_FONT_FAMILY }}>
                           {entry.sourceName ?? buildId}
                        </TableCell>
                        <TableCell sx={{ fontFamily: MONO_FONT_FAMILY }}>
                           {entry.physicalTableName ?? "-"}
                        </TableCell>
                        <TableCell sx={{ fontFamily: MONO_FONT_FAMILY }}>
                           {entry.connectionName ?? "-"}
                        </TableCell>
                        <TableCell>{entry.realization ?? "-"}</TableCell>
                        <TableCell align="right">
                           {entry.rowCount ?? "-"}
                        </TableCell>
                     </TableRow>
                  ))}
               </TableBody>
            </Table>
         )}
      </Box>
   );
}
