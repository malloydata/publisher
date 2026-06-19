import {
   Box,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Typography,
} from "@mui/material";
import { ManifestEntry } from "../../client";
import { MONO_FONT_FAMILY } from "../styles";

type ManifestViewProps = {
   entries: { [buildId: string]: ManifestEntry } | undefined;
};

/**
 * Read-only view of a materialization's Round 2 build manifest: the physical
 * tables the control plane produced from the build plan. The publisher no longer
 * brokers a package-level manifest; this renders the manifest carried on a
 * single materialization resource.
 */
export default function ManifestView({ entries }: ManifestViewProps) {
   const rows = Object.entries(entries ?? {});

   return (
      <Box>
         <Typography variant="subtitle2" gutterBottom>
            Build manifest
         </Typography>
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
