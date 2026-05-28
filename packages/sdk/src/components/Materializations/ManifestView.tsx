import RefreshIcon from "@mui/icons-material/Refresh";
import {
   Box,
   Button,
   Stack,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Typography,
} from "@mui/material";
import { ManifestEntry } from "../../client";
import { ApiError, ApiErrorDisplay } from "../ApiErrorDisplay";
import { MONO_FONT_FAMILY } from "../styles";

type ManifestViewProps = {
   entries: { [buildId: string]: ManifestEntry } | undefined;
   mutable: boolean;
   isReloading: boolean;
   isError?: boolean;
   error?: ApiError | null;
   onReload: () => void;
};

export default function ManifestView({
   entries,
   mutable,
   isReloading,
   isError,
   error,
   onReload,
}: ManifestViewProps) {
   const rows = Object.entries(entries ?? {});

   return (
      <Box>
         <Stack
            direction="row"
            alignItems="flex-start"
            justifyContent="space-between"
            sx={{ mb: 1 }}
         >
            <Box>
               <Typography
                  variant="h6"
                  sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
               >
                  Build manifest
               </Typography>
               <Typography variant="body2" color="text.secondary">
                  Tables produced by materializing this package
               </Typography>
            </Box>
            {mutable && (
               <Button
                  size="small"
                  startIcon={<RefreshIcon />}
                  loading={isReloading}
                  onClick={onReload}
                  aria-label="Reload manifest"
               >
                  Reload manifest
               </Button>
            )}
         </Stack>

         {isError ? (
            <ApiErrorDisplay error={error ?? null} context="Build manifest" />
         ) : rows.length === 0 ? (
            <Typography
               variant="body2"
               color="text.secondary"
               sx={{ py: 1, fontStyle: "italic" }}
            >
               No materialized tables yet.
            </Typography>
         ) : (
            <Table size="small">
               <TableHead>
                  <TableRow>
                     <TableCell>Build ID</TableCell>
                     <TableCell>Table name</TableCell>
                  </TableRow>
               </TableHead>
               <TableBody>
                  {rows.map(([buildId, entry]) => (
                     <TableRow key={buildId}>
                        <TableCell sx={{ fontFamily: MONO_FONT_FAMILY }}>
                           {buildId}
                        </TableCell>
                        <TableCell sx={{ fontFamily: MONO_FONT_FAMILY }}>
                           {entry.tableName ?? "-"}
                        </TableCell>
                     </TableRow>
                  ))}
               </TableBody>
            </Table>
         )}
      </Box>
   );
}
