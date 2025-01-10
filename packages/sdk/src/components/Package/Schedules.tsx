import {
   Box,
   Divider,
   Paper,
   Table,
   TableBody,
   TableCell,
   TableContainer,
   TableHead,
   TableRow,
   Typography,
} from "@mui/material";
import { QueryClient, useQuery } from "@tanstack/react-query";
import axios from "axios";
import { Configuration, SchedulesApi } from "../../client";
import { StyledCard, StyledCardContent } from "../styles";
import { FileTreeView } from "./FileTreeView";

axios.defaults.baseURL = "http://localhost:4000";
const schedulesApi = new SchedulesApi(new Configuration());
const queryClient = new QueryClient();

interface PackageProps {
   server?: string;
   packageName: string;
   versionId?: string;
   accessToken: string;
}

export default function Package({
   server,
   packageName,
   versionId,
   accessToken,
}: PackageProps) {
   const { data, isError, isLoading, error } = useQuery(
      {
         queryKey: ["databases", server, packageName, versionId],
         queryFn: () =>
            schedulesApi.listSchedules(packageName, versionId, {
               baseURL: server,
               withCredentials: !accessToken,
               headers: {
                  Authorization: accessToken && `Bearer ${accessToken}`,
               },
            }),
         retry: false,
      },
      queryClient,
   );

   if (isLoading) {
      return (
         <Typography variant="body2" sx={{ p: "20px", m: "auto" }}>
            Fetching Schedules...
         </Typography>
      );
   }

   if (isError) {
      return (
         <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
            {`${packageName} > ${versionId} - ${error.message}`}
         </Typography>
      );
   }

   if (!data.data.length) {
      return null;
   }

   return (
      <StyledCard variant="outlined" sx={{ padding: "10px", width: "100%" }}>
         <StyledCardContent>
            <Typography variant="overline" fontWeight="bold">
               Scheduler
            </Typography>
            <Divider />
            <Box
               sx={{
                  mt: "10px",
                  maxHeight: "200px",
                  overflowY: "auto",
               }}
            >
               <TableContainer component={Paper}>
                  <Table sx={{ minWidth: 300 }} size="small">
                     <TableHead>
                        <TableRow>
                           <TableCell align="left">Resource</TableCell>
                           <TableCell align="left">Schedule</TableCell>
                           <TableCell align="left">Action</TableCell>
                           <TableCell align="left">Connection</TableCell>
                           <TableCell align="left">Last Run</TableCell>
                           <TableCell align="left">Status</TableCell>
                        </TableRow>
                     </TableHead>
                     <TableBody>
                        {data.data.map((m) => (
                           <TableRow
                              key={m.resource}
                              sx={{
                                 "&:last-child td, &:last-child th": {
                                    border: 0,
                                 },
                              }}
                           >
                              <TableCell align="left">{m.resource}</TableCell>
                              <TableCell align="left">{m.schedule}</TableCell>
                              <TableCell align="left">{m.action}</TableCell>
                              <TableCell align="left">{m.connection}</TableCell>
                              <TableCell align="left">
                                 {m.lastRunTime
                                    ? new Date(m.lastRunTime).toLocaleString()
                                    : "n/a"}
                              </TableCell>
                              <TableCell align="left">
                                 {m.lastRunStatus}
                              </TableCell>
                           </TableRow>
                        ))}
                     </TableBody>
                  </Table>
               </TableContainer>
            </Box>
         </StyledCardContent>
      </StyledCard>
   );
}
