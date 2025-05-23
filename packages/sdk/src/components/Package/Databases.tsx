import {
   Box,
   Dialog,
   DialogContent,
   DialogTitle,
   Divider,
   IconButton,
   Table,
   TableBody,
   TableCell,
   TableContainer,
   TableHead,
   TableRow,
   Tooltip,
   Typography,
} from "@mui/material";
import { QueryClient, useQuery } from "@tanstack/react-query";
import { Configuration, Database, DatabasesApi } from "../../client";
import { StyledCard, StyledCardContent } from "../styles";
import React from "react";

const databasesApi = new DatabasesApi(new Configuration());
const queryClient = new QueryClient();

interface DatabaseProps {
   server?: string;
   projectName: string;
   packageName: string;
   versionId?: string;
   accessToken: string;
}

export default function DatabaseView({
   server,
   projectName,
   packageName,
   versionId,
   accessToken,
}: DatabaseProps) {
   const { data, isSuccess, isError, error } = useQuery(
      {
         queryKey: ["databases", server, projectName, packageName, versionId],
         queryFn: () =>
            databasesApi.listDatabases(projectName, packageName, versionId, {
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
   const formatRowSize = (size: number) => {
      if (size >= 1024 * 1024 * 1024 * 1024) {
         return `${(size / (1024 * 1024 * 1024)).toFixed(2)} T`;
      } else if (size >= 1024 * 1024 * 1024) {
         return `${(size / (1024 * 1024 * 1024)).toFixed(2)} G`;
      } else if (size >= 1024 * 1024) {
         return `${(size / (1024 * 1024)).toFixed(2)} M`;
      } else {
         return `${(size / 1024).toFixed(2)} K`;
      }
   };
   return (
      <StyledCard variant="outlined" sx={{ padding: "10px", width: "100%" }}>
         <StyledCardContent>
            <Typography variant="overline" fontWeight="bold">
               Embedded Databases
            </Typography>
            <Divider />
            <Box
               sx={{
                  mt: "10px",
                  maxHeight: "200px",
                  overflowY: "auto",
               }}
            >
               {!isSuccess && !isError && (
                  <Typography variant="body2" sx={{ p: "20px", m: "auto" }}>
                     Fetching Databases...
                  </Typography>
               )}
               {isSuccess && data.data.length > 0 && (
                  <TableContainer>
                     <Table
                        size="small"
                        sx={{ "& .MuiTableCell-root": { padding: "10px" } }}
                     >
                        <TableHead>
                           <TableRow>
                              <TableCell>Database Name</TableCell>
                              <TableCell align="right">Table Rows</TableCell>
                           </TableRow>
                        </TableHead>
                        <TableBody>
                           {data.data.map((database) => (
                              <TableRow key={database.path}>
                                 <TableCell>
                                    <NameAndSchema database={database} />
                                 </TableCell>
                                 <TableCell align="right">
                                    <Typography variant="body2">
                                       {formatRowSize(database.info.rowCount)}
                                    </Typography>
                                 </TableCell>
                              </TableRow>
                           ))}
                        </TableBody>
                     </Table>
                  </TableContainer>
               )}
               {isSuccess && data.data.length === 0 && (
                  <Typography variant="body2">No Embedded Databases</Typography>
               )}
               {isError && (
                  <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
                     {`${projectName} > ${packageName} > ${versionId} - ${error.message}`}
                  </Typography>
               )}
            </Box>
         </StyledCardContent>
      </StyledCard>
   );
}

function NameAndSchema({ database }: { database: Database }) {
   const [open, setOpen] = React.useState(false);
   return (
      <Box
         sx={{ display: "flex", alignItems: "center" }}
         onClick={() => setOpen(!open)}
         style={{ cursor: "pointer" }}
      >
         <Typography
            variant="body2"
            color="primary"
            sx={{
               maxWidth: "200px",
               overflow: "hidden",
               textOverflow: "ellipsis",
               whiteSpace: "nowrap",
            }}
         >
            {database.path}
         </Typography>
         &nbsp;
         <Box sx={{ display: "flex", alignItems: "center" }}>
            <Box sx={{ mr: 1, display: "flex", alignItems: "center" }}>
               <svg
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  xmlns="http://www.w3.org/2000/svg"
               >
                  <path
                     d="M11 7H6C5.46957 7 4.96086 7.21071 4.58579 7.58579C4.21071 7.96086 4 8.46957 4 9V18C4 18.5304 4.21071 19.0391 4.58579 19.4142C4.96086 19.7893 5.46957 20 6 20H15C15.5304 20 16.0391 19.7893 16.4142 19.4142C16.7893 19.0391 17 18.5304 17 18V13"
                     stroke="currentColor"
                     strokeWidth="2"
                     strokeLinecap="round"
                     strokeLinejoin="round"
                  />
                  <path
                     d="M9 15L20 4"
                     stroke="currentColor"
                     strokeWidth="2"
                     strokeLinecap="round"
                     strokeLinejoin="round"
                  />
                  <path
                     d="M15 4H20V9"
                     stroke="currentColor"
                     strokeWidth="2"
                     strokeLinecap="round"
                     strokeLinejoin="round"
                  />
               </svg>
            </Box>
         </Box>
         <SchemaButton
            database={database}
            open={open}
            setClose={() => setOpen(false)}
         />
      </Box>
   );
}

function SchemaButton({
   database,
   open,
   setClose,
}: {
   open: boolean;
   setClose: () => void;
   database: Database;
}) {
   return (
      <Dialog open={open} onClose={setClose} maxWidth="sm" fullWidth>
         <DialogTitle>
            Schema:{" "}
            <Typography
               fontSize="large"
               variant="body2"
               fontFamily="monospace"
               component="span"
            >
               {database.path}
            </Typography>
            <IconButton
               aria-label="close"
               onClick={setClose}
               sx={{ position: "absolute", right: 8, top: 8 }}
            >
               <Box
                  sx={{
                     width: 24,
                     height: 24,
                     display: "flex",
                     alignItems: "center",
                     justifyContent: "center",
                  }}
               >
                  X
               </Box>
            </IconButton>
         </DialogTitle>
         <DialogContent>
            <TableContainer>
               <Table
                  size="small"
                  sx={{ "& .MuiTableCell-root": { padding: "10px" } }}
               >
                  <TableHead>
                     <TableRow>
                        <TableCell>NAME</TableCell>
                        <TableCell>TYPE</TableCell>
                     </TableRow>
                  </TableHead>
                  <TableBody>
                     {database.info.columns.map((row) => (
                        <TableRow key={row.name}>
                           <TableCell>{row.name}</TableCell>
                           <TableCell>{row.type}</TableCell>
                        </TableRow>
                     ))}
                  </TableBody>
               </Table>
            </TableContainer>
         </DialogContent>
      </Dialog>
   );
}
