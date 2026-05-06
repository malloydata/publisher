import {
   Box,
   Dialog,
   DialogContent,
   DialogTitle,
   Table,
   TableBody,
   TableCell,
   TableContainer,
   TableHead,
   TableRow,
   Typography,
   IconButton,
} from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";
import React from "react";
import { Database } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import {
   PackageCard,
   PackageCardContent,
   PackageSectionTitle,
} from "../styles";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";

type Props = {
   resourceUri: string;
};

export default function Databases({ resourceUri }: Props) {
   const { apiClients } = useServer();
   const {
      environmentName: environmentName,
      packageName: packageName,
      versionId: versionId,
   } = parseResourceUri(resourceUri);

   const [open, setOpen] = React.useState(false);
   const [selectedDatabase, setSelectedDatabase] =
      React.useState<Database | null>(null);

   const handleOpen = (database: Database) => {
      setSelectedDatabase(database);
      setOpen(true);
   };

   const handleClose = () => {
      setOpen(false);
      setSelectedDatabase(null);
   };

   const { data, isError, error, isSuccess } = useQueryWithApiError({
      queryKey: ["databases", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.databases.listDatabases(
            environmentName,
            packageName,
            versionId,
         ),
   });
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
      <>
         <PackageCard>
            <PackageCardContent>
               <PackageSectionTitle>Embedded Databases</PackageSectionTitle>
               <Box
                  sx={{
                     maxHeight: "200px",
                     overflowY: "auto",
                  }}
               >
                  {!isSuccess && !isError && (
                     <Loading text="Fetching Databases..." />
                  )}
                  {isError && (
                     <ApiErrorDisplay
                        error={error}
                        context={`${environmentName} > ${packageName} > Databases`}
                     />
                  )}
                  {isSuccess && data.data.length > 0 && (
                     <Table
                        size="small"
                        sx={{
                           borderCollapse: "collapse",
                           "& .MuiTableCell-root": {
                              borderBottom: "1px solid #e0e0e0",
                           },
                           "& .MuiTableRow-root:last-child .MuiTableCell-root":
                              {
                                 borderBottom: "none",
                              },
                        }}
                     >
                        <TableBody>
                           <TableRow>
                              <TableCell>
                                 <Typography
                                    variant="body2"
                                    fontWeight="500"
                                    color="text.secondary"
                                 >
                                    Name
                                 </Typography>
                              </TableCell>
                              <TableCell align="right">
                                 <Typography
                                    variant="body2"
                                    fontWeight="500"
                                    color="text.secondary"
                                 >
                                    Rows
                                 </Typography>
                              </TableCell>
                           </TableRow>
                           {data.data.map((database) => (
                              <TableRow
                                 key={database.path}
                                 onClick={() => handleOpen(database)}
                                 sx={{
                                    cursor: "pointer",
                                    "&:hover": {
                                       backgroundColor: "action.hover",
                                    },
                                 }}
                              >
                                 <TableCell component="th" scope="row">
                                    <Box
                                       sx={{
                                          display: "flex",
                                          alignItems: "center",
                                          gap: 1,
                                       }}
                                    >
                                       <Typography variant="body2">
                                          {database.path}
                                       </Typography>
                                       <SearchIcon
                                          sx={{
                                             fontSize: "1rem",
                                             color: "action.active",
                                             opacity: 0.7,
                                          }}
                                       />
                                    </Box>
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
                  )}
                  {isSuccess && data.data.length === 0 && (
                     <Typography variant="body2" sx={{ m: "auto" }}>
                        No databases found
                     </Typography>
                  )}
               </Box>
            </PackageCardContent>
         </PackageCard>

         <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
            <DialogTitle>
               {selectedDatabase?.path}
               <IconButton
                  aria-label="close"
                  onClick={handleClose}
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
               {selectedDatabase?.info?.columns && (
                  <TableContainer>
                     <Table size="small">
                        <TableHead>
                           <TableRow>
                              <TableCell>Column</TableCell>
                              <TableCell>Type</TableCell>
                           </TableRow>
                        </TableHead>
                        <TableBody>
                           {selectedDatabase.info.columns.map((column) => (
                              <TableRow key={column.name}>
                                 <TableCell component="th" scope="row">
                                    {column.name}
                                 </TableCell>
                                 <TableCell>{column.type}</TableCell>
                              </TableRow>
                           ))}
                        </TableBody>
                     </Table>
                  </TableContainer>
               )}
            </DialogContent>
         </Dialog>
      </>
   );
}
