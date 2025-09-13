import SearchIcon from "@mui/icons-material/Search";
import {
   Box,
   Dialog,
   DialogContent,
   DialogTitle,
   IconButton,
   Snackbar,
   Table,
   TableBody,
   TableCell,
   TableRow,
   Typography,
} from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Connection as ApiConnection } from "../../client/api";
import {
   useMutationWithApiError,
   useQueryWithApiError,
} from "../../hooks/useQueryWithApiError";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import AddConnectionDialog from "../Connections/AddConnectionDialog";
import DeleteConnectionDialog from "../Connections/DeleteConnectionDialog";
import EditConnectionDialog from "../Connections/EditConnectionDialog";
import ConnectionExplorer from "../Project/ConnectionExplorer";
import { useServer } from "../ServerProvider";
import {
   PackageCard,
   PackageCardContent,
   PackageSectionTitle,
} from "../styles";

type ConnectionProps = {
   connection: ApiConnection;
   onClick: () => void;
   onEdit: (connection: ApiConnection) => Promise<unknown>;
   onDelete: (connection: ApiConnection) => Promise<unknown>;
   isMutating: boolean;
   mutable: boolean;
};

// TODO(jjs) - Move this UI to the ConnectionExplorer component
function Connection({
   connection,
   onClick,
   onEdit,
   onDelete,
   isMutating,
   mutable,
}: ConnectionProps) {
   return (
      <TableRow
         key={connection.name}
         sx={{
            "&:hover": {
               backgroundColor: "action.hover",
            },
         }}
      >
         <TableCell
            sx={{
               cursor: "pointer",
               flexGrow: 1,
            }}
            onClick={onClick}
         >
            <Box
               sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1,
               }}
            >
               <Typography variant="body2">{connection.name}</Typography>
               <SearchIcon
                  sx={{
                     fontSize: "1rem",
                     color: "action.active",
                     opacity: 0.7,
                  }}
               />
            </Box>
         </TableCell>
         <TableCell sx={{ minWidth: "120px" }}>
            <Typography variant="body2">{connection.type}</Typography>
         </TableCell>
         {mutable && (
            <TableCell sx={{ minWidth: "120px" }}>
               <EditConnectionDialog
                  connection={connection}
                  onSubmit={onEdit}
                  isSubmitting={isMutating}
               />
               <DeleteConnectionDialog
                  connection={connection}
                  onCloseDialog={() => { }}
                  isMutating={isMutating}
                  onDelete={() => onDelete(connection)}
               />
            </TableCell>
         )}
      </TableRow>
   );
}

type ConnectionsProps = {
   resourceUri: string;
};

export default function Connections({ resourceUri }: ConnectionsProps) {
   const { apiClients, mutable } = useServer();
   const queryClient = useQueryClient();
   const { projectName: projectName } = parseResourceUri(resourceUri);
   const [notificationMessage, setNotificationMessage] = useState("");
   const [selectedConnection, setSelectedConnection] = useState<string | null>(
      null,
   );
   const selectedConnectionResourceUri = encodeResourceUri({
      projectName: projectName,
      connectionName: selectedConnection,
   });
   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["connections", projectName],
      queryFn: () => apiClients.connections.listConnections(projectName),
   });

   const handleConnectionClick = (connectionName: string) => {
      setSelectedConnection(connectionName);
   };

   const handleCloseDialog = () => {
      setSelectedConnection(null);
   };

   const addConnection = useMutationWithApiError({
      mutationFn: (payload: ApiConnection) => {
         return apiClients.projects.updateProject(projectName, {
            name: projectName,
            connections: [...data.data, payload],
         });
      },
      onSuccess() {
         setNotificationMessage("Connection added successfully");
         queryClient.invalidateQueries({
            queryKey: ["connections", projectName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const updateConnection = useMutationWithApiError({
      mutationFn: (payload: ApiConnection) => {
         return apiClients.projects.updateProject(projectName, {
            name: projectName,
            connections: data.data.map((conn) =>
               conn.name === payload.name ? payload : conn,
            ),
         });
      },
      onSuccess(_data, variables) {
         setNotificationMessage(
            `Connection ${variables.name} updated successfully`,
         );
         queryClient.invalidateQueries({
            queryKey: ["connections", projectName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const deleteConnection = useMutationWithApiError({
      mutationFn: (payload: ApiConnection) => {
         return apiClients.projects.updateProject(projectName, {
            name: projectName,
            connections: data.data.filter((conn) => conn.name !== payload.name),
         });
      },
      onSuccess(_data, variables) {
         setNotificationMessage(
            `Connection ${variables.name} deleted successfully`,
         );
         queryClient.invalidateQueries({
            queryKey: ["connections", projectName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   return (
      <>
         <PackageCard>
            <PackageCardContent>
               <PackageSectionTitle>Database Connections</PackageSectionTitle>
               <Box
                  sx={{
                     mb: 1,
                     maxHeight: "200px",
                     overflowY: "auto",
                  }}
               >
                  {!isSuccess && !isError && (
                     <Typography variant="body2" sx={{ m: "auto" }}>
                        Fetching Connections...
                     </Typography>
                  )}
                  {isSuccess && data.data.length > 0 && (
                     <Table
                        size="small"
                        sx={{
                           borderCollapse: "collapse",
                           "& .MuiTableRow-root": {
                              display: "flex",
                              width: "100%",
                           },
                           "& .MuiTableCell-root": {
                              borderBottom: "1px solid #e0e0e0",
                              display: "flex",
                              alignItems: "center",
                           },
                           "& .MuiTableRow-root:last-child .MuiTableCell-root":
                           {
                              borderBottom: "none",
                           },
                        }}
                     >
                        <TableBody>
                           <TableRow>
                              <TableCell sx={{ flexGrow: 1 }}>
                                 <Typography
                                    variant="body2"
                                    fontWeight="500"
                                    color="text.secondary"
                                 >
                                    Connection Name
                                 </Typography>
                              </TableCell>
                              <TableCell sx={{ minWidth: "120px" }}>
                                 <Typography
                                    variant="body2"
                                    fontWeight="500"
                                    color="text.secondary"
                                 >
                                    Type
                                 </Typography>
                              </TableCell>
                              {mutable && (
                                 <TableCell sx={{ minWidth: "120px" }}>
                                    <Typography
                                       variant="body2"
                                       fontWeight="500"
                                       color="text.secondary"
                                       sx={{ mx: "auto" }}
                                    >
                                       Actions
                                    </Typography>
                                 </TableCell>
                              )}
                           </TableRow>
                           {data.data.map((conn) => (
                              <Connection
                                 key={conn.name}
                                 connection={conn}
                                 onClick={() =>
                                    handleConnectionClick(conn.name)
                                 }
                                 onEdit={(payload) =>
                                    updateConnection.mutateAsync(payload)
                                 }
                                 onDelete={(payload) =>
                                    deleteConnection.mutateAsync(payload)
                                 }
                                 isMutating={
                                    updateConnection.isPending ||
                                    deleteConnection.isPending
                                 }
                                 mutable={mutable}
                              />
                           ))}
                        </TableBody>
                     </Table>
                  )}
                  {isSuccess && data.data.length === 0 && (
                     <Typography variant="body2">No Connections</Typography>
                  )}
                  {isError && (
                     <ApiErrorDisplay
                        error={error}
                        context={`${projectName} > Connections`}
                     />
                  )}
               </Box>
               {mutable && (
                  <Box sx={{ display: "flex", justifyContent: "flex-end" }}>
                     <AddConnectionDialog
                        onSubmit={(payload) => addConnection.mutateAsync(payload)}
                        isSubmitting={addConnection.isPending}
                     />
                  </Box>
               )}
            </PackageCardContent>
         </PackageCard>

         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />

         {/* Connection Explorer Dialog */}
         <Dialog
            open={selectedConnection !== null}
            onClose={handleCloseDialog}
            maxWidth="lg"
            fullWidth
         >
            <DialogTitle>
               Connection Explorer: {selectedConnection}
               <IconButton
                  aria-label="close"
                  onClick={handleCloseDialog}
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
               {selectedConnection && (
                  <ConnectionExplorer
                     resourceUri={selectedConnectionResourceUri}
                     connectionName={selectedConnection}
                  />
               )}
            </DialogContent>
         </Dialog>
      </>
   );
}
