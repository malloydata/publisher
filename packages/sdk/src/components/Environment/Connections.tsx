import { MoreVert } from "@mui/icons-material";
import StorageOutlinedIcon from "@mui/icons-material/StorageOutlined";
import {
   Box,
   Card,
   CardContent,
   Dialog,
   DialogContent,
   DialogTitle,
   Grid,
   IconButton,
   Menu,
   Snackbar,
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
import { useServer } from "../ServerProvider";
import ConnectionExplorer from "./ConnectionExplorer";

const CONNECTION_TYPE_LABELS: Record<string, string> = {
   bigquery: "BigQuery",
   snowflake: "Snowflake",
   postgres: "PostgreSQL",
   mysql: "MySQL",
   trino: "Trino",
   databricks: "Databricks",
   duckdb: "DuckDB",
   ducklake: "DuckLake",
   s3: "S3",
   gcs: "GCS",
   azure: "Azure",
};

function typeLabel(type: string | undefined): string {
   if (!type) return "";
   return CONNECTION_TYPE_LABELS[type] ?? type;
}

type ConnectionsProps = {
   resourceUri: string;
};

export default function Connections({ resourceUri }: ConnectionsProps) {
   const { apiClients, mutable } = useServer();
   const queryClient = useQueryClient();
   const { environmentName } = parseResourceUri(resourceUri);
   const [notificationMessage, setNotificationMessage] = useState("");
   const [selectedConnection, setSelectedConnection] = useState<string | null>(
      null,
   );
   const selectedConnectionResourceUri = encodeResourceUri({
      environmentName,
      connectionName: selectedConnection,
   });

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["connections", environmentName],
      queryFn: () => apiClients.connections.listConnections(environmentName),
   });

   const handleCloseDialog = () => {
      setSelectedConnection(null);
   };

   const addConnection = useMutationWithApiError({
      mutationFn: (payload: ApiConnection) => {
         return apiClients.environments.updateEnvironment(environmentName, {
            name: environmentName,
            connections: [...data!.data, payload],
         });
      },
      onSuccess() {
         setNotificationMessage("Connection added successfully");
         queryClient.invalidateQueries({
            queryKey: ["connections", environmentName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const updateConnection = useMutationWithApiError({
      mutationFn: (payload: ApiConnection) => {
         return apiClients.environments.updateEnvironment(environmentName, {
            name: environmentName,
            connections: data!.data.map((conn) =>
               conn.name === payload.name ? payload : conn,
            ),
         });
      },
      onSuccess(_data, variables) {
         setNotificationMessage(
            `Connection ${variables.name} updated successfully`,
         );
         queryClient.invalidateQueries({
            queryKey: ["connections", environmentName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const deleteConnection = useMutationWithApiError({
      mutationFn: (payload: ApiConnection) => {
         return apiClients.environments.updateEnvironment(environmentName, {
            name: environmentName,
            connections: data!.data.filter(
               (conn) => conn.name !== payload.name,
            ),
         });
      },
      onSuccess(_data, variables) {
         setNotificationMessage(
            `Connection ${variables.name} deleted successfully`,
         );
         queryClient.invalidateQueries({
            queryKey: ["connections", environmentName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const connections = isSuccess
      ? [...data.data].sort((a, b) =>
           (a.name ?? "").localeCompare(b.name ?? ""),
        )
      : [];

   const isMutating =
      addConnection.isPending ||
      updateConnection.isPending ||
      deleteConnection.isPending;

   return (
      <Box>
         <Box
            sx={{
               display: "flex",
               alignItems: "flex-start",
               justifyContent: "space-between",
               mb: 3,
            }}
         >
            <Box>
               <Typography
                  variant="h6"
                  sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
               >
                  Connections
               </Typography>
               <Typography variant="body2" color="text.secondary">
                  Database connections available to packages in this environment
               </Typography>
            </Box>
            {mutable && isSuccess && (
               <AddConnectionDialog
                  onSubmit={(payload) => addConnection.mutateAsync(payload)}
                  isSubmitting={addConnection.isPending}
               />
            )}
         </Box>

         {!isSuccess && !isError && (
            <Typography variant="body2" color="text.secondary">
               Fetching Connections...
            </Typography>
         )}
         {isError && (
            <ApiErrorDisplay
               error={error}
               context={`${environmentName} > Connections`}
            />
         )}
         {isSuccess && connections.length === 0 && (
            <Typography variant="body2" color="text.secondary">
               No connections yet.
            </Typography>
         )}
         {isSuccess && connections.length > 0 && (
            <Grid container spacing={2}>
               {connections.map((conn) => (
                  <Grid size={{ xs: 12, sm: 6, md: 4 }} key={conn.name}>
                     <ConnectionCard
                        connection={conn}
                        mutable={mutable}
                        isMutating={isMutating}
                        onOpenExplorer={() =>
                           setSelectedConnection(conn.name ?? null)
                        }
                        onEdit={(payload) =>
                           updateConnection.mutateAsync(payload)
                        }
                        onDelete={(payload) => {
                           if (!conn.resource) {
                              deleteConnection.mutateAsync(payload);
                           } else {
                              setNotificationMessage(
                                 "Cannot delete this connection",
                              );
                           }
                        }}
                     />
                  </Grid>
               ))}
            </Grid>
         )}

         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />

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
                     connection={data?.data?.find(
                        (c) => c.name === selectedConnection,
                     )}
                  />
               )}
            </DialogContent>
         </Dialog>
      </Box>
   );
}

type ConnectionCardProps = {
   connection: ApiConnection;
   mutable: boolean;
   isMutating: boolean;
   onOpenExplorer: () => void;
   onEdit: (connection: ApiConnection) => Promise<unknown>;
   onDelete: (connection: ApiConnection) => Promise<unknown> | void;
};

function ConnectionCard({
   connection,
   mutable,
   isMutating,
   onOpenExplorer,
   onEdit,
   onDelete,
}: ConnectionCardProps) {
   const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
   const menuOpen = Boolean(menuAnchorEl);

   const handleMenuClick = (event: React.MouseEvent<HTMLElement>) => {
      event.stopPropagation();
      setMenuAnchorEl(event.currentTarget);
   };

   const handleMenuClose = () => {
      setMenuAnchorEl(null);
   };

   return (
      <Card
         variant="outlined"
         onClick={onOpenExplorer}
         sx={{
            height: "100%",
            cursor: "pointer",
            borderRadius: 3,
            borderColor: "divider",
            boxShadow: "none",
            transition: "all 0.2s ease-in-out",
            "&:hover": { boxShadow: 2, borderColor: "primary.main" },
         }}
      >
         <CardContent sx={{ p: 2.5, "&:last-child": { pb: 2.5 } }}>
            <Box sx={{ display: "flex", alignItems: "flex-start", gap: 1.5 }}>
               <Box
                  sx={{
                     width: 36,
                     height: 36,
                     borderRadius: 1.5,
                     bgcolor: "warning.light",
                     display: "flex",
                     alignItems: "center",
                     justifyContent: "center",
                     flexShrink: 0,
                     color: "warning.main",
                  }}
               >
                  <StorageOutlinedIcon sx={{ fontSize: 20 }} />
               </Box>
               <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography
                     variant="subtitle1"
                     component="h6"
                     noWrap
                     sx={{ fontWeight: 600, mb: 0.5 }}
                  >
                     {connection.name}
                  </Typography>
                  <Typography variant="body2" color="text.secondary" noWrap>
                     {typeLabel(connection.type)}
                  </Typography>
               </Box>
               {mutable && (
                  <>
                     <IconButton
                        size="small"
                        onClick={handleMenuClick}
                        aria-label={`Connection actions for ${connection.name ?? ""}`.trim()}
                        sx={{ flexShrink: 0, mt: -0.5, mr: -0.5 }}
                     >
                        <MoreVert fontSize="small" />
                     </IconButton>
                     <Menu
                        anchorEl={menuAnchorEl}
                        open={menuOpen}
                        onClose={handleMenuClose}
                        onClick={(e) => e.stopPropagation()}
                        anchorOrigin={{
                           vertical: "bottom",
                           horizontal: "right",
                        }}
                        transformOrigin={{
                           vertical: "top",
                           horizontal: "right",
                        }}
                     >
                        <EditConnectionDialog
                           connection={connection}
                           onSubmit={onEdit}
                           isSubmitting={isMutating}
                           onCloseDialog={handleMenuClose}
                        />
                        <DeleteConnectionDialog
                           connection={connection}
                           onCloseDialog={handleMenuClose}
                           isMutating={isMutating}
                           onDelete={() => onDelete(connection)}
                        />
                     </Menu>
                  </>
               )}
            </Box>
         </CardContent>
      </Card>
   );
}
