import {
   Box,
   Divider,
   FormControlLabel,
   Grid,
   List,
   ListItemButton,
   ListItemText,
   Paper,
   Switch,
   Table,
   TableBody,
   TableCell,
   TableContainer,
   TableHead,
   TableRow,
   TextField,
   Typography,
} from "@mui/material";
import React, { useState } from "react";
import { Connection as ApiConnection, Column } from "../../client/api";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";

interface ConnectionExplorerProps {
   connectionName: string;
   schema?: string;
   resourceUri: string;
   connection?: ApiConnection;
}

/** Check if a schema name corresponds to an Azure attached database */
function isAzureSchema(
   connection: ApiConnection | undefined,
   schemaName: string,
): boolean {
   if (!connection || connection.type !== "duckdb") return false;
   const attachedDbs = connection.duckdbConnection?.attachedDatabases || [];
   return attachedDbs.some(
      (db) => db.type === "azure" && db.name === schemaName,
   );
}

/** Check if a schema name corresponds to a cloud storage path */
function isCloudStorageSchema(schemaName: string): boolean {
   return (
      schemaName.startsWith("gs://") ||
      schemaName.startsWith("s3://") ||
      schemaName.startsWith("https://") ||
      schemaName.startsWith("abfss://") ||
      schemaName.startsWith("az://")
   );
}

export default function ConnectionExplorer({
   connectionName,
   resourceUri,
   schema,
   connection,
}: ConnectionExplorerProps) {
   const { apiClients } = useServer();
   const { environmentName: environmentName } = parseResourceUri(resourceUri);
   const [selectedTableResource, setSelectedTableResource] = React.useState<
      string | null
   >(null);
   const [selectedSchema, setSelectedSchema] = React.useState<string | null>(
      schema || null,
   );
   const [showHiddenSchemas, setShowHiddenSchemas] = React.useState(false);
   const {
      data: schemasData,
      isError: schemasError,
      isLoading: schemasLoading,
      error: schemasErrorObj,
   } = useQueryWithApiError({
      queryKey: ["schemas", environmentName, connectionName],
      queryFn: () =>
         apiClients.connections.listSchemas(environmentName, connectionName),
   });

   const availableSchemas = schemasData?.data || [];

   return (
      <Grid container spacing={1}>
         {!schema && (
            <Grid size={{ xs: 12, md: 4 }}>
               <Paper sx={{ p: 1, m: 0 }}>
                  <Box
                     sx={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        mb: 0,
                     }}
                  >
                     <Typography variant="overline" fontWeight="bold">
                        Table Paths
                     </Typography>
                     <FormControlLabel
                        control={
                           <Switch
                              checked={showHiddenSchemas}
                              onChange={(e) =>
                                 setShowHiddenSchemas(e.target.checked)
                              }
                           />
                        }
                        label="Hidden Schemas"
                     />
                  </Box>
                  <Divider sx={{ mt: "2px" }} />
                  <Box
                     sx={{ mt: "2px", maxHeight: "600px", overflowY: "auto" }}
                  >
                     {schemasLoading && <Loading text="Loading schemas..." />}
                     {schemasError && (
                        <ApiErrorDisplay
                           error={schemasErrorObj}
                           context={`${environmentName} > ${connectionName} > Schemas`}
                        />
                     )}
                     {!schemasLoading &&
                        !schemasError &&
                        availableSchemas.length === 0 && (
                           <Typography variant="body2">No Schemas</Typography>
                        )}
                     {!schemasLoading &&
                        !schemasError &&
                        availableSchemas.length > 0 && (
                           <List dense disablePadding>
                              {availableSchemas.map(
                                 (schema: {
                                    name: string;
                                    isHidden: boolean;
                                 }) => {
                                    const schemaName = schema.name;
                                    const isHidden = schema.isHidden;
                                    if (isHidden && !showHiddenSchemas) {
                                       return null;
                                    }
                                    const isSelected =
                                       selectedSchema === schemaName;
                                    return (
                                       <ListItemButton
                                          key={schemaName}
                                          selected={isSelected}
                                          onClick={() => {
                                             setSelectedSchema(schemaName);
                                             setSelectedTableResource(null);
                                          }}
                                       >
                                          <ListItemText primary={schemaName} />
                                       </ListItemButton>
                                    );
                                 },
                              )}
                           </List>
                        )}
                  </Box>
               </Paper>
            </Grid>
         )}
         <Grid size={{ xs: 12, md: schema ? 6 : 4 }}>
            {selectedSchema && (
               <Paper sx={{ p: 1, m: 0 }}>
                  <TablesInSchema
                     connectionName={connectionName}
                     schemaName={selectedSchema}
                     onTableSelect={(resource) => {
                        setSelectedTableResource(resource);
                     }}
                     resourceUri={resourceUri}
                     connection={connection}
                  />
               </Paper>
            )}
         </Grid>
         <Grid size={{ xs: 12, md: schema ? 6 : 4 }}>
            {selectedSchema && selectedTableResource && (
               <SelectedTableDetailPanel
                  environmentName={environmentName}
                  connectionName={connectionName}
                  schemaName={selectedSchema}
                  tableResource={selectedTableResource}
               />
            )}
         </Grid>
      </Grid>
   );
}

type TableSchemaViewerProps = {
   table: { resource: string; columns: Column[] };
   loading?: boolean;
};

function SelectedTableDetailPanel({
   environmentName,
   connectionName,
   schemaName,
   tableResource,
}: {
   environmentName: string;
   connectionName: string;
   schemaName: string;
   tableResource: string;
}) {
   const { apiClients } = useServer();
   const {
      data: tableDetailRes,
      isLoading: tableDetailLoading,
      isError: tableDetailError,
      error: tableDetailErrorObj,
   } = useQueryWithApiError({
      queryKey: [
         "connectionTableDetail",
         environmentName,
         connectionName,
         schemaName,
         tableResource,
      ],
      queryFn: () =>
         apiClients.connections.getTable(
            environmentName,
            connectionName,
            schemaName,
            tableResource,
         ),
      enabled: Boolean(schemaName) && Boolean(tableResource),
   });

   const table = {
      resource: tableDetailRes?.data?.resource ?? tableResource,
      columns: tableDetailRes?.data?.columns ?? [],
   };

   return (
      <Paper sx={{ p: 1, m: 0 }}>
         {tableDetailError && (
            <ApiErrorDisplay
               error={tableDetailErrorObj}
               context={`${environmentName} > ${connectionName} > ${schemaName} > ${tableResource}`}
            />
         )}
         {!tableDetailError && (
            <TableSchemaViewer table={table} loading={tableDetailLoading} />
         )}
      </Paper>
   );
}

function TableSchemaViewer({ table, loading }: TableSchemaViewerProps) {
   if (loading) {
      return <Loading text="Loading columns..." />;
   }
   return (
      <>
         <Typography
            variant="overline"
            fontWeight="bold"
            sx={{
               display: "block",
               wordBreak: "break-all",
            }}
         >
            {table.resource.includes("://") ||
            /\.(parquet|csv|json|tsv|ndjson)$/i.test(table.resource)
               ? "File"
               : "Table"}
            : {table.resource}
         </Typography>
         <Divider />
         <Box sx={{ mt: "10px", maxHeight: "600px", overflowY: "auto" }}>
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
                     {table.columns
                        ?.sort((a: { name: string }, b: { name: string }) =>
                           a.name.localeCompare(b.name),
                        )
                        ?.map((column: { name: string; type: string }) => (
                           <TableRow key={column.name}>
                              <TableCell>{column.name}</TableCell>
                              <TableCell>{column.type}</TableCell>
                           </TableRow>
                        ))}
                  </TableBody>
               </Table>
            </TableContainer>
         </Box>
      </>
   );
}

interface TablesInSchemaProps {
   connectionName: string;
   schemaName: string;
   onTableSelect: (tableResource: string) => void;
   resourceUri: string;
   connection?: ApiConnection;
}

function TablesInSchema({
   connectionName,
   schemaName,
   onTableSelect,
   resourceUri,
   connection,
}: TablesInSchemaProps) {
   const { environmentName: environmentName } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();
   const [searchTerm, setSearchTerm] = useState("");
   const { data, isSuccess, isError, error, isLoading } = useQueryWithApiError({
      queryKey: ["tablesInSchema", environmentName, connectionName, schemaName],
      queryFn: () =>
         apiClients.connections.listTables(
            environmentName,
            connectionName,
            schemaName,
         ),
   });

   const isAzure = isAzureSchema(connection, schemaName);
   const getDisplayName = (resource: string) =>
      isAzure ? resource : resource.split(".").pop() || resource;

   const filteredTables =
      isSuccess && data?.data
         ? data.data
              .filter((table: { resource: string }) => {
                 return getDisplayName(table.resource)
                    .toLowerCase()
                    .includes(searchTerm.toLowerCase());
              })
              .sort((a: { resource: string }, b: { resource: string }) => {
                 return getDisplayName(a.resource).localeCompare(
                    getDisplayName(b.resource),
                 );
              })
         : [];

   return (
      <>
         <Typography variant="overline" fontWeight="bold">
            {isCloudStorageSchema(schemaName) ||
            isAzureSchema(connection, schemaName)
               ? `Files in ${schemaName}`
               : `Tables in ${schemaName}`}
         </Typography>
         <Divider />
         <Box sx={{ mt: 1, mb: 1 }}>
            <TextField
               size="small"
               fullWidth
               placeholder="Search tables..."
               value={searchTerm}
               onChange={(e) => setSearchTerm(e.target.value)}
               variant="outlined"
            />
         </Box>
         <Divider />
         <Box sx={{ mt: "2px", maxHeight: "600px", overflowY: "auto" }}>
            {isLoading && <Loading text="Fetching Tables..." />}
            {isError && (
               <ApiErrorDisplay
                  error={error}
                  context={`${environmentName} > ${connectionName} > ${schemaName}`}
               />
            )}
            {isSuccess && filteredTables.length === 0 && (
               <Typography variant="body2">No Tables</Typography>
            )}
            {isSuccess && data?.data && data.data.length > 0 && (
               <List dense disablePadding>
                  {filteredTables.map(
                     (table: {
                        resource: string;
                        columns: Array<{ name: string; type: string }>;
                     }) => {
                        let tableName = getDisplayName(table.resource);
                        if (!isAzure && table.resource.includes("://")) {
                           tableName =
                              table.resource.split("/").pop() || table.resource;
                        }
                        return (
                           <ListItemButton
                              key={table.resource}
                              onClick={() => onTableSelect(table.resource)}
                           >
                              <ListItemText
                                 primary={tableName}
                                 secondary={
                                    table.columns.length > 0
                                       ? `${table.columns.length} columns`
                                       : undefined
                                 }
                              />
                           </ListItemButton>
                        );
                     },
                  )}
               </List>
            )}
         </Box>
      </>
   );
}
