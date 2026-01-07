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
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";

interface ConnectionExplorerProps {
   connectionName: string;
   schema?: string;
   resourceUri: string;
}

export default function ConnectionExplorer({
   connectionName,
   resourceUri,
   schema,
}: ConnectionExplorerProps) {
   const { apiClients } = useServer();
   const { projectName: projectName } = parseResourceUri(resourceUri);
   const [selectedTable, setSelectedTable] = React.useState<
      | { resource: string; columns: Array<{ name: string; type: string }> }
      | undefined
   >(undefined);
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
      queryKey: ["schemas", projectName, connectionName],
      queryFn: () =>
         apiClients.connections.listSchemas(projectName, connectionName),
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
                           context={`${projectName} > ${connectionName} > Schemas`}
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
                                             {
                                                setSelectedSchema(schemaName);
                                                setSelectedTable(undefined);
                                             }
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
                     onTableClick={(table) => {
                        setSelectedTable(table);
                     }}
                     resourceUri={resourceUri}
                  />
               </Paper>
            )}
         </Grid>
         <Grid size={{ xs: 12, md: schema ? 6 : 4 }}>
            {selectedTable && (
               <Paper sx={{ p: 1, m: 0 }}>
                  <TableSchemaViewer table={selectedTable} />
               </Paper>
            )}
         </Grid>
      </Grid>
   );
}

type TableSchemaViewerProps = {
   table: { resource: string; columns: Array<{ name: string; type: string }> };
};

function TableSchemaViewer({ table }: TableSchemaViewerProps) {
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
            Schema: {table.resource}
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
   onTableClick: (table: {
      resource: string;
      columns: Array<{ name: string; type: string }>;
   }) => void;
   resourceUri: string;
}

function TablesInSchema({
   connectionName,
   schemaName,
   onTableClick,
   resourceUri,
}: TablesInSchemaProps) {
   const { projectName: projectName } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();
   const [searchTerm, setSearchTerm] = useState("");
   const { data, isSuccess, isError, error, isLoading } = useQueryWithApiError({
      queryKey: ["tablesInSchema", projectName, connectionName, schemaName],
      queryFn: () =>
         apiClients.connections.listTables(
            projectName,
            connectionName,
            schemaName,
         ),
   });

   const filteredTables =
      isSuccess && data?.data
         ? data.data
              .filter((table: { resource: string }) => {
                 const tableName =
                    table.resource.split(".").pop()?.toLowerCase() || "";
                 return tableName.includes(searchTerm.toLowerCase());
              })
              .sort((a: { resource: string }, b: { resource: string }) => {
                 const tableNameA = a.resource.split(".").pop() || a.resource;
                 const tableNameB = b.resource.split(".").pop() || b.resource;
                 return tableNameA.localeCompare(tableNameB);
              })
         : [];

   return (
      <>
         <Typography variant="overline" fontWeight="bold">
            {schemaName.includes("gs") || schemaName.includes("s3")
               ? `Table Files in ${schemaName}`
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
                  context={`${projectName} > ${connectionName} > ${schemaName}`}
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
                        let tableName = "";
                        if (
                           table.resource.includes("gs://") ||
                           table.resource.includes("s3://")
                        ) {
                           tableName =
                              table.resource.split("/").pop() || table.resource;
                        } else {
                           // Extract table name from resource path (e.g., "schema.table_name" -> "table_name")
                           tableName =
                              table.resource.split(".").pop() || table.resource;
                        }
                        return (
                           <ListItemButton
                              key={table.resource}
                              onClick={() => onTableClick(table)}
                           >
                              <ListItemText
                                 primary={tableName}
                                 secondary={`${table.columns.length} columns`}
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
