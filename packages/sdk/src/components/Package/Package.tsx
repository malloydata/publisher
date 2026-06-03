import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import CloseIcon from "@mui/icons-material/Close";
import {
   Box,
   Container,
   Dialog,
   DialogContent,
   DialogTitle,
   IconButton,
   Link,
   Stack,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Typography,
} from "@mui/material";
import React, { useState } from "react";
import { Database } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { RetrievalFunction } from "../filter/DimensionFilter";
import { Loading } from "../Loading";
import { Notebook } from "../Notebook";
import { useServer } from "../ServerProvider";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { MALLOY_BRAND, MONO_FONT_FAMILY } from "../styles";
import ContentTypeIcon from "./ContentTypeIcon";

const README_NOTEBOOK = "README.malloynb";

interface PackageProps {
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
   /** Optional retrieval function for semantic search filters */
   retrievalFn?: RetrievalFunction;
}

export default function Package({
   onClickPackageFile,
   resourceUri,
   retrievalFn,
}: PackageProps) {
   const { apiClients } = useServer();
   const onClick =
      onClickPackageFile ??
      ((to: string) => {
         window.location.href = to;
      });
   const { environmentName, packageName, versionId } =
      parseResourceUri(resourceUri);

   const [schemaDatabase, setSchemaDatabase] = useState<Database | null>(null);

   const pkgQuery = useQueryWithApiError({
      queryKey: ["package", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.packages.getPackage(
            environmentName,
            packageName,
            versionId,
            false,
         ),
   });

   const notebooksQuery = useQueryWithApiError({
      queryKey: ["notebooks", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.notebooks.listNotebooks(
            environmentName,
            packageName,
            versionId,
         ),
   });

   const modelsQuery = useQueryWithApiError({
      queryKey: ["models", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.models.listModels(environmentName, packageName, versionId),
   });

   const databasesQuery = useQueryWithApiError({
      queryKey: ["databases", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.databases.listDatabases(
            environmentName,
            packageName,
            versionId,
         ),
   });

   const notebooks = (notebooksQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => a.path.localeCompare(b.path));
   const models = (modelsQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => a.path.localeCompare(b.path));
   const databases = (databasesQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => a.path.localeCompare(b.path));

   const description = pkgQuery.data?.data?.description ?? "";
   const hasReadme = notebooks.some((n) => n.path === README_NOTEBOOK);
   const readmeResourceUri = encodeResourceUri({
      environmentName,
      packageName,
      versionId,
      modelPath: README_NOTEBOOK,
   });

   const isLoading = !notebooksQuery.isSuccess && !notebooksQuery.isError;

   if (pkgQuery.isError) {
      return (
         <ApiErrorDisplay
            error={pkgQuery.error}
            context={`${environmentName} > ${packageName}`}
         />
      );
   }

   return (
      <Container
         maxWidth={false}
         sx={{ maxWidth: 1024, mx: "auto", px: 3, py: 6 }}
      >
         <Box sx={{ mb: 4 }}>
            <Link
               onClick={(event: React.MouseEvent) =>
                  onClick(`/${environmentName}/`, event)
               }
               underline="none"
               sx={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 0.5,
                  cursor: "pointer",
                  color: "text.secondary",
                  fontSize: "0.875rem",
                  mb: 2,
                  "&:hover": { color: "primary.main" },
               }}
            >
               <ArrowBackIcon sx={{ fontSize: 18 }} />
               Back to {environmentName}
            </Link>
            <Typography
               variant="h4"
               component="h1"
               sx={{
                  fontWeight: 600,
                  letterSpacing: "-0.025em",
                  mb: 0.5,
               }}
            >
               {packageName}
            </Typography>
            {description && (
               <Typography variant="body2" color="text.secondary">
                  {description}
               </Typography>
            )}
         </Box>

         {isLoading && <Loading text="Loading package..." />}

         {!isLoading && (
            <>
               <PackageSection
                  title="Governed Reports"
                  count={notebooks.length}
               >
                  {notebooks.map((notebook) => (
                     <PackageItemRow
                        key={notebook.path}
                        icon={<ContentTypeIcon type="report" />}
                        tint={MALLOY_BRAND.teal}
                        label={notebook.path}
                        onClick={(event) => onClick(notebook.path, event)}
                     />
                  ))}
                  {notebooks.length === 0 && <EmptyRow label="No notebooks" />}
               </PackageSection>

               <PackageSection title="Semantic Models" count={models.length}>
                  {models.map((model) => (
                     <PackageItemRow
                        key={model.path}
                        icon={<ContentTypeIcon type="model" />}
                        tint={MALLOY_BRAND.orange}
                        label={model.path}
                        onClick={(event) => onClick(model.path, event)}
                     />
                  ))}
                  {models.length === 0 && <EmptyRow label="No models" />}
               </PackageSection>

               <PackageSection title="Package Data" count={databases.length}>
                  {databases.map((database) => (
                     <PackageItemRow
                        key={database.path}
                        icon={<ContentTypeIcon type="data" />}
                        tint={MALLOY_BRAND.darkBlue}
                        label={database.path}
                        rightLabel={formatRowCount(database.info.rowCount)}
                        onClick={() => setSchemaDatabase(database)}
                     />
                  ))}
                  {databases.length === 0 && <EmptyRow label="No data files" />}
               </PackageSection>

               <PackageSection title="Materializations">
                  <PackageItemRow
                     icon={<ContentTypeIcon type="materialization" />}
                     tint={MALLOY_BRAND.teal}
                     label="Materializations & Manifest"
                     onClick={(event) =>
                        onClick(
                           `/${environmentName}/${packageName}/materializations`,
                           event,
                        )
                     }
                  />
               </PackageSection>

               {hasReadme && (
                  <Box sx={{ mt: 6 }}>
                     <Notebook
                        resourceUri={readmeResourceUri}
                        retrievalFn={retrievalFn}
                     />
                  </Box>
               )}
            </>
         )}

         <Dialog
            open={schemaDatabase !== null}
            onClose={() => setSchemaDatabase(null)}
            maxWidth="sm"
            fullWidth
         >
            <DialogTitle sx={{ pr: 6 }}>
               {schemaDatabase?.path}
               <IconButton
                  aria-label="close"
                  onClick={() => setSchemaDatabase(null)}
                  sx={{ position: "absolute", right: 8, top: 8 }}
               >
                  <CloseIcon fontSize="small" />
               </IconButton>
            </DialogTitle>
            <DialogContent>
               {schemaDatabase?.info?.columns && (
                  <Table size="small">
                     <TableHead>
                        <TableRow>
                           <TableCell>Column</TableCell>
                           <TableCell>Type</TableCell>
                        </TableRow>
                     </TableHead>
                     <TableBody>
                        {schemaDatabase.info.columns.map((column) => (
                           <TableRow key={column.name}>
                              <TableCell component="th" scope="row">
                                 {column.name}
                              </TableCell>
                              <TableCell>{column.type}</TableCell>
                           </TableRow>
                        ))}
                     </TableBody>
                  </Table>
               )}
            </DialogContent>
         </Dialog>
      </Container>
   );
}

function PackageSection({
   title,
   count,
   children,
}: {
   title: string;
   count?: number;
   children: React.ReactNode;
}) {
   return (
      <Box sx={{ mb: 4 }}>
         <Stack
            direction="row"
            alignItems="baseline"
            spacing={1}
            sx={{ mb: 1 }}
         >
            <Typography
               variant="h6"
               sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
            >
               {title}
            </Typography>
            {count !== undefined && (
               <Typography variant="caption" color="text.secondary">
                  ({count})
               </Typography>
            )}
         </Stack>
         <Box>{children}</Box>
      </Box>
   );
}

function PackageItemRow({
   icon,
   tint,
   label,
   rightLabel,
   onClick,
}: {
   icon: React.ReactNode;
   tint: string;
   label: string;
   rightLabel?: string;
   onClick?: (event: React.MouseEvent) => void;
}) {
   const interactive = !!onClick;
   const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (!onClick) return;
      if (event.key === "Enter" || event.key === " ") {
         event.preventDefault();
         onClick(event as unknown as React.MouseEvent);
      }
   };
   return (
      <Box
         onClick={onClick}
         onKeyDown={interactive ? handleKeyDown : undefined}
         role={interactive ? "button" : undefined}
         tabIndex={interactive ? 0 : undefined}
         sx={{
            display: "flex",
            alignItems: "center",
            gap: 1.5,
            py: 1,
            px: 1,
            mx: -1,
            cursor: interactive ? "pointer" : "default",
            borderRadius: 1.5,
            transition: "background-color 0.1s",
            "&:hover": interactive
               ? { backgroundColor: "grey.100" }
               : undefined,
            "&:focus-visible": interactive
               ? {
                    outline: "2px solid",
                    outlineColor: "primary.main",
                    outlineOffset: 2,
                 }
               : undefined,
         }}
      >
         <Box
            sx={{
               width: 32,
               height: 32,
               borderRadius: 1,
               bgcolor: tint,
               color: "#FFFFFF",
               display: "flex",
               alignItems: "center",
               justifyContent: "center",
               flexShrink: 0,
            }}
         >
            {icon}
         </Box>
         <Typography
            variant="body2"
            sx={{
               fontFamily: MONO_FONT_FAMILY,
               flex: 1,
               minWidth: 0,
               overflow: "hidden",
               textOverflow: "ellipsis",
               whiteSpace: "nowrap",
            }}
         >
            {label}
         </Typography>
         {rightLabel && (
            <Typography
               variant="caption"
               color="text.secondary"
               sx={{ flexShrink: 0 }}
            >
               {rightLabel}
            </Typography>
         )}
      </Box>
   );
}

function EmptyRow({ label }: { label: string }) {
   return (
      <Typography
         variant="body2"
         color="text.secondary"
         sx={{ py: 1, fontStyle: "italic" }}
      >
         {label}
      </Typography>
   );
}

function formatRowCount(rows: number): string {
   if (rows >= 1_000_000_000)
      return `${(rows / 1_000_000_000).toFixed(1)} B rows`;
   if (rows >= 1_000_000) return `${(rows / 1_000_000).toFixed(1)} M rows`;
   if (rows >= 1_000) return `${(rows / 1_000).toFixed(1)} K rows`;
   return `${rows} rows`;
}
