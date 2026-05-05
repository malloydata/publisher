import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import { Box, Container, Link, Stack, Typography } from "@mui/material";
import React from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { RetrievalFunction } from "../filter/DimensionFilter";
import { Loading } from "../Loading";
import { Notebook } from "../Notebook";
import { useServer } from "../ServerProvider";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import ContentTypeIcon from "./ContentTypeIcon";

const README_NOTEBOOK = "README.malloynb";

// Malloy brand colors — exact hex values from publisher/packages/app/public/logo.svg.
const malloyTeal = "#14b3cb"; // report — light wing of the M
const malloyOrange = "#e47404"; // model — right wing of the M
const malloyDarkBlue = "#1474a4"; // data — deep shadow of the M
const monoFontFamily =
   '"JetBrains Mono", "ui-monospace", "SFMono-Regular", "Menlo", monospace';

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
   const { projectName, packageName, versionId } =
      parseResourceUri(resourceUri);

   const pkgQuery = useQueryWithApiError({
      queryKey: ["package", projectName, packageName, versionId],
      queryFn: () =>
         apiClients.packages.getPackage(
            projectName,
            packageName,
            versionId,
            false,
         ),
   });

   const notebooksQuery = useQueryWithApiError({
      queryKey: ["notebooks", projectName, packageName, versionId],
      queryFn: () =>
         apiClients.notebooks.listNotebooks(
            projectName,
            packageName,
            versionId,
         ),
   });

   const modelsQuery = useQueryWithApiError({
      queryKey: ["models", projectName, packageName, versionId],
      queryFn: () =>
         apiClients.models.listModels(projectName, packageName, versionId),
   });

   const databasesQuery = useQueryWithApiError({
      queryKey: ["databases", projectName, packageName, versionId],
      queryFn: () =>
         apiClients.databases.listDatabases(
            projectName,
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
      projectName,
      packageName,
      versionId,
      modelPath: README_NOTEBOOK,
   });

   const isLoading = !notebooksQuery.isSuccess && !notebooksQuery.isError;

   if (pkgQuery.isError) {
      return (
         <ApiErrorDisplay
            error={pkgQuery.error}
            context={`${projectName} > ${packageName}`}
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
                  onClick(`/${projectName}/`, event)
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
               Back to {projectName}
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
                        tint={malloyTeal}
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
                        tint={malloyOrange}
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
                        tint={malloyDarkBlue}
                        label={database.path}
                        rightLabel={formatRowCount(database.info.rowCount)}
                     />
                  ))}
                  {databases.length === 0 && <EmptyRow label="No data files" />}
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
      </Container>
   );
}

function PackageSection({
   title,
   count,
   children,
}: {
   title: string;
   count: number;
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
            <Typography variant="caption" color="text.secondary">
               ({count})
            </Typography>
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
   return (
      <Box
         onClick={onClick}
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
               fontFamily: monoFontFamily,
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
