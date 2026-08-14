import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import CloseIcon from "@mui/icons-material/Close";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
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
   Tooltip,
   Typography,
} from "@mui/material";
import React, { useState } from "react";
import { Database } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { Notebook } from "../Notebook";
import { useServer } from "../ServerProvider";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { serverBaseUrl } from "../../utils/dataAppEmbed";
import { MALLOY_BRAND, MONO_FONT_FAMILY } from "../styles";
import ContentTypeIcon from "./ContentTypeIcon";

const README_NOTEBOOK = "README.malloynb";

interface PackageProps {
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Package({
   onClickPackageFile,
   resourceUri,
}: PackageProps) {
   const { apiClients, server } = useServer();
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

   // List of in-package HTML data apps bundled inside the package.
   // Goes through the configured API client so consumers using a non-default
   // baseURL or Bearer auth (via <ServerProvider>) get the same plumbing as
   // every other endpoint.
   // No versionId in the key: /data-apps serves static files, which aren't
   // versioned (listDataApps takes only env + package), so keying on
   // versionId would fragment the cache and prevent DataAppViewer's identical
   // query from deduping.
   const dataAppsQuery = useQueryWithApiError({
      queryKey: ["data-apps", environmentName, packageName],
      queryFn: async () => {
         try {
            return await apiClients.dataApps.listDataApps(
               environmentName,
               packageName,
            );
         } catch (e) {
            // A 404 or transport-level failure (older Publisher without the
            // /data-apps route, network blip) is non-fatal: render the package
            // page without a Data Apps section. A genuinely missing package
            // surfaces its own error via the package query above, so an empty
            // list here can't hide it.
            const status = (e as { response?: { status?: number } })?.response
               ?.status;
            if (status === 404 || status === undefined) {
               return { data: [] } as Awaited<
                  ReturnType<typeof apiClients.dataApps.listDataApps>
               >;
            }
            throw e;
         }
      },
   });
   const dataApps = dataAppsQuery.data?.data ?? [];

   // No versionId, for the same reason as data apps: the dashboards endpoint
   // takes only env + package.
   const dashboardsQuery = useQueryWithApiError({
      queryKey: ["dashboards", environmentName, packageName],
      queryFn: async () => {
         try {
            return await apiClients.dashboards.listDashboards(
               environmentName,
               packageName,
            );
         } catch (e) {
            // Non-fatal for the same reasons as the data-apps list above: an
            // older Publisher without the route should render a package page
            // without a Dashboards section, not an error.
            const status = (e as { response?: { status?: number } })?.response
               ?.status;
            if (status === 404 || status === undefined) {
               return { data: [] } as Awaited<
                  ReturnType<typeof apiClients.dashboards.listDashboards>
               >;
            }
            throw e;
         }
      },
   });
   const dashboards = (dashboardsQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));

   const notebooks = (notebooksQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => a.path.localeCompare(b.path));
   // A dashboard is listed once, under Dashboards. Its file is a model like any
   // other, so it would otherwise appear a second time under Semantic Models
   // where clicking it opens the Explorer rather than the dashboard. Untagged
   // shared includes in `dashboards/` are not dashboards and stay in the model
   // list, which is where they belong.
   const dashboardPaths = new Set(
      dashboards
         .map((dashboard) => dashboard.path)
         .filter((path): path is string => path !== undefined),
   );
   const models = (modelsQuery.data?.data ?? [])
      .slice()
      .filter((model) => !dashboardPaths.has(model.path))
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
               {/* First: the at-a-glance artifact a visitor most likely wants,
                   ahead of the notebooks and models it is built on. Hidden when
                   empty, like Data Apps. */}
               {dashboards.length > 0 && (
                  <PackageSection title="Dashboards" count={dashboards.length}>
                     {dashboards.map((dashboard) => {
                        // A title equal to the slug is what the server falls
                        // back to when the file names itself neither way, so
                        // showing both would print the same word twice.
                        const hasTitle =
                           !!dashboard.title &&
                           dashboard.title !== dashboard.name;
                        return (
                           <PackageItemRow
                              key={dashboard.name}
                              icon={<ContentTypeIcon type="dashboard" />}
                              tint={MALLOY_BRAND.teal}
                              label={
                                 hasTitle ? dashboard.title! : dashboard.name!
                              }
                              rightLabel={hasTitle ? dashboard.name : undefined}
                              onClick={(event) =>
                                 onClick(
                                    `/${environmentName}/${packageName}/dashboards/` +
                                       // The slug comes from a filename, which
                                       // can hold characters that would read as
                                       // structure in a path. The server encodes
                                       // it in `resource` for the same reason.
                                       encodeURIComponent(dashboard.name ?? ""),
                                    event,
                                 )
                              }
                           />
                        );
                     })}
                  </PackageSection>
               )}

               <PackageSection title="Notebooks" count={notebooks.length}>
                  {notebooks.map((notebook) => {
                     // Named the way dashboards and data apps are: a notebook
                     // that titles itself is listed by that title, with the
                     // filename kept as the secondary label so the path a
                     // reader needs to find the file is never lost.
                     const hasTitle =
                        !!notebook.title && notebook.title !== notebook.path;
                     return (
                        <PackageItemRow
                           key={notebook.path}
                           icon={<ContentTypeIcon type="report" />}
                           tint={MALLOY_BRAND.teal}
                           label={hasTitle ? notebook.title! : notebook.path}
                           rightLabel={hasTitle ? notebook.path : undefined}
                           onClick={(event) =>
                              onClick(
                                 `/${environmentName}/${packageName}/${notebook.path}`,
                                 event,
                              )
                           }
                        />
                     );
                  })}
                  {notebooks.length === 0 && <EmptyRow label="No notebooks" />}
               </PackageSection>

               {dataApps.length > 0 && (
                  <PackageSection title="Data Apps" count={dataApps.length}>
                     {dataApps.map((dataApp) => {
                        const hasTitle =
                           !!dataApp.title && dataApp.title !== dataApp.path;
                        // Standalone (raw) URL: the Publisher static-file route.
                        // dataApp.resource is the root-relative path; we join it
                        // with the data origin (the API base minus /api/v0),
                        // which may differ from the SPA origin when the SDK is
                        // embedded in a host app on another domain.
                        const standaloneUrl = `${serverBaseUrl(server)}${
                           dataApp.resource
                        }`;
                        return (
                           <PackageItemRow
                              key={dataApp.path}
                              icon={<ContentTypeIcon type="dataApp" />}
                              tint={MALLOY_BRAND.teal}
                              label={hasTitle ? dataApp.title : dataApp.path}
                              rightLabel={hasTitle ? dataApp.path : undefined}
                              onClick={(event) => {
                                 if (onClickPackageFile) {
                                    // Host app routes within SPA to an embedded
                                    // <DataAppViewer> that iframes the standalone
                                    // URL. The `data-apps/` prefix lets the
                                    // router branch off the existing model-path
                                    // catch-all.
                                    onClickPackageFile(
                                       `/${environmentName}/${packageName}/data-apps/${dataApp.path}`,
                                       event,
                                    );
                                 } else {
                                    // No host app — navigate to standalone HTML.
                                    if (
                                       event &&
                                       (event.metaKey || event.ctrlKey)
                                    ) {
                                       window.open(standaloneUrl, "_blank");
                                    } else {
                                       window.location.href = standaloneUrl;
                                    }
                                 }
                              }}
                              trailingAction={
                                 <Tooltip title="Open standalone in new tab">
                                    <IconButton
                                       size="small"
                                       href={standaloneUrl}
                                       target="_blank"
                                       rel="noopener noreferrer"
                                       aria-label="Open standalone in new tab"
                                       onClick={(event) =>
                                          event.stopPropagation()
                                       }
                                       sx={{ color: "text.secondary" }}
                                    >
                                       <OpenInNewIcon fontSize="small" />
                                    </IconButton>
                                 </Tooltip>
                              }
                           />
                        );
                     })}
                  </PackageSection>
               )}

               <PackageSection title="Semantic Models" count={models.length}>
                  {models.map((model) => (
                     <PackageItemRow
                        key={model.path}
                        icon={<ContentTypeIcon type="model" />}
                        tint={MALLOY_BRAND.orange}
                        label={model.path}
                        onClick={(event) =>
                           onClick(
                              `/${environmentName}/${packageName}/${model.path}`,
                              event,
                           )
                        }
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
                        rightLabel={
                           // A file the server could not probe is listed with
                           // `error` and no `info`; show that instead of a
                           // row count rather than reading through undefined.
                           database.info
                              ? formatRowCount(database.info.rowCount)
                              : "Unreadable"
                        }
                        onClick={() => setSchemaDatabase(database)}
                     />
                  ))}
                  {databases.length === 0 && <EmptyRow label="No data files" />}
               </PackageSection>

               <PackageSection title="Materializations">
                  <PackageItemRow
                     icon={<ContentTypeIcon type="materialization" />}
                     tint={MALLOY_BRAND.teal}
                     label="Materializations"
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
                        onNavigate={onClick}
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
               {schemaDatabase?.error && (
                  <Typography variant="body2" color="error">
                     {schemaDatabase.error}
                  </Typography>
               )}
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
   trailingAction,
}: {
   icon: React.ReactNode;
   tint: string;
   label: string;
   rightLabel?: string;
   onClick?: (event: React.MouseEvent) => void;
   /** Optional element rendered at the end of the row (e.g. an
    *  "open in new tab" icon button). Clicks on it should
    *  `event.stopPropagation()` so the row click doesn't also fire. */
   trailingAction?: React.ReactNode;
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
         sx={(theme) => ({
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
               ? {
                    backgroundColor:
                       theme.palette.mode === "dark"
                          ? "rgba(255, 255, 255, 0.08)"
                          : "grey.100",
                 }
               : undefined,
            "&:focus-visible": interactive
               ? {
                    outline: "2px solid",
                    outlineColor: "primary.main",
                    outlineOffset: 2,
                 }
               : undefined,
         })}
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
         {trailingAction && (
            <Box sx={{ flexShrink: 0, display: "flex", alignItems: "center" }}>
               {trailingAction}
            </Box>
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
