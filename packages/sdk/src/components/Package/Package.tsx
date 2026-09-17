// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import {
   Alert,
   Box,
   Container,
   IconButton,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Tooltip,
   Typography,
} from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import React, { useCallback, useEffect, useState } from "react";
import { Database } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import type { DocumentLocator } from "../DocumentStorage/DocumentStorage";
import { useOptionalDocumentStorage } from "../DocumentStorage/DocumentStorageProvider";
import { Notebook } from "../Notebook";
import { useServer } from "../ServerProvider";
import { NewDashboardDialog } from "./NewDashboardDialog";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { serverBaseUrl } from "../../utils/dataAppEmbed";
import ContentTypeIcon, {
   CONTENT_TINT,
   type ContentType,
} from "./ContentTypeIcon";
import { AddButton } from "../buttons";
import { AppDialog } from "../AppDialog";
import { BackLink } from "../BackLink";
import { ItemRow } from "../ItemRow";
import { Materializations } from "../Materializations";
import { PackageSection } from "../PackageSection";

const README_NOTEBOOK = "README.malloynb";

interface PackageProps {
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Package({
   onClickPackageFile,
   resourceUri,
}: PackageProps) {
   const { apiClients, server, mutable } = useServer();
   const queryClient = useQueryClient();
   const onClick =
      onClickPackageFile ??
      ((to: string) => {
         window.location.href = to;
      });
   const { environmentName, packageName, versionId } =
      parseResourceUri(resourceUri);

   const [schemaDatabase, setSchemaDatabase] = useState<Database | null>(null);
   const [creating, setCreating] = useState(false);

   // Dashboards the host keeps for this package and the package does not have:
   // the builder's drafts, listed so they are found rather than stumbled on.
   // Each carries the workspace's own description, because where a document is
   // kept is the backend's to say and only one host's answer is "this browser".
   const storage = useOptionalDocumentStorage()?.documentStorage;
   const [drafts, setDrafts] = useState<
      { locator: DocumentLocator; where: string }[]
   >([]);
   const draftPrefix = `${environmentName}/${packageName}/dashboards/`;
   const refreshDrafts = useCallback(async () => {
      if (!storage) return;
      const workspaces = await storage.listWorkspaces(true);
      const found: { locator: DocumentLocator; where: string }[] = [];
      for (const workspace of workspaces)
         for (const locator of await storage.listDocuments(
            workspace,
            "dashboard",
         ))
            if (locator.path.startsWith(draftPrefix))
               found.push({ locator, where: workspace.description });
      setDrafts(found);
   }, [storage, draftPrefix]);
   useEffect(() => {
      void refreshDrafts();
   }, [refreshDrafts]);
   const draftSlug = (locator: DocumentLocator) =>
      locator.path.slice(draftPrefix.length).replace(/\.malloy$/, "");

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

   const dashboardsQuery = useQueryWithApiError({
      queryKey: ["dashboards", environmentName, packageName, versionId],
      queryFn: async () => {
         try {
            return await apiClients.dashboards.listDashboards(
               environmentName,
               packageName,
               versionId,
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
   // Sorted by the string the row actually SHOWS, not by the slug or the path
   // underneath it. A list labelled by title and ordered by filename reads as
   // unsorted: `overview` titled "Business Overview" sorts ahead of `regions`
   // titled "Regional Sales". Notebooks acquired that mismatch here too, when
   // they started being listed by title while still being ordered by path.
   const dashboardLabel = (dashboard: { name?: string; title?: string }) =>
      dashboard.title && dashboard.title !== dashboard.name
         ? dashboard.title
         : (dashboard.name ?? "");
   const notebookLabel = (notebook: { path?: string; title?: string }) =>
      notebook.title && notebook.title !== notebook.path
         ? notebook.title
         : (notebook.path ?? "");

   const dashboards = (dashboardsQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => dashboardLabel(a).localeCompare(dashboardLabel(b)));

   const notebooks = (notebooksQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => notebookLabel(a).localeCompare(notebookLabel(b)));
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

   // The dashboards list is part of the gate, not just the notebooks one,
   // because the models list is FILTERED by it. Gated on notebooks alone, a
   // dashboards call that resolves after the models call rendered the sections
   // with an empty `dashboardPaths`, so `dashboards/overview.malloy` appeared
   // under Semantic Models and then vanished: the double listing the filter
   // exists to prevent, briefly on screen.
   //
   // What this covers and what it COSTS, since the two are different questions.
   // It cannot hang the page on a FAILURE: the query above turns a 404 or a
   // transport failure into an empty list. It does add LATENCY, and to every
   // section rather than to the one that needs it: Notebooks, Semantic Models,
   // Data Apps and Databases now wait on the dashboards call where they used to
   // render as soon as notebooks resolved. The only thing that actually needs
   // the gate is the `dashboardPaths` filter on the models list.
   //
   // Accepted rather than narrowed because the cost is bounded and small:
   // `listDashboards` is a synchronous read off the already-loaded package
   // (`service/package.ts`, reached with `getPackage(name, false)`, so no
   // reload), which makes this one more parallel request and not a compile.
   // Narrowing it means gating only the models section, which trades this
   // whole-page wait for a models list that pops in after its neighbours.
   const isLoading =
      (!notebooksQuery.isSuccess && !notebooksQuery.isError) ||
      (!dashboardsQuery.isSuccess && !dashboardsQuery.isError);

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
            <BackLink
               label={environmentName}
               onClick={(event) => onClick(`/${environmentName}/`, event)}
            />
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

         <NewDashboardDialog
            open={creating}
            environmentName={environmentName}
            packageName={packageName}
            models={models
               .map((model) => model.path)
               .filter(
                  (path): path is string =>
                     typeof path === "string" &&
                     path.endsWith(".malloy") &&
                     !path.startsWith("dashboards/"),
               )}
            existing={dashboards
               .map((dashboard) => dashboard.name)
               .filter((name): name is string => typeof name === "string")}
            onClose={() => setCreating(false)}
            onCreated={(slug) => {
               setCreating(false);
               void queryClient.invalidateQueries({ queryKey: ["dashboards"] });
               onClick(
                  `/${environmentName}/${packageName}/dashboards/${encodeURIComponent(slug)}/edit`,
               );
            }}
         />

         {!isLoading && (
            <>
               {/* First: the at-a-glance artifact a visitor most likely wants,
                   ahead of the notebooks and models it is built on. Hidden when
                   empty, like Data Apps. */}
               {/* A listing that FAILED renders identically to a package with
                   no dashboards: `dashboards` falls back to `[]` and the
                   section hides itself. Only `pkgQuery` reaches the error page,
                   so nothing else here would say a word. Now that the listing
                   carries a `versionId`, a host that resolves it on some routes
                   and refuses it on others has a new way in, and "this package
                   has no dashboards" is the wrong thing to conclude from a
                   transport error. The 404 above stays swallowed: that one is
                   an older Publisher with no route, which genuinely has none. */}
               {dashboardsQuery.isError && (
                  <Box sx={{ mb: 4 }}>
                     <Alert severity="warning">
                        Could not list this package&apos;s dashboards, so any it
                        has are missing from this page.
                     </Alert>
                  </Box>
               )}
               {(dashboards.length > 0 || mutable) && (
                  <PackageSection
                     title="Dashboards"
                     count={dashboards.length}
                     action={
                        mutable ? (
                           <AddButton
                              label="Dashboard"
                              onClick={() => setCreating(true)}
                           />
                        ) : undefined
                     }
                  >
                     {dashboards.length === 0 && (
                        <EmptyRow label="No dashboards yet" />
                     )}
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
                              type="dashboard"
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
               {drafts.length > 0 && (
                  <PackageSection title="Drafts" count={drafts.length}>
                     {drafts.map(({ locator, where }) => (
                        <PackageItemRow
                           key={locator.path}
                           type="dashboard"
                           label={draftSlug(locator)}
                           rightLabel={where}
                           onClick={(event) =>
                              onClick(
                                 `/${environmentName}/${packageName}/dashboards/${encodeURIComponent(draftSlug(locator))}/edit`,
                                 event,
                              )
                           }
                           trailingAction={
                              <Tooltip title="Delete this draft">
                                 <IconButton
                                    size="small"
                                    aria-label={`Delete draft ${draftSlug(locator)}`}
                                    onClick={(event) => {
                                       event.stopPropagation();
                                       void storage
                                          ?.deleteDocument(locator)
                                          .then(refreshDrafts);
                                    }}
                                 >
                                    <DeleteOutlineIcon fontSize="small" />
                                 </IconButton>
                              </Tooltip>
                           }
                        />
                     ))}
                  </PackageSection>
               )}

               {/* Hidden when empty, like Dashboards and Data Apps. A package
                   that holds no notebooks is not a package missing them, and a
                   heading over the words "No notebooks" is a row of furniture
                   saying nothing. */}
               {notebooks.length > 0 && (
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
                              type="report"
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
                  </PackageSection>
               )}

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
                              type="dataApp"
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
                                    // No host app: navigate to standalone HTML.
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
                        type="model"
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
                        type="data"
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

               <Materializations resourceUri={resourceUri} />

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

         <AppDialog
            open={schemaDatabase !== null}
            onClose={() => setSchemaDatabase(null)}
            title={schemaDatabase?.path ?? "Columns"}
            showClose
         >
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
         </AppDialog>
      </Container>
   );
}

function PackageItemRow({
   type,
   label,
   rightLabel,
   onClick,
   trailingAction,
}: {
   type: ContentType;
   label: string;
   rightLabel?: string;
   onClick?: (event: React.MouseEvent) => void;
   /** Optional element rendered at the end of the row (e.g. an
    *  "open in new tab" icon button). Clicks on it should
    *  `event.stopPropagation()` so the row click doesn't also fire. */
   trailingAction?: React.ReactNode;
}) {
   return (
      <ItemRow
         icon={<ContentTypeIcon type={type} />}
         tint={CONTENT_TINT[type]}
         label={label}
         mono
         {...(rightLabel === undefined ? {} : { rightLabel })}
         {...(onClick === undefined ? {} : { onClick })}
         {...(trailingAction === undefined ? {} : { trailingAction })}
      />
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
