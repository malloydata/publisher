// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import ArrowDropDownIcon from "@mui/icons-material/ArrowDropDown";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import {
   Alert,
   Box,
   Button,
   Container,
   IconButton,
   Menu,
   MenuItem,
   Stack,
   Table,
   TableBody,
   TableCell,
   TableHead,
   TableRow,
   Tooltip,
   Typography,
} from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Database } from "../../client";
import { useNarrowScreen } from "../../hooks/useNarrowScreen";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { chooseWorkspace } from "../DashboardBuilder/documentSession";
import { createRoute, type CreateTarget } from "../DocumentCreate";
import type {
   DocumentLocator,
   DocumentType,
   Workspace,
} from "../DocumentStorage/DocumentStorage";
import { useOptionalDocumentStorage } from "../DocumentStorage/DocumentStorageProvider";
import { Notebook } from "../Notebook";
import { useServer } from "../ServerProvider";
import { NewDocumentDialog } from "./NewDocumentDialog";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { serverBaseUrl } from "../../utils/dataAppEmbed";
import ContentTypeIcon, {
   CONTENT_TINT,
   type ContentType,
} from "./ContentTypeIcon";
import { AppDialog } from "../AppDialog";
import { ItemRow } from "../ItemRow";
import { Materializations } from "../Materializations";
import { PackageSection } from "../PackageSection";
import {
   documentRoute,
   documentSlug,
   type DocumentKind,
} from "./documentLocation";

// The pinned README: the root `.malloynb`, or a served notebook named README in `notebooks/`.
const README_NOTEBOOK = "README.malloynb";
const isServedReadme = (path: string | undefined) =>
   path?.toLowerCase() === "notebooks/readme.malloy";

const draftPrefix = (
   environmentName: string,
   packageName: string,
   kind: DocumentType,
) => `${environmentName}/${packageName}/${kind}s/`;

/** A served notebook opens by slug, like a dashboard; a `.malloynb` opens by path. */

/** A dashboard or notebook the package page asks its host to open, and how. */
export interface OpenDocumentRequest {
   kind: DocumentKind;
   slug: string;
   /** `edit` for a document just created or a draft picked up; `view` otherwise. */
   mode: "view" | "edit";
}

interface PackageProps {
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
   /**
    * Open a dashboard or notebook, to read or to edit. Where reading and
    * editing live is the host's business, not the package page's, so a host
    * with a builder of its own routes this. Absent, the page navigates to the
    * Console's own routes: the document's, and `/edit` under it to edit.
    */
   onOpenDocument?: (
      request: OpenDocumentRequest,
      event?: React.MouseEvent,
   ) => void;
   resourceUri: string;
}

export default function Package({
   onClickPackageFile,
   onOpenDocument,
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
   const openDocument =
      onOpenDocument ??
      ((request: OpenDocumentRequest, event?: React.MouseEvent) => {
         const to = `${documentRoute(environmentName, packageName, request.kind, request.slug)}${request.mode === "edit" ? "/edit" : ""}`;
         // Without an event, called exactly as before this hook existed.
         if (event) onClick(to, event);
         else onClick(to);
      });

   const [schemaDatabase, setSchemaDatabase] = useState<Database | null>(null);
   const [creating, setCreating] = useState<DocumentType | undefined>(
      undefined,
   );
   const [newMenu, setNewMenu] = useState<HTMLElement | null>(null);
   const narrow = useNarrowScreen();

   // Dashboards and notebooks the host keeps for this package and the package
   // does not have: the builder's drafts, listed so they are found rather than
   // stumbled on.
   // Each carries the workspace's own description, because where a document is
   // kept is the backend's to say and only one host's answer is "this browser".
   const storage = useOptionalDocumentStorage()?.documentStorage;
   const [drafts, setDrafts] = useState<
      { locator: DocumentLocator; where: string }[]
   >([]);
   // Where a create may land is the chosen workspace's to say, so New waits
   // for the answer; a host whose workspaces cannot be listed gets no New
   // rather than a guess that could write the package under its record.
   const [workspace, setWorkspace] = useState<
      { state: "pending" | "failed" } | { state: "ready"; chosen?: Workspace }
   >({ state: storage ? "pending" : "ready" });
   const refreshDrafts = useCallback(async () => {
      if (!storage) return;
      let workspaces: Workspace[];
      try {
         workspaces = await storage.listWorkspaces(false);
      } catch {
         setWorkspace({ state: "failed" });
         return;
      }
      setWorkspace({ state: "ready", chosen: chooseWorkspace(workspaces) });
      const found: { locator: DocumentLocator; where: string }[] = [];
      for (const candidate of workspaces.filter((w) => w.writeable))
         for (const kind of ["dashboard", "notebook"] as const)
            for (const locator of await storage.listDocuments(candidate, kind))
               if (
                  locator.path.startsWith(
                     draftPrefix(environmentName, packageName, kind),
                  )
               )
                  found.push({ locator, where: candidate.description });
      setDrafts(found);
   }, [storage, environmentName, packageName]);
   useEffect(() => {
      void refreshDrafts();
   }, [refreshDrafts]);
   const draftSlug = (locator: DocumentLocator) =>
      locator.path
         .slice(draftPrefix(environmentName, packageName, locator.type).length)
         .replace(/\.malloy$/, "");

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
      queryFn: async () => {
         try {
            return await apiClients.notebooks.listNotebooks(
               environmentName,
               packageName,
               versionId,
            );
         } catch (e) {
            // Non-fatal like the dashboards list: an older Publisher without
            // the route has no notebooks to list.
            const status = (e as { response?: { status?: number } })?.response
               ?.status;
            if (status === 404 || status === undefined) {
               return { data: [] } as Awaited<
                  ReturnType<typeof apiClients.notebooks.listNotebooks>
               >;
            }
            throw e;
         }
      },
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
   // No versionId in the key: this lists the apps of the version a request
   // naming none gets, which is also what DataAppViewer asks for, so the two
   // identical queries dedupe. (The route accepts a versionId now; this page
   // does not pin one.)
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
   const dashboards = dashboardsQuery.data?.data ?? [];
   const notebooks = (notebooksQuery.data?.data ?? [])
      .slice()
      .sort((a, b) => (a.path ?? "").localeCompare(b.path ?? ""));
   const artifacts = artifactRows(dashboards, notebooks);
   // A dashboard is listed once, under Artifacts. Its file is a model like any
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

   // A non-authoritative browser workspace is never a create target: a
   // document made there would look saved and exist for one reader only.
   const route =
      workspace.state === "ready"
         ? createRoute({
              authoritative: workspace.chosen?.authoritative === true,
              mutable,
              canStore: workspace.chosen?.writeable === true,
              ...(versionId === undefined ? {} : { versionId }),
           })
         : undefined;
   // A name is chosen against the listings, so New waits for all three rather than offering names a still-loading listing would have taken.
   const listingsSettled = [dashboardsQuery, notebooksQuery, modelsQuery].every(
      (q) => q.isSuccess || q.isError,
   );
   const canCreate = route !== undefined && listingsSettled;
   // The editors step aside on a narrow screen, so no entry point to one is offered there.
   const canNew = canCreate && !narrow;
   const createTarget = useMemo((): CreateTarget | undefined => {
      if (!canCreate) return undefined;
      const existing = [
         ...(dashboardsQuery.data?.data ?? []).flatMap((d) =>
            d.path ? [d.path] : [],
         ),
         ...(notebooksQuery.data?.data ?? []).flatMap((n) =>
            n.path ? [n.path] : [],
         ),
         ...(modelsQuery.data?.data ?? []).flatMap((m) =>
            m.path ? [m.path] : [],
         ),
      ];
      if (route === "storage" && storage && workspace.state === "ready") {
         if (!workspace.chosen) return undefined;
         return {
            route,
            storage,
            workspace: workspace.chosen,
            environmentName,
            packageName,
            existing,
         };
      }
      if (route !== "package") return undefined;
      return {
         route,
         existing,
         write: async (path, source) => {
            await apiClients.models.updateModelSource(
               environmentName,
               packageName,
               path,
               { source },
            );
         },
      };
   }, [
      canCreate,
      route,
      storage,
      workspace,
      environmentName,
      packageName,
      apiClients,
      dashboardsQuery.data,
      notebooksQuery.data,
      modelsQuery.data,
   ]);

   const description = pkgQuery.data?.data?.description ?? "";
   // The root `.malloynb` wins when both exist, so a listing order never flips the pin.
   const readmePath = (
      notebooks.find((n) => n.path === README_NOTEBOOK) ??
      notebooks.find((n) => isServedReadme(n.path))
   )?.path;
   const readmeResourceUri = encodeResourceUri({
      environmentName,
      packageName,
      versionId,
      modelPath: readmePath,
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

         <Menu
            anchorEl={newMenu}
            open={newMenu !== null}
            onClose={() => setNewMenu(null)}
         >
            {(["dashboard", "notebook"] as const).map((kind) => (
               <MenuItem
                  key={kind}
                  onClick={() => {
                     setNewMenu(null);
                     setCreating(kind);
                  }}
               >
                  {kind === "dashboard" ? "Dashboard" : "Notebook"}
               </MenuItem>
            ))}
         </Menu>

         {createTarget && (
            <NewDocumentDialog
               open={creating !== undefined}
               kind={creating ?? "dashboard"}
               environmentName={environmentName}
               packageName={packageName}
               {...(versionId !== undefined ? { versionId } : {})}
               models={models
                  .map((model) => model.path)
                  .filter(
                     (path): path is string =>
                        typeof path === "string" &&
                        path.endsWith(".malloy") &&
                        !path.startsWith("dashboards/") &&
                        !path.startsWith("notebooks/"),
                  )}
               modelsLoading={modelsQuery.isPending}
               {...(modelsQuery.isError
                  ? { modelsError: modelsQuery.error }
                  : {})}
               onRetryModels={() => void modelsQuery.refetch()}
               target={createTarget}
               onClose={() => setCreating(undefined)}
               onCreated={(created) => {
                  setCreating(undefined);
                  for (const key of ["dashboards", "notebooks", "models"])
                     void queryClient.invalidateQueries({ queryKey: [key] });
                  openDocument({
                     kind: created.kind,
                     slug: created.slug,
                     mode: "edit",
                  });
               }}
            />
         )}

         {!isLoading && (
            <>
               {/* A listing that FAILED renders identically to a package with
                   none: the list falls back to `[]` and the section hides
                   itself. Only `pkgQuery` reaches the error page, so nothing
                   else here would say a word, and one failed list hides only
                   half the artifacts. The 404 swallowed in each query is an
                   older Publisher with no route, which genuinely has none. */}
               {(dashboardsQuery.isError || notebooksQuery.isError) && (
                  <Box sx={{ mb: 4 }}>
                     <Alert severity="warning">
                        Could not list some artifacts, so any this package has
                        are missing from this page.
                     </Alert>
                  </Box>
               )}
               {(artifacts.length > 0 || canCreate) && (
                  <PackageSection
                     title="Artifacts"
                     count={artifacts.length}
                     {...(canNew
                        ? {
                             action: (
                                <Button
                                   variant="outlined"
                                   size="small"
                                   endIcon={<ArrowDropDownIcon />}
                                   aria-haspopup="menu"
                                   aria-label="New"
                                   onClick={(event) =>
                                      setNewMenu(event.currentTarget)
                                   }
                                >
                                   New
                                </Button>
                             ),
                          }
                        : {})}
                  >
                     {artifacts.length === 0 && (
                        <EmptyRow
                           label="No artifacts yet"
                           {...(canNew
                              ? {
                                   action: {
                                      label: "New artifact",
                                      onClick: (event) =>
                                         setNewMenu(event.currentTarget),
                                   },
                                }
                              : {})}
                        />
                     )}
                     {artifacts.map((artifact) => (
                        <PackageItemRow
                           key={`${artifact.kind}:${artifact.path}`}
                           type={
                              artifact.kind === "dashboard"
                                 ? "dashboard"
                                 : "report"
                           }
                           label={artifact.label}
                           {...(artifact.secondary === undefined
                              ? {}
                              : { description: artifact.secondary })}
                           onClick={(event) =>
                              artifact.slug === undefined
                                 ? onClick(
                                      `/${environmentName}/${packageName}/${artifact.path}`,
                                      event,
                                   )
                                 : openDocument(
                                      {
                                         kind: artifact.kind,
                                         slug: artifact.slug,
                                         mode: "view",
                                      },
                                      event,
                                   )
                           }
                        />
                     ))}
                  </PackageSection>
               )}
               {drafts.length > 0 && (
                  <PackageSection title="Drafts" count={drafts.length}>
                     {drafts.map(({ locator, where }) => (
                        <PackageItemRow
                           key={`${locator.type}:${locator.path}`}
                           type={
                              locator.type === "dashboard"
                                 ? "dashboard"
                                 : "report"
                           }
                           label={draftSlug(locator)}
                           rightLabel={where}
                           onClick={(event) =>
                              openDocument(
                                 {
                                    kind: locator.type,
                                    slug: draftSlug(locator),
                                    mode: "edit",
                                 },
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

               {readmePath && (
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
   description,
   rightLabel,
   onClick,
   trailingAction,
}: {
   type: ContentType;
   label: string;
   description?: string;
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
         {...(description === undefined ? {} : { description })}
         {...(rightLabel === undefined ? {} : { rightLabel })}
         {...(onClick === undefined ? {} : { onClick })}
         {...(trailingAction === undefined ? {} : { trailingAction })}
      />
   );
}

interface ArtifactRow {
   kind: DocumentKind;
   path: string;
   /** Absent for a `.malloynb`, which opens by path. */
   slug: string | undefined;
   label: string;
   /** The path, when the label alone does not say which file this is. */
   secondary: string | undefined;
}

/** Dashboards and notebooks as one list: the title when set, else the slug, ordered by what is shown. */
function artifactRows(
   dashboards: readonly { name?: string; path?: string; title?: string }[],
   notebooks: readonly { path?: string; title?: string }[],
): ArtifactRow[] {
   const rows = [
      ...dashboards.map((d) => ({
         kind: "dashboard" as const,
         path: d.path ?? "",
         slug: d.name ?? documentSlug(d.path),
         title: d.title,
      })),
      ...notebooks.map((n) => ({
         kind: "notebook" as const,
         path: n.path ?? "",
         slug: documentSlug(n.path),
         title: n.title,
      })),
   ];
   const slugCount = new Map<string, number>();
   for (const row of rows)
      if (row.slug !== undefined)
         slugCount.set(row.slug, (slugCount.get(row.slug) ?? 0) + 1);
   return rows
      .map(({ title, ...row }): ArtifactRow => {
         // A title equal to the slug or path is the server's fallback for a file that names itself neither way.
         const hasTitle = !!title && title !== row.slug && title !== row.path;
         const ambiguous =
            row.slug !== undefined && (slugCount.get(row.slug) ?? 0) > 1;
         return {
            ...row,
            label: hasTitle ? title : (row.slug ?? row.path),
            secondary: (row.slug === undefined ? hasTitle : ambiguous)
               ? row.path
               : undefined,
         };
      })
      .sort(
         (a, b) =>
            a.label.localeCompare(b.label) || a.path.localeCompare(b.path),
      );
}

function EmptyRow({
   label,
   action,
}: {
   label: string;
   action?: {
      label: string;
      onClick: (event: React.MouseEvent<HTMLElement>) => void;
   };
}) {
   return (
      <Stack direction="row" sx={{ alignItems: "center", gap: 1, py: 1 }}>
         <Typography
            variant="body2"
            color="text.secondary"
            sx={{ fontStyle: "italic" }}
         >
            {label}
         </Typography>
         {action && (
            <Button size="small" onClick={action.onClick}>
               {action.label}
            </Button>
         )}
      </Stack>
   );
}

function formatRowCount(rows: number): string {
   if (rows >= 1_000_000_000)
      return `${(rows / 1_000_000_000).toFixed(1)} B rows`;
   if (rows >= 1_000_000) return `${(rows / 1_000_000).toFixed(1)} M rows`;
   if (rows >= 1_000) return `${(rows / 1_000).toFixed(1)} K rows`;
   return `${rows} rows`;
}
