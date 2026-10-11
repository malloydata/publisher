// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import {
   Alert,
   Box,
   Button,
   Container,
   IconButton,
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
   Workspace,
} from "../DocumentStorage/DocumentStorage";
import { useOptionalDocumentStorage } from "../DocumentStorage/DocumentStorageProvider";
import { Report } from "../Report";
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
   documentSlug,
   kindOfLayout,
   reportRoute,
   type DocumentKind,
   type ListedReport,
   type ReportLayout,
} from "./documentLocation";
import { fetchReportListing, reportsQueryKey } from "./useDocumentLocation";

/**
 * The pinned README: a report at the package root whose basename is README,
 * in any case. The legacy root `README.malloynb` wins when both exist, so a
 * listing order never flips the pin.
 */
const README_NOTEBOOK = "README.malloynb";
const isRootReadme = (path: string | undefined) =>
   !!path &&
   !path.includes("/") &&
   documentSlug(path)?.toLowerCase() === "readme";

/** Drafts are keyed by the report's path under the package: `<env>/<pkg>/<path>`. */
const draftPrefix = (environmentName: string, packageName: string) =>
   `${environmentName}/${packageName}/`;

/** A report the package page asks its host to open, and how. */
export interface OpenDocumentRequest {
   /** The report's package path, `sales/q3.malloy`: its identity, and how to open it. */
   path: string;
   layout: ReportLayout;
   /** @deprecated The layout's legacy name; read `layout`. */
   kind: DocumentKind;
   /** @deprecated The basename, a label only: two reports may share it. Open by `path`. */
   slug: string;
   /** `edit` for a document just created or a draft picked up; `view` otherwise. */
   mode: "view" | "edit";
}

interface PackageProps {
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
   /**
    * Open a report, to read or to edit. Where reading and editing live is the
    * host's business, not the package page's, so a host with a builder of its
    * own routes this. Absent, the page navigates to the Console's own routes:
    * the report's path, and `/edit` under it to edit.
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
         const to = `${reportRoute(environmentName, packageName, request.path)}${request.mode === "edit" ? "/edit" : ""}`;
         // Without an event, called exactly as before this hook existed.
         if (event) onClick(to, event);
         else onClick(to);
      });

   const [schemaDatabase, setSchemaDatabase] = useState<Database | null>(null);
   const [creating, setCreating] = useState(false);
   const narrow = useNarrowScreen();

   // Reports the host keeps for this package and the package does not have: the builder's drafts, listed so they are found rather than
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
      // One listing per workspace, every type: a draft is found by its path under this package, whatever layout it was filed as.
      for (const candidate of workspaces.filter((w) => w.writeable))
         for (const locator of await storage.listDocuments(candidate))
            if (
               locator.path.startsWith(
                  draftPrefix(environmentName, packageName),
               )
            )
               found.push({ locator, where: candidate.description });
      setDrafts(found);
   }, [storage, environmentName, packageName]);
   useEffect(() => {
      void refreshDrafts();
   }, [refreshDrafts]);
   const draftPath = (locator: DocumentLocator) =>
      locator.path.slice(draftPrefix(environmentName, packageName).length);

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

   // One listing for both layouts, by path.
   // One listing for both layouts, by path. Shares its cache entry with every
   // other reader of the listing, so the shape is the shared fetcher's.
   const reportsQuery = useQueryWithApiError({
      queryKey: reportsQueryKey(environmentName, packageName, versionId),
      queryFn: () =>
         fetchReportListing(
            apiClients,
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

   const reports: ListedReport[] = (reportsQuery.data ?? []).filter(
      (r) => !!r.path,
   );
   const artifacts = reportRows(reports);
   // A report is listed once, under Reports. Its file is a model like any
   // other, so it would otherwise appear a second time under Semantic Models
   // where clicking it opens the Explorer rather than the report. The listed
   // paths decide it, never a directory name.
   const reportPaths = new Set(
      reports
         .map((report) => report.path)
         .filter((path): path is string => path !== undefined),
   );
   const models = (modelsQuery.data?.data ?? [])
      .slice()
      .filter((model) => !reportPaths.has(model.path))
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
   // A name is chosen against the listings, so New waits for both rather than offering names a still-loading listing would have taken.
   const listingsSettled = [reportsQuery, modelsQuery].every(
      (q) => q.isSuccess || q.isError,
   );
   const canCreate = route !== undefined && listingsSettled;
   // The editors step aside on a narrow screen, so no entry point to one is offered there.
   const canNew = canCreate && !narrow;
   const createTarget = useMemo((): CreateTarget | undefined => {
      if (!canCreate) return undefined;
      const existing = [
         ...(reportsQuery.data ?? []).flatMap((r) => (r.path ? [r.path] : [])),
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
      reportsQuery.data,
      modelsQuery.data,
   ]);

   const description = pkgQuery.data?.data?.description ?? "";
   const readmePath = (
      reports.find((r) => r.path === README_NOTEBOOK) ??
      reports.find((r) => isRootReadme(r.path))
   )?.path;

   // The models list is FILTERED by the report listing, so every section waits
   // for it: gated on anything less, a listing that resolved after the models
   // call showed a report under Semantic Models and then took it away. A
   // FAILURE cannot hang the page, since the query turns a 404 or a transport
   // failure into an empty list; the cost is one parallel request's latency.
   const isLoading = !reportsQuery.isSuccess && !reportsQuery.isError;

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

         {createTarget && (
            <NewDocumentDialog
               open={creating}
               layout="linear"
               environmentName={environmentName}
               packageName={packageName}
               {...(versionId !== undefined ? { versionId } : {})}
               models={models
                  .map((model) => model.path)
                  .filter(
                     (path): path is string =>
                        typeof path === "string" &&
                        path.endsWith(".malloy") &&
                        !reportPaths.has(path),
                  )}
               modelsLoading={modelsQuery.isPending}
               {...(modelsQuery.isError
                  ? { modelsError: modelsQuery.error }
                  : {})}
               onRetryModels={() => void modelsQuery.refetch()}
               target={createTarget}
               onClose={() => setCreating(false)}
               onCreated={(created) => {
                  setCreating(false);
                  for (const key of [
                     "reports",
                     "dashboards",
                     "notebooks",
                     "models",
                  ])
                     void queryClient.invalidateQueries({ queryKey: [key] });
                  openDocument({
                     path: created.path,
                     layout: created.layout,
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
               {reportsQuery.isError && (
                  <Box sx={{ mb: 4 }}>
                     <Alert severity="warning">
                        Could not list this package&apos;s reports, so any it
                        has are missing from this page.
                     </Alert>
                  </Box>
               )}
               {(artifacts.length > 0 || canCreate) && (
                  <PackageSection
                     title="Reports"
                     count={artifacts.length}
                     {...(canNew
                        ? {
                             action: (
                                <Button
                                   variant="outlined"
                                   size="small"
                                   onClick={() => setCreating(true)}
                                >
                                   New report
                                </Button>
                             ),
                          }
                        : {})}
                  >
                     {artifacts.length === 0 && (
                        <EmptyRow
                           label="No reports yet"
                           {...(canNew
                              ? {
                                   action: {
                                      label: "New report",
                                      onClick: () => setCreating(true),
                                   },
                                }
                              : {})}
                        />
                     )}
                     {artifacts.map((artifact) => (
                        <PackageItemRow
                           key={artifact.path}
                           type={
                              artifact.layout === "grid"
                                 ? "dashboard"
                                 : "report"
                           }
                           label={artifact.label}
                           rightLabel={artifact.layout}
                           {...(artifact.secondary === undefined
                              ? {}
                              : { description: artifact.secondary })}
                           onClick={(event) =>
                              artifact.path.endsWith(".malloynb")
                                 ? onClick(
                                      `/${environmentName}/${packageName}/${artifact.path}`,
                                      event,
                                   )
                                 : openDocument(
                                      {
                                         path: artifact.path,
                                         layout: artifact.layout,
                                         kind: kindOfLayout(artifact.layout),
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
                           key={`${locator.workspace}:${locator.path}`}
                           type={
                              locator.type === "dashboard"
                                 ? "dashboard"
                                 : "report"
                           }
                           label={draftPath(locator)}
                           rightLabel={where}
                           onClick={(event) =>
                              openDocument(
                                 {
                                    path: draftPath(locator),
                                    layout:
                                       locator.type === "dashboard"
                                          ? "grid"
                                          : "linear",
                                    kind: locator.type,
                                    slug:
                                       documentSlug(draftPath(locator)) ?? "",
                                    mode: "edit",
                                 },
                                 event,
                              )
                           }
                           trailingAction={
                              <Tooltip title="Delete this draft">
                                 <IconButton
                                    size="small"
                                    aria-label={`Delete draft ${draftPath(locator)}`}
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
                     <Report
                        resourceUri={encodeResourceUri({
                           environmentName,
                           packageName,
                           versionId,
                        })}
                        path={readmePath}
                        onLinkNavigate={onClick}
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

interface ReportRow {
   path: string;
   layout: ReportLayout;
   /** The basename, a label. */
   slug: string;
   label: string;
   /** The path, when the label alone does not say which file this is. */
   secondary: string | undefined;
}

/** Every report as one list: the title when set, else the basename, ordered by what is shown. */
function reportRows(reports: readonly ListedReport[]): ReportRow[] {
   const rows = reports.map((r) => ({
      path: r.path ?? "",
      layout: (r.layout === "linear" ? "linear" : "grid") as ReportLayout,
      slug: documentSlug(r.path) ?? r.path ?? "",
      title: r.title,
   }));
   const slugCount = new Map<string, number>();
   for (const row of rows)
      slugCount.set(row.slug, (slugCount.get(row.slug) ?? 0) + 1);
   return rows
      .map(({ title, ...row }): ReportRow => {
         // A title equal to the basename or path is the server's fallback for a file that names itself neither way.
         const hasTitle = !!title && title !== row.slug && title !== row.path;
         // The path is shown whenever the label alone does not name the file: a title, a basename two reports share, or a report below the root.
         const ambiguous = (slugCount.get(row.slug) ?? 0) > 1;
         // A legacy `.malloynb` with no title of its own is named by its file, extension included.
         const label = hasTitle
            ? title
            : row.path.endsWith(".malloynb")
              ? row.path
              : row.slug;
         return {
            ...row,
            label,
            secondary:
               label !== row.path &&
               (hasTitle || ambiguous || row.path.includes("/"))
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
