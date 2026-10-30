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
   Tooltip,
   Typography,
} from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Database, type AgentSummary } from "../../client";
import { useNarrowScreen } from "../../hooks/useNarrowScreen";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { AddButton } from "../buttons";
import { PackageProblems } from "./PackageProblems";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { chooseWorkspace } from "../ReportBuilder/documentSession";
import { createRoute, type CreateTarget } from "../DocumentCreate";
import type {
   DocumentLocator,
   Workspace,
} from "../DocumentStorage/DocumentStorage";
import { useOptionalDocumentStorage } from "../DocumentStorage/DocumentStorageProvider";
import { Report } from "../Report";
import { useServer } from "../ServerProvider";
import { NewDocumentDialog } from "./NewDocumentDialog";
import type { ReportEventHandler } from "../Report/telemetry";
import { DataSchemaDialog, formatRowCount } from "./DataSchemaDialog";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { serverBaseUrl } from "../../utils/dataAppEmbed";
import ContentTypeIcon, {
   CONTENT_TINT,
   type ContentType,
} from "./ContentTypeIcon";
import { ItemRow } from "../ItemRow";
import { Materializations } from "../Materializations";
import { describeCron } from "../Materializations/cron";
import { PackageSection } from "../PackageSection";
import { draftKey } from "../DocumentCreate/documentPath";
import {
   documentSlug,
   listedLayout,
   reportEditRoute,
   reportRoute,
   type ListedReport,
   type ReportLayout,
} from "./documentLocation";
import { invalidateReportQueries } from "./reportQueries";
import { useReportListing } from "./useDocumentLocation";
import { usePackageVersions } from "./usePackageVersions";
import { VersionSwitch } from "./VersionSwitch";

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

/** A report the package page asks its host to open, and how. */
export interface OpenDocumentRequest {
   /** The report's package path, `sales/q3.malloy`: its identity, and how to open it. */
   path: string;
   layout: ReportLayout;
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
    * the report's path, and the same path with `?edit` to edit.
    */
   onOpenDocument?: (
      request: OpenDocumentRequest,
      event?: React.MouseEvent,
   ) => void;
   resourceUri: string;
   /** What the New report dialog creates, or why it could not; see `ReportEvent`. */
   onEvent?: ReportEventHandler;
   /**
    * Open another version of the package, picked from the title's version
    * switch. Absent, the page navigates to the Console's own route for it,
    * `/<env>/<pkg>/<version>`. Only a package with published versions shows
    * the switch.
    */
   onSelectVersion?: (versionId: string, event?: React.MouseEvent) => void;
}

export default function Package({
   onClickPackageFile,
   onOpenDocument,
   resourceUri,
   onEvent,
   onSelectVersion,
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
   // A package with published versions is immutable: it shows which version
   // is on screen, and offers nothing that writes into the package.
   const { versions } = usePackageVersions(environmentName, packageName);
   const versioned = versions.length > 0;
   const selectVersion =
      onSelectVersion ??
      ((id: string, event?: React.MouseEvent) =>
         onClick(
            `/${encodeURIComponent(environmentName)}/${encodeURIComponent(packageName)}/${encodeURIComponent(id)}`,
            event,
         ));
   const openDocument =
      onOpenDocument ??
      ((request: OpenDocumentRequest, event?: React.MouseEvent) => {
         const to =
            request.mode === "edit"
               ? reportEditRoute(environmentName, packageName, request.path)
               : reportRoute(environmentName, packageName, request.path);
         // Without an event, called exactly as before this hook existed.
         if (event) onClick(to, event);
         else onClick(to);
      });

   const [schemaDatabase, setSchemaDatabase] = useState<Database | null>(null);
   const [creating, setCreating] = useState(false);
   const narrow = useNarrowScreen();

   // Every report the host keeps for this package in a writeable workspace: the
   // builder's drafts, both of reports the package does not have yet and of
   // reports it does, listed so they are found rather than stumbled on.
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
      // One listing per workspace, every layout: a draft is found by its path under this package.
      const prefix = draftKey(environmentName, packageName, "");
      for (const candidate of workspaces.filter((w) => w.writeable))
         for (const locator of await storage.listDocuments(candidate))
            if (locator.path.startsWith(prefix))
               found.push({ locator, where: candidate.description });
      setDrafts(found);
   }, [storage, environmentName, packageName]);
   useEffect(() => {
      void refreshDrafts();
   }, [refreshDrafts]);
   const draftPath = (locator: DocumentLocator) =>
      locator.path.slice(draftKey(environmentName, packageName, "").length);

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

   // One listing for both layouts, by path, shared with every other reader.
   const reportListing = useReportListing(
      environmentName,
      packageName,
      versionId,
   );

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
   // A published version lists its own data apps, each `resource` naming that
   // version, so the key carries it, the same key DataAppViewer reads.
   const dataAppsQuery = useQueryWithApiError({
      queryKey: ["data-apps", environmentName, packageName, versionId],
      queryFn: async () => {
         try {
            return await apiClients.dataApps.listDataApps(
               environmentName,
               packageName,
               versionId,
            );
         } catch (e) {
            // A 404 or transport-level failure (older Publisher without the
            // /data-apps route, network blip) is non-fatal: render the package
            // page without an Apps section. A genuinely missing package
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

   const reports: ListedReport[] = reportListing.reports.filter(
      (r) => !!r.path,
   );
   const artifacts = reportRows(reports);
   // A report is listed once, under Reports. Its file is a model like any
   // other, so it would otherwise appear a second time under Models
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
              mutable: mutable && !versioned,
              canStore: workspace.chosen?.writeable === true,
              ...(versionId === undefined ? {} : { versionId }),
           })
         : undefined;
   // A name is chosen against the listings, so New waits for both rather than offering names a still-loading listing would have taken.
   const listingsSettled =
      reportListing.settled && (modelsQuery.isSuccess || modelsQuery.isError);
   const canCreate = route !== undefined && listingsSettled;
   // The editors step aside on a narrow screen, so no entry point to one is offered there.
   const canNew = canCreate && !narrow;
   const createTarget = useMemo((): CreateTarget | undefined => {
      if (!canCreate) return undefined;
      const existing = [
         ...reportListing.reports.flatMap((r) => (r.path ? [r.path] : [])),
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
      reportListing.reports,
      modelsQuery.data,
   ]);

   const description = pkgQuery.data?.data?.description ?? "";
   const agents = pkgQuery.data?.data?.agents ?? [];
   const readmePath = (
      reports.find((r) => r.path === README_NOTEBOOK) ??
      reports.find((r) => isRootReadme(r.path))
   )?.path;

   // The models list is FILTERED by the report listing, so every section waits
   // for it: gated on anything less, a late listing would show a report under
   // Models and then take it away. A failure cannot hang the page, since the
   // query turns a 404 or a transport failure into an empty list; the cost is
   // one parallel request's latency.
   const isLoading = !reportListing.settled;

   const versionSwitch = versioned ? (
      <VersionSwitch
         environmentName={environmentName}
         packageName={packageName}
         versions={versions}
         {...(versionId !== undefined ? { selectedVersionId: versionId } : {})}
         onSelectVersion={selectVersion}
         // Deleting the only version deletes the package: back to its
         // environment.
         onPackageDeleted={() =>
            onClick(`/${encodeURIComponent(environmentName)}`)
         }
         canManage={mutable}
      />
   ) : null;

   const title = (
      <Stack direction="row" alignItems="center" spacing={1.5} sx={{ mb: 0.5 }}>
         <Typography
            variant="h4"
            component="h1"
            sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
         >
            {packageName}
         </Typography>
         {versionSwitch}
      </Stack>
   );

   if (pkgQuery.isError) {
      const error = (
         <ApiErrorDisplay
            error={pkgQuery.error}
            context={`${environmentName} > ${packageName}`}
         />
      );
      // A version that cannot be served (archived, unknown, or no `latest`
      // yet) still offers the switch, the way to one that can.
      return versionSwitch ? (
         <Container
            maxWidth={false}
            sx={{ maxWidth: 1024, mx: "auto", px: 3, py: 6 }}
         >
            <Box sx={{ mb: 4 }}>{title}</Box>
            {error}
         </Container>
      ) : (
         error
      );
   }

   return (
      <Container
         maxWidth={false}
         sx={{ maxWidth: 1024, mx: "auto", px: 3, py: 6 }}
      >
         <Box sx={{ mb: 4 }}>
            {title}
            {description && (
               <Typography variant="body2" color="text.secondary">
                  {description}
               </Typography>
            )}
         </Box>

         <PackageProblems warnings={pkgQuery.data?.data?.warnings} />

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
               {...(onEvent ? { onEvent } : {})}
               onClose={() => setCreating(false)}
               onCreated={(created) => {
                  setCreating(false);
                  invalidateReportQueries(
                     queryClient,
                     environmentName,
                     packageName,
                  );
                  void queryClient.invalidateQueries({ queryKey: ["models"] });
                  openDocument({
                     path: created.path,
                     layout: created.layout,
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
               {reportListing.isError && (
                  <Box sx={{ mb: 4 }}>
                     <Alert
                        severity="warning"
                        action={
                           <Button
                              color="inherit"
                              size="small"
                              onClick={() => void reportListing.refetch()}
                           >
                              Retry
                           </Button>
                        }
                     >
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
                             // The section heading's add button, as every
                             // other section's: "+ Report".
                             action: (
                                <AddButton
                                   label="Report"
                                   onClick={() => setCreating(true)}
                                />
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
                                      label: "Add report",
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
                              locator.type === "grid" ? "dashboard" : "report"
                           }
                           label={draftPath(locator)}
                           rightLabel={where}
                           onClick={(event) =>
                              openDocument(
                                 {
                                    path: draftPath(locator),
                                    layout: locator.type,
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
                  <PackageSection title="Apps" count={dataApps.length}>
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

               <PackageSection title="Models" count={models.length}>
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

               {agents.length > 0 && (
                  <PackageSection
                     title="Agents"
                     count={agents.length}
                     description="Declared in publisher.json. Name one in an agent session to work as it. Schedules are listed only; nothing runs them yet."
                  >
                     {agents.map((agent) => (
                        <PackageAgentRow key={agent.name} agent={agent} />
                     ))}
                  </PackageSection>
               )}

               <PackageSection title="Data" count={databases.length}>
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

               <Materializations
                  resourceUri={resourceUri}
                  versioned={versioned}
               />

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

         <DataSchemaDialog
            database={schemaDatabase}
            onClose={() => setSchemaDatabase(null)}
         />
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

function PackageAgentRow({ agent }: { agent: AgentSummary }) {
   return (
      <>
         <PackageItemRow
            type="agent"
            label={agent.name ?? ""}
            {...(agent.description ? { description: agent.description } : {})}
            {...(agent.model && agent.model !== "inherit"
               ? { rightLabel: agent.model }
               : {})}
         />
         {(agent.schedules ?? []).map((schedule) => {
            const cron = describeCron(schedule.cron ?? "");
            return (
               <Typography
                  key={`${schedule.cron}|${schedule.task}`}
                  variant="caption"
                  color="text.secondary"
                  sx={{ display: "block", ml: 6, mb: 0.5 }}
               >
                  {cron.valid ? cron.description : schedule.cron} (UTC), task{" "}
                  {schedule.task}. Not run yet.
               </Typography>
            );
         })}
      </>
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
      layout: listedLayout(r),
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
