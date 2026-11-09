// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Snackbar, Stack, Typography } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import React, { useState } from "react";
import {
   Materialization,
   MaterializationActionActionEnum,
   PackageScopeEnum,
} from "../../client";
import {
   useMutationWithApiError,
   useQueryWithApiError,
} from "../../hooks/useQueryWithApiError";
import { reporting } from "../../telemetry/consoleEvents";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { PackageSection } from "../PackageSection";
import { useServer } from "../ServerProvider";
import { describeCron, formatNextRun } from "./cron";
import CreateMaterializationDialog from "./CreateMaterializationDialog";
import MaterializationDetailDialog, {
   type BuildPlanState,
} from "./MaterializationDetailDialog";
import MaterializationRunsList from "./MaterializationRunsList";
import { isActiveStatus } from "./utils";

const MATERIALIZATION_POLL_MS = 3000;

interface MaterializationsProps {
   resourceUri: string;
}

/**
 * The package's materializations, as a section of its page: what has been
 * built, and a new build. A package's scope and schedule come from its
 * publisher.json, which a published version never changes, so the section
 * shows them and offers no control to change them. The runs are the
 * package's own history, so they sit beside its dashboards and models rather
 * than one click away on a page of their own.
 */
export default function Materializations({
   resourceUri,
}: MaterializationsProps) {
   const { apiClients, mutable } = useServer();
   const queryClient = useQueryClient();
   // A pinned version lists, builds and stops its own runs; absent, `latest`.
   const { environmentName, packageName, versionId } =
      parseResourceUri(resourceUri);
   const [notificationMessage, setNotificationMessage] = useState("");
   const [selectedId, setSelectedId] = useState<string | null>(null);

   if (!packageName) {
      throw new Error(
         "Materializations requires a package in the resource URI",
      );
   }

   const listQuery = useQueryWithApiError({
      queryKey: ["materializations", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.materializations.listMaterializations(
            environmentName,
            packageName,
            undefined,
            undefined,
            versionId,
         ),
      refetchInterval: (query) => {
         const payload = query.state.data as
            | { data?: Materialization[] }
            | undefined;
         const rows = payload?.data ?? [];
         return rows.some((row) => isActiveStatus(row.status))
            ? MATERIALIZATION_POLL_MS
            : false;
      },
   });

   // The package's materialization settings (schedule, freshness, scope).
   const packageQuery = useQueryWithApiError({
      queryKey: ["package", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.packages.getPackage(
            environmentName,
            packageName,
            versionId,
         ),
   });

   // The build plan of the package as loaded, from its own route. Only the
   // detail dialog shows it, so it is fetched when a run is opened rather than
   // on mount. A server that predates the route answers 404, and its package
   // response still carries the plan inline, so that is read instead (`inline`
   // below). Any other failure — a plan that failed at load answers 409
   // BUILD_PLAN_UNAVAILABLE — is shown as unavailable, never as "no persist
   // sources".
   const buildPlanQuery = useQueryWithApiError({
      queryKey: ["package-build-plan", environmentName, packageName, versionId],
      queryFn: async ({ signal }) => {
         try {
            const response = await apiClients.packages.getPackageBuildPlan(
               environmentName,
               packageName,
               versionId,
               { signal },
            );
            return { inline: false, plan: response.data };
         } catch (error) {
            const status = (error as { response?: { status?: number } })
               ?.response?.status;
            if (status === 404) return { inline: true, plan: null };
            throw error;
         }
      },
      enabled: selectedId !== null,
   });

   const invalidateList = () =>
      queryClient.invalidateQueries({
         queryKey: ["materializations", environmentName, packageName],
      });

   // Auto-run: the publisher compiles, builds every persist source, and loads
   // the resulting manifest in a single pass.
   const createMaterialization = useMutationWithApiError({
      mutationFn: reporting(
         "materialization",
         "create",
         (opts: { forceRefresh: boolean }) =>
            apiClients.materializations.createMaterialization(
               environmentName,
               packageName,
               {
                  forceRefresh: opts.forceRefresh,
                  ...(versionId !== undefined ? { versionId } : {}),
               },
            ),
      ),
      onSuccess() {
         setNotificationMessage("Materialization requested");
         invalidateList();
      },
      onError(error) {
         setNotificationMessage(error.message);
         invalidateList();
      },
   });

   const stopMaterialization = useMutationWithApiError({
      mutationFn: reporting(
         "materialization",
         "update",
         (materialization: Materialization) =>
            apiClients.materializations.materializationAction(
               environmentName,
               packageName,
               materialization.id as string,
               MaterializationActionActionEnum.Stop,
               versionId,
            ),
      ),
      onSuccess() {
         setNotificationMessage("Materialization stopped");
         invalidateList();
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const deleteMaterialization = useMutationWithApiError({
      mutationFn: reporting(
         "materialization",
         "delete",
         ({
            materialization,
            dropTables,
         }: {
            materialization: Materialization;
            dropTables: boolean;
         }) =>
            apiClients.materializations.deleteMaterialization(
               environmentName,
               packageName,
               materialization.id as string,
               dropTables,
               versionId,
            ),
      ),
      onSuccess() {
         setNotificationMessage("Materialization deleted");
         invalidateList();
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const currentPackage = packageQuery.data?.data;

   const materializations = (listQuery.data?.data ?? []) as Materialization[];
   const selected =
      materializations.find((row) => row.id === selectedId) ?? null;
   const hasActive = materializations.some((row) => isActiveStatus(row.status));
   const isMutating =
      createMaterialization.isPending ||
      stopMaterialization.isPending ||
      deleteMaterialization.isPending;

   const buildPlan: BuildPlanState = buildPlanQuery.isError
      ? { status: "unavailable" }
      : !buildPlanQuery.isSuccess
        ? { status: "loading" }
        : !buildPlanQuery.data.inline
          ? { status: "ready", plan: buildPlanQuery.data.plan }
          : packageQuery.isSuccess
            ? // `buildPlan` is deprecated on the package response; it is read
              // only from a server old enough to lack the route.
              { status: "ready", plan: currentPackage?.buildPlan ?? null }
            : packageQuery.isError
              ? { status: "unavailable" }
              : { status: "loading" };

   const schedule = currentPackage?.materialization?.schedule ?? null;
   const scope =
      currentPackage?.scope === PackageScopeEnum.Version
         ? "version"
         : "package";
   const orchestrated = Boolean(currentPackage?.manifestLocation);
   const hasFreshness = Boolean(currentPackage?.materialization?.freshness);
   const cron = schedule ? describeCron(schedule) : null;

   // What the settings currently say, in one line. The controls that change
   // them are buttons on the heading row, so the page no longer needs a card to
   // hold them — but a reader still has to see what is in force without opening
   // anything.
   const summary = orchestrated
      ? "Control-plane managed: refresh is driven by the control plane, not the built-in scheduler."
      : [
           cron
              ? cron.valid
                 ? `${cron.description} (${schedule}), next run ${formatNextRun(cron.nextRun)}`
                 : `Unrecognized cron expression (${schedule})`
              : hasFreshness
                ? "Refreshed by a freshness policy; a cron schedule does not apply"
                : "On demand only, no schedule set",
           `scope: ${scope}`,
        ].join(" · ");

   return (
      <>
         <PackageSection
            title="Materializations"
            count={listQuery.isSuccess ? materializations.length : undefined}
            action={
               mutable && (
                  <Stack direction="row" spacing={1} alignItems="center">
                     <CreateMaterializationDialog
                        onSubmit={(opts) =>
                           createMaterialization.mutateAsync(opts)
                        }
                        isSubmitting={createMaterialization.isPending}
                        disabled={hasActive}
                        disabledReason="A materialization is already pending or running for this package."
                     />
                  </Stack>
               )
            }
         >
            {packageQuery.isSuccess && (
               <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ mb: 1.5 }}
               >
                  {summary}
               </Typography>
            )}
            {listQuery.isError && (
               <ApiErrorDisplay
                  error={listQuery.error}
                  context={`${environmentName} > ${packageName} > Materializations`}
               />
            )}
            {!listQuery.isSuccess && !listQuery.isError && (
               <Loading text="Loading materializations..." />
            )}
            {listQuery.isSuccess && (
               <MaterializationRunsList
                  materializations={materializations}
                  mutable={mutable}
                  isMutating={isMutating}
                  onStop={(materialization) =>
                     stopMaterialization.mutate(materialization)
                  }
                  onDelete={(materialization, dropTables) =>
                     deleteMaterialization.mutate({
                        materialization,
                        dropTables,
                     })
                  }
                  onViewDetails={(materialization) =>
                     setSelectedId(materialization.id ?? null)
                  }
               />
            )}
         </PackageSection>

         <MaterializationDetailDialog
            materialization={selected}
            buildPlan={buildPlan}
            onClose={() => setSelectedId(null)}
         />

         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />
      </>
   );
}
