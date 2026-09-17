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
import MaterializationDetailDialog from "./MaterializationDetailDialog";
import MaterializationRunsList from "./MaterializationRunsList";
import ScopeButton from "./ScopeButton";
import SetScheduleDialog from "./SetScheduleDialog";
import { isActiveStatus } from "./utils";

const MATERIALIZATION_POLL_MS = 3000;

interface MaterializationsProps {
   resourceUri: string;
}

/**
 * The package's materializations, as a section of its page: what has been
 * built, and the three controls that change it — scope, schedule, and a new
 * build. The runs are the package's own history, so they sit beside its
 * dashboards and models rather than one click away on a page of their own.
 */
export default function Materializations({
   resourceUri,
}: MaterializationsProps) {
   const { apiClients, mutable } = useServer();
   const queryClient = useQueryClient();
   const { environmentName, packageName } = parseResourceUri(resourceUri);
   const [notificationMessage, setNotificationMessage] = useState("");
   const [selectedId, setSelectedId] = useState<string | null>(null);

   if (!packageName) {
      throw new Error(
         "Materializations requires a package in the resource URI",
      );
   }

   const listQuery = useQueryWithApiError({
      queryKey: ["materializations", environmentName, packageName],
      queryFn: () =>
         apiClients.materializations.listMaterializations(
            environmentName,
            packageName,
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

   // The build plan is a property of the compiled package (Package.buildPlan),
   // not of a historical run, so fetch it from the package for the detail view.
   const packageQuery = useQueryWithApiError({
      queryKey: ["package", environmentName, packageName],
      queryFn: () =>
         apiClients.packages.getPackage(environmentName, packageName),
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

   // Edit the package's materialization.schedule (persisted to publisher.json).
   // A schedule is legal only on a version-scoped package, so enabling one also
   // sets scope: version; clearing (null) leaves scope untouched — scope is an
   // explicit control (updateScope below), so clearing a schedule no longer
   // strands scope: version with no way back. The running scheduler re-arms from
   // the new cron on its next tick — no reload needed.
   const updateSchedule = useMutationWithApiError({
      mutationFn: reporting("schedule", "update", (schedule: string | null) =>
         apiClients.packages.updatePackage(environmentName, packageName, {
            name: packageName,
            // updatePackage overwrites description from the body — carry the
            // current value through so a schedule edit doesn't drop it.
            description: currentPackage?.description,
            ...(schedule ? { scope: PackageScopeEnum.Version } : {}),
            materialization: { schedule },
         }),
      ),
      onSuccess(_data, schedule) {
         setNotificationMessage(
            schedule ? "Schedule updated" : "Schedule cleared",
         );
         queryClient.invalidateQueries({
            queryKey: ["package", environmentName, packageName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   // Set the persist scope explicitly (package | version). Independent of the
   // schedule so the version flip a schedule requires can be undone after the
   // schedule is cleared. The server rejects scope: package while a schedule is
   // still set (publish-gate Rule 2), so the UI only offers this when no
   // schedule is active.
   const updateScope = useMutationWithApiError({
      mutationFn: reporting("scope", "update", (scope: PackageScopeEnum) =>
         apiClients.packages.updatePackage(environmentName, packageName, {
            name: packageName,
            description: currentPackage?.description,
            scope,
         }),
      ),
      onSuccess() {
         setNotificationMessage("Scope updated");
         queryClient.invalidateQueries({
            queryKey: ["package", environmentName, packageName],
         });
      },
      onError(error) {
         setNotificationMessage(error.message);
      },
   });

   const materializations = (listQuery.data?.data ?? []) as Materialization[];
   const selected =
      materializations.find((row) => row.id === selectedId) ?? null;
   const hasActive = materializations.some((row) => isActiveStatus(row.status));
   const isMutating =
      createMaterialization.isPending ||
      stopMaterialization.isPending ||
      deleteMaterialization.isPending;

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
                     {!orchestrated && (
                        <ScopeButton
                           scope={scope}
                           // A schedule requires version scope, so the server
                           // refuses `package` while one is set; clearing the
                           // schedule is the way back.
                           disabled={Boolean(schedule)}
                           disabledReason="A schedule requires version scope. Clear the schedule to change it."
                           isSubmitting={updateScope.isPending}
                           onChange={(next) =>
                              updateScope.mutateAsync(
                                 next === "version"
                                    ? PackageScopeEnum.Version
                                    : PackageScopeEnum.Package,
                              )
                           }
                        />
                     )}
                     {!orchestrated && (
                        <SetScheduleDialog
                           currentSchedule={schedule}
                           isSubmitting={updateSchedule.isPending}
                           disabled={hasFreshness}
                           disabledReason="This package declares a freshness policy; a schedule and freshness are mutually exclusive."
                           onSubmit={(next) => updateSchedule.mutateAsync(next)}
                        />
                     )}
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
            buildPlan={packageQuery.data?.data?.buildPlan ?? null}
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
