import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import {
   Box,
   Container,
   Link,
   Snackbar,
   Stack,
   Typography,
} from "@mui/material";
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
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import CreateMaterializationDialog from "./CreateMaterializationDialog";
import MaterializationDetailDialog from "./MaterializationDetailDialog";
import MaterializationRunsList from "./MaterializationRunsList";
import ScheduleCard from "./ScheduleCard";
import { isActiveStatus } from "./utils";

const MATERIALIZATION_POLL_MS = 3000;

interface MaterializationsProps {
   resourceUri: string;
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
}

export default function Materializations({
   resourceUri,
   onClickPackageFile,
}: MaterializationsProps) {
   const { apiClients, mutable } = useServer();
   const queryClient = useQueryClient();
   const { environmentName, packageName } = parseResourceUri(resourceUri);
   const [notificationMessage, setNotificationMessage] = useState("");
   const [selectedId, setSelectedId] = useState<string | null>(null);

   const onClick =
      onClickPackageFile ??
      ((to: string) => {
         window.location.href = to;
      });

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
      mutationFn: (opts: { forceRefresh: boolean }) =>
         apiClients.materializations.createMaterialization(
            environmentName,
            packageName,
            {
               forceRefresh: opts.forceRefresh,
            },
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
      mutationFn: (materialization: Materialization) =>
         apiClients.materializations.materializationAction(
            environmentName,
            packageName,
            materialization.id as string,
            MaterializationActionActionEnum.Stop,
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
      mutationFn: ({
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
      mutationFn: (schedule: string | null) =>
         apiClients.packages.updatePackage(environmentName, packageName, {
            name: packageName,
            // updatePackage overwrites description from the body — carry the
            // current value through so a schedule edit doesn't drop it.
            description: currentPackage?.description,
            ...(schedule ? { scope: PackageScopeEnum.Version } : {}),
            materialization: { schedule },
         }),
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
      mutationFn: (scope: PackageScopeEnum) =>
         apiClients.packages.updatePackage(environmentName, packageName, {
            name: packageName,
            description: currentPackage?.description,
            scope,
         }),
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

   return (
      <Container
         maxWidth={false}
         sx={{ maxWidth: 1024, mx: "auto", px: 3, py: 6 }}
      >
         <Box sx={{ mb: 4 }}>
            <Link
               onClick={(event: React.MouseEvent) =>
                  onClick(`/${environmentName}/${packageName}`, event)
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
               Back to {packageName}
            </Link>
            <Stack
               direction="row"
               alignItems="flex-start"
               justifyContent="space-between"
            >
               <Box>
                  <Typography
                     variant="h4"
                     component="h1"
                     sx={{
                        fontWeight: 600,
                        letterSpacing: "-0.025em",
                        mb: 0.5,
                     }}
                  >
                     Materializations
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                     Materialize the persist sources in {packageName} into
                     tables and serve queries from them
                  </Typography>
               </Box>
               {mutable && (
                  <CreateMaterializationDialog
                     onSubmit={(opts) =>
                        createMaterialization.mutateAsync(opts)
                     }
                     isSubmitting={createMaterialization.isPending}
                     disabled={hasActive}
                     disabledReason="A materialization is already pending or running for this package."
                  />
               )}
            </Stack>
         </Box>

         {packageQuery.isSuccess && (
            <ScheduleCard
               schedule={currentPackage?.materialization?.schedule ?? null}
               scope={
                  currentPackage?.scope === PackageScopeEnum.Version
                     ? "version"
                     : "package"
               }
               manifestLocation={currentPackage?.manifestLocation ?? null}
               hasFreshness={Boolean(
                  currentPackage?.materialization?.freshness,
               )}
               mutable={mutable}
               isSubmitting={updateSchedule.isPending}
               isScopeMutating={updateScope.isPending}
               onSubmit={(schedule) => updateSchedule.mutateAsync(schedule)}
               onScopeChange={(scope) =>
                  updateScope.mutateAsync(
                     scope === "version"
                        ? PackageScopeEnum.Version
                        : PackageScopeEnum.Package,
                  )
               }
            />
         )}

         <Box sx={{ mb: 6 }}>
            <Typography
               variant="h6"
               sx={{ fontWeight: 600, letterSpacing: "-0.025em", mb: 1 }}
            >
               Runs
            </Typography>
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
         </Box>

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
      </Container>
   );
}
