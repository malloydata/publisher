import { Box, Button, Snackbar, Stack, Typography } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import React, { useState } from "react";
import { Materialization, MaterializationActionActionEnum } from "../../client";
import {
   useMutationWithApiError,
   useQueryWithApiError,
} from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import CreateEnvMaterializationDialog from "./CreateEnvMaterializationDialog";
import MaterializationDetailDialog from "./MaterializationDetailDialog";
import MaterializationRunsList from "./MaterializationRunsList";
import { isActiveStatus } from "./utils";

const MATERIALIZATION_POLL_MS = 3000;
const PAGE_SIZE = 10;

interface EnvironmentMaterializationsProps {
   resourceUri: string;
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
}

export default function EnvironmentMaterializations({
   resourceUri,
   onClickPackageFile,
}: EnvironmentMaterializationsProps) {
   const { apiClients, mutable } = useServer();
   const queryClient = useQueryClient();
   const { environmentName } = parseResourceUri(resourceUri);
   const [notificationMessage, setNotificationMessage] = useState("");
   const [selectedId, setSelectedId] = useState<string | null>(null);
   // Show the most recent runs and page in more on demand — a busy environment
   // (e.g. a frequent schedule) accumulates runs without bound, so never render
   // the full history at once.
   const [limit, setLimit] = useState(PAGE_SIZE);

   const onClick =
      onClickPackageFile ??
      ((to: string) => {
         window.location.href = to;
      });

   const listQuery = useQueryWithApiError({
      queryKey: ["env-materializations", environmentName, limit],
      queryFn: () =>
         apiClients.materializations.listEnvironmentMaterializations(
            environmentName,
            limit,
         ),
      // "Show more" grows `limit` (part of the key), so keep the current rows on
      // screen while the larger page loads instead of flashing back to the
      // loading state.
      placeholderData: (previousData) => previousData,
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

   const packagesQuery = useQueryWithApiError({
      queryKey: ["packages", environmentName],
      queryFn: () => apiClients.packages.listPackages(environmentName),
   });

   const invalidateList = () =>
      queryClient.invalidateQueries({
         queryKey: ["env-materializations", environmentName],
      });

   // Fire one create per selected package. Each package builds independently, so
   // a failure on one (e.g. an already-active materialization) does not block the
   // others — report an aggregate outcome.
   const createMaterializations = useMutationWithApiError({
      mutationFn: async (opts: {
         packageNames: string[];
         forceRefresh: boolean;
      }) => {
         const results = await Promise.allSettled(
            opts.packageNames.map((pkg) =>
               apiClients.materializations.createMaterialization(
                  environmentName,
                  pkg,
                  { forceRefresh: opts.forceRefresh },
               ),
            ),
         );
         const failed = results.filter((r) => r.status === "rejected").length;
         return { total: opts.packageNames.length, failed };
      },
      onSuccess(result) {
         const started = result.total - result.failed;
         setNotificationMessage(
            result.failed === 0
               ? `Started ${started} materialization${started === 1 ? "" : "s"}`
               : `Started ${started} of ${result.total} (${result.failed} could not start — a build may already be active)`,
         );
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
            materialization.packageName as string,
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
            materialization.packageName as string,
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

   const materializations = (listQuery.data?.data ?? []) as Materialization[];
   const packages = (packagesQuery.data?.data ?? []) as { name: string }[];
   const selected =
      materializations.find((row) => row.id === selectedId) ?? null;
   const isMutating =
      createMaterializations.isPending ||
      stopMaterialization.isPending ||
      deleteMaterialization.isPending;
   // A full page implies there may be older runs beyond the current window.
   const hasMore = materializations.length >= limit;

   return (
      <Box>
         <Box
            sx={{
               display: "flex",
               alignItems: "flex-start",
               justifyContent: "space-between",
               mb: 3,
            }}
         >
            <Box>
               <Typography
                  variant="h6"
                  sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
               >
                  Materializations
               </Typography>
               <Typography variant="body2" color="text.secondary">
                  All materialization builds across the packages in this
                  environment
               </Typography>
            </Box>
            {mutable && (
               <CreateEnvMaterializationDialog
                  packages={packages}
                  onSubmit={(opts) => createMaterializations.mutateAsync(opts)}
                  isSubmitting={createMaterializations.isPending}
               />
            )}
         </Box>

         {listQuery.isError && (
            <ApiErrorDisplay
               error={listQuery.error}
               context={`${environmentName} > Materializations`}
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
               showPackage
               onClickPackage={(packageName) =>
                  onClick(`/${environmentName}/${packageName}/materializations`)
               }
               onStop={(materialization) =>
                  stopMaterialization.mutate(materialization)
               }
               onDelete={(materialization, dropTables) =>
                  deleteMaterialization.mutate({ materialization, dropTables })
               }
               onViewDetails={(materialization) =>
                  setSelectedId(materialization.id ?? null)
               }
            />
         )}
         {listQuery.isSuccess && materializations.length > 0 && (
            <Stack
               direction="row"
               alignItems="center"
               justifyContent="center"
               spacing={2}
               sx={{ mt: 2 }}
            >
               <Typography variant="caption" color="text.secondary">
                  {hasMore
                     ? `Showing ${materializations.length} most recent`
                     : `${materializations.length} materialization${materializations.length === 1 ? "" : "s"}`}
               </Typography>
               {hasMore && (
                  <Button
                     size="small"
                     onClick={() => setLimit((l) => l + PAGE_SIZE)}
                     disabled={listQuery.isFetching}
                  >
                     Show more
                  </Button>
               )}
            </Stack>
         )}

         <MaterializationDetailDialog
            materialization={selected}
            buildPlan={null}
            onClose={() => setSelectedId(null)}
         />

         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />
      </Box>
   );
}
