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
import { Materialization, MaterializationActionActionEnum } from "../../client";
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

   const invalidateList = () =>
      queryClient.invalidateQueries({
         queryKey: ["materializations", environmentName, packageName],
      });

   // Round 1 only: ask the publisher to compile and produce a build plan. The
   // control plane drives Round 2 (the build) from that plan, so there is no
   // "start" call here.
   const createMaterialization = useMutationWithApiError({
      mutationFn: (opts: { forceRefresh: boolean }) =>
         apiClients.materializations.createMaterialization(
            environmentName,
            packageName,
            { forceRefresh: opts.forceRefresh },
         ),
      onSuccess() {
         setNotificationMessage("Build plan requested");
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
      mutationFn: (materialization: Materialization) =>
         apiClients.materializations.deleteMaterialization(
            environmentName,
            packageName,
            materialization.id as string,
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
                     Plan persist sources for {packageName}; the control plane
                     drives the build and produces the manifest
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
                  onDelete={(materialization) =>
                     deleteMaterialization.mutate(materialization)
                  }
                  onViewDetails={(materialization) =>
                     setSelectedId(materialization.id ?? null)
                  }
               />
            )}
         </Box>

         <MaterializationDetailDialog
            materialization={selected}
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
