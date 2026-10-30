// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import ArchiveOutlinedIcon from "@mui/icons-material/ArchiveOutlined";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import PushPinOutlinedIcon from "@mui/icons-material/PushPinOutlined";
import UnarchiveOutlinedIcon from "@mui/icons-material/UnarchiveOutlined";
import {
   Alert,
   Box,
   Button,
   Chip,
   Divider,
   IconButton,
   ListItemIcon,
   ListItemText,
   Menu,
   MenuItem,
   Snackbar,
   Stack,
   Tooltip,
   Typography,
   alpha,
} from "@mui/material";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import React, { useRef, useState } from "react";
import type { PackageVersion } from "../../client";
import { AppDialog } from "../AppDialog";
import { useServer } from "../ServerProvider";
import { useInvalidatePackage } from "./usePackageVersions";

/** The server's own words for a refusal, when it sent any. */
const errorDetail = (error: unknown): string => {
   const message = (error as { response?: { data?: { message?: unknown } } })
      ?.response?.data?.message;
   if (typeof message === "string" && message !== "") return message;
   return error instanceof Error ? error.message : "Unknown error";
};

const publishedOn = (iso: string): string => {
   const date = new Date(iso);
   return Number.isNaN(date.getTime())
      ? ""
      : date.toLocaleDateString("en-US", {
           month: "short",
           day: "numeric",
           year: "numeric",
        });
};

const isArchived = (version: PackageVersion) =>
   version.archiveStatus === "archive";

/**
 * Why the server would refuse to archive or delete `version`, given all of
 * the package's versions; undefined when it would not. These mirror the
 * server's checks, so the menu says why up front instead of answering a
 * click with a 409. A package that has versions always keeps one in service,
 * so its `latest` and its last version in service cannot be archived, and
 * while other versions remain they cannot be deleted either. Deleting the
 * only version is allowed: it deletes the package.
 */
export function versionRefusals(
   version: PackageVersion,
   versions: PackageVersion[],
): { archive?: string; delete?: string } {
   const lastInService =
      !isArchived(version) &&
      versions.filter((other) => !isArchived(other)).length <= 1;
   const onlyVersion = versions.length <= 1;
   return {
      archive: version.latest
         ? "The latest version can't be archived"
         : lastInService
           ? "The last version in service can't be archived"
           : undefined,
      delete: onlyVersion
         ? undefined
         : version.latest
           ? "Pin another version as latest first"
           : lastInService
             ? "The last version in service; delete the archived versions first"
             : undefined,
   };
}

export interface VersionSwitchProps {
   environmentName: string;
   packageName: string;
   /** The package's versions, highest first. */
   versions: PackageVersion[];
   /** The version on screen; absent means the package's `latest`. */
   selectedVersionId?: string;
   /** Open another version. */
   onSelectVersion: (versionId: string, event?: React.MouseEvent) => void;
   /**
    * Called after the package's only version was deleted, which deletes the
    * package. Absent, the switch stays where it is.
    */
   onPackageDeleted?: () => void;
   /** Offer Pin as latest, Archive, Unarchive and Delete. False on a frozen server. */
   canManage: boolean;
}

/**
 * The package title's version switch: `v1.2.0 ▾`, a menu of every version
 * (highest first, the one on screen marked), and on each row a ⋯ menu that
 * pins `latest` there, archives or unarchives it, or deletes it.
 *
 * A version is state, never a page: picking one changes what the package page
 * shows, and its ⋯ changes the version itself. Archived versions are listed
 * (they can be unarchived or deleted) but cannot be opened; the server
 * answers 410 for them.
 */
export function VersionSwitch({
   environmentName,
   packageName,
   versions,
   selectedVersionId,
   onSelectVersion,
   onPackageDeleted,
   canManage,
}: VersionSwitchProps) {
   const { apiClients } = useServer();
   const queryClient = useQueryClient();
   const invalidatePackage = useInvalidatePackage(environmentName, packageName);
   const [listAnchor, setListAnchor] = useState<HTMLElement | null>(null);
   const [actionsFor, setActionsFor] = useState<{
      version: PackageVersion;
      anchor: HTMLElement;
   } | null>(null);
   const [pinning, setPinning] = useState<PackageVersion | null>(null);
   const [deleting, setDeleting] = useState<PackageVersion | null>(null);
   const [failure, setFailure] = useState<string | null>(null);
   const pinCancelRef = useRef<HTMLButtonElement>(null);
   const deleteCancelRef = useRef<HTMLButtonElement>(null);

   const latest = versions.find((version) => version.latest)?.id;
   const shown = selectedVersionId ?? latest;

   // The environment's package list shows each package's latest version, so
   // it is refreshed with the package.
   const refresh = () =>
      Promise.all([
         invalidatePackage(),
         queryClient.invalidateQueries({
            queryKey: ["packages", environmentName],
         }),
      ]);

   const setLatest = useMutation({
      mutationFn: (version: string) =>
         apiClients.versions.setPackageLatestVersion(
            environmentName,
            packageName,
            { version },
         ),
      onSuccess: async () => {
         setPinning(null);
         await refresh();
      },
   });

   const changeArchive = useMutation({
      mutationFn: ({
         id,
         archiveStatus,
      }: {
         id: string;
         archiveStatus: "archive" | "unarchive";
      }) =>
         apiClients.versions.updatePackageVersion(
            environmentName,
            packageName,
            id,
            { archiveStatus },
         ),
      onSuccess: () => refresh(),
      onError: (error, { id, archiveStatus }) =>
         setFailure(`Couldn't ${archiveStatus} v${id}: ${errorDetail(error)}`),
   });
   const updating = changeArchive.isPending
      ? changeArchive.variables?.id
      : undefined;

   const deleteVersion = useMutation({
      mutationFn: (version: string) =>
         apiClients.versions.deletePackageVersion(
            environmentName,
            packageName,
            version,
         ),
      onSuccess: async (_response, id) => {
         setDeleting(null);
         if (versions.length <= 1) {
            // The package went with its only version.
            await queryClient.invalidateQueries({
               queryKey: ["packages", environmentName],
            });
            onPackageDeleted?.();
            return;
         }
         // The version on screen is gone: show latest, or with no latest the
         // highest version still in service.
         if (id === shown) {
            const next =
               latest !== undefined && latest !== id
                  ? latest
                  : versions.find(
                       (version) => version.id !== id && !isArchived(version),
                    )?.id;
            if (next !== undefined) onSelectVersion(next);
         }
         await refresh();
      },
   });

   const closeActions = () => setActionsFor(null);
   const refusals = actionsFor
      ? versionRefusals(actionsFor.version, versions)
      : {};
   const deletesPackage = deleting !== null && versions.length <= 1;

   const deleteItem = actionsFor && (
      <MenuItem
         key="delete"
         disabled={refusals.delete !== undefined}
         onClick={() => {
            setDeleting(actionsFor.version);
            deleteVersion.reset();
            closeActions();
            setListAnchor(null);
         }}
      >
         <ListItemIcon>
            <DeleteOutlineIcon fontSize="small" />
         </ListItemIcon>
         <ListItemText primary="Delete version" secondary={refusals.delete} />
      </MenuItem>
   );

   return (
      <>
         <Button
            size="small"
            variant="outlined"
            endIcon={<KeyboardArrowDownIcon />}
            aria-label="Select package version"
            aria-haspopup="menu"
            onClick={(event) => setListAnchor(event.currentTarget)}
            sx={{
               borderRadius: 999,
               textTransform: "none",
               fontWeight: 500,
               px: 1.5,
               minWidth: 0,
               whiteSpace: "nowrap",
            }}
         >
            {shown ? `v${shown}` : "Select a version"}
         </Button>

         <Menu
            anchorEl={listAnchor}
            open={listAnchor !== null}
            onClose={() => setListAnchor(null)}
            slotProps={{
               paper: {
                  sx: { minWidth: 320, maxWidth: 400, mt: 1, borderRadius: 3 },
               },
               list: { "aria-label": `Versions of ${packageName}` },
            }}
         >
            {versions.map((version) => {
               const active = version.id === shown;
               const archived = isArchived(version);
               const details = [
                  archived ? "Archived" : undefined,
                  publishedOn(version.createdAt),
                  version.gitCommitSha
                     ? version.gitCommitSha.slice(0, 7)
                     : undefined,
               ]
                  .filter(Boolean)
                  .join(" · ");
               return (
                  <MenuItem
                     key={version.id}
                     aria-current={active ? "true" : undefined}
                     data-testid="version-row"
                     onClick={(event) => {
                        if (active || archived) return;
                        setListAnchor(null);
                        onSelectVersion(version.id, event);
                     }}
                     sx={(theme) => ({
                        gap: 1,
                        borderLeft: "3px solid",
                        borderLeftColor: active
                           ? theme.palette.primary.main
                           : "transparent",
                        backgroundColor: active
                           ? alpha(theme.palette.primary.main, 0.04)
                           : undefined,
                        cursor: active || archived ? "default" : "pointer",
                     })}
                  >
                     <ListItemText
                        primary={version.id}
                        secondary={details}
                        sx={{ opacity: archived ? 0.6 : 1 }}
                        slotProps={{
                           primary: { sx: { fontWeight: active ? 600 : 400 } },
                        }}
                     />
                     {version.latest && (
                        <Chip
                           size="small"
                           label="Latest"
                           color="info"
                           variant="outlined"
                           sx={{ height: 20, fontSize: 11, fontWeight: 600 }}
                        />
                     )}
                     {canManage && (
                        <Tooltip
                           title={
                              updating === version.id
                                 ? "Updating this version…"
                                 : ""
                           }
                        >
                           <span>
                              <IconButton
                                 size="small"
                                 aria-label={`More actions for version ${version.id}`}
                                 aria-haspopup="menu"
                                 disabled={updating === version.id}
                                 onClick={(event) => {
                                    event.stopPropagation();
                                    setActionsFor({
                                       version,
                                       anchor: event.currentTarget,
                                    });
                                 }}
                              >
                                 <MoreHorizIcon fontSize="small" />
                              </IconButton>
                           </span>
                        </Tooltip>
                     )}
                  </MenuItem>
               );
            })}
         </Menu>

         <Menu
            anchorEl={actionsFor?.anchor ?? null}
            open={actionsFor !== null}
            onClose={closeActions}
            slotProps={{
               list: {
                  "aria-label": actionsFor
                     ? `Actions for version ${actionsFor.version.id}`
                     : undefined,
               },
            }}
         >
            {actionsFor && isArchived(actionsFor.version)
               ? [
                    <MenuItem
                       key="unarchive"
                       onClick={() => {
                          const { id } = actionsFor.version;
                          closeActions();
                          changeArchive.mutate({
                             id,
                             archiveStatus: "unarchive",
                          });
                       }}
                    >
                       <ListItemIcon>
                          <UnarchiveOutlinedIcon fontSize="small" />
                       </ListItemIcon>
                       <ListItemText primary="Unarchive" />
                    </MenuItem>,
                    <Divider key="divider" />,
                    deleteItem,
                 ]
               : actionsFor && [
                    <MenuItem
                       key="pin-latest"
                       disabled={actionsFor.version.latest}
                       onClick={() => {
                          setPinning(actionsFor.version);
                          setLatest.reset();
                          closeActions();
                          setListAnchor(null);
                       }}
                    >
                       <ListItemIcon>
                          <PushPinOutlinedIcon fontSize="small" />
                       </ListItemIcon>
                       <ListItemText
                          primary="Pin as latest"
                          secondary={
                             actionsFor.version.latest
                                ? "Already the latest version"
                                : undefined
                          }
                       />
                    </MenuItem>,
                    <MenuItem
                       key="archive"
                       disabled={refusals.archive !== undefined}
                       onClick={() => {
                          const { id } = actionsFor.version;
                          closeActions();
                          changeArchive.mutate({
                             id,
                             archiveStatus: "archive",
                          });
                       }}
                    >
                       <ListItemIcon>
                          <ArchiveOutlinedIcon fontSize="small" />
                       </ListItemIcon>
                       <ListItemText
                          primary="Archive"
                          secondary={refusals.archive}
                       />
                    </MenuItem>,
                    <Divider key="divider" />,
                    deleteItem,
                 ]}
         </Menu>

         <AppDialog
            open={pinning !== null}
            onClose={() => {
               if (!setLatest.isPending) setPinning(null);
            }}
            title={pinning ? `Pin v${pinning.id} as latest` : ""}
            description={
               pinning
                  ? `Requests that name no version will be served from v${pinning.id}.`
                  : undefined
            }
            initialFocus={pinCancelRef}
            actions={
               <>
                  <Button
                     ref={pinCancelRef}
                     disabled={setLatest.isPending}
                     onClick={() => setPinning(null)}
                  >
                     Cancel
                  </Button>
                  <Button
                     variant="contained"
                     loading={setLatest.isPending}
                     onClick={() => pinning && setLatest.mutate(pinning.id)}
                  >
                     Pin as latest
                  </Button>
               </>
            }
         >
            <Stack spacing={1.5}>
               {latest && pinning && latest !== pinning.id && (
                  <Typography variant="body2" color="text.secondary">
                     {`The latest version is v${latest} now. Links and requests that name a version keep answering as before.`}
                  </Typography>
               )}
               {setLatest.isError && pinning && (
                  <Alert severity="error" role="alert">
                     {`Couldn't pin v${pinning.id} as latest: ${errorDetail(setLatest.error)}`}
                  </Alert>
               )}
            </Stack>
         </AppDialog>

         <AppDialog
            open={deleting !== null}
            onClose={() => {
               if (!deleteVersion.isPending) setDeleting(null);
            }}
            title={deleting ? `Delete v${deleting.id}` : ""}
            initialFocus={deleteCancelRef}
            actions={
               <>
                  <Button
                     ref={deleteCancelRef}
                     disabled={deleteVersion.isPending}
                     onClick={() => setDeleting(null)}
                  >
                     Cancel
                  </Button>
                  <Button
                     variant="contained"
                     color="error"
                     loading={deleteVersion.isPending}
                     onClick={() =>
                        deleting && deleteVersion.mutate(deleting.id)
                     }
                  >
                     {deletesPackage ? "Delete package" : "Delete version"}
                  </Button>
               </>
            }
         >
            <Stack spacing={1.5}>
               {deleting && (
                  <Typography variant="body2">
                     {deletesPackage
                        ? `v${deleting.id} is the only version of ${packageName}, so deleting it deletes the package. This cannot be undone.`
                        : `v${deleting.id} stops being served and its files are removed. Links that name it stop working, and this cannot be undone.`}
                  </Typography>
               )}
               {deleteVersion.isError && deleting && (
                  <Alert severity="error" role="alert">
                     {`Couldn't delete v${deleting.id}: ${errorDetail(deleteVersion.error)}`}
                  </Alert>
               )}
            </Stack>
         </AppDialog>

         <Snackbar
            open={failure !== null}
            autoHideDuration={8000}
            onClose={() => setFailure(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
         >
            <Box>
               <Alert
                  severity="error"
                  variant="filled"
                  onClose={() => setFailure(null)}
               >
                  {failure}
               </Alert>
            </Box>
         </Snackbar>
      </>
   );
}
