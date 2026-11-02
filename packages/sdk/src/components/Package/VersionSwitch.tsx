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
import {
   useFetchPackageVersions,
   useInvalidatePackage,
} from "./usePackageVersions";

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

/** What archiving or deleting a version drops, said in both dialogs. */
const TABLES_DROPPED =
   "Tables built for this version alone (under scope: version) are dropped.";

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
    * Show another version in place of the one on screen, which was archived
    * or deleted: a version's id, or undefined for the package's `latest`.
    * Replaces the address rather than adding to history, so Back does not
    * return to a version that is gone. Absent, the switch opens the version
    * by id with `onSelectVersion`.
    */
   onReplaceVersion?: (versionId: string | undefined) => void;
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
   onReplaceVersion,
   onPackageDeleted,
   canManage,
}: VersionSwitchProps) {
   const { apiClients } = useServer();
   const queryClient = useQueryClient();
   const invalidatePackage = useInvalidatePackage(environmentName, packageName);
   const fetchVersions = useFetchPackageVersions(environmentName, packageName);
   const [listAnchor, setListAnchor] = useState<HTMLElement | null>(null);
   const [actionsFor, setActionsFor] = useState<{
      version: PackageVersion;
      anchor: HTMLElement;
   } | null>(null);
   const [pinning, setPinning] = useState<PackageVersion | null>(null);
   const [archiving, setArchiving] = useState<PackageVersion | null>(null);
   // The version to delete, with the package's versions read as its dialog
   // opened (absent while that read is under way): whether the delete takes
   // the package with it is decided from that read, never from a list
   // another tab may have changed.
   const [deleting, setDeleting] = useState<{
      version: PackageVersion;
      versions?: PackageVersion[];
   } | null>(null);
   const [failure, setFailure] = useState<string | null>(null);
   const pinCancelRef = useRef<HTMLButtonElement>(null);
   const archiveCancelRef = useRef<HTMLButtonElement>(null);
   const deleteCancelRef = useRef<HTMLButtonElement>(null);
   // Each row's ⋯, so the keyboard opens a row's actions where the mouse does.
   const moreButtons = useRef(new Map<string, HTMLElement>());

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

   // The version on screen was archived or deleted: show the package's
   // latest, or with no latest the highest version still in service, by the
   // versions as they are now (`after`, else read afresh). A listing that
   // fails leaves the page where it is.
   const leaveVersion = async (gone: string, after?: PackageVersion[]) => {
      const now = after ?? (await fetchVersions().catch(() => undefined));
      if (now === undefined) return;
      const latestAfter = now.find((version) => version.latest)?.id;
      const next =
         latestAfter ??
         now.find((version) => version.id !== gone && !isArchived(version))?.id;
      if (next === undefined) return;
      if (onReplaceVersion) {
         onReplaceVersion(next === latestAfter ? undefined : next);
      } else {
         onSelectVersion(next);
      }
   };

   // A refusal means the list on screen was out of date, so it is read again
   // as it is after a success.
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
      onError: () => void refresh(),
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
      onSuccess: async (_response, { id, archiveStatus }) => {
         setArchiving(null);
         if (archiveStatus === "archive" && id === shown) {
            await leaveVersion(id);
         }
         await refresh();
      },
      onError: (error, { id, archiveStatus }) => {
         // An archive is confirmed in a dialog, which shows its refusal.
         if (archiveStatus === "unarchive") {
            setFailure(`Couldn't unarchive v${id}: ${errorDetail(error)}`);
         }
         void refresh();
      },
   });
   const updating = changeArchive.isPending
      ? changeArchive.variables?.id
      : undefined;

   const deleteVersion = useMutation({
      // `read`: the package's versions the dialog was confirmed against.
      mutationFn: ({ id }: { id: string; read: PackageVersion[] }) =>
         apiClients.versions.deletePackageVersion(
            environmentName,
            packageName,
            id,
         ),
      onSuccess: async (_response, { id, read }) => {
         setDeleting(null);
         // What is left decides what happened, not the list the dialog read:
         // a package with no versions left (its listing answers 404, read as
         // none) went with the version. Only when that cannot be read does the
         // dialog's read stand in, so a deleted package never leaves the page
         // on its dead route.
         const after = await fetchVersions().catch(() => undefined);
         const left = after ?? read.filter((version) => version.id !== id);
         if (left.length === 0) {
            await queryClient.invalidateQueries({
               queryKey: ["packages", environmentName],
            });
            onPackageDeleted?.();
            return;
         }
         if (id === shown) await leaveVersion(id, left);
         await refresh();
      },
      onError: () => void refresh(),
   });

   const closeActions = () => setActionsFor(null);
   const refusals = actionsFor
      ? versionRefusals(actionsFor.version, versions)
      : {};
   const allRefused =
      actionsFor !== null &&
      !isArchived(actionsFor.version) &&
      actionsFor.version.latest &&
      refusals.archive !== undefined &&
      refusals.delete !== undefined;
   // The dialog is open and its read of the versions is under way.
   const checkingDelete = deleting !== null && deleting.versions === undefined;
   const deletesPackage =
      deleting?.versions !== undefined && deleting.versions.length <= 1;
   // What the server would refuse, by the list read as the dialog opened.
   const deleteRefusal =
      deleting?.versions !== undefined
         ? versionRefusals(deleting.version, deleting.versions).delete
         : undefined;

   // Read the versions afresh, then confirm against that read.
   const chooseDelete = async (chosen: PackageVersion) => {
      closeActions();
      setListAnchor(null);
      deleteVersion.reset();
      // Open at once, saying it is checking, so the click visibly did something.
      setDeleting({ version: chosen });
      // Only into the dialog this call opened: one cancelled meanwhile stays shut.
      const settle = (
         next: { version: PackageVersion; versions: PackageVersion[] } | null,
      ) =>
         setDeleting((open) =>
            open !== null &&
            open.version.id === chosen.id &&
            open.versions === undefined
               ? next
               : open,
         );
      let fresh: PackageVersion[];
      try {
         fresh = await fetchVersions();
      } catch (error) {
         settle(null);
         setFailure(
            `Couldn't check v${chosen.id} before deleting it: ${errorDetail(error)}`,
         );
         return;
      }
      const current = fresh.find((version) => version.id === chosen.id);
      if (current === undefined) {
         settle(null);
         setFailure(`v${chosen.id} is no longer a version of ${packageName}.`);
         void refresh();
         return;
      }
      settle({ version: current, versions: fresh });
   };

   // A row's ⋯ menu, anchored at its ⋯ as a click there anchors it.
   const openActions = (version: PackageVersion, fallback: HTMLElement) => {
      if (!canManage || updating === version.id) return;
      setActionsFor({
         version,
         anchor: moreButtons.current.get(version.id) ?? fallback,
      });
   };

   // From the keyboard: ArrowRight, Shift+F10 or the menu key.
   const openActionsByKey = (
      event: React.KeyboardEvent<HTMLElement>,
      version: PackageVersion,
   ) => {
      const opens =
         event.key === "ArrowRight" ||
         event.key === "ContextMenu" ||
         (event.key === "F10" && event.shiftKey);
      if (!canManage || !opens) return;
      event.preventDefault();
      event.stopPropagation();
      openActions(version, event.currentTarget);
   };

   const deleteItem = actionsFor && (
      <MenuItem
         key="delete"
         disabled={refusals.delete !== undefined}
         onClick={() => void chooseDelete(actionsFor.version)}
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
            // Holds the visible text, so a speech user can say what they see.
            aria-label={
               shown ? `Version v${shown}, change version` : "Select a version"
            }
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
               // A row marked disabled stays reachable by keyboard, so what
               // it says is read too.
               list: {
                  "aria-label": `Versions of ${packageName}`,
                  disabledItemsFocusable: true,
               },
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
                     // An archived version is listed but does not open (the
                     // server answers 410): choosing it offers its actions
                     // instead. With none to offer, it is marked disabled; it
                     // is not otherwise, since that would disable its ⋯ too.
                     aria-disabled={archived && !canManage ? "true" : undefined}
                     aria-haspopup={canManage ? "menu" : undefined}
                     data-testid="version-row"
                     onKeyDown={(event) => openActionsByKey(event, version)}
                     onClick={(event) => {
                        if (archived) {
                           openActions(version, event.currentTarget);
                           return;
                        }
                        if (active) return;
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
                        cursor:
                           active || (archived && !canManage)
                              ? "default"
                              : "pointer",
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
                                 ref={(element: HTMLButtonElement | null) => {
                                    if (element) {
                                       moreButtons.current.set(
                                          version.id,
                                          element,
                                       );
                                    } else {
                                       moreButtons.current.delete(version.id);
                                    }
                                 }}
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
                  // A refused action is focusable, so its reason is read.
                  disabledItemsFocusable: true,
                  // Latest refuses every action, and the menu then focuses
                  // no item: focus the list, so the arrow keys reach them.
                  autoFocus: allRefused,
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
                          setArchiving(actionsFor.version);
                          changeArchive.reset();
                          closeActions();
                          setListAnchor(null);
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
            open={archiving !== null}
            onClose={() => {
               if (!changeArchive.isPending) setArchiving(null);
            }}
            title={archiving ? `Archive v${archiving.id}` : ""}
            description={
               archiving
                  ? `v${archiving.id} stops being served and is unloaded. Its files stay, so it can be unarchived.`
                  : undefined
            }
            initialFocus={archiveCancelRef}
            actions={
               <>
                  <Button
                     ref={archiveCancelRef}
                     disabled={changeArchive.isPending}
                     onClick={() => setArchiving(null)}
                  >
                     Cancel
                  </Button>
                  <Button
                     variant="contained"
                     loading={changeArchive.isPending}
                     onClick={() =>
                        archiving &&
                        changeArchive.mutate({
                           id: archiving.id,
                           archiveStatus: "archive",
                        })
                     }
                  >
                     Archive
                  </Button>
               </>
            }
         >
            <Stack spacing={1.5}>
               <Typography variant="body2" color="text.secondary">
                  {`${TABLES_DROPPED} Its scheduled builds stop, and once unarchived it serves live until it is built again.`}
               </Typography>
               {changeArchive.isError && archiving && (
                  <Alert severity="error" role="alert">
                     {`Couldn't archive v${archiving.id}: ${errorDetail(changeArchive.error)}`}
                  </Alert>
               )}
            </Stack>
         </AppDialog>

         <AppDialog
            open={deleting !== null}
            onClose={() => {
               if (!deleteVersion.isPending) setDeleting(null);
            }}
            title={deleting ? `Delete v${deleting.version.id}` : ""}
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
                     loading={deleteVersion.isPending || checkingDelete}
                     disabled={deleteRefusal !== undefined}
                     onClick={() =>
                        deleting?.versions &&
                        deleteVersion.mutate({
                           id: deleting.version.id,
                           read: deleting.versions,
                        })
                     }
                  >
                     {deletesPackage ? "Delete package" : "Delete version"}
                  </Button>
               </>
            }
         >
            <Stack spacing={1.5}>
               {checkingDelete && deleting && (
                  <Typography variant="body2">
                     {`Checking ${packageName}'s versions before deleting v${deleting.version.id}…`}
                  </Typography>
               )}
               {deleting && !checkingDelete && (
                  <Typography variant="body2">
                     {deletesPackage
                        ? `v${deleting.version.id} is the only version of ${packageName}, so deleting it deletes the package. ${TABLES_DROPPED} This cannot be undone.`
                        : `v${deleting.version.id} stops being served and its files are removed. ${TABLES_DROPPED} Links that name it stop working, and this cannot be undone.`}
                  </Typography>
               )}
               {deleteRefusal !== undefined && deleting && (
                  <Alert severity="warning" role="alert">
                     {`v${deleting.version.id} can't be deleted now: ${deleteRefusal}.`}
                  </Alert>
               )}
               {deleteVersion.isError && deleting && (
                  <Alert severity="error" role="alert">
                     {`Couldn't delete v${deleting.version.id}: ${errorDetail(deleteVersion.error)}`}
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
