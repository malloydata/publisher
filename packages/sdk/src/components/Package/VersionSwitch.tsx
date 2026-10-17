// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import {
   Alert,
   Box,
   Button,
   Chip,
   IconButton,
   ListItemText,
   Menu,
   MenuItem,
   Snackbar,
   Stack,
   Tooltip,
   Typography,
   alpha,
} from "@mui/material";
import { useMutation } from "@tanstack/react-query";
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

export interface VersionSwitchProps {
   environmentName: string;
   packageName: string;
   /** The package's versions, highest first. */
   versions: PackageVersion[];
   /** The version on screen; absent means the package's `latest`. */
   selectedVersionId?: string;
   /** Open another version. */
   onSelectVersion: (versionId: string, event?: React.MouseEvent) => void;
   /** Offer Set as latest, Archive and Unarchive. False on a frozen server. */
   canManage: boolean;
}

/**
 * The package title's version switch: `v1.2.0 ▾`, a menu of every version
 * (highest first, the one on screen marked), and on each row a ⋯ menu that
 * moves `latest` there, archives it or unarchives it.
 *
 * A version is state, never a page: picking one changes what the package page
 * shows, and its ⋯ changes the version itself. Archived versions are listed
 * (they can be unarchived) but cannot be opened; the server answers 410 for
 * them.
 */
export function VersionSwitch({
   environmentName,
   packageName,
   versions,
   selectedVersionId,
   onSelectVersion,
   canManage,
}: VersionSwitchProps) {
   const { apiClients } = useServer();
   const invalidatePackage = useInvalidatePackage(environmentName, packageName);
   const [listAnchor, setListAnchor] = useState<HTMLElement | null>(null);
   const [actionsFor, setActionsFor] = useState<{
      version: PackageVersion;
      anchor: HTMLElement;
   } | null>(null);
   const [confirming, setConfirming] = useState<PackageVersion | null>(null);
   const [failure, setFailure] = useState<string | null>(null);
   const cancelRef = useRef<HTMLButtonElement>(null);

   const latest = versions.find((version) => version.latest)?.id;
   const shown = selectedVersionId ?? latest;

   const setLatest = useMutation({
      mutationFn: (version: string) =>
         apiClients.versions.setPackageLatestVersion(
            environmentName,
            packageName,
            { version },
         ),
      onSuccess: async () => {
         setConfirming(null);
         await invalidatePackage();
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
      onSuccess: () => invalidatePackage(),
      onError: (error, { id, archiveStatus }) =>
         setFailure(`Couldn't ${archiveStatus} v${id}: ${errorDetail(error)}`),
   });
   const updating = changeArchive.isPending
      ? changeArchive.variables?.id
      : undefined;

   const closeActions = () => setActionsFor(null);

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
               const archived = version.archiveStatus === "archive";
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
            {actionsFor?.version.archiveStatus === "archive" ? (
               <MenuItem
                  onClick={() => {
                     const { id } = actionsFor.version;
                     closeActions();
                     changeArchive.mutate({ id, archiveStatus: "unarchive" });
                  }}
               >
                  <ListItemText primary="Unarchive" />
               </MenuItem>
            ) : (
               actionsFor && [
                  <MenuItem
                     key="set-latest"
                     disabled={actionsFor.version.latest}
                     onClick={() => {
                        setConfirming(actionsFor.version);
                        setLatest.reset();
                        closeActions();
                        setListAnchor(null);
                     }}
                  >
                     <ListItemText
                        primary="Set as latest"
                        secondary={
                           actionsFor.version.latest
                              ? "Already the latest version"
                              : undefined
                        }
                     />
                  </MenuItem>,
                  <MenuItem
                     key="archive"
                     disabled={actionsFor.version.latest}
                     onClick={() => {
                        const { id } = actionsFor.version;
                        closeActions();
                        changeArchive.mutate({ id, archiveStatus: "archive" });
                     }}
                  >
                     <ListItemText
                        primary="Archive"
                        secondary={
                           actionsFor.version.latest
                              ? "The latest version can't be archived"
                              : undefined
                        }
                     />
                  </MenuItem>,
               ]
            )}
         </Menu>

         <AppDialog
            open={confirming !== null}
            onClose={() => {
               if (!setLatest.isPending) setConfirming(null);
            }}
            title={confirming ? `Set v${confirming.id} as latest` : ""}
            description={
               confirming
                  ? `Requests that name no version will be served from v${confirming.id}.`
                  : undefined
            }
            initialFocus={cancelRef}
            actions={
               <>
                  <Button
                     ref={cancelRef}
                     disabled={setLatest.isPending}
                     onClick={() => setConfirming(null)}
                  >
                     Cancel
                  </Button>
                  <Button
                     variant="contained"
                     loading={setLatest.isPending}
                     onClick={() =>
                        confirming && setLatest.mutate(confirming.id)
                     }
                  >
                     Set as latest
                  </Button>
               </>
            }
         >
            <Stack spacing={1.5}>
               {latest && confirming && latest !== confirming.id && (
                  <Typography variant="body2" color="text.secondary">
                     {`The latest version is v${latest} now. Versions pinned by a link or an explicit versionId keep answering as before.`}
                  </Typography>
               )}
               {setLatest.isError && confirming && (
                  <Alert severity="error" role="alert">
                     {`Couldn't set v${confirming.id} as latest: ${errorDetail(setLatest.error)}`}
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
