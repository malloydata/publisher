// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Button, Stack } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useRef } from "react";
import type { Given } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { BuilderEvent } from "./telemetry";
import { now } from "../../utils/clock";
import { useOptionalDocumentStorage } from "../DocumentStorage";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import type { DocumentKind } from "./document";
import {
   resolveEditorTarget,
   saveCaption,
   withWorkspace,
} from "./documentSession";
import { EditorSurface } from "./EditorSurface";
import { useHostCopy } from "./useHostCopy";
import { useOpenedDocument } from "./useOpenedDocument";
import { useSaveChannel } from "./useSaveChannel";

/**
 * The builder, opened on a package dashboard, with everything a host has to
 * supply already wired: the file's text and manifest from the server, a live
 * control row and live tiles that follow the document, a catalog for the
 * filter window's field search, and saving.
 *
 * SAVING goes wherever the record is. A {@link Workspace} the host marks
 * `authoritative` IS the record, so the editor opens that copy and writes back
 * to it, and the package file is a deploy of it. Otherwise the record is the
 * package: the editor writes there on a server that takes writes, and
 * otherwise keeps a copy beside it in the host's {@link DocumentStorage} — the
 * Console's default is this browser, where the copy is offered back on the
 * next visit. A host with neither still gets the editor, without Save.
 *
 * What that costs is stated in the toolbar: a control added here is live in the
 * editor (its value is written into each tile's query) but reaches the package
 * only when the file is saved into it.
 */
export type DashboardEditorProps = (
   | {
        /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
        resourceUri: string;
        /** The dashboard's slug, as listed by the dashboards endpoint. */
        dashboard: string;
     }
   | {
        /** @deprecated Pass `resourceUri` and `dashboard` instead. */
        environmentName: string;
        /** @deprecated Pass `resourceUri` and `dashboard` instead. */
        packageName: string;
        /**
         * @deprecated Pass `resourceUri` and `dashboard` instead. The
         * dashboard's slug: `overview`, not `dashboards/overview.malloy`.
         */
        dashboardName: string;
     }
) & {
   /**
    * What the editor does — opened, saved, refused — for the host to log or
    * count; see `DashboardEvent`. A notebook reports `NotebookEvent`s instead.
    */
   onEvent?: (event: BuilderEvent) => void;
   /**
    * What the document is to the host: its storage locator and event names.
    * Default `dashboard`. A notebook is a dashboard with one column, and is
    * read with the authoring flag and served through the notebook endpoint.
    */
   kind?: DocumentKind;
   /**
    * The document's file within the package, when it is not where its kind
    * puts a new one (`dashboards/<slug>.malloy`, `notebooks/<slug>.malloy`):
    * the kind comes from the file's own tag, so a document may sit in either.
    */
   path?: string;
   /**
    * Whether there are edits the record does not have, on every change and
    * whenever the editor opens a document.
    *
    * For a host that owns the way out: a route change, a tab close, its own
    * "are you sure". The editor will not discard unsaved work on its own, so
    * without this a host cannot tell whether leaving costs anything.
    */
   onDirtyChange?: (dirty: boolean) => void;
   /**
    * Leave the editor: opted into, it draws Close after Save, which asks first
    * when edits are unsaved. A host with a way out of its own (the Console's
    * header View) leaves it unset and guards with `onDirtyChange`.
    */
   onExit?: () => void;
};

export function DashboardEditor(props: DashboardEditorProps) {
   const { onEvent, onDirtyChange, onExit, kind = "dashboard", path } = props;
   const notebook = kind === "notebook";
   // Degraded, not thrown, on a bad URI: a throw in a render body takes the host's whole tree down.
   const {
      environmentName,
      packageName,
      versionId,
      namesBoth: uriNamesBoth,
   } = resolveEditorTarget(props);
   const dashboardName =
      "resourceUri" in props ? props.dashboard : props.dashboardName;
   const noun = notebook ? "notebook" : "dashboard";
   const refusedEvent = notebook
      ? ("notebook.open_refused" as const)
      : ("dashboard.open_refused" as const);

   const { apiClients, mutable, isLoadingStatus } = useServer();
   const queryClient = useQueryClient();
   // When the editor was asked for — or the reader chose what to open — so
   // "opened" can say how long it took. Read through refs by the open effect,
   // so a host's handler changing identity does not re-open the document.
   const startedAt = useRef(now());
   const onEventRef = useRef(onEvent);
   onEventRef.current = onEvent;
   const onDirtyChangeRef = useRef(onDirtyChange);
   onDirtyChangeRef.current = onDirtyChange;
   const storage = useOptionalDocumentStorage()?.documentStorage;
   const legacyFormat = notebook && /\.malloynb$/i.test(dashboardName);
   const modelPath = path ?? `${kind}s/${dashboardName}.malloy`;

   // The file as the package has it. `sourceText` rides on the compiled model.
   const modelQuery = useQueryWithApiError({
      queryKey: [
         "dashboard-editor-model",
         environmentName,
         packageName,
         modelPath,
         versionId,
         notebook,
      ],
      // A curated package otherwise withholds the text of a notebook reading an off-surface source.
      queryFn: () =>
         apiClients.models.getModel(
            environmentName,
            packageName,
            modelPath,
            versionId,
            notebook ? true : undefined,
         ),
      enabled: uriNamesBoth && !legacyFormat,
   });
   const packageText = (
      modelQuery.data?.data as { sourceText?: string } | undefined
   )?.sourceText;

   const {
      workspace,
      draft,
      setDraft,
      draftChecked,
      offered,
      setOffered,
      readFailure,
   } = useHostCopy({
      storage,
      kind,
      notebook,
      environmentName,
      packageName,
      modelPath,
   });
   const {
      authoritative,
      blockedOnRecord,
      resume,
      setResume,
      fromDraft,
      opened,
      setOpened,
      openError,
      savedHashRef,
      packageBaseRef,
      setWrote,
      fetchedAtRef,
      setDirty,
      setSeen,
      held,
      choose,
      acceptHeld,
   } = useOpenedDocument({
      workspace,
      draft,
      draftChecked,
      readFailure,
      packageText,
      fetchedAt: modelQuery.dataUpdatedAt,
      packageSettled: modelQuery.isSuccess && !modelQuery.isFetching,
      modelPath,
      notebook,
      noun,
      refusedEvent,
      legacyFormat,
      startedAt,
      onEventRef,
   });
   const {
      save,
      savesTo,
      writer,
      pinnedPackageSave,
      takesWrites,
      supersedeFailure,
   } = useSaveChannel({
      storage,
      workspace,
      kind,
      notebook,
      environmentName,
      packageName,
      modelPath,
      versionId,
      apiClients,
      queryClient,
      mutable,
      readFailure,
      authoritative,
      resume,
      setResume,
      setOpened,
      setDraft,
      setOffered,
      setSeen,
      setWrote,
      savedHashRef,
      packageBaseRef,
      fetchedAtRef,
   });
   const workspaceName = workspace?.name;
   const reportEvent = useCallback(
      (event: BuilderEvent) => {
         onEventRef.current?.(withWorkspace(event, workspaceName));
      },
      [workspaceName],
   );
   // Stable: the builder fires this from an effect that depends on it.
   const reportDirty = useCallback(
      (value: boolean) => {
         setDirty(value);
         onDirtyChangeRef.current?.(value);
      },
      [setDirty],
   );

   const saveLabel =
      savesTo === "host" && workspace?.description
         ? workspace.description
         : undefined;
   const caption =
      !authoritative && mutable === undefined
         ? isLoadingStatus
            ? "Checking whether this server takes writes."
            : "This server did not say whether it takes writes, so Save is off."
         : saveCaption({
              authoritative,
              mutable: takesWrites,
              pinnedPackageSave,
              ...(workspace ? { workspace } : {}),
              ...(readFailure !== undefined ? { readFailure } : {}),
              ...(versionId !== undefined ? { versionId } : {}),
           });
   // The description is already the Save caption, so the note does not repeat it.
   const note = caption === saveLabel ? "" : caption;

   // After every hook, so the hook order does not depend on the URI. Same
   // reasoning as `Dashboard`'s own check.
   if (!uriNamesBoth && "resourceUri" in props)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            A {noun} resource URI must name an environment and a package.
            Received: {props.resourceUri}
         </Alert>
      );
   if (openError)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            This {noun} cannot be opened in the builder: {openError}
         </Alert>
      );
   // Only before an open: after one, a failed refetch is a banner above the builder so unsaved edits survive it.
   if (modelQuery.isError && !opened)
      return (
         <ApiErrorDisplay
            error={modelQuery.error}
            context={`Opening the ${noun}`}
         />
      );
   if (!opened && (!packageText || !draftChecked))
      return <Loading text={`Opening the ${noun}…`} />;
   // Where the host's copy IS the document, a copy that could not be read
   // leaves nothing safe to edit: the package file is a deploy of the record,
   // so opening it and arming Save would publish it over the record.
   if (blockedOnRecord)
      return (
         <Alert severity="error" sx={{ m: 2 }}>
            This {noun} cannot be opened: {readFailure}
         </Alert>
      );
   const draftDiffers = draft !== undefined && draft !== packageText;
   return (
      <Stack sx={{ gap: 2 }}>
         {modelQuery.isError && (
            <Alert severity="warning">
               The {noun} could not be re-read from the server:{" "}
               {modelQuery.error?.message}. Your edits are still here.
            </Alert>
         )}
         {!notebook &&
            !authoritative &&
            offered &&
            draftDiffers &&
            resume === undefined && (
               <Alert
                  severity="info"
                  action={
                     <Stack direction="row" sx={{ gap: 1 }}>
                        <Button size="small" onClick={() => choose(true)}>
                           Resume
                        </Button>
                        <Button size="small" onClick={() => choose(false)}>
                           Start from the package
                        </Button>
                     </Stack>
                  }
               >
                  You have edits to this dashboard saved in this browser that
                  the package does not have.
               </Alert>
            )}
         {held !== undefined && (
            <Alert
               severity="warning"
               action={
                  <Button size="small" onClick={acceptHeld}>
                     Load it
                  </Button>
               }
            >
               {savesTo === "package"
                  ? "This dashboard changed since you opened it. Your edits are still here; loading the new version replaces them, and until you do, saving is refused."
                  : savesTo === "host"
                    ? "This dashboard changed since you opened it. Your edits are still here; loading the new version replaces them, and saving keeps yours and writes over it."
                    : "The package's copy of this dashboard changed since you opened it. Your edits are still here; loading the new version replaces them."}
            </Alert>
         )}
         {supersedeFailure !== undefined && (
            <Alert severity="warning">
               The file was saved into the package, but the copy kept beside it
               could not be cleared, so it will be offered again:{" "}
               {supersedeFailure}
            </Alert>
         )}
         {opened && (
            <EditorSurface
               key={opened.generation}
               kind={kind}
               environmentName={environmentName}
               packageName={packageName}
               modelPath={modelPath}
               slug={dashboardName}
               modelGivens={
                  (modelQuery.data?.data as { givens?: Given[] } | undefined)
                     ?.givens
               }
               {...(versionId !== undefined ? { versionId } : {})}
               opened={opened}
               {...(fromDraft &&
               writer === "package" &&
               opened.packageText !== undefined
                  ? { replaces: opened.packageText }
                  : {})}
               onSave={save}
               onDirtyChange={reportDirty}
               {...(onExit ? { onExit } : {})}
               savesTo={savesTo}
               {...(saveLabel ? { saveLabel } : {})}
               {...(onEvent ? { onEvent: reportEvent } : {})}
               note={note}
            />
         )}
      </Stack>
   );
}
