// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { clearReportDraft, reportDraftKey } from "./useReportDraft";
import { Alert, Button, Stack } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useRef } from "react";
import type { CompiledModel } from "../../client";
import type { HostGivenValue } from "../../hooks/givenValue";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import {
   failureCategory,
   reportEmitter,
   type ReportEvent,
   type ReportEventHandler,
} from "../Report/telemetry";
import { now } from "../../utils/clock";
import { useOptionalDocumentStorage } from "../DocumentStorage";
import type { ReactNode } from "react";
import { Loading } from "../Loading";
import { documentSlug } from "../Package/documentLocation";
import { useServer } from "../ServerProvider";
import {
   layoutOfKind,
   legacyNamedPath,
   type DocumentKind,
} from "../legacy/kindAdapter";
import type { ReportLayout } from "../Package/documentLocation";
import {
   resolveEditorTarget,
   saveCaption,
   withWorkspace,
} from "./documentSession";
import { EditorSurface } from "./EditorSurface";
import { useHostCopy } from "./useHostCopy";
import { useOpenedDocument } from "./useOpenedDocument";
import { useSaveChannel } from "./useSaveChannel";
import type { BuilderControlsHandler } from "./builderControls";

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
export type ReportEditorProps = (
   | {
        /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
        resourceUri: string;
        /**
         * The report's package path, `sales/q3.malloy`: its identity, and what
         * the editor is keyed by. Give this.
         */
        path: string;
        /** A display label; default the path's basename. */
        dashboard?: string;
     }
   | {
        /** `publisher://environments/{env}/packages/{pkg}`, optionally `?versionId=`. */
        resourceUri: string;
        /**
         * @deprecated Pass `path`. The report's basename, which opens
         * `<kind>s/<name>.malloy` (`dashboards/` unless `kind` says
         * `notebook`) when no `path` is given.
         */
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
    * What the editor does — opened, failed to load, saved, refused, switched
    * layout — for the host to log or count; see `ReportEvent`.
    */
   onEvent?: ReportEventHandler;
   /**
    * What to draw while the report opens (its text read, the builder's data
    * loaded); default a spinner. A host that just showed the report can pass
    * the report itself, so switching to edit replaces the page in place
    * rather than blanking it to a spinner first.
    */
   loading?: ReactNode;
   /**
    * The report's layout as the host knows it before the text is read: from
    * the package's listing, or for a draft the layout its store recorded.
    * Default `grid`. It decides what is read when the editor opens (a linear
    * report's copy is read back only from an authoritative store) and is what
    * a save records until the text says otherwise; once open, the document's
    * own tag decides how it is drawn, what a save records and which events it
    * reports.
    */
   layout?: ReportLayout;
   /** @deprecated Pass `layout`: `dashboard` is grid, `notebook` linear. */
   kind?: DocumentKind;
   /**
    * The report's file within the package, wherever it lives: its identity.
    * Without it, the deprecated name forms open `<kind>s/<name>.malloy`.
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
   /**
    * Open a model by its package path (`storefront.malloy`). Offered as "Open
    * the model" on a filter a model sets, which the builder can set but not
    * change; omitted, the filter only says where it is set.
    */
   onOpenModel?: (modelPath: string) => void;
   /**
    * Take over the builder's page-level controls (save status and Save, undo
    * and redo, Add): handed over as `BuilderControls`
    * whenever they change, with no toolbar drawn on the page. Draw them with
    * `BuilderBar` in the host's header, or place each in its own chrome.
    */
   onControlsChange?: BuilderControlsHandler;
   /**
    * Keep unsaved edits in this browser, per report, and put them back when
    * the report is next opened for editing here: leaving never loses work, so
    * the host's way out can be the controls' `done` (write, then leave) with
    * no "leave with unsaved changes?" on the way.
    */
   keepDraft?: boolean;
   /** Edit a document held as text; see {@link TextSourceOptions}. */
   textSource?: TextSourceOptions;
   /**
    * The package version the builder's queries run at, apart from the
    * document's own identity: every tile, the report's manifest, filter
    * options and suggestions, the report listing and the field catalog.
    *
    * The document is still the one `resourceUri` and `path` name, so the kept
    * draft, where a save goes, the save caption and whether a save is a pinned
    * one all follow `resourceUri` alone. A host editing a package that is
    * republished on every change passes an unversioned `resourceUri` and the
    * version its reader ran at here: the builder then reuses the reader's
    * results instead of running every tile again, and kept edits survive the
    * next republish.
    *
    * Unset, queries run at the `resourceUri`'s version, as before. A text-held
    * document's compile has no version, so it is compiled as today; its tiles
    * do run at this version.
    */
   queryVersionId?: string;
};

/**
 * Open a document the host keeps as TEXT, rather than a file in a package.
 *
 * The text comes from the host's authoritative {@link DocumentStorage} and is
 * compiled by the server, as the viewer, on top of `modelPath`: the model whose
 * sources the document may name. The manifest is that compile's `document`, and
 * every tile, cell and control option runs as the document's definitions
 * followed by one `run:`, so each viewer sees what their own identity allows.
 * The server's compile refuses an `import`, `##!` or `given:` in the text, so
 * "Add filter" is off and only the model's givens can be bound, unless the
 * host resolves the document's own givens itself (`compileSource`).
 */
export interface TextSourceOptions {
   /** The model the text is compiled and run on top of. */
   modelPath: string;
   /** Givens the host sets itself: no control is shown for them. */
   hiddenGivens?: readonly string[];
   /** The values for the givens the host sets: sent with the compile and with each tile that reads one. */
   givens?: Record<string, HostGivenValue>;
   /**
    * The text to compile in place of the document's own, for a host that
    * resolves what the server's compile refuses in held text: the document's
    * own `given:` declarations, say, written out as literal values. Called
    * with the text as opened; the editor still edits and saves that text.
    *
    * A host that passes it owns how its text compiles, so "Add filter" is
    * on: it writes a `given:` into the text like any report, and the builder
    * runs each tile with that filter's value written in as a literal, since
    * the server does not know the given. Without it, "Add filter" is off,
    * because the server refuses a `given:` in held text.
    */
   compileSource?: (text: string) => string | Promise<string>;
   /**
    * Names a filter added here must not take: with `compileSource`, every
    * given the model declares, including one only a gate reads, which the
    * page never shows. A document given of the same name would collide with
    * the model's once the report is published.
    */
   takenGivenNames?: readonly string[];
}

export function ReportEditor(props: ReportEditorProps) {
   const {
      onEvent,
      loading,
      onDirtyChange,
      onExit,
      onOpenModel,
      onControlsChange,
      keepDraft,
      kind,
      path,
      textSource,
      queryVersionId,
   } = props;
   const layout: ReportLayout =
      props.layout ?? (kind ? layoutOfKind(kind) : "grid");
   const notebook = layout === "linear";
   const textHeld = textSource !== undefined;
   // Degraded, not thrown, on a bad URI: a throw in a render body takes the host's whole tree down.
   const {
      environmentName,
      packageName,
      versionId,
      namesBoth: uriNamesBoth,
   } = resolveEditorTarget(props);
   const named = "resourceUri" in props ? props.dashboard : props.dashboardName;
   // The label the builder shows; the path is what it opens.
   const dashboardName = named ?? documentSlug(path) ?? "";
   const noun = "report";

   const { apiClients, mutable, isLoadingStatus, server } = useServer();
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
   const modelPath =
      path ?? legacyNamedPath(kind ?? "dashboard", dashboardName);
   const legacyFormat =
      /\.malloynb$/i.test(modelPath) ||
      (notebook && /\.malloynb$/i.test(dashboardName));

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
      enabled: uriNamesBoth && !legacyFormat && !textHeld,
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
      layout,
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
      layout,
      notebook,
      noun,
      legacyFormat,
      textHeld,
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
      layout,
      notebook,
      environmentName,
      packageName,
      modelPath,
      versionId,
      apiClients,
      queryClient,
      mutable,
      textHeld,
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
      (event: ReportEvent) => {
         onEventRef.current?.(withWorkspace(event, workspaceName));
      },
      [workspaceName],
   );
   // The file could not be fetched before anything opened: once per mount,
   // since a refetch that fails again is the same failure.
   const fetchFailed = modelQuery.isError && !opened;
   const fetchFailureReported = useRef(false);
   useEffect(() => {
      if (!fetchFailed || fetchFailureReported.current) return;
      fetchFailureReported.current = true;
      reportEmitter({ path: modelPath, layout }, onEventRef.current).emit({
         type: "report.load_failed",
         surface: "edit",
         category: failureCategory(modelQuery.error),
      });
   }, [fetchFailed, modelQuery.error, modelPath, layout]);
   // Stable: the builder fires this from an effect that depends on it.
   // The editor's own record is every unsaved edit: it decides whether a
   // newer version of the file may replace what is open, and with a draft
   // kept, reporting only what leaving would lose let the other author's
   // version replace the author's edits. The host's leave guard gets what
   // leaving would lose.
   const reportDirty = useCallback(
      (value: boolean, wouldLose: boolean) => {
         setDirty(value);
         onDirtyChangeRef.current?.(wouldLose);
      },
      [setDirty],
   );

   // One per report, version and server: a draft is never restored onto a
   // file it was not made on.
   const draftKey = keepDraft
      ? reportDraftKey({
           server,
           environmentName,
           packageName,
           versionId,
           path: path ?? dashboardName,
        })
      : undefined;
   const saveLabel =
      savesTo === "host" && workspace?.description
         ? workspace.description
         : undefined;
   const caption =
      !authoritative && !textHeld && mutable === undefined
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
   // Only what is out of the ordinary: Save off, a pinned version, a copy
   // that could not be read. Where an ordinary save goes is the bar's status
   // to say (its tooltip), and a line under every report repeating it was
   // noise. The description is already the Save caption, so it is not
   // repeated either.
   const ordinary =
      savesTo === "package" &&
      takesWrites === true &&
      !pinnedPackageSave &&
      readFailure === undefined;
   const note = ordinary || caption === saveLabel ? "" : caption;

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
   if (!opened && (!(textHeld || packageText) || !draftChecked))
      return loading ?? <Loading text={`Opening the ${noun}…`} />;
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
                  You have edits to this report saved in this browser that the
                  package does not have.
               </Alert>
            )}
         {held !== undefined && (
            <Alert
               severity="warning"
               action={
                  <Button
                     size="small"
                     onClick={() => {
                        // Loading the other version is dropping these edits,
                        // so the draft holding them goes too: kept, it would
                        // be offered straight back over the version chosen.
                        if (draftKey !== undefined) clearReportDraft(draftKey);
                        acceptHeld();
                     }}
                  >
                     Load it
                  </Button>
               }
            >
               {savesTo === "package"
                  ? "This report changed since you opened it. Your edits are still here; loading the new version replaces them, and until you do, saving is refused."
                  : savesTo === "host"
                    ? "This report changed since you opened it. Your edits are still here; loading the new version replaces them, and saving keeps yours and writes over it."
                    : "The package's copy of this report changed since you opened it. Your edits are still here; loading the new version replaces them."}
            </Alert>
         )}
         {supersedeFailure !== undefined && (
            <Alert severity="warning">
               The file was saved into the package, but the copy kept beside it
               could not be cleared, so it will be offered again:{" "}
               {supersedeFailure}
            </Alert>
         )}
         {/* The text is in hand but the document opens a pass later: the
          loading view meanwhile, never an empty page. */}
         {!opened && (loading ?? <Loading text={`Opening the ${noun}…`} />)}
         {opened && (
            <EditorSurface
               key={opened.generation}
               environmentName={environmentName}
               packageName={packageName}
               modelPath={modelPath}
               {...(textSource ? { textSource } : {})}
               slug={dashboardName}
               {...(versionId !== undefined ? { versionId } : {})}
               {...((queryVersionId ?? versionId) !== undefined
                  ? { queryVersionId: queryVersionId ?? versionId }
                  : {})}
               opened={opened}
               {...(fromDraft &&
               writer === "package" &&
               opened.packageText !== undefined
                  ? { replaces: opened.packageText }
                  : {})}
               onSave={save}
               onDirtyChange={reportDirty}
               {...(onExit ? { onExit } : {})}
               {...(onOpenModel ? { onOpenModel } : {})}
               {...(onControlsChange ? { onControlsChange } : {})}
               {...(draftKey !== undefined ? { draftKey } : {})}
               savesTo={savesTo}
               {...(saveLabel ? { saveLabel } : {})}
               {...(onEvent ? { onEvent: reportEvent } : {})}
               note={note}
               {...(!textSource && modelQuery.data?.data
                  ? { fileModel: modelQuery.data.data as CompiledModel }
                  : {})}
            />
         )}
      </Stack>
   );
}
