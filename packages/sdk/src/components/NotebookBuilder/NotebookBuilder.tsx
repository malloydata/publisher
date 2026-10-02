// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import DragIndicatorIcon from "@mui/icons-material/DragIndicator";
import PlaylistAddIcon from "@mui/icons-material/PlaylistAdd";
import VerticalAlignBottomIcon from "@mui/icons-material/VerticalAlignBottom";
import VerticalAlignTopIcon from "@mui/icons-material/VerticalAlignTop";
import { DragDropProvider } from "@dnd-kit/react";
import { useSortable } from "@dnd-kit/react/sortable";
import CheckIcon from "@mui/icons-material/Check";
import {
   Alert,
   Box,
   Button,
   IconButton,
   Stack,
   TextField,
   Tooltip,
} from "@mui/material";
import {
   useCallback,
   useEffect,
   useMemo,
   useId,
   useRef,
   useState,
   type KeyboardEvent,
   type ReactNode,
} from "react";
import type { Given } from "../../client";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import { GIVEN_SETTLE_MS, useSettled } from "../../hooks/useSettled";
import { SecondaryButton } from "../buttons";
import { now } from "../Dashboard/telemetry";
import { UnsavedChangesDialog } from "../UnsavedChangesDialog";
import { BuilderToolbar } from "../DashboardBuilder/BuilderToolbar";
import { DiffDialog } from "../DashboardBuilder/DiffDialog";
import type { CatalogSource } from "../DashboardBuilder/catalog";
import { builderSensors } from "../DashboardBuilder/sortable";
import { useBuilderShortcuts } from "../DashboardBuilder/useBuilderShortcuts";
import { useExitGuard } from "../DashboardBuilder/useExitGuard";
import type { NavigationClick } from "../click_helper";
import { GivensPanel } from "../given";
import { givensToRequest } from "../given/paramCodec";
import type { ProseLinkContext } from "../Prose";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { AddQueryDialog } from "./AddQueryDialog";
import { mergeSources, notebookImports } from "./imports";
import { cellQueries, cellSlices, runTargetOf, withChart } from "./cellText";
import { ChartPicker } from "../DashboardBuilder/ChartPicker";
import { chartLocked, pickerState } from "./cellChart";
import {
   DefinitionCell,
   MarkdownCell,
   QueryCell,
   type QueryTarget,
} from "./EditorCells";
import { queryCellText, type QueryRun } from "./queryCell";
import { QueryCaptionField } from "./QueryCaptionField";
import type { NotebookSource } from "./readNotebookSource";
import {
   canInsertQuery,
   notebookDocumentOf,
   type NotebookDocument,
   type NotebookDocumentCell,
} from "./spliceNotebook";
import type { NotebookEventHandler } from "./telemetry";
import { useCellReorder } from "./useCellReorder";
import { useNotebookEditor } from "./useNotebookEditor";

/** Query cells preview their own text, less its prose notes, through the model query route. */
export interface NotebookBuilderProps extends QueryTarget {
   /** The file being edited. */
   source: string;
   /** `readNotebookSource(source)`: where each cell is in that file. */
   notebook: NotebookSource;
   /** What the notebook's own compiled model offers; an added query picks from it. Absent while that model loads. */
   sources?: CatalogSource[];
   /** The notebook's model could not be read, so `sources` will not arrive. */
   sourcesFailed?: boolean;
   /** Sources the notebook imports, with their views, once read; a curated package's model leaves them out of `sources`. */
   importedSources?: CatalogSource[];
   /** Imported sources are still being read. */
   importsPending?: boolean;
   /** Package paths of imports that could not be read, so their sources are not offered. */
   importsFailed?: string[];
   /** The add-query dialog or a chart picker was opened: the host may now read the imported sources. */
   onSourcesWanted?: () => void;
   /** The notebook's `given:` declarations, for the control row. */
   givens?: Given[];
   /** Where the controls start, from the notebook's `## givens { … }`. */
   startingGivens?: Record<string, string>;
   /** False holds control changes behind Apply, from the notebook's `autorun=false`. */
   autorun?: boolean;
   /** Persist the patched file. Left out, the builder edits without saving. */
   onSave?: (source: string) => Promise<void> | void;
   /** The document as it stands, on every edit and on mount. */
   onChange?: (document: NotebookDocument) => void;
   /** Whether the document differs from what was last saved, on every change and on mount. */
   onDirtyChange?: (dirty: boolean) => void;
   /** Saves and refusals, for the host to log. */
   onEvent?: NotebookEventHandler;
   /** Where `onSave` puts the file, for the event it reports. */
   savesTo?: "package" | "browser" | "host";
   /** The host's own actions for the edit bar, such as Done. */
   toolbar?: ReactNode;
   /** Leave the builder: renders "Done editing", which asks first when there are unsaved edits. */
   onExit?: () => void;
   /** SPA navigation for links in prose. */
   onNavigate?: (to: string, event?: NavigationClick) => void;
   maxResultSize?: number;
}

const CELL_TYPE = "notebook-cell";

const KIND_LABEL: Record<NotebookDocumentCell["kind"], string> = {
   markdown: "text",
   query: "query",
   definition: "definition",
};

/** A cell as a sortable item; a component because a hook cannot be called from a list's callback. */
function CellSortable({
   id,
   index,
   children,
}: {
   id: string;
   index: number;
   children: (handles: {
      ref: (element: Element | null) => void;
      handleRef: (element: Element | null) => void;
      isDragSource: boolean;
   }) => ReactNode;
}) {
   const { ref, handleRef, isDragSource } = useSortable({
      id,
      index,
      type: CELL_TYPE,
      accept: CELL_TYPE,
   });
   return <>{children({ ref, handleRef, isDragSource })}</>;
}

const VISUALLY_HIDDEN = {
   position: "absolute",
   width: 1,
   height: 1,
   overflow: "hidden",
   clip: "rect(0 0 0 0)",
   whiteSpace: "nowrap",
} as const;

/** Off, not `disabled`: it stays focusable, a screen reader gets the reason, and a click says it too. */
const offProps = (reasonId: string) => ({
   "aria-disabled": true,
   "aria-describedby": reasonId,
   disableRipple: true,
   sx: { opacity: 0.5, cursor: "not-allowed" },
});

function CellButton({
   label,
   disabled,
   reason,
   onClick,
   onBlocked,
   children,
}: {
   label: string;
   disabled?: boolean;
   /** Why the button is off, read with its name and shown when it is clicked. */
   reason?: string;
   onClick: () => void;
   /** Told the reason when an off button is clicked. */
   onBlocked?: (reason: string) => void;
   children: ReactNode;
}) {
   const reasonId = useId();
   const off = disabled === true && reason !== undefined;
   return (
      <Tooltip title={off ? `${label}: ${reason}` : label}>
         <IconButton
            size="small"
            aria-label={label}
            {...(off ? offProps(reasonId) : {})}
            onClick={() => (off ? onBlocked?.(reason) : onClick())}
         >
            {children}
            {off && (
               <Box component="span" id={reasonId} sx={VISUALLY_HIDDEN}>
                  {reason}
               </Box>
            )}
         </IconButton>
      </Tooltip>
   );
}

export function NotebookBuilder({
   source,
   notebook,
   sources,
   sourcesFailed,
   importedSources,
   importsPending,
   importsFailed,
   onSourcesWanted,
   environmentName,
   packageName,
   modelPath,
   versionId,
   givens,
   startingGivens,
   autorun = true,
   onSave,
   onChange,
   onDirtyChange,
   onEvent,
   savesTo = "package",
   toolbar,
   onExit,
   onNavigate,
   maxResultSize,
}: NotebookBuilderProps) {
   const initial = useMemo(() => notebookDocumentOf(notebook), [notebook]);
   // Keyed on the names, so a re-read of the model that changes nothing does not re-create the editor's writer.
   const imports = useMemo(
      () => notebookImports(notebook, modelPath),
      [notebook, modelPath],
   );
   const offered = useMemo(
      () => (sources ? mergeSources(sources, importedSources) : undefined),
      [sources, importedSources],
   );
   // A named import is reachable from the file's own text, before its model has been read.
   const reachableKey = JSON.stringify(
      offered
         ? [
              ...new Set([
                 ...offered.map((s) => s.name),
                 ...imports.flatMap((i) =>
                    i.kind === "names" ? i.names.map((n) => n.as) : [],
                 ),
              ]),
           ]
         : null,
   );
   const reachableSources = useMemo(
      () => (JSON.parse(reachableKey) as string[] | null) ?? undefined,
      [reachableKey],
   );
   const editor = useNotebookEditor({
      source,
      document: initial,
      ...(onSave ? { onSave } : {}),
      ...(reachableSources ? { reachableSources } : {}),
   });
   const { document: doc } = editor;
   // The file as opened: a read cell's text there, which the document overrides for a chart edit or an added query.
   const slices = useMemo(() => cellSlices(notebook), [notebook]);
   const queries = useMemo(() => cellQueries(notebook), [notebook]);
   const [editing, setEditing] = useState<string | undefined>(undefined);
   const [notice, setNotice] = useState<string | undefined>(undefined);
   const [saving, setSaving] = useState(false);
   const [adding, setAdding] = useState<number | undefined>(undefined);
   const [pendingSave, setPendingSave] = useState<
      | {
           before: string;
           after: string;
           removedComments: string[];
           clearsHistory: boolean;
           structural: boolean;
        }
      | undefined
   >(undefined);
   const nextId = useRef(0);
   const blockedId = useId();
   // The would-be file, shown to select by hand when the clipboard is not there to take it.
   const [selectable, setSelectable] = useState<string | undefined>(undefined);

   useEffect(() => {
      onChange?.(doc);
   }, [doc, onChange]);
   useEffect(() => {
      onDirtyChange?.(editor.dirty);
   }, [editor.dirty, onDirtyChange]);
   // A host guarding navigation on this must not be left holding a stale "dirty" once the builder is gone.
   const onDirtyChangeRef = useRef(onDirtyChange);
   onDirtyChangeRef.current = onDirtyChange;
   useEffect(() => () => onDirtyChangeRef.current?.(false), []);

   const controls = useDocumentControls({
      specs: givens ?? [],
      loaded: true,
      startingValues: startingGivens,
      documentKey: modelPath,
      autorun,
      environmentName,
      packageName,
      modelPath,
      versionId,
      documentName: modelPath,
   });
   const { applied, declaredTypes } = controls;
   const typed = useMemo(
      () => givensToRequest(applied, declaredTypes),
      [applied, declaredTypes],
   );
   // A text control autoruns per keystroke; every query cell would put a query on the warehouse each time. Apply is a click, so it runs at once.
   const settled = useSettled(typed, GIVEN_SETTLE_MS);
   const request = autorun ? settled : typed;
   const target = useMemo(
      () => ({
         environmentName,
         packageName,
         modelPath,
         ...(versionId ? { versionId } : {}),
      }),
      [environmentName, packageName, modelPath, versionId],
   );
   const links: ProseLinkContext = {
      environmentName,
      packageName,
      sourcePath: modelPath,
      onNavigate,
   };

   const update = useCallback(
      (change: (draft: NotebookDocument) => void) => {
         setNotice(undefined);
         editor.update(change);
      },
      [editor],
   );

   /** Why `from` cannot go to `to`, or undefined when it can. */
   const moveBlocked = useCallback(
      (from: number, to: number) => {
         if (to < 0) return "This is already the first cell.";
         if (to >= doc.cells.length) return "This is already the last cell.";
         if (editor.canMove(from, to)) return undefined;
         return doc.cells[from].kind === "definition"
            ? "Setup lines stay where they are."
            : "Queries stay below the setup lines they may read.";
      },
      [doc.cells, editor],
   );

   const moveCell = useCallback(
      (from: number, to: number) => {
         const blocked = moveBlocked(from, to);
         if (blocked) {
            setNotice(blocked);
            return;
         }
         update((draft) => {
            const [cell] = draft.cells.splice(from, 1);
            draft.cells.splice(to, 0, cell);
         });
      },
      [moveBlocked, update],
   );

   const altArrowMove = (event: KeyboardEvent<HTMLElement>, at: number) => {
      if (!event.altKey) return;
      if (event.key === "ArrowUp") {
         event.preventDefault();
         moveCell(at, at - 1);
      } else if (event.key === "ArrowDown") {
         event.preventDefault();
         moveCell(at, at + 1);
      }
   };

   const freshId = () => {
      const taken = new Set(doc.cells.map((cell) => cell.id));
      let id: string;
      do id = `added-${++nextId.current}`;
      while (taken.has(id));
      return id;
   };

   const addText = (at: number) => {
      const id = freshId();
      update((draft) => {
         draft.cells.splice(at, 0, {
            id,
            kind: "markdown",
            markdown: "",
            added: true,
         });
      });
      setEditing(id);
   };

   const openAddQuery = (at: number) => {
      onSourcesWanted?.();
      setAdding(at);
   };

   const addQuery = (at: number, run: QueryRun) => {
      const id = freshId();
      update((draft) => {
         // Explicit, so a later chart pick on the saved cell is a change from "default", which undo can step back over.
         draft.cells.splice(at, 0, {
            id,
            kind: "query",
            run,
            chart: "default",
            added: true,
         });
      });
      setAdding(undefined);
   };

   const removeCell = (index: number) => {
      setEditing(undefined);
      update((draft) => {
         draft.cells.splice(index, 1);
      });
   };

   /** Why a query cannot be added at `at`, or undefined when it can. */
   const queryBlocked = (at: number) => {
      if (sources === undefined)
         return sourcesFailed
            ? "The notebook's sources could not be read."
            : "The notebook's sources are loading.";
      if (sources.length === 0 && imports.length === 0)
         return "This notebook reads no source.";
      if (offered?.length === 0 && importsFailed?.length)
         return `Could not read ${importsFailed.join(", ")}, which this notebook imports.`;
      if (!canInsertQuery(doc, at))
         return "Queries go below the setup lines (imports, givens, saved queries). Add it further down.";
      return undefined;
   };

   const { preview, dragging, onDragStart, onDragOver, onDragEnd } =
      useCellReorder({
         ids: doc.cells.map((cell) => cell.id),
         canMove: editor.canMove,
         commit: moveCell,
      });

   const copyChanges = () => {
      void editor.preview().then(async (result) => {
         if (!result.ok) {
            setNotice(
               "There is no file to copy until the problem above is fixed.",
            );
            return;
         }
         try {
            await navigator.clipboard.writeText(result.source);
            setSelectable(undefined);
            setNotice("Copied the notebook as it would have been saved.");
         } catch {
            setSelectable(result.source);
         }
      });
   };

   const commitSave = useCallback(() => {
      setPendingSave(undefined);
      setSaving(true);
      const started = now();
      const cells = editor.document.cells.length;
      const structural = editor.structural;
      return editor
         .save()
         .then((outcome) => {
            if (outcome.ok === true)
               onEvent?.({
                  type: "notebook.saved",
                  cells,
                  where: savesTo,
                  structural,
                  durationMs: now() - started,
               });
            else
               onEvent?.({
                  type: "notebook.save_refused",
                  reason: outcome.reason,
               });
         })
         .finally(() => setSaving(false));
   }, [editor, onEvent, savesTo]);

   const save = useCallback((): Promise<void> | void => {
      if (!onSave || !editor.dirty || saving) return;
      if (!editor.structural && !editor.clearsHistory) return commitSave();
      return Promise.all([editor.preview(), editor.removedComments()]).then(
         ([result, removedComments]) => {
            if (result.ok)
               setPendingSave({
                  before: editor.source,
                  after: result.source,
                  removedComments,
                  clearsHistory: editor.clearsHistory,
                  structural: editor.structural,
               });
            else return commitSave();
         },
      );
   }, [onSave, editor, saving, commitSave]);
   const exitGuard = useExitGuard({
      dirty: editor.dirty,
      saving,
      reviewing: pendingSave !== undefined,
      canSave: !!onSave,
      save,
      onExit: () => onExit?.(),
   });

   useBuilderShortcuts(
      useMemo(
         () => ({
            undo: editor.undo,
            redo: editor.redo,
            ...(onSave ? { save } : {}),
            // The open text field handles its own Escape, committing; dropping it here would lose the draft.
            escape: () => {},
         }),
         [editor, onSave, save],
      ),
   );

   const byId = new Map(doc.cells.map((cell) => [cell.id, cell]));
   const shown = preview
      ? preview.flatMap((id) => byId.get(id) ?? [])
      : doc.cells;
   const readById = new Map(notebook.cells.map((cell) => [cell.id, cell]));

   /** A query cell's text and runnable text from the document: an added cell's own, a read cell's with the chart line the picker holds. */
   const queryDisplay = (cell: NotebookDocumentCell) => {
      if (cell.added && cell.run)
         return {
            text: queryCellText(cell.run, cell.chart),
            query: queryCellText(
               { source: cell.run.source, view: cell.run.view },
               cell.chart,
            ),
         };
      const text = slices.get(cell.id) ?? "";
      const query = queries.get(cell.id) ?? "";
      const chart = readById.get(cell.id)?.chart;
      if (!chart || chartLocked(chart)) return { text, query };
      const lines = chart.lines.map((line) => line.text);
      return {
         text: withChart(text, cell.chart, lines),
         query: withChart(query, cell.chart, lines),
      };
   };

   const chartPicker = (cell: NotebookDocumentCell, index: number) => {
      const read = readById.get(cell.id);
      const run = cell.added
         ? cell.run
         : runTargetOf(slices.get(cell.id) ?? "");
      const view = offered
         ?.find((s) => s.name === run?.source)
         ?.views.find((v) => v.name === run?.view);
      const locked = cell.added ? undefined : chartLocked(read?.chart);
      return (
         <ChartPicker
            state={pickerState(cell.chart, read?.chart)}
            view={view}
            {...(view ? {} : { viewStatus: offered ? "unlisted" : "loading" })}
            cellLabel={`cell ${index + 1}`}
            {...(onSourcesWanted ? { onOpen: onSourcesWanted } : {})}
            {...(locked ? { disabledReason: locked } : {})}
            onChange={(next) =>
               update((draft) => {
                  const target = draft.cells.find((c) => c.id === cell.id);
                  if (target) target.chart = next;
               })
            }
         />
      );
   };

   const renderCell = (cell: NotebookDocumentCell) => {
      if (cell.kind === "markdown")
         return (
            <MarkdownCell
               markdown={cell.markdown ?? ""}
               editing={editing === cell.id}
               links={links}
               onEdit={() => setEditing(cell.id)}
               onCommit={(next) =>
                  update((draft) => {
                     const target = draft.cells.find((c) => c.id === cell.id);
                     if (target) target.markdown = next;
                  })
               }
               onClose={() => {
                  setEditing((was) => (was === cell.id ? undefined : was));
                  // A cell added this session that ends empty was never wanted; it would only be refused at Save.
                  if (!editor.isInFile(cell.id))
                     update((draft) => {
                        const at = draft.cells.findIndex(
                           (c) => c.id === cell.id,
                        );
                        const target = draft.cells[at];
                        if (target?.added && !(target.markdown ?? "").trim())
                           draft.cells.splice(at, 1);
                     });
               }}
            />
         );
      const markdown = readById.get(cell.id)?.markdown;
      if (cell.kind === "definition")
         return (
            <DefinitionCell
               text={slices.get(cell.id) ?? ""}
               {...(markdown ? { markdown } : {})}
               links={links}
            />
         );
      const { text, query } = queryDisplay(cell);
      return (
         <Stack spacing={1}>
            <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
               {chartPicker(
                  cell,
                  doc.cells.findIndex((c) => c.id === cell.id),
               )}
               {cell.added && cell.run && !editor.isInFile(cell.id) && (
                  <QueryCaptionField
                     caption={cell.run.caption ?? ""}
                     onCommit={(next) =>
                        update((draft) => {
                           const target = draft.cells.find(
                              (c) => c.id === cell.id,
                           );
                           if (target?.run) {
                              if (next.trim()) target.run.caption = next.trim();
                              else delete target.run.caption;
                           }
                        })
                     }
                  />
               )}
            </Stack>
            <QueryCell
               text={text}
               query={query}
               {...(markdown ? { markdown } : {})}
               links={links}
               target={target}
               givens={request}
               {...(maxResultSize !== undefined ? { maxResultSize } : {})}
            />
         </Stack>
      );
   };

   return (
      <Stack sx={{ gap: 0 }}>
         <BuilderToolbar
            canUndo={editor.canUndo}
            canRedo={editor.canRedo}
            onUndo={editor.undo}
            onRedo={editor.redo}
            dirty={editor.dirty}
            saving={saving}
            {...(onSave ? { onSave: save } : {})}
            {...(toolbar || onExit
               ? {
                    actions: (
                       <>
                          {toolbar}
                          {onExit && (
                             <SecondaryButton
                                label="Done editing"
                                icon={<CheckIcon />}
                                onClick={exitGuard.requestExit}
                             />
                          )}
                       </>
                    ),
                 }
               : {})}
         />
         <CleanNotebookContainer>
            <CleanNotebookSection>
               <Stack spacing={2} component="section">
                  <GivensPanel {...controls.panel} />
                  {editor.error && (
                     // The edit is still here; this says what kept it from the file.
                     <Alert
                        severity="warning"
                        action={
                           <Button
                              color="inherit"
                              size="small"
                              onClick={copyChanges}
                           >
                              Copy my changes
                           </Button>
                        }
                     >
                        {editor.error}
                     </Alert>
                  )}
                  {editor.error && selectable !== undefined && (
                     <TextField
                        multiline
                        minRows={4}
                        maxRows={12}
                        fullWidth
                        value={selectable}
                        inputProps={{
                           readOnly: true,
                           "aria-label": "Your changes",
                        }}
                        onFocus={(event) => event.target.select()}
                     />
                  )}
                  {notice && (
                     <Alert severity="info" role="status">
                        {notice}
                     </Alert>
                  )}
                  {doc.cells.length === 0 && (
                     <Stack direction="row" spacing={1}>
                        <Button
                           startIcon={<AddIcon />}
                           onClick={() => addText(0)}
                        >
                           Add text
                        </Button>
                        <Button
                           startIcon={<PlaylistAddIcon />}
                           aria-label="Add query"
                           {...(queryBlocked(0) ? offProps(blockedId) : {})}
                           onClick={() => {
                              const blocked = queryBlocked(0);
                              if (blocked) setNotice(blocked);
                              else openAddQuery(0);
                           }}
                        >
                           Add query
                           {queryBlocked(0) && (
                              <Box
                                 component="span"
                                 id={blockedId}
                                 sx={VISUALLY_HIDDEN}
                              >
                                 {queryBlocked(0)}
                              </Box>
                           )}
                        </Button>
                     </Stack>
                  )}
                  <DragDropProvider
                     sensors={builderSensors}
                     onDragStart={(event) => {
                        const id = event.operation.source?.id;
                        if (id !== undefined) onDragStart(String(id));
                     }}
                     onDragOver={onDragOver}
                     onDragEnd={onDragEnd}
                  >
                     {shown.map((cell, index) => {
                        const at = doc.cells.findIndex((c) => c.id === cell.id);
                        const refused =
                           dragging !== undefined &&
                           dragging !== at &&
                           !editor.canMove(dragging, at);
                        const label = `Cell ${at + 1}, ${KIND_LABEL[cell.kind]}`;
                        return (
                           <CellSortable
                              key={cell.id}
                              id={cell.id}
                              index={index}
                           >
                              {({ ref, handleRef, isDragSource }) => (
                                 <Box
                                    ref={ref}
                                    role="group"
                                    aria-label={label}
                                    tabIndex={0}
                                    data-drop={refused ? "refused" : undefined}
                                    onKeyDown={(event) => {
                                       if (event.target !== event.currentTarget)
                                          return;
                                       altArrowMove(event, at);
                                    }}
                                    sx={{
                                       position: "relative",
                                       pl: 5,
                                       opacity: isDragSource ? 0.4 : 1,
                                       ...(refused && {
                                          cursor: "not-allowed",
                                          outline: "2px dashed",
                                          outlineColor: "divider",
                                          outlineOffset: 4,
                                          borderRadius: 1,
                                          filter: "grayscale(1)",
                                       }),
                                       "&:hover .notebook-cell-tools, &:focus-within .notebook-cell-tools":
                                          { opacity: 1 },
                                    }}
                                 >
                                    <Box
                                       ref={handleRef}
                                       aria-label={`Move ${label}`}
                                       onKeyDown={(event) =>
                                          altArrowMove(event, at)
                                       }
                                       sx={{
                                          position: "absolute",
                                          left: 0,
                                          top: 0,
                                          cursor: "grab",
                                          color: "text.secondary",
                                       }}
                                    >
                                       <DragIndicatorIcon fontSize="small" />
                                    </Box>
                                    <Stack
                                       direction="row"
                                       className="notebook-cell-tools"
                                       aria-label={`Tools for ${label}`}
                                       sx={{
                                          justifyContent: "flex-end",
                                          // Resting icons stay at 3:1 or better against the page in both themes.
                                          opacity: 0.8,
                                          transition: "opacity 120ms",
                                       }}
                                    >
                                       <CellButton
                                          label="Move up"
                                          disabled={
                                             moveBlocked(at, at - 1) !==
                                             undefined
                                          }
                                          reason={moveBlocked(at, at - 1)}
                                          onBlocked={setNotice}
                                          onClick={() => moveCell(at, at - 1)}
                                       >
                                          <ArrowUpwardIcon fontSize="small" />
                                       </CellButton>
                                       <CellButton
                                          label="Move down"
                                          disabled={
                                             moveBlocked(at, at + 1) !==
                                             undefined
                                          }
                                          reason={moveBlocked(at, at + 1)}
                                          onBlocked={setNotice}
                                          onClick={() => moveCell(at, at + 1)}
                                       >
                                          <ArrowDownwardIcon fontSize="small" />
                                       </CellButton>
                                       <CellButton
                                          label="Add text above"
                                          onClick={() => addText(at)}
                                       >
                                          <VerticalAlignTopIcon fontSize="small" />
                                       </CellButton>
                                       <CellButton
                                          label="Add text below"
                                          onClick={() => addText(at + 1)}
                                       >
                                          <VerticalAlignBottomIcon fontSize="small" />
                                       </CellButton>
                                       <CellButton
                                          label="Add query above"
                                          disabled={
                                             queryBlocked(at) !== undefined
                                          }
                                          reason={queryBlocked(at)}
                                          onBlocked={setNotice}
                                          onClick={() => openAddQuery(at)}
                                       >
                                          <PlaylistAddIcon fontSize="small" />
                                       </CellButton>
                                       <CellButton
                                          label="Add query below"
                                          disabled={
                                             queryBlocked(at + 1) !== undefined
                                          }
                                          reason={queryBlocked(at + 1)}
                                          onBlocked={setNotice}
                                          onClick={() => openAddQuery(at + 1)}
                                       >
                                          <PlaylistAddIcon fontSize="small" />
                                       </CellButton>
                                       {cell.kind === "markdown" && (
                                          <CellButton
                                             label="Remove text"
                                             onClick={() => removeCell(at)}
                                          >
                                             <DeleteOutlineIcon fontSize="small" />
                                          </CellButton>
                                       )}
                                       {cell.kind === "query" && (
                                          <CellButton
                                             label="Remove query"
                                             onClick={() => removeCell(at)}
                                          >
                                             <DeleteOutlineIcon fontSize="small" />
                                          </CellButton>
                                       )}
                                    </Stack>
                                    {renderCell(cell)}
                                 </Box>
                              )}
                           </CellSortable>
                        );
                     })}
                  </DragDropProvider>
               </Stack>
            </CleanNotebookSection>
         </CleanNotebookContainer>
         <AddQueryDialog
            open={adding !== undefined}
            sources={offered}
            {...(sourcesFailed ? { failed: true } : {})}
            {...(importsPending ? { pending: true } : {})}
            {...(importsFailed?.length ? { failedImports: importsFailed } : {})}
            onClose={() => setAdding(undefined)}
            onAdd={(run) => {
               if (adding !== undefined) addQuery(adding, run);
            }}
         />
         <DiffDialog
            open={pendingSave !== undefined}
            before={pendingSave?.before ?? ""}
            after={pendingSave?.after ?? ""}
            onConfirm={commitSave}
            onClose={() => setPendingSave(undefined)}
            description={
               <>
                  {pendingSave?.structural
                     ? "A text or query cell was added or removed."
                     : "A chart line was changed."}{" "}
                  Lines outside the cells and chart lines this save changes are
                  kept as they were; check they still read right.
                  {pendingSave?.clearsHistory && (
                     <>
                        {" "}
                        Saving this clears undo: a query already in the file, or
                        a chart line the editor cannot rewrite, cannot be put
                        back once it is changed, so you cannot step back past
                        this save.
                     </>
                  )}
                  {pendingSave && pendingSave.removedComments.length > 0 && (
                     <>
                        {" "}
                        Removing a cell also removes the comment directly above
                        it, which travels with the cell:
                        {/* A span: the description is a paragraph, which cannot hold a pre. */}
                        <Box
                           component="span"
                           aria-label="Comments removed with their cell"
                           sx={{
                              display: "block",
                              whiteSpace: "pre",
                              fontFamily: "monospace",
                              fontSize: 12,
                              my: 1,
                           }}
                        >
                           {pendingSave.removedComments.join("\n")}
                        </Box>
                     </>
                  )}
               </>
            }
         />
         <UnsavedChangesDialog {...exitGuard.dialog} />
      </Stack>
   );
}
