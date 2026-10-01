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
import { Alert, Box, Button, IconButton, Stack, Tooltip } from "@mui/material";
import {
   useCallback,
   useEffect,
   useMemo,
   useRef,
   useState,
   type KeyboardEvent,
   type ReactNode,
} from "react";
import type { Given } from "../../client";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import { now } from "../Dashboard/telemetry";
import { BuilderToolbar } from "../DashboardBuilder/BuilderToolbar";
import { DiffDialog } from "../DashboardBuilder/DiffDialog";
import type { CatalogSource } from "../DashboardBuilder/catalog";
import { builderSensors } from "../DashboardBuilder/sortable";
import { useBuilderShortcuts } from "../DashboardBuilder/useBuilderShortcuts";
import type { NavigationClick } from "../click_helper";
import { GivensPanel } from "../given";
import { givensToRequest } from "../given/paramCodec";
import type { ProseLinkContext } from "../Prose";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { AddQueryDialog } from "./AddQueryDialog";
import { cellQueries, cellSlices, runTargetOf, withChart } from "./cellText";
import { ChartPicker, chartLocked, pickerState } from "./ChartPicker";
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

function CellButton({
   label,
   disabled,
   reason,
   onClick,
   children,
}: {
   label: string;
   disabled?: boolean;
   /** Why the button is off, shown with its name. */
   reason?: string;
   onClick: () => void;
   children: ReactNode;
}) {
   return (
      <Tooltip title={disabled && reason ? `${label}: ${reason}` : label}>
         {/* A span, so the tooltip still shows on a disabled button. */}
         <span>
            <IconButton
               size="small"
               aria-label={label}
               disabled={disabled}
               onClick={onClick}
            >
               {children}
            </IconButton>
         </span>
      </Tooltip>
   );
}

export function NotebookBuilder({
   source,
   notebook,
   sources,
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
   onNavigate,
   maxResultSize,
}: NotebookBuilderProps) {
   const initial = useMemo(() => notebookDocumentOf(notebook), [notebook]);
   // Keyed on the names, so a re-read of the model that changes nothing does not re-create the editor's writer.
   const reachableKey = JSON.stringify(sources?.map((s) => s.name) ?? null);
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
        }
      | undefined
   >(undefined);
   const nextId = useRef(0);

   useEffect(() => {
      onChange?.(doc);
   }, [doc, onChange]);
   useEffect(() => {
      onDirtyChange?.(editor.dirty);
   }, [editor.dirty, onDirtyChange]);

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
   const request = useMemo(
      () => givensToRequest(applied, declaredTypes),
      [applied, declaredTypes],
   );
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

   const moveCell = useCallback(
      (from: number, to: number) => {
         if (to < 0 || to >= doc.cells.length) return;
         if (!editor.canMove(from, to)) {
            setNotice(
               doc.cells[from].kind === "definition"
                  ? "Definitions keep their place in this editor."
                  : "A query cannot move above a definition it may read.",
            );
            return;
         }
         update((draft) => {
            const [cell] = draft.cells.splice(from, 1);
            draft.cells.splice(to, 0, cell);
         });
      },
      [doc.cells, editor, update],
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

   const addQuery = (at: number, run: QueryRun) => {
      const id = freshId();
      update((draft) => {
         draft.cells.splice(at, 0, { id, kind: "query", run, added: true });
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
      if (sources === undefined) return "The notebook's sources are loading.";
      if (sources.length === 0) return "This notebook reads no source.";
      if (!canInsertQuery(doc, at))
         return "A query cannot go above a definition: Malloy reads nothing below it.";
      return undefined;
   };

   const { preview, dragging, onDragStart, onDragOver, onDragEnd } =
      useCellReorder({
         ids: doc.cells.map((cell) => cell.id),
         canMove: editor.canMove,
         commit: moveCell,
      });

   const commitSave = useCallback(() => {
      setPendingSave(undefined);
      setSaving(true);
      const started = now();
      const cells = editor.document.cells.length;
      const structural = editor.structural;
      void editor
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

   const save = useCallback(() => {
      if (!onSave || !editor.dirty || saving) return;
      if (!editor.structural) {
         commitSave();
         return;
      }
      void Promise.all([editor.preview(), editor.removedComments()]).then(
         ([result, removedComments]) => {
            if (result.ok)
               setPendingSave({
                  before: editor.source,
                  after: result.source,
                  removedComments,
                  clearsHistory: editor.clearsHistory,
               });
            else commitSave();
         },
      );
   }, [onSave, editor, saving, commitSave]);

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

   const chartPicker = (cell: NotebookDocumentCell) => {
      const read = readById.get(cell.id);
      const run = cell.added
         ? cell.run
         : runTargetOf(slices.get(cell.id) ?? "");
      const view = sources
         ?.find((s) => s.name === run?.source)
         ?.views.find((v) => v.name === run?.view);
      const locked = cell.added ? undefined : chartLocked(read?.chart);
      return (
         <ChartPicker
            state={pickerState(cell.chart, read?.chart)}
            view={view}
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
               onClose={() =>
                  setEditing((was) => (was === cell.id ? undefined : was))
               }
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
               {chartPicker(cell)}
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
            {...(toolbar ? { actions: toolbar } : {})}
         />
         <CleanNotebookContainer>
            <CleanNotebookSection>
               <Stack spacing={2} component="section">
                  <GivensPanel {...controls.panel} />
                  {editor.error && (
                     // The edit is still here; this says what kept it from the file.
                     <Alert severity="warning">{editor.error}</Alert>
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
                        <Tooltip title={queryBlocked(0) ?? ""}>
                           <span>
                              <Button
                                 startIcon={<PlaylistAddIcon />}
                                 disabled={queryBlocked(0) !== undefined}
                                 onClick={() => setAdding(0)}
                              >
                                 Add query
                              </Button>
                           </span>
                        </Tooltip>
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
                                          opacity: 0.35,
                                          transition: "opacity 120ms",
                                       }}
                                    >
                                       <CellButton
                                          label="Move up"
                                          disabled={
                                             at === 0 ||
                                             !editor.canMove(at, at - 1)
                                          }
                                          onClick={() => moveCell(at, at - 1)}
                                       >
                                          <ArrowUpwardIcon fontSize="small" />
                                       </CellButton>
                                       <CellButton
                                          label="Move down"
                                          disabled={
                                             at === doc.cells.length - 1 ||
                                             !editor.canMove(at, at + 1)
                                          }
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
                                          onClick={() => setAdding(at)}
                                       >
                                          <PlaylistAddIcon fontSize="small" />
                                       </CellButton>
                                       <CellButton
                                          label="Add query below"
                                          disabled={
                                             queryBlocked(at + 1) !== undefined
                                          }
                                          reason={queryBlocked(at + 1)}
                                          onClick={() => setAdding(at + 1)}
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
            sources={sources}
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
                  A text or query cell was added or removed. Everything else in
                  the file is kept as it was; check it still reads right.
                  {pendingSave?.clearsHistory && (
                     <>
                        {" "}
                        Saving this clears undo: a query that is already in the
                        file cannot be written back once it is removed, so you
                        cannot step back past this save.
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
      </Stack>
   );
}
