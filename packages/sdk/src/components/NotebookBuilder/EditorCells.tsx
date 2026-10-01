// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import {
   Box,
   Button,
   IconButton,
   Stack,
   TextField,
   Tooltip,
   Typography,
} from "@mui/material";
import { useEffect, useId, useMemo, useState } from "react";
import { useQueryResult } from "../../hooks/useQueryResult";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { useDraft } from "../DashboardBuilder/useDraft";
import { highlight } from "../highlighter";
import { Loading } from "../Loading";
import { definitionSummary } from "../Notebook/cellKind";
import { Prose, type ProseLinkContext } from "../Prose";
import ResultContainer from "../RenderedResult/ResultContainer";
import { NOTEBOOK_CELL_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { CleanMetricCard } from "../styles";
import { cellFailure } from "./cellResult";
import { captionOf, queryCode } from "./cellText";

/** Code as the viewer shows it: highlighted once the highlighter has loaded, plain until then. */
function Code({ code }: { code: string }) {
   const { mode } = usePublisherTheme();
   const [html, setHtml] = useState<string | undefined>(undefined);
   useEffect(() => {
      let live = true;
      void highlight(code, "malloy", mode).then((out) => {
         if (live) setHtml(out);
      });
      return () => {
         live = false;
      };
   }, [code, mode]);
   const sx = {
      m: 0,
      overflow: "auto",
      fontFamily: "monospace",
      fontSize: "13px",
      "& pre": { m: 0 },
   };
   return html === undefined ? (
      <Box component="pre" sx={sx}>
         {code}
      </Box>
   ) : (
      <Box sx={sx} dangerouslySetInnerHTML={{ __html: html }} />
   );
}

/** A markdown cell: its prose, and on a click a textarea whose change commits once, when it closes. */
export function MarkdownCell({
   markdown,
   editing,
   links,
   onEdit,
   onCommit,
   onClose,
}: {
   markdown: string;
   editing: boolean;
   links: ProseLinkContext;
   onEdit: () => void;
   onCommit: (next: string) => void;
   onClose: () => void;
}) {
   // One object per value, or the draft would reset on every render while open.
   const value = useMemo(
      () => (editing ? { text: markdown } : undefined),
      [editing, markdown],
   );
   const { draft, patch, close } = useDraft<{ text: string }>(
      value,
      editing,
      (next) => onCommit(next.text),
      onClose,
   );
   if (!editing || draft === undefined)
      return (
         <Stack direction="row" spacing={1} sx={{ alignItems: "flex-start" }}>
            <Box
               // A click on the prose edits it too, except on a link, which keeps its own meaning.
               onClick={(event) => {
                  if (!(event.target as Element).closest("a")) onEdit();
               }}
               sx={{ cursor: "text", minHeight: 24, flex: 1, minWidth: 0 }}
            >
               {markdown.trim() ? (
                  <Prose variant="document" links={links}>
                     {markdown}
                  </Prose>
               ) : (
                  <Typography
                     sx={{ color: "text.secondary", fontStyle: "italic" }}
                  >
                     Empty text.
                  </Typography>
               )}
            </Box>
            <Tooltip title="Edit text">
               <IconButton size="small" aria-label="Edit text" onClick={onEdit}>
                  <EditOutlinedIcon fontSize="small" />
               </IconButton>
            </Tooltip>
         </Stack>
      );
   return (
      <Stack spacing={1}>
         <TextField
            multiline
            autoFocus
            minRows={3}
            fullWidth
            value={draft.text}
            inputProps={{ "aria-label": "Markdown" }}
            onChange={(event) =>
               patch((next) => {
                  next.text = event.target.value;
               })
            }
            onKeyDown={(event) => {
               if (event.key === "Escape") close();
            }}
            // Leaving the field is leaving the edit, so switching to another cell never drops this one.
            onBlur={close}
         />
         {draft.text.trim() && (
            <Box aria-label="Preview">
               <Prose variant="document" links={links}>
                  {draft.text}
               </Prose>
            </Box>
         )}
         <Box>
            <Button size="small" variant="outlined" onClick={close}>
               Done
            </Button>
         </Box>
      </Stack>
   );
}

/** A definition cell, folded to its kind and name. */
export function DefinitionCell({
   text,
   markdown,
   links,
}: {
   text: string;
   markdown?: string;
   links: ProseLinkContext;
}) {
   const [open, setOpen] = useState(false);
   const region = useId();
   return (
      <Box>
         {markdown && (
            <Prose variant="document" links={links}>
               {markdown}
            </Prose>
         )}
         <Box
            component="button"
            type="button"
            aria-expanded={open}
            aria-controls={region}
            onClick={() => setOpen((was) => !was)}
            sx={{
               display: "flex",
               alignItems: "center",
               gap: "4px",
               p: 0,
               border: 0,
               background: "none",
               cursor: "pointer",
               fontFamily: "monospace",
               fontSize: "13px",
               color: "text.secondary",
            }}
         >
            {open ? (
               <ExpandMoreIcon fontSize="small" />
            ) : (
               <ChevronRightIcon fontSize="small" />
            )}
            {definitionSummary({ text: queryCode(text) })}
         </Box>
         {open && (
            <CleanMetricCard id={region} sx={{ mt: 1, padding: "12px 24px" }}>
               <Code code={queryCode(text)} />
            </CleanMetricCard>
         )}
      </Box>
   );
}

export interface QueryTarget {
   environmentName: string;
   packageName: string;
   /** The notebook itself, which the cell's text runs against. */
   modelPath: string;
   versionId?: string;
}

/** A query cell: its prose, caption and code, and its result from the model query route. */
export function QueryCell({
   text,
   query,
   markdown,
   links,
   target,
   givens,
   maxResultSize,
}: {
   /** The cell's exact text in the file, shown as its caption and code. */
   text: string;
   /** What runs: the cell's text without its prose and caption notes. */
   query: string;
   markdown?: string;
   links: ProseLinkContext;
   target: QueryTarget;
   /** The applied givens, encoded for the request. */
   givens: Record<string, unknown>;
   maxResultSize?: number;
}) {
   // Keyed by (query, givens) through the query cache, so a reorder shows the cached result rather than re-running.
   const state = useQueryResult({ ...target, query, givens });
   const caption = captionOf(text);
   return (
      <Stack spacing={1}>
         {markdown && (
            <Prose variant="document" links={links}>
               {markdown}
            </Prose>
         )}
         {caption && (
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
               {caption}
            </Typography>
         )}
         <CleanMetricCard sx={{ padding: "8px 16px" }}>
            <Code code={queryCode(text)} />
         </CleanMetricCard>
         <CellResult state={state} maxResultSize={maxResultSize} />
      </Stack>
   );
}

function CellResult({
   state,
   maxResultSize,
}: {
   state: ReturnType<typeof useQueryResult>;
   maxResultSize?: number;
}) {
   const { data, isSuccess, isError, error } = state;
   if (isError) {
      const failure = cellFailure(error);
      if (failure === "error")
         return <ApiErrorDisplay error={error} context="This cell" />;
      return (
         <CleanMetricCard sx={{ p: 2 }} role="status">
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
               {failure === "unavailable"
                  ? "Result unavailable: this cell reads a source the editor cannot query here."
                  : "Preview unavailable in the editor: this cell uses a construct the editor's preview may not run, such as raw SQL, a table reference or an import. The notebook renders it."}
            </Typography>
         </CleanMetricCard>
      );
   }
   if (!isSuccess) return <Loading text="Running…" />;
   return (
      <CleanMetricCard>
         <ResultContainer
            result={data.data.result}
            maxHeight={NOTEBOOK_CELL_MAX_HEIGHT}
            maxResultSize={maxResultSize}
            renderLogs={data.data.renderLogs}
         />
      </CleanMetricCard>
   );
}
