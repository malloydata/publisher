// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, CircularProgress, Collapse, Tooltip } from "@mui/material";
import { useContext, useMemo, useState } from "react";
import { viewDeclaration } from "./codeDisplay";
import { tagNames } from "./docAnnotations";
import { EmptyRow } from "./EmptyRow";
import { useModelViewerExtensions } from "./extensions";
import type { FieldCodeLookup } from "./fieldCode";
import { hasDefinitionToShow } from "./fieldExpansion";
import { RowLine } from "./FieldRow";
import { GivenFacts, GivenPill } from "./GivenFacts";
import { viewRunQuery } from "./malloyRunText";
import { RowGlyph } from "./RowGlyph";
import { RunResult, type ShownRun } from "./RunResult";
import type { ExplorerField } from "./schemaModel";
import { StatusPill } from "./StatusPill";
import { MONO_FONT_FAMILY } from "./theme";
import { LoadErrorAlert, QueryCode } from "./ui";
import { runQueryKey, type RunContext } from "./useRunQuery";
import { useServer } from "../ServerProvider";
import { givensReadBy } from "./givenUsage";
import type { GivenInfo } from "./modelGivens";
import { ModelGivensContext } from "./modelGivensContext";
import { useSectionOpen } from "./sectionOpenCommand";
import { RowLayerSwitch, useRowLayer, type RowLayer } from "./rowLayers";

/**
 * A source's view, or a model's named query, as a row of the page — the same
 * line a measure or dimension has (RowLine), opened and closed the same way. Open,
 * it shows its result card under the line, a notebook cell's shape: the Result |
 * Code switch (and a host's actions) and the result, drawn by RunResult.
 *
 * <p>It runs when first opened, on its Result, and never on render: browsing a
 * source costs nothing. The card mounts on first open and stays mounted while
 * closed, so reopening shows the result already drawn; the result itself is the
 * viewer's cache entry (runQueryOptions, kept for the page), so a remount
 * (switching source and back) reopens it without a second request.
 *
 * <p>A row with nowhere to run (no `run`) opens on its Code, Result disabled.
 */
export function ViewRow({
   field,
   modelPath,
   sourceName,
   fieldCode,
   run,
   runQuery,
   runGivens,
}: {
   field: ExplorerField;
   modelPath: string;
   /** The source the view belongs to; for a named query, the source it runs
    *  against when known (for its required filters), else "". */
   sourceName: string;
   /** The view's Malloy, when the source's definitions are available. */
   fieldCode?: FieldCodeLookup;
   /** Where to run it; absent where it can't be run. */
   run?: RunContext;
   /** A named query's `run:` text; a view's is built from its source and name.
    *  Its code is then the whole `query:` declaration, shown as written. */
   runQuery?: string;
   /** The givens a run reads, where its source can't say (see RunResult). */
   runGivens?: ReadonlyArray<GivenInfo>;
}) {
   const query = runQuery ?? viewRunQuery(sourceName, field.name);
   const noun = runQuery ? "query" : "view";
   // Open or shut by the reader, and by its section's Expand / Collapse.
   const [open, setOpen] = useSectionOpen();
   const [shownRun, setShownRun] = useState<ShownRun | undefined>(undefined);
   const showDefinition = hasDefinitionToShow(fieldCode);
   const modelGivens = useContext(ModelGivensContext);
   const { renderRunActions } = useModelViewerExtensions();
   const { server } = useServer();
   const givensRead = useMemo(
      () => givensReadBy(fieldCode?.code, modelGivens),
      [fieldCode?.code, modelGivens],
   );
   // Its tags as the names on its chips (`line_chart`), and the tags in full for
   // their hover — only where the chips left something out. Where no tag names
   // anything (`# size=lg`, `# -bar_chart`), the chips are the tags as written,
   // so a tagged view never shows no tag at all.
   const { chartNames, tagsInFull } = useMemo(() => {
      const tags = field.tags ?? [];
      const named = tagNames(tags);
      const names = named.length > 0 ? named : tags;
      return {
         chartNames: names,
         tagsInFull:
            tags.join(" ") === names.join(" ")
               ? undefined
               : tags.map((tag) => `# ${tag}`).join("\n"),
      };
   }, [field.tags]);
   // One card at a time: its result, its Malloy, or the givens it reads. Result
   // and Code keep their places even where one can't be shown (disabled, with
   // why), so the switch doesn't change shape between rows.
   const available: RowLayer[] = [
      "result",
      "code",
      ...(givensRead.length > 0 ? (["givens"] as const) : []),
   ];
   const { layers, layer, setLayer } = useRowLayer(
      available,
      run ? "result" : "code",
   );
   const view = layer ?? "code";

   // Called only while Code or Givens is shown, so a card on its result never
   // runs the declaration regexes.
   const renderCode = () =>
      view === "givens" ? (
         <GivenFacts givens={givensRead} />
      ) : fieldCode?.loading ? (
         <CircularProgress size={12} sx={{ color: "text.secondary" }} />
      ) : fieldCode?.code ? (
         <QueryCode
            testId="model-viewer-definition"
            code={
               runQuery
                  ? fieldCode.code
                  : viewDeclaration(field.name, fieldCode.code)
            }
         />
      ) : fieldCode?.error ? (
         <LoadErrorAlert message="Couldn't load this model's code right now." />
      ) : (
         <EmptyRow message={`No Malloy code to show for this ${noun}.`} />
      );

   return (
      <Box
         data-testid="model-viewer-view-row"
         data-name={field.name}
         sx={open ? { pb: 1.5 } : undefined}
      >
         <RowLine
            glyph={<RowGlyph kind="view" />}
            name={field.name}
            doc={field.doc}
            open={open}
            expandable
            onToggle={() => setOpen((o) => !o)}
            // Open, its Result | Code (| Givens) and a host's actions sit on the
            // row's own line, right of its name and description, as a notebook
            // cell's sit over its result.
            trailing={
               open && (
                  <Box
                     sx={{ display: "flex", alignItems: "center", gap: "2px" }}
                  >
                     <RowLayerSwitch
                        aria-label={`Show for ${field.name}`}
                        layers={layers}
                        value={view}
                        onChange={setLayer}
                        disabledReasons={{
                           result: run
                              ? undefined
                              : `This ${noun} can't be run here`,
                           code: showDefinition
                              ? undefined
                              : `No Malloy code to show for this ${noun}`,
                        }}
                     />
                     {run &&
                        renderRunActions?.({
                           kind: runQuery ? "query" : "view",
                           modelPath,
                           sourceName: runQuery ? "" : sourceName,
                           name: field.name,
                           query,
                           givens: run.givenValues,
                           queryKey: runQueryKey(server, run, modelPath, query),
                           shown: shownRun,
                        })}
                  </Box>
               )
            }
            // What it is (its chart type, its given) after its name. Not stacked:
            // the name keeps its width and the chips wrap under it, so no chip
            // ever cuts the name to an ellipsis or off the line.
            descriptors={
               <>
                  {chartNames.map((name) => {
                     // How the view presents is part of what it is; a token from the file.
                     const chip = (
                        <StatusPill
                           key={name}
                           voice="token"
                           tone="quiet"
                           sx={{ alignSelf: "center" }}
                        >
                           {name}
                        </StatusPill>
                     );
                     // Its parameters are on its Code; where the chip left them out,
                     // its hover shows the tag in full, for a reader who wants it
                     // without opening the row. Its accessible name is that text too.
                     return tagsInFull ? (
                        <Tooltip
                           key={name}
                           title={tagsInFull}
                           slotProps={{
                              tooltip: {
                                 sx: {
                                    whiteSpace: "pre-line",
                                    fontFamily: MONO_FONT_FAMILY,
                                 },
                              },
                           }}
                        >
                           {chip}
                        </Tooltip>
                     ) : (
                        chip
                     );
                  })}
                  <GivenPill givens={givensRead} />
               </>
            }
         />
         <Collapse in={open} timeout="auto" mountOnEnter>
            {/* The result in the app's quiet card, as a code block or a dashboard tile
                frames its content; under the name on a wide page, the whole column at
                phone width. */}
            <Box
               data-testid="model-viewer-view-card"
               sx={{ mx: "10px", mt: 1, minWidth: 0 }}
            >
               {/* At phone width the result takes the column, and a table wider than
                   it scrolls rather than being cut off at the edge */}
               <Box sx={{ minWidth: 0, overflowX: "auto" }}>
                  {run ? (
                     <RunResult
                        run={run}
                        modelPath={modelPath}
                        query={query}
                        sourceName={sourceName || undefined}
                        givens={runGivens}
                        mode={view === "result" ? "result" : "code"}
                        renderCode={renderCode}
                        onShownChange={setShownRun}
                     />
                  ) : (
                     renderCode()
                  )}
               </Box>
            </Box>
         </Collapse>
      </Box>
   );
}
