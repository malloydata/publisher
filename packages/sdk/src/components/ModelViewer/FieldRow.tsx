// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, CircularProgress, Collapse, Tooltip } from "@mui/material";
import {
   useContext,
   useId,
   useMemo,
   type MouseEvent,
   type ReactNode,
} from "react";
import { focusRingSx } from "./clickableBox";
import { stripDefinitionWrapper } from "./codeDisplay";
import { useModelViewerExtensions } from "./extensions";
import type { FieldCodeLookup } from "./fieldCode";
import { hasDefinitionToShow, isIndexedDimension } from "./fieldExpansion";
import { GivenFacts, GivenPill } from "./GivenFacts";
import { givensReadBy } from "./givenUsage";
import { isDocCut } from "./InlineDoc";
import { ModelGivensContext } from "./modelGivensContext";
import { RowGlyph } from "./RowGlyph";
import type { RowLayer } from "./rowLayers";
import { SampleValues } from "./SampleValues";
import type { ExplorerField } from "./schemaModel";
import { useSectionOpen } from "./sectionOpenCommand";
import { StatusPill } from "./StatusPill";
import { PANE_CARD_SX, PANE_INSET } from "./surface";
import { fontSizes, roles, TEXT_TERTIARY } from "./theme";
import { ENTITY_NAME_SX, ROW_META_SX } from "./tokens";
import { LoadErrorAlert, QueryCode } from "./ui";
import type { RunContext } from "./useRunQuery";

export type { FieldCodeLookup } from "./fieldCode";

/** The row line's own horizontal inset; what the row opens to hangs under the
 *  name, this far in. */
const ROW_PAD_X = 10;
/**
 * The name column's width on a wide page, so descriptions start in one column.
 * A share of the row rather than a fixed size, so a wider page gives long names
 * the room (every row of a page is the same width, so the column still lines up),
 * between a floor that keeps short pages as they were and a cap that leaves the
 * description most of the line.
 */
const NAME_COLUMN_WIDTH = "clamp(340px, 40%, 520px)";

interface FieldRowProps {
   /** A dimension, measure or calculation; a view or named query is a ViewRow. */
   field: ExplorerField;
   /** Where a dimension's sample values are read; absent, the row offers none. */
   run?: RunContext;
   modelPath: string;
   /** The source that declares the field (a joined source's, for a join's field). */
   sourceName: string;
   /** Where the field's Malloy definition comes from, when the source's
    *  definitions are available at all. A lookup that resolves to no code
    *  leaves the row with nothing to open. */
   fieldCode?: FieldCodeLookup;
   /** A joined source's field, shown inside a join on another source's page:
    *  it offers no Values card, since its values are that source's. */
   joined?: boolean;
}

/**
 * Compact field row: mono name in a fixed column, then its type and
 * annotations, then what the field MEANS on the rest of the line. What it's
 * MADE OF opens in place under it: its Malloy, a dimension's sample values, the
 * givens it reads, and whatever a host adds. A row with nothing to open is plain
 * text; one with something is a button (Enter or Space opens it too) whose
 * `aria-expanded` says which.
 */
export function FieldRow({
   field,
   run,
   modelPath,
   sourceName,
   fieldCode,
   joined = false,
}: FieldRowProps) {
   // Open or shut by the reader, and by its section's Expand / Collapse.
   const [open, setOpen] = useSectionOpen();
   const {
      fetchSamples,
      samplesGeneration,
      renderFieldStatus,
      renderFieldAction,
      renderFieldDetail,
   } = useModelViewerExtensions();
   const context = { modelPath, sourceName, field, joined };
   const hostStatus = renderFieldStatus?.(context);
   const hostAction = renderFieldAction?.(context);

   // What the row explains. An authored `#(doc)` always wins: it is the
   // modeler's own words about this field. Only where there are none does a
   // host's description of the column it passes through stand in.
   const generatedDoc = field.doc ? undefined : fieldCode?.columnDoc;
   const doc = field.doc ?? generatedDoc;

   // Whether there is a definition to show: its own code, or, for an inherited
   // column, where it comes from.
   const showDefinition = hasDefinitionToShow(fieldCode);
   // The givens its definition reads: a "Given" pill on the line, and what each
   // is in the card it opens to.
   const modelGivens = useContext(ModelGivensContext);
   const givensRead = useMemo(
      () => givensReadBy(fieldCode?.code, modelGivens),
      [fieldCode?.code, modelGivens],
   );

   // Sample values for a dimension the model tags `#(index)`, on the source
   // that declares it: a joined field's values are its own source's.
   const offersValues = isIndexedDimension(field) && !joined && !!run;
   const layers: RowLayer[] = [
      ...(showDefinition ? (["code"] as const) : []),
      ...(offersValues ? (["values"] as const) : []),
      ...(givensRead.length > 0 ? (["givens"] as const) : []),
   ];
   const shows = (l: RowLayer) => layers.includes(l);
   const glyphKind =
      field.kind === "measure"
         ? "measure"
         : field.kind === "calculate"
           ? "calculate"
           : "dimension";

   // The row's own card: its code, then its values and the givens it reads,
   // each parted from the one above by a hairline. A host may keep it, add to
   // it, or show its own in its place (`renderFieldDetail`).
   const builtIn =
      layers.length > 0 ? (
         <Box
            data-testid="model-viewer-field-card"
            sx={{
               ...PANE_CARD_SX,
               "& > * + *": {
                  borderTop: 1,
                  borderColor: "divider",
               },
            }}
         >
            {shows("code") &&
               (fieldCode?.loading ? (
                  <Box sx={{ p: PANE_INSET, display: "flex" }}>
                     <CircularProgress
                        size={12}
                        sx={{ color: "text.secondary" }}
                     />
                  </Box>
               ) : fieldCode?.code ? (
                  // The `name is` wrapper dropped, since the row already names it.
                  <QueryCode
                     testId="model-viewer-definition"
                     code={stripDefinitionWrapper(field.name, fieldCode.code)}
                     framed={false}
                  />
               ) : fieldCode?.inheritedFrom ? (
                  // An inherited column has no definition of its own; its
                  // Code is where it comes from, said in a comment over the
                  // line so the row stays one card.
                  <QueryCode
                     testId="model-viewer-definition"
                     code={`${
                        fieldCode.inheritedFrom.fromTable
                           ? "// A column of the table this source reads, as it is"
                           : "// Passed through from the query this source is built on"
                     }\n${fieldCode.inheritedFrom.code}`}
                     framed={false}
                  />
               ) : (
                  // The read failed: about the pane rather than the field.
                  <Box sx={{ p: 1 }}>
                     <LoadErrorAlert message="Couldn't load this model's code right now." />
                  </Box>
               ))}
            {shows("values") && run && (
               <Box sx={{ p: PANE_INSET }}>
                  <SampleValues
                     run={run}
                     fetchSamples={fetchSamples}
                     generation={samplesGeneration}
                     modelPath={modelPath}
                     sourceName={sourceName}
                     field={field}
                  />
               </Box>
            )}
            {shows("givens") && (
               <GivenFacts givens={givensRead} framed={false} />
            )}
         </Box>
      ) : undefined;
   const detail = renderFieldDetail
      ? renderFieldDetail(context, builtIn)
      : builtIn;
   const opens = detail !== undefined && detail !== null && detail !== false;

   return (
      <Box
         data-testid="model-viewer-field-row"
         data-name={field.name}
         // No fill of its own: an open field's code is its own card (QueryCode), so
         // a little air below is all that sets it apart from the next row.
         sx={open && opens ? { pb: 1.25 } : undefined}
      >
         <RowLine
            glyph={<RowGlyph kind={glyphKind} dataType={field.dataType} />}
            name={field.name}
            typeLabel={field.dataType}
            typeTip={field.typeTip}
            typeAsChip
            stackPills
            doc={doc}
            generatedDoc={!!generatedDoc}
            open={open}
            expandable={opens}
            onToggle={() => setOpen((o) => !o)}
            descriptors={
               // What the field IS, beside its type on its own line: whether it
               // reads a given. Its display tags (`label="…"`, `currency`) show
               // in its code when opened.
               <GivenPill givens={givensRead} />
            }
            pills={hostStatus}
            action={hostAction}
         />
         {opens && (
            <Collapse in={open} timeout="auto" unmountOnExit>
               {/* Hangs under the field name: the row's card, or what a host
                   shows in its place. */}
               <Box
                  data-testid="model-viewer-field-detail"
                  sx={{
                     mx: `${ROW_PAD_X}px`,
                     mt: "2px",
                     display: "flex",
                     flexDirection: "column",
                     gap: 1.5,
                  }}
               >
                  {detail}
               </Box>
            </Collapse>
         )}
      </Box>
   );
}

/**
 * THE line of a model page's row — a view, a named query, a measure or a
 * dimension alike, so the page reads as one list: the name in a fixed column
 * (with its type, marks, pills and one action), then what it means on the rest of
 * the line. A row with something to open is a button on its name (Enter or Space
 * opens it, `aria-expanded` says whether it is) and toggles from anywhere on the
 * line; one with nothing to open is plain text.
 */
export function RowLine({
   glyph,
   name,
   typeLabel,
   typeAsChip = false,
   stackPills = false,
   typeTip,
   descriptors,
   pills,
   action,
   trailing,
   doc,
   generatedDoc = false,
   open,
   expandable,
   onToggle,
}: {
   /** The row's kind glyph (`RowGlyph`), before its name. */
   glyph?: ReactNode;
   name: string;
   typeLabel?: string;
   /** A field's type as a chip, apart from the name; a
    *  join's "N fields" count stays text. */
   typeAsChip?: boolean;
   /** A field's layout: its name and type on the first line, always together,
    *  and its pills and action on a second line under them (only when there are
    *  any) — so its state never squeezes the name, and the rebuild sits beside
    *  the pill it rebuilds. */
   stackPills?: boolean;
   /** The type chip's tooltip, where its label can't say it all (a record's fields). */
   typeTip?: string;
   /** Descriptor chips after the type, on the name's own line (a field's
    *  display tags, its "given"). */
   descriptors?: ReactNode;
   /** Status and declaration pills after the name. */
   pills?: ReactNode;
   /** The row's one action, beside its pills (Rebuild / Build). */
   action?: ReactNode;
   /** Controls at the line's right edge — an open view's Result | Code | Download. */
   trailing?: ReactNode;
   doc?: string;
   /** `doc` is a host's description, not the modeler's: marked as generated. */
   generatedDoc?: boolean;
   open: boolean;
   expandable: boolean;
   onToggle: () => void;
}) {
   // The whole line toggles, not only the name: a click on the description
   // opens the row too. A click on another control on the line (Rebuild) is that
   // control's own. The name is the row's button, so a keyboard's Enter or Space
   // reaches here as its click.
   const pillsId = useId();
   const onRowClick = (e: MouseEvent<HTMLElement>) => {
      if (!expandable) return;
      // A click in the action slot is the action's, even on a disabled one: a
      // disabled button takes no pointer events, so its click lands on the
      // tooltip's wrapper, which is no control at all.
      if ((e.target as Element).closest("[data-row-action]")) return;
      const control = (e.target as Element).closest(
         "button, a, [role='button']",
      );
      if (control && !control.hasAttribute("data-field-toggle")) return;
      onToggle();
   };
   return (
      <Box
         onClick={onRowClick}
         sx={{
            display: "flex",
            // Top-aligned while open, where the description may wrap to
            // several lines: the name stays on the first one.
            alignItems: open ? "flex-start" : "center",
            borderRadius: 1.5,
            transition: "background 0.1s",
            cursor: expandable ? "pointer" : "default",
            // A read-only row takes neither the tint nor the pointer, so it
            // never reads as an editable cell.
            ...(expandable
               ? {
                    "&:hover": { bgcolor: roles.background.muted },
                    // A quiet chip's fill is the hover's grey; lift it a step so it
                    // doesn't vanish into the row.
                    "&:hover [data-pill-tone='quiet']": {
                       bgcolor: roles.background.strong,
                    },
                 }
               : {}),
         }}
      >
         {/* The name column: the row's identity and its one action (Rebuild,
             beside the pill it rebuilds). Exactly NAME_COLUMN_WIDTH on a wide page —
             a long name truncates rather than widening it — so the descriptions
             after it start in one column down the section. */}
         <Box
            sx={{
               display: "flex",
               flexDirection: stackPills ? "column" : "row",
               alignItems: stackPills ? "stretch" : "center",
               minWidth: 0,
               flexShrink: 0,
               width: { md: NAME_COLUMN_WIDTH },
               overflow: { md: "hidden" },
            }}
         >
            <Box
               component={expandable ? "button" : "div"}
               type={expandable ? "button" : undefined}
               aria-expanded={expandable ? open : undefined}
               // Stacked, the pills sit outside the button, so it names them as
               // its description: they are still read with the row.
               aria-describedby={stackPills ? pillsId : undefined}
               data-field-toggle={expandable ? "" : undefined}
               sx={{
                  appearance: "none",
                  border: "none",
                  background: "none",
                  font: "inherit",
                  color: "inherit",
                  textAlign: "left",
                  // Hugs its content so the Rebuild that follows sits right
                  // beside the pill; `minWidth: 0` lets a long name truncate.
                  minWidth: 0,
                  px: `${ROW_PAD_X}px`,
                  py: "6px",
                  borderRadius: 1.5,
                  // Inset: the name column clips its overflow, which would cut
                  // the browser's outside ring.
                  ...focusRingSx,
                  display: "flex",
                  alignItems: "baseline",
                  gap: "8px",
                  // Stacked: the name and its type on one line, never split (a name
                  // too long for both truncates). Otherwise the chips wrap under
                  // the name rather than squeezing it into an ellipsis.
                  flexWrap: stackPills ? "nowrap" : "wrap",
                  rowGap: "4px",
                  cursor: expandable ? "pointer" : "default",
               }}
            >
               {glyph}
               {/* Identity (Base): name + type. */}
               <Box
                  component="span"
                  // Its own box only for grouping: its name and chips flow in the
                  // row's one wrapping line with the pills after them, so chips
                  // fill the name's line first and only what doesn't fit drops below.
                  sx={{
                     display: "contents",
                     "& > *": { flexShrink: 0 },
                     // Stacked, the name is what gives way on the one line.
                     ...(stackPills
                        ? {
                             "& > *:first-of-type": {
                                flexShrink: 1,
                                minWidth: 0,
                             },
                          }
                        : {}),
                  }}
               >
                  <Box
                     component="span"
                     // The full name on hover when the column cut it.
                     title={name}
                     sx={{
                        ...ENTITY_NAME_SX,
                        // Truncates only when the name alone is wider than the column.
                        maxWidth: "100%",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                     }}
                  >
                     {name}
                  </Box>
                  {typeLabel &&
                     (typeAsChip ? (
                        // The type is Malloy's word, so the token voice.
                        <Tooltip
                           title={
                              typeTip ? (
                                 <Box sx={{ whiteSpace: "pre-line" }}>
                                    {typeTip}
                                 </Box>
                              ) : (
                                 ""
                              )
                           }
                        >
                           <StatusPill
                              voice="token"
                              tone="quiet"
                              sx={{ alignSelf: "center" }}
                           >
                              {typeLabel}
                           </StatusPill>
                        </Tooltip>
                     ) : (
                        <Box component="span" sx={ROW_META_SX}>
                           {typeLabel}
                        </Box>
                     ))}
                  {descriptors}
               </Box>
               {!stackPills && pills}
            </Box>
            {stackPills ? (
               // The second line: the field's states and its one action, together.
               // Hidden when empty, so a row with no state stays one line.
               <Box
                  id={pillsId}
                  sx={{
                     display: "flex",
                     alignItems: "center",
                     flexWrap: "wrap",
                     gap: "8px",
                     px: `${ROW_PAD_X}px`,
                     pb: "6px",
                     mt: "-2px",
                     "&:empty": { display: "none" },
                  }}
               >
                  {pills}
                  {action && (
                     <Box
                        component="span"
                        data-row-action=""
                        sx={{ display: "inline-flex" }}
                     >
                        {action}
                     </Box>
                  )}
               </Box>
            ) : (
               // A sibling of the name button, never inside it (a button in a
               // button is invalid and reads as one target).
               action && (
                  <Box
                     component="span"
                     data-row-action=""
                     sx={{ display: "inline-flex" }}
                  >
                     {action}
                  </Box>
               )
            )}
         </Box>
         {/* What the field means, on the line's spare width: the list is read by
             scanning names, so the description never goes UNDER the name. Cut to
             one line while shut, carrying the whole of it as a plain title when
             cut; an open row wraps it to its full text. */}
         {doc && (
            <Box
               component="span"
               data-testid="model-viewer-field-doc"
               onMouseEnter={(e: MouseEvent<HTMLElement>) => {
                  const el = e.currentTarget;
                  el.title = !open && isDocCut(el, doc) ? doc : "";
               }}
               sx={{
                  flex: 1,
                  minWidth: 0,
                  ml: 1,
                  pr: `${ROW_PAD_X}px`,
                  py: open ? "6px" : 0,
                  fontSize: fontSizes.small,
                  color: TEXT_TERTIARY,
                  ...(open
                     ? { whiteSpace: "pre-line" }
                     : {
                          whiteSpace: "nowrap",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                       }),
               }}
            >
               {generatedDoc && (
                  // Written by the host, not the modeler: marked, so the two never
                  // read as one voice.
                  <StatusPill
                     voice="state"
                     tone="quiet"
                     data-testid="model-viewer-field-doc-generated"
                     sx={{ mr: 0.75, verticalAlign: "baseline" }}
                  >
                     Generated
                  </StatusPill>
               )}
               {open ? doc : doc.split("\n")[0]}
            </Box>
         )}
         {trailing && (
            <Box
               sx={{
                  ml: "auto",
                  flexShrink: 0,
                  pl: 1,
                  pr: `${ROW_PAD_X}px`,
                  display: "inline-flex",
                  alignItems: "center",
                  // The name line's height, so the icons center on the name even
                  // when an open row's description wraps.
                  height: 32,
               }}
            >
               {trailing}
            </Box>
         )}
      </Box>
   );
}
