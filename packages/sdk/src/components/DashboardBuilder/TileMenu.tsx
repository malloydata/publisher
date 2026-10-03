// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Button, Divider, Popover, Stack, Typography } from "@mui/material";
import { useId } from "react";
import { useDraft } from "./useDraft";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogView } from "./catalog";
import { isQueryTile, type DashboardTile, type QueryTile } from "./document";
import { presetSpan } from "../Dashboard/DashboardGrid";
import { ChartPicker } from "./ChartPicker";

/**
 * A tile's own settings, on the tile: a popover off its menu button, so
 * editing it never means scrolling away from it. Its chart, width presets,
 * clickable cells and removal; its title and subtitle are edited on the tile. Its row is set by dragging it — a drop
 * into the empty end of a row is what "start a new row" means — and its card
 * is the reader's to decide, so neither is a toggle here. Which controls it
 * answers to is not here either: filters are configured in one place, the
 * strip under the header. Edits commit on close (`useDraft`).
 *
 * A text tile has only a width (when the grid has more than one column) and
 * Remove: its words are written on the tile itself.
 */
export interface TileMenuProps {
   anchor: HTMLElement | null;
   tile: DashboardTile | undefined;
   onClose: () => void;
   /** Apply the edited tile. Called once, on close, only when something changed. */
   onCommit: (next: DashboardTile) => void;
   /** Take the tile off the dashboard. Offered on every tile: order is this file's. */
   onRemove: () => void;
   /** Why removing is refused, shown beside a Remove that stays focusable; absent when it is allowed. */
   removeBlocked?: string;
   /** The grid's width, which the width presets are fractions of. */
   columns: number;
   /** Open the clickable-cells window for this tile's source. */
   onDrills: () => void;
   /** The catalog's view the tile shows, when the catalog knows it: what decides which charts are offered. */
   view?: Pick<CatalogView, "chart" | "aggregateOnly">;
}

const customChart = (lines: string[] | undefined) =>
   lines && lines.length === 1
      ? `This tile's chart line (${lines[0]}) is not one the builder models, so its chart cannot be changed here.`
      : `This tile has ${lines?.length ?? "several"} chart lines${lines ? ` (${lines.join(" and ")})` : ""}, so its chart cannot be changed here.`;
const INHERITED_CHART =
   "This tile's view is declared on its source, so its chart is set in the model.";

/** Width presets, as fractions of this grid. A tile's width is otherwise a column count, which cannot say "a third". */
function WidthPresets({
   columns,
   colspan,
   onPick,
}: {
   columns: number;
   colspan: number | undefined;
   onPick: (span: number) => void;
}) {
   const { theme } = usePublisherTheme();
   return (
      <Stack direction="row" sx={{ gap: 0.5, alignItems: "center" }}>
         <Typography variant="caption" sx={{ color: theme.tileTitle, mr: 0.5 }}>
            Width
         </Typography>
         {(
            [
               ["Full", 1],
               ["½", 2],
               ["⅓", 3],
               ["¼", 4],
            ] as const
         ).map(([label, share]) => {
            const span = presetSpan(columns, share);
            const active = (colspan ?? 1) === span;
            return (
               <Button
                  key={label}
                  size="small"
                  variant={active ? "contained" : "outlined"}
                  aria-label={`Width ${label}`}
                  aria-pressed={active}
                  onClick={() => onPick(span)}
                  sx={{ minWidth: 40, px: 1 }}
               >
                  {label}
               </Button>
            );
         })}
      </Stack>
   );
}

export function TileMenu({
   anchor,
   tile,
   onClose,
   onCommit,
   onRemove,
   removeBlocked,
   columns,
   onDrills,
   view,
}: TileMenuProps) {
   const removeReasonId = useId();
   const { theme } = usePublisherTheme();
   const { draft, patch, close, discard } = useDraft(
      tile,
      anchor !== null,
      onCommit,
      onClose,
   );

   const query = draft !== undefined && isQueryTile(draft) ? draft : undefined;
   const editable =
      query !== undefined && query.declaration.kind !== "inherited";
   // The draft is a query tile whenever the controls that call this are shown.
   const patchQuery = (change: (t: QueryTile) => void) =>
      patch((t) => {
         if (isQueryTile(t)) change(t);
      });
   const originalChart = tile && isQueryTile(tile) ? tile.chart : undefined;

   return (
      <Popover
         open={anchor !== null && draft !== undefined}
         anchorEl={anchor}
         onClose={close}
         anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
         transformOrigin={{ vertical: "top", horizontal: "right" }}
         slotProps={{ paper: { sx: { width: 320, p: 2 } } }}
      >
         {draft && (
            <Stack sx={{ gap: 1.5 }}>
               <Typography
                  variant="overline"
                  sx={{ color: theme.tileTitle, lineHeight: 1.5 }}
               >
                  {query
                     ? `${query.source} → ${query.name}`
                     : `Text · ${draft.name}`}
               </Typography>

               {!query && columns > 1 && (
                  <WidthPresets
                     columns={columns}
                     colspan={draft.colspan}
                     onPick={(span) =>
                        patch((t) => {
                           t.colspan = span;
                        })
                     }
                  />
               )}

               {query && editable && (
                  <>
                     <ChartPicker
                        state={query.chart ?? "default"}
                        view={view}
                        cellLabel={`${query.source} ${query.name}`}
                        {...(query.chart === "custom"
                           ? { disabledReason: customChart(query.chartLines) }
                           : {})}
                        onChange={(next) =>
                           patchQuery((t) => {
                              const was = originalChart;
                              // Back to where it started is no edit, so the draft must not differ from the tile.
                              if (next === (was ?? "default")) {
                                 if (was === undefined) delete t.chart;
                                 else t.chart = was;
                                 delete t.chartCarried;
                              } else {
                                 t.chart = next;
                                 if (view)
                                    t.chartCarried = view.chart
                                       ? [view.chart]
                                       : [];
                                 else delete t.chartCarried;
                              }
                           })
                        }
                     />
                     {columns > 1 && (
                        <WidthPresets
                           columns={columns}
                           colspan={query.colspan}
                           onPick={(span) =>
                              patchQuery((t) => {
                                 t.colspan = span;
                              })
                           }
                        />
                     )}
                  </>
               )}
               {query && !editable && (
                  <>
                     <Typography
                        variant="body2"
                        sx={{ color: theme.tileTitle }}
                     >
                        Declared on its source, so its title and layout are set
                        in the model. It can still be moved.
                     </Typography>
                     <ChartPicker
                        state="default"
                        view={view}
                        cellLabel={`${query.source} ${query.name}`}
                        disabledReason={INHERITED_CHART}
                        onChange={() => {}}
                     />
                  </>
               )}
               {query?.declaration.kind === "opaque" && (
                  <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                     Its body is {query.declaration.why}, so a filter has no
                     single place to go. Everything else here is editable.
                  </Typography>
               )}
               <Divider />
               <Stack direction="row" sx={{ justifyContent: "space-between" }}>
                  {editable ? (
                     <Button
                        size="small"
                        onClick={() => {
                           close();
                           onDrills();
                        }}
                     >
                        Drill-through…
                     </Button>
                  ) : (
                     <span />
                  )}
                  <Button
                     color="error"
                     size="small"
                     // aria-disabled, not disabled, so the reason stays reachable by keyboard.
                     aria-disabled={removeBlocked !== undefined || undefined}
                     aria-describedby={
                        removeBlocked !== undefined ? removeReasonId : undefined
                     }
                     disableRipple={removeBlocked !== undefined}
                     sx={
                        removeBlocked !== undefined
                           ? { opacity: 0.5, cursor: "default" }
                           : undefined
                     }
                     onClick={() => {
                        if (removeBlocked !== undefined) return;
                        discard();
                        onRemove();
                     }}
                  >
                     Remove tile
                  </Button>
               </Stack>
               {removeBlocked !== undefined && (
                  <Typography
                     id={removeReasonId}
                     variant="caption"
                     sx={{ color: theme.tileTitle }}
                  >
                     {removeBlocked}
                  </Typography>
               )}
            </Stack>
         )}
      </Popover>
   );
}
