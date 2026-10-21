// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Menu, MenuItem } from "@mui/material";
import { useCallback, useMemo, useState, type ReactNode } from "react";
import { drillClauseLabels } from "./drillClauses";
import type { DrillField } from "./resolveDrill";
import {
   canShowRows,
   dimensionCode,
   rowDimensionsOf,
   rowsDimensions,
   rowsTargetOf,
   type RowsDimension,
   type RowsField,
} from "./rowsTarget";
import {
   DRILL_SELF,
   drillDestinations,
   drillGivenName,
   drillValueLabel,
   drillValueToFilter,
   humanizeSlug,
   resolveDrill,
   type DrillClickPayload,
   type DrillIntent,
   type DrillNavigation,
   type DrillRowsRequest,
} from "./resolveDrill";

export interface UseDrillOptions {
   /**
    * Open another dashboard, seeded with the clicked value. Omitted on a surface
    * that cannot navigate, which makes those destinations inert rather than
    * dead-ended.
    */
   onNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   /**
    * Apply the clicked value to this surface's own control row (`to=self`).
    * Omitted on a surface that has none, such as a notebook cell.
    */
   onSelf?: (given: string, rawValue: unknown) => void;
   /**
    * Whether a `to=self` drill onto this given can actually be honoured: for a
    * notebook, whether it declares that given at all.
    *
    * Without it the affordance says yes to every self-drill and the click is
    * refused afterwards with a console warning, which is the dead link this
    * module exists to avoid. Absent means "any self-drill is fine", the right
    * default for a surface that binds whatever it is handed.
    */
   canSelf?: (given: string) => boolean;
   /**
    * What a `to=self` entry starts with, before the `Field: value` it sets.
    * Defaults to `Filter to`.
    */
   selfLabel?: string;
   /**
    * A `to=` destination's title, for its menu entry. Undefined, or no
    * function, and the entry names the destination's file name instead.
    */
   destinationTitle?: (destination: string) => string | undefined;
   /**
    * Show the rows behind a grouped value. With it, a dimension cell whose
    * result names everything its rows query needs (`canShowRows`) offers
    * "Show rows" in its menu, beside any `# drill` actions. Omitted on a
    * surface with nowhere to show them, and cells then offer only what a tag
    * says.
    */
   onRows?: (request: DrillRowsRequest) => void;
}

/**
 * What a surface hands to its result renderer to become drillable: the click
 * handler, and the predicate that decides which cells say so.
 *
 * One object because the two must agree. Passing only the handler is what
 * produced the original asymmetry: clicks worked, but nothing looked clickable.
 */
export interface DrillBinding {
   /**
    * Every cell click. Most carry no `# drill` and mean nothing, so this
    * tolerates being called constantly.
    */
   onClick: (payload: DrillClickPayload) => void;
   /** Whether this field's cells should read as clickable. */
   canDrill: (field: DrillField) => boolean;
}

export interface UseDrillResult {
   /** Hand to `ResultContainer`'s (or `RenderedResult`'s) `drill` prop. */
   drill: DrillBinding;
   /**
    * The binding for one result on a surface with several, such as a
    * dashboard's tiles: its clicks carry `context`, and a field opens rows
    * only where `rowsGate` also says so. `rowsGate: false` turns rows off for
    * that result, leaving its tag drills.
    */
   bindingFor: (
      context: string | undefined,
      rowsGate: ((field: DrillField) => boolean) | false,
   ) => DrillBinding;
   /** Render anywhere in the subtree: the menu every drillable click opens. */
   drillMenu: ReactNode;
}

interface DrillMenuItem {
   key: string;
   label: string;
   run: () => void;
}

interface DrillMenuState {
   items: DrillMenuItem[];
   top: number;
   left: number;
}

/** A menu item is cut to this width; its full text is its tooltip and accessible name. */
const MENU_ITEM_MAX_WIDTH = 360;

/** A field as its column header reads: its `# label`, else its name. */
function fieldLabel(field: DrillField | RowsField | undefined): string {
   if (!field) return "";
   const header = (field as { getLabel?: () => string }).getLabel?.();
   return (
      header ??
      (field as DrillField).tag?.text("label") ??
      (field as RowsField).metadataTag?.text("label") ??
      field.name
   );
}

/**
 * The values the rows behind a click are filtered on, each named by its
 * column header: `Region: West, Month: 2025-12`, outermost first. Without the
 * row's clauses, the clicked value alone.
 */
function rowsValuesLabel(rows: DrillRowsRequest, field: RowsField): string {
   const parts = rows.drillClauses
      ? drillClauseLabels(rows.drillClauses, rows.timezone)
      : [];
   if (parts.length === 0) return `${fieldLabel(field)}: ${rows.label}`;
   const row = rowDimensionsOf(field);
   return parts
      .map((part) => {
         const key = JSON.stringify(part.expression);
         const match = row.find(
            (d) => JSON.stringify(d.drillStableExpression) === key,
         );
         return `${match ? fieldLabel(match) : part.field}: ${part.label}`;
      })
      .join(", ");
}

/**
 * Click-to-drill for any surface that renders a Malloy result.
 *
 * Every drillable click opens one small menu that says what it can do, even
 * when there is only one thing, each item naming the field and value it acts
 * on: `Filter to Region: West` for a `to=self` drill, `Open Regional review
 * (Region: West)` for each `to=` report, then `Show rows (Region: West, Month:
 * 2025-12)`. A field is named as its column header reads. A `# drill` tag
 * adds actions; it never replaces the rows, and a click never acts on a tag
 * the reader cannot see.
 *
 * Shared between the dashboard viewer and notebook cells on purpose: `# drill`
 * is declared on a model *dimension*, so it fires wherever that dimension is
 * grouped, and a notebook can therefore drill into a dashboard with no notebook-
 * specific code. What differs between surfaces is only what they can do about it,
 * which is what the two callbacks express.
 */
export function useDrill({
   onNavigate,
   onSelf,
   selfLabel = "Filter to",
   destinationTitle,
   canSelf,
   onRows,
}: UseDrillOptions): UseDrillResult {
   // A cell inside the renderer's own DOM is the anchor, and this component
   // holds no ref to it, so the menu is positioned from the click coordinates.
   const [menu, setMenu] = useState<DrillMenuState | null>(null);

   // What this surface can actually do about a destination. Applied to the click
   // and to the affordance from one place, so a cell reads as clickable exactly
   // when clicking it would do something.
   const honorable = useCallback(
      (destinations: string[], given: string) =>
         destinations.filter((destination) =>
            destination === DRILL_SELF
               ? onSelf !== undefined && (canSelf?.(given) ?? true)
               : onNavigate !== undefined,
         ),
      [onNavigate, onSelf, canSelf],
   );

   const go = useCallback(
      (intent: DrillIntent, destination: string, event?: MouseEvent) => {
         if (destination === DRILL_SELF) {
            // The raw value, not `intent.value`: only this surface knows the
            // declared type of its own given, so only it can encode correctly.
            onSelf?.(intent.given, intent.rawValue);
            return;
         }
         onNavigate?.(
            {
               path: destination,
               dashboard: destination,
               givens: { [intent.given]: intent.value },
            },
            event,
         );
      },
      [onNavigate, onSelf],
   );

   // The rows behind this click, when the surface shows rows and the result
   // names everything the query needs (`canShowRows`). A header names the
   // field and has no value; a measure has no rows of its own. Without the
   // row's clauses the clicked value is the whole filter, so an empty cell is
   // refused as a likely misclick.
   const rowsBehind = useCallback(
      (payload: DrillClickPayload): DrillRowsRequest | undefined => {
         if (!onRows || payload.noRows || payload.isHeader || !payload.field)
            return undefined;
         const field = payload.field as RowsField;
         if (!canShowRows(field)) return undefined;
         const target = rowsTargetOf(field);
         if (target === undefined) return undefined;
         const clauses = payload.drillClauses;
         // With the row: every dimension of it. Without one (a click the
         // renderer reports no row for), the clicked value alone, which names
         // the rows only when it is the row's one dimension.
         let rows: { dimensions: RowsDimension[] } | { code: string };
         if (clauses !== undefined) {
            const dimensions = rowsDimensions(field, clauses);
            if (dimensions === undefined) return undefined;
            rows = { dimensions };
         } else {
            const code = dimensionCode(field);
            if (
               code === undefined ||
               rowDimensionsOf(field).length !== 1 ||
               drillValueToFilter(payload.value) === undefined
            )
               return undefined;
            rows = { code };
         }
         return {
            field: payload.field.name,
            rawValue: payload.value,
            label: drillValueLabel(payload.value),
            target,
            ...rows,
            ...(payload.context === undefined
               ? {}
               : { context: payload.context }),
            ...(clauses === undefined ? {} : { drillClauses: clauses }),
            ...(payload.timezone === undefined
               ? {}
               : { timezone: payload.timezone }),
         };
      },
      [onRows],
   );

   const onClick = useCallback(
      (payload: DrillClickPayload) => {
         // Runs on every cell click, so "not a drill" is the ordinary answer.
         const intent = resolveDrill(payload);
         const rows = rowsBehind(payload);
         // Offer only what this surface can actually do, so the menu never lists
         // an action that would be a no-op when picked. Deduplicated: `# drill
         // { to=[overview, overview] }` is a typo rather than a request for two
         // identical rows. `to=self` first, then reports in the tag's order.
         const destinations = intent
            ? Array.from(new Set(honorable(intent.to, intent.given))).sort(
                 (a, b) => Number(b === DRILL_SELF) - Number(a === DRILL_SELF),
              )
            : [];
         const value = `${fieldLabel(payload.field)}: ${
            intent?.label ?? drillValueLabel(payload.value)
         }`;
         const items: DrillMenuItem[] = destinations.map((destination) => ({
            key: `to:${destination}`,
            label:
               destination === DRILL_SELF
                  ? `${selfLabel} ${value}`
                  : // The report's title, else its file name (a path from the package root).
                    `Open ${
                       destinationTitle?.(destination) ??
                       humanizeSlug(destination.split("/").pop() ?? destination)
                    } (${value})`,
            run: () => {
               if (intent) go(intent, destination, payload.event);
            },
         }));
         if (rows)
            items.push({
               key: "rows",
               label: `Show rows (${rowsValuesLabel(rows, payload.field as RowsField)})`,
               run: () => onRows?.(rows),
            });
         if (items.length === 0) return;
         setMenu({
            items,
            top: payload.event?.clientY ?? 0,
            left: payload.event?.clientX ?? 0,
         });
      },
      [go, honorable, onRows, rowsBehind, selfLabel, destinationTitle],
   );

   const canDrillTag = useCallback(
      (field: DrillField) =>
         honorable(drillDestinations(field), drillGivenName(field)).length > 0,
      [honorable],
   );
   const canDrillRows = useCallback(
      (field: DrillField) =>
         onRows !== undefined && canShowRows(field as RowsField),
      [onRows],
   );

   const drill = useMemo<DrillBinding>(
      () => ({
         onClick,
         canDrill: (field) => canDrillTag(field) || canDrillRows(field),
      }),
      [onClick, canDrillTag, canDrillRows],
   );

   const bindingFor = useCallback(
      (
         context: string | undefined,
         rowsGate: ((field: DrillField) => boolean) | false,
      ): DrillBinding => ({
         canDrill: (field) =>
            canDrillTag(field) ||
            (rowsGate !== false && rowsGate(field) && canDrillRows(field)),
         onClick: (payload) =>
            onClick({
               ...payload,
               ...(context === undefined ? {} : { context }),
               ...(rowsGate === false ||
               (payload.field !== undefined && !rowsGate(payload.field))
                  ? { noRows: true }
                  : {}),
            }),
      }),
      [onClick, canDrillTag, canDrillRows],
   );

   const close = useCallback(() => setMenu(null), []);

   return {
      drill,
      bindingFor,
      drillMenu: (
         <Menu
            open={menu !== null}
            onClose={close}
            anchorReference="anchorPosition"
            anchorPosition={{ top: menu?.top ?? 0, left: menu?.left ?? 0 }}
         >
            {/* Only actions: each item names the value it acts on, so there is
                no header row that reads like an item and does nothing. */}
            {(menu?.items ?? []).map((item) => (
               <MenuItem
                  key={item.key}
                  title={item.label}
                  aria-label={item.label}
                  onClick={() => {
                     close();
                     item.run();
                  }}
                  sx={{ maxWidth: MENU_ITEM_MAX_WIDTH }}
               >
                  <span
                     style={{
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                     }}
                  >
                     {item.label}
                  </span>
               </MenuItem>
            ))}
         </Menu>
      ),
   };
}
