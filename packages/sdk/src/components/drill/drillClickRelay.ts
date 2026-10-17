// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DrillClause } from "./drillClauses";
import type { DrillClickPayload } from "./resolveDrill";

/** As much of the renderer's `DrillData` as the relay reads. */
export interface RendererDrill {
   stableDrillClauses: DrillClause[] | undefined;
}

/**
 * Joins the renderer's two reports of one table-cell click into one payload.
 *
 * `onClick` names the clicked field and value; only `onDrill` knows the row,
 * and the renderer calls it only with `tableConfig.enableDrill` set. For a
 * table cell both run inside the same event dispatch, `onClick` first (the
 * cell's handler, then its row-cell wrapper's), so a click is held for one
 * microtask and the row lands on it before `forward` sees it. A click with no
 * row, such as a dashboard item, goes out as it came.
 */
export function drillClickRelay(
   forward: (payload: DrillClickPayload) => void,
): {
   onClick: (payload: DrillClickPayload) => void;
   onDrill: (data: RendererDrill) => void;
} {
   let held: DrillClickPayload | undefined;
   return {
      onClick: (payload) => {
         const click: DrillClickPayload = {
            ...payload,
            ...timezoneOf(payload),
         };
         held = click;
         queueMicrotask(() => {
            if (held === click) held = undefined;
            forward(click);
         });
      },
      onDrill: (data) => {
         const click = held;
         held = undefined;
         // Undefined when a clause has no stable form: the click then stands
         // on its own value rather than on part of a row.
         if (click && data.stableDrillClauses !== undefined)
            click.drillClauses = data.stableDrillClauses;
      },
   };
}

/**
 * The query timezone the clicked field was computed in, which a truncated
 * timestamp in the row's clauses is spelled in. The renderer's field answers
 * it; the narrowed payload type does not declare it.
 */
function timezoneOf(payload: DrillClickPayload): { timezone?: string } {
   const field = payload.field as
      | { getEffectiveQueryTimezone?: () => string | undefined }
      | undefined;
   try {
      const timezone = field?.getEffectiveQueryTimezone?.();
      return timezone ? { timezone } : {};
   } catch {
      return {};
   }
}
