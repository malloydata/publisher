// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { StringFilterExpression } from "@malloydata/malloy-filter";
import type { FilterSelection } from "../hooks/useDimensionFilters";
import { formatMalloyValue, malloyFilterLiteral } from "./malloyLiteral";

/**
 * One dimension filter's selection as a Malloy condition on `fieldName` (an
 * already-quoted path such as `` `name` ``), or "" when the selection says
 * nothing a condition can.
 *
 * The one place a dimension filter's value becomes Malloy text. A "Contains"
 * value is written in the string filter grammar, where `,` separates
 * alternatives and `%` and `_` are wildcards, so it goes out through
 * `@malloydata/malloy-filter`'s own printer rather than being spliced into
 * `%…%`: spliced, "a,b" matched `a` and `b` but not "a,b", and "50%" also
 * matched "500".
 */
export function filterCondition(
   fieldName: string,
   selection: FilterSelection,
): string {
   const { matchType, value, value2 } = selection;
   if (Array.isArray(value)) {
      // Only a multi-select, which matches any of its picks.
      if (matchType !== "Equals" && matchType !== "Semantic Search") return "";
      if (value.length === 0) return "";
      const conditions = value.map(
         (v) => `${fieldName} = ${formatMalloyValue(v)}`,
      );
      return `(${conditions.join(" or ")})`;
   }
   switch (matchType) {
      case "Equals":
      case "Semantic Search":
         return `${fieldName} = ${formatMalloyValue(value)}`;

      case "Contains":
         if (typeof value === "string") {
            const filter = StringFilterExpression.unparse({
               operator: "contains",
               values: [value],
            });
            return `${fieldName} ~ ${malloyFilterLiteral(filter)}`;
         }
         return `${fieldName} = ${formatMalloyValue(value)}`;

      case "After":
      case "Greater Than":
         return `${fieldName} > ${formatMalloyValue(value)}`;

      case "Before":
      case "Less Than":
         return `${fieldName} < ${formatMalloyValue(value)}`;

      case "Between":
         if (value2 !== undefined) {
            return `${fieldName} >= ${formatMalloyValue(value)} and ${fieldName} <= ${formatMalloyValue(value2)}`;
         }
         return `${fieldName} >= ${formatMalloyValue(value)}`;

      default:
         return "";
   }
}
