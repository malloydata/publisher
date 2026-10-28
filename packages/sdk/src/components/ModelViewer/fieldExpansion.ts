// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a field row opens to — the one rule behind both whether the row is a
 * button (FieldRow) and the order fields appear in (FieldGroup). Kept in a pure
 * module so those two can't drift: a row that opens to nothing, or an
 * "expandable first" sort that hoists rows with no expansion, are the same bug
 * seen from either side.
 */

import type { FieldCodeLookup } from "./fieldCode";
import type { ExplorerField } from "./schemaModel";

/** A dimension whose VALUES are in the search index (`#(index)`), so the row can
 *  show sample values — content in its own right, with or without a definition. */
export function isIndexedDimension(field: ExplorerField): boolean {
   return field.kind === "dimension" && field.indexed === true;
}

/**
 * Whether the expansion has a definition section to draw. True while the fetch
 * is in flight (the answer isn't known yet, and a row that opens late is
 * better than one that vanishes under the pointer) and true on failure, which
 * is a fact about the pane worth surfacing rather than reading as "no
 * definition". False for an inherited table column — its story is told on the
 * row's hover instead.
 */
export function hasDefinitionToShow(
   fieldCode: FieldCodeLookup | undefined,
): boolean {
   return (
      !!fieldCode &&
      (fieldCode.loading ||
         !!fieldCode.code ||
         !!fieldCode.inheritedFrom ||
         !!fieldCode.error)
   );
}

export function isFieldExpandable(
   field: ExplorerField,
   fieldCode: FieldCodeLookup | undefined,
): boolean {
   return hasDefinitionToShow(fieldCode) || isIndexedDimension(field);
}

/**
 * Expandable fields first, each block keeping the model's declaration order.
 *
 * A group of a table-backed source's dimensions is mostly passthrough columns,
 * and the handful the modeler actually wrote were scattered among them — the
 * interesting rows found by hunting for the ones that open. Hoisting them puts the
 * source's own work at the top of the group and leaves the inherited columns as
 * the tail they are.
 *
 * **The row and the order answer "loading" differently, on purpose.**
 * `isFieldExpandable` is optimistic while the definitions read is in flight —
 * a row that becomes openable late beats one that stops being so under the pointer. Order
 * cannot be optimistic the same way: taking `loading` as expandable puts EVERY
 * field in the hoisted block, so `rest` is empty, the identity short-circuit
 * below returns the list unsorted, and the group then re-sorts under the reader
 * when the read lands. So ordering asks a stricter question — is this field
 * expandable *on what is known now*.
 *
 * **This narrows the reorder; it does not remove it.** A row is knowable at
 * first paint only through `isIndexedDimension`, which reads `field.indexed`
 * off the compiled schema. A plain written measure still hoists when its
 * definition's read lands (the model text's parse, or a host's entities),
 * because written-vs-inherited is only known from that read. Removing the rest
 * needs the compiled schema to say what a field is.
 */
export function expandableFirst(
   items: ExplorerField[],
   getFieldCode: (field: ExplorerField) => FieldCodeLookup | undefined,
): ExplorerField[] {
   const expandable: ExplorerField[] = [];
   const rest: ExplorerField[] = [];
   for (const field of items) {
      const fieldCode = getFieldCode(field);
      // `loading` is the one input that would flip this row's answer when the
      // read lands, so it does not count here.
      const knownExpandable =
         (!!fieldCode &&
            !fieldCode.loading &&
            (!!fieldCode.code || !!fieldCode.error)) ||
         isIndexedDimension(field);
      (knownExpandable ? expandable : rest).push(field);
   }
   // Nothing to reorder — return the original array so React sees the same
   // reference and the rows don't remount on every parent render.
   return rest.length === 0 || expandable.length === 0
      ? items
      : [...expandable, ...rest];
}
