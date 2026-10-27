// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Result } from "@malloydata/malloy";

/**
 * Correct the drill metadata Malloy writes into a result, where as written
 * it would make a client's "rows behind this value" return the wrong rows.
 *
 * A client builds those rows as
 * `run: <source> -> { where: <each dimension's code> = <its value>; select: * }`,
 * reading each dimension's code from `drill_expression.code` and whether a
 * level can be drilled at all from `drillable`. Malloy 0.0.436 gets three of
 * those wrong, and only the server still holds what it needs to fix them:
 *
 * - The root of a pipeline of more than one stage is marked `drillable`.
 *   Its dimensions are an earlier stage's outputs, which the source cannot be
 *   filtered by. Malloy already leaves a nested pipeline unmarked; the root
 *   is unmarked the same way.
 * - A computed dimension declared in a joined source (`users.signup_year is
 *   created_at.year`) gets its declaration text as its code, which reads the
 *   root source's `created_at`. Its dotted name (`users.signup_year`) is
 *   written instead.
 * - A time truncation with no name of its own (`group_by: created_at.month`
 *   in a view or a nest) gets the bare field as its code, without the
 *   truncation and without a join path. Where the result still says which
 *   field and truncation it was, that is written; otherwise the dimension
 *   gets no code, so a client offers no rows for it rather than wrong ones.
 *
 * Remove each rule once a Malloy release writes the metadata this way.
 */
export function correctDrillMetadata(result: Result): void {
   const structs = result._queryResult?.structs as unknown as
      | ResultField[]
      | undefined;
   const root = structs?.[structs.length - 1];
   if (root === undefined) return;
   if (root.resultMetadata?.sourceField === LAST_STAGE)
      root.resultMetadata.drillable = false;
   correctFields(root.fields ?? []);
}

/** What Malloy names the final stage of a pipeline of more than one. */
const LAST_STAGE = "~computeLastStage~";

/** A result field, as much of Malloy's internal definition as this reads. */
interface ResultField {
   name: string;
   timeframe?: string;
   fields?: ResultField[];
   resultMetadata?: {
      sourceField?: string;
      sourceExpression?: string;
      fieldKind?: string;
      drillable?: boolean;
      drillExpression?: {
         kind: string;
         truncation?: string;
         field_reference?: { name: string; path?: string[] };
      };
   };
}

function correctFields(fields: ResultField[]): void {
   for (const field of fields) {
      if (field.fields) correctFields(field.fields);
      const meta = field.resultMetadata;
      if (meta?.fieldKind !== "dimension") continue;
      const code = drillCode(field, meta);
      // Malloy writes a code for every dimension, so to write none the
      // field is no longer reported as one; nothing else reads `fieldKind`
      // once the query has run.
      if (code === null) delete meta.fieldKind;
      else if (code !== undefined) meta.sourceExpression = code;
   }
}

/**
 * The code to write for a dimension: a string, `undefined` to keep what
 * Malloy writes, or `null` to write none.
 */
function drillCode(
   field: ResultField,
   meta: NonNullable<ResultField["resultMetadata"]>,
): string | null | undefined {
   const { sourceField, sourceExpression, drillExpression } = meta;
   // A dotted source field is a field declared in a join. Its declaration
   // text names the join's fields, so its path is the code.
   if (sourceExpression)
      return sourceField?.includes(".") ? sourceField : undefined;
   if (field.timeframe === undefined) return undefined;
   const reference = drillExpression?.field_reference;
   if (
      drillExpression?.kind === "time_truncation" &&
      reference !== undefined &&
      drillExpression.truncation !== undefined
   )
      return [
         ...(reference.path ?? []),
         reference.name,
         drillExpression.truncation,
      ].join(".");
   return null;
}
