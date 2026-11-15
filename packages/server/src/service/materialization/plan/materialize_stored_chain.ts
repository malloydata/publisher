// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The text a chained `storage=` extension table's build compiles in its
 * destination: its parent's build source declared over the parent's stored
 * table, then the declarations from the parent to the extension as the build
 * copy writes them, and nothing else of the file.
 *
 * Read by the companion compiler, which checks the chain compiles before it
 * chains, and by the `storage=` build.
 *
 * Pure: text in, text out.
 */

import type { MaterializePlan, StoredChain } from "../domain/table_plans";
import { copyEdits, ACCESS_FLAG } from "./materialize_build_copy";
import { offsetOf } from "../text/malloy_lexer";
import { applyEdits, documentFlagLines } from "../text/declaration_text";
import { identifier, stringLiteral, annotationValue } from "../../malloy_text";
import { buildSourceBody, importAlias } from "./build_file_generator";

/**
 * The text a `storage=` extension table's build compiles in its destination
 * (P10): the parent's declaration and each one on the path to the
 * extension's, edited as the build copy edits them (the parent re-rooted on
 * its build source, the extension's given-reading terms out) and nothing else
 * of the file, so the build reads no source but the parent's stored table.
 * Each join_one the parent inlines is read from the parent's stored
 * `<join>__<field>` columns, as the served copy reads it: its definition taken
 * out of the parent's block and a record of those columns in its place,
 * under the join's own modifier, so an extension that reads `<join>.<field>`
 * builds over the parent's table rather than the raw chain. Undefined when
 * the copy's edits cannot be made, or a declaration is not where the plan
 * says.
 */
export function storedChainText(
   text: string,
   plan: MaterializePlan,
   parent: MaterializePlan,
   options: { liftAccess?: boolean } = {},
): StoredChain | undefined {
   const over = plan.buildsOver;
   if (!over) return undefined;
   const edits = copyEdits(text, plan, parent, options.liftAccess, true);
   if ("reason" in edits) return undefined;
   const statements: string[] = [];
   for (const { range } of [over.declaration, ...over.path]) {
      const from = offsetOf(text, range.start);
      const to = offsetOf(text, range.end);
      if (from < 0 || to < from) return undefined;
      const inside = edits.edits
         .filter((e) => e.start >= from && e.end <= to)
         .map((e) => ({ ...e, start: e.start - from, end: e.end - from }));
      const edited = applyEdits(text.slice(from, to), inside);
      if (edited === undefined) return undefined;
      // malloy-text: the author's own declaration, edited as its build copy edits it
      statements.push(`source: ${edited}`);
   }
   return {
      parentBuildSourceName: parent.buildSourceName,
      // The lift's `include { public: * }` needs its flag here too (P3).
      flags: [
         ...documentFlagLines(text),
         ...(options.liftAccess ? [ACCESS_FLAG] : []),
      ],
      declarations: statements.join("\n"),
   };
}

/** A stored column of the parent, as {@link extensionOverStoredModel} declares it. */
export interface StoredColumnType {
   name: string;
   /** A Malloy basic type: `string`, `number`, `boolean`, `date`, `timestamp` or `json`. */
   type: string;
}

/**
 * The model a `storage=` extension table builds from in its destination
 * (P10): the parent's build source declared as the virtual source of its
 * stored table on `connectionName`, the extension's {@link StoredChain} over
 * it, and the extension's generated build source, `#@ persist storage=`, last.
 * Its given-reading terms are not written: the build leaves them out, and the
 * table's address is the companion's, not this model's.
 */
export function extensionOverStoredModel(params: {
   plan: MaterializePlan;
   chain: StoredChain;
   /** The connection the stored table is read through: the destination. */
   connectionName: string;
   /** The virtualMap handle of the parent's stored table. */
   virtualHandle: string;
   /** The parent's stored columns. */
   columns: readonly StoredColumnType[];
}): string {
   const { plan, chain } = params;
   const typeName = identifier(`${chain.parentBuildSourceName}__shape`);
   // malloy-text: names through identifier, each type a Malloy basic type name
   const fields = params.columns
      .map((c) => `   ${identifier(c.name)}::${c.type}`)
      .join(",\n");
   // malloy-text: buildSourceBody writes every name through identifier
   const body = buildSourceBody(plan);
   const flags = [
      // malloy-text: the author file's own `##!` lines, carried as written
      ...chain.flags,
      "##! experimental { persistence virtual_source }",
   ].join("\n");
   // malloy-text: the author's own declarations, edited as the build copy edits them (storedChainText)
   const declarations = chain.declarations;
   return `${flags}
type: ${typeName} is {
${fields}
}
source: ${identifier(chain.parentBuildSourceName)} is ${identifier(params.connectionName)}.virtual(${stringLiteral(params.virtualHandle)})::${typeName}
${declarations}
source: ${identifier(importAlias(plan))} is ${identifier(plan.sourceName)}
#@ persist storage=${annotationValue(params.connectionName)}
source: ${identifier(plan.buildSourceName)} is ${identifier(importAlias(plan))} -> {
${body}
}
`;
}
