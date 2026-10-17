// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { ModelCompilationError } from "../errors";

/** A line whose first non-whitespace content is a `#@ persist` directive. */
const PERSIST_LINE_PATTERN = /^\s*#@\s+persist\b/;
/**
 * A bare (unquoted) `name=` value. The lookbehind requires a standalone key:
 * `tablename=` and `queryMetadata.name=` are not the persist name. `\b` would
 * miss the dotted case, since `.` is itself a boundary.
 */
const UNQUOTED_NAME_PATTERN = /(?<![.\w])name\s*=\s*(?!["'])/;

/**
 * A quoted standalone `name=` value, capturing the quote and the content. Global
 * because a line may repeat the key; every occurrence is validated, so which one
 * the tag parser keeps does not matter.
 */
const QUOTED_NAME_VALUE_PATTERN = /(?<![.\w])name\s*=\s*(["'])(.*?)\1/g;

/**
 * The only safe persist name: dot-separated `[A-Za-z0-9_-]` segments. The value
 * is inlined into `CREATE OR REPLACE TABLE` / `DROP TABLE IF EXISTS` DDL, so a
 * quote, backtick, `;` or whitespace would be an injection vector.
 *
 * Must match the control plane's `PhysicalTableName` grammar, or the two
 * services disagree on which names are legal. Hyphens and leading digits are
 * allowed for hyphenated BigQuery project ids (`my-proj.mydataset.events`).
 */
const SAFE_NAME_PATH = /^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$/;

/**
 * Reject `#@ persist name=<value>` annotations whose name is unusable or unsafe.
 *
 * Both failures are fatal at publish/load (HTTP 424):
 *  - Unquoted: the build plan silently drops a bare value, so the source would
 *    serve but never materialize.
 *  - Quoted but not {@link SAFE_NAME_PATH}: the value is inlined into DDL, so it
 *    could close the identifier and append statements.
 *
 * Scans raw source lines rather than compiled annotations, because the tag
 * parser discards whether the value was quoted.
 *
 * @throws {ModelCompilationError} listing every offending annotation.
 */
export function assertPersistNamesQuoted(
   modelSource: string,
   modelPath: string,
): void {
   const unquoted: string[] = [];
   const unsafe: string[] = [];
   for (const rawLine of modelSource.split("\n")) {
      if (!PERSIST_LINE_PATTERN.test(rawLine)) continue;
      if (UNQUOTED_NAME_PATTERN.test(rawLine)) {
         unquoted.push(rawLine.trim());
         continue;
      }
      for (const match of rawLine.matchAll(QUOTED_NAME_VALUE_PATTERN)) {
         if (!SAFE_NAME_PATH.test(match[2])) {
            unsafe.push(rawLine.trim());
            break;
         }
      }
   }
   if (unquoted.length > 0) {
      throw new ModelCompilationError({
         message:
            `${modelPath}: persist annotation name must be quoted. Write a quoted ` +
            `value like name="engaged_events" (or a dialect table path such as ` +
            `name="my_dataset.engaged_events"), not a bare value -- an unquoted ` +
            `persist name is dropped from the build plan, so the source would ` +
            `publish but never materialize. Offending annotation(s): ` +
            `${unquoted.join("; ")}.`,
      });
   }
   if (unsafe.length > 0) {
      throw new ModelCompilationError({
         message:
            `${modelPath}: persist annotation name must be a plain identifier path -- ` +
            `dot-separated segments of letters, digits, underscores, and hyphens ` +
            `(e.g. name="engaged_events", name="my_dataset.engaged_events", or a ` +
            `hyphenated container path name="my-proj.mydataset.engaged_events"). ` +
            `The name is inlined into the materialization table DDL, so a value ` +
            `containing a quote, backtick, semicolon, or space is rejected. ` +
            `Offending annotation(s): ${unsafe.join("; ")}.`,
      });
   }
}
