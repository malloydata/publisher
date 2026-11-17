// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Publish-time checks of `#@ persist` lines, read from the model's raw text
 * because the tag parser discards whether a value was quoted.
 *
 * Surface: `persistNameProblems`, which `package.ts` collects over its models
 * as a package loads. It refuses a `name=` that is unquoted (the build plan
 * would drop it, so the source would never build) or that is not a plain
 * identifier path (the name is inlined into table DDL, so anything else is an
 * injection vector). Pure.
 */

import { BadRequestError } from "../../../errors";
import { codedLine, PERSIST_INVALID_CODE } from "../../annotation_reader";
import type { Annotations } from "@malloydata/malloy";
import { hasEnvReference, routeReadsEnv } from "../../motly";
import { hasReservedTableSuffix } from "../domain/table_name";
import {
   EMPTY_STORAGE_REASON,
   placementOf,
   readRouteIfAny,
} from "./declaration";

/** What a materialization line that reads `@env.` is told, on every route. */
export const ENV_REFERENCE_REASON =
   "one of its lines reads the server's environment (`@env.`), which a " +
   "materialization annotation may not: Publisher never reads it, so the " +
   "line's directives are lost. Write the value into the model";

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
 * Why an author's table `name=` is invalid, on any route, or undefined: it is
 * not a {@link SAFE_NAME_PATH} (it is inlined into DDL), or its table segment
 * ends the way Publisher names its own tables (`__p<hash>`,
 * `__v<version>_<hash>`), so it could be another package's table.
 */
export function tableNameProblem(name: string): string | undefined {
   if (!SAFE_NAME_PATH.test(name)) {
      return (
         `its name= "${name}" is not a plain identifier path: dot-separated ` +
         "segments of letters, digits, underscores and hyphens, since the " +
         "name is inlined into the table's DDL"
      );
   }
   if (hasReservedTableSuffix(name)) {
      return (
         `its name= "${name}" ends the way Publisher names a package's or a ` +
         "version's own tables (__p<hash>, __v<version>_<hash>), so it could " +
         "be another package's table; give it another name="
      );
   }
   return undefined;
}

/** One `#@ persist name=` mistake: its load-failure line, and an unsafe value. */
export interface PersistNameProblem {
   line: string;
   /** The quoted value that is not a {@link SAFE_NAME_PATH}, when it is one. */
   unsafeValue?: string;
}

/**
 * The `#@ persist name=<value>` annotations of one model whose name is
 * unusable or unsafe, one coded `persist-invalid` line each; the package
 * fails the publish and the load with every one, beside its other invalid
 * annotations:
 *  - Unquoted: the build plan silently drops a bare value, so the source would
 *    serve but never materialize.
 *  - Quoted but not {@link SAFE_NAME_PATH}: the value is inlined into DDL, so it
 *    could close the identifier and append statements.
 *
 * Scans raw source lines rather than compiled annotations, because the tag
 * parser discards whether the value was quoted, and keeps one of a repeated
 * key.
 */
export function persistNameProblems(
   modelSource: string,
   modelPath: string,
): PersistNameProblem[] {
   const problems: PersistNameProblem[] = [];
   for (const rawLine of modelSource.split("\n")) {
      if (!PERSIST_LINE_PATTERN.test(rawLine)) continue;
      // A line that reads the environment is invalid whatever its name says
      // (persist-invalid, a 400 with its code, persistDeclarationProblems).
      if (hasEnvReference(rawLine)) continue;
      const line = rawLine.trim();
      if (UNQUOTED_NAME_PATTERN.test(rawLine)) {
         problems.push({
            line: codedLine(
               `${modelPath}: persist annotation name must be quoted. Write a quoted ` +
                  `value like name="engaged_events" (or a dialect table path such as ` +
                  `name="my_dataset.engaged_events"), not a bare value: an unquoted ` +
                  `persist name is dropped from the build plan, so the source would ` +
                  `publish but never materialize. Offending annotation: ${line}`,
               PERSIST_INVALID_CODE,
            ),
         });
         continue;
      }
      const unsafe = [...rawLine.matchAll(QUOTED_NAME_VALUE_PATTERN)].find(
         (match) => !SAFE_NAME_PATH.test(match[2]),
      );
      if (unsafe) {
         problems.push({
            line: codedLine(
               `${modelPath}: persist annotation name must be a plain identifier path, ` +
                  `dot-separated segments of letters, digits, underscores, and hyphens ` +
                  `(e.g. name="engaged_events", name="my_dataset.engaged_events", or a ` +
                  `hyphenated container path name="my-proj.mydataset.engaged_events"). ` +
                  `The name is inlined into the materialization table DDL, so a value ` +
                  `containing a quote, backtick, semicolon, or space is rejected. ` +
                  `Offending annotation: ${line}`,
               PERSIST_INVALID_CODE,
            ),
            unsafeValue: unsafe[2],
         });
      }
   }
   return problems;
}

/** {@link persistNameProblems}, thrown as one 400 when there are any. */
export function assertPersistNamesQuoted(
   modelSource: string,
   modelPath: string,
): void {
   const problems = persistNameProblems(modelSource, modelPath);
   if (problems.length > 0) {
      throw new BadRequestError(problems.map((p) => p.line).join("\n"));
   }
}

/**
 * Why a `#@ persist` declaration (on a source, a view or a named query) is
 * invalid, beyond its keys (annotation_reader reports those): a line that
 * reads `@env.`, a `storage=` that names nothing, or an unusable `name=`.
 * Empty when it is valid. Each fails the publish and the load.
 */
export function persistDeclarationProblems(annotations: Annotations): string[] {
   // The reader drops the whole `#@` route for such a line, so nothing else
   // can be read off it.
   if (routeReadsEnv(annotations, "@")) return [ENV_REFERENCE_REASON];
   const tag = readRouteIfAny(annotations, "@");
   if (!tag?.has("persist")) return [];
   const placed = placementOf(tag, "persist");
   if ("refused" in placed) return [EMPTY_STORAGE_REASON];
   const name = placed.placement.name?.trim();
   const problem = name ? tableNameProblem(name) : undefined;
   return problem ? [problem] : [];
}
