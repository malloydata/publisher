// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { ModelCompilationError } from "../errors";

/** A line whose first non-whitespace content is a `#@ persist` directive. */
const PERSIST_LINE_PATTERN = /^\s*#@\s+persist\b/;
/**
 * A `name=` key whose value is NOT immediately opened by a single or double
 * quote -- i.e. a bare `name=engaged_events` (whitespace around `=` tolerated).
 * `name="..."` and `name='...'` pass.
 *
 * The lookbehind requires a standalone key: neither an `_`-joined neighbour
 * (`tablename=`, `realization_name=`) nor a DOTTED one (`queryMetadata.name=`)
 * is the persist name. A `\b` word boundary would catch the underscore case but
 * not the dotted one, because `.` is itself a boundary -- so a legitimate
 * `#@ persist queryMetadata.name=finance` would fail the load with a 424 about
 * quoting a persist name it never declared.
 */
const UNQUOTED_NAME_PATTERN = /(?<![.\w])name\s*=\s*(?!["'])/;

/**
 * A standalone `name=` key (not an `_`-joined or dotted neighbour) whose value is
 * a single- or double-quoted string, capturing the quote char and the inner
 * content. Mirrors {@link UNQUOTED_NAME_PATTERN}'s lookbehind so it targets the
 * persist name and not `tablename=`/`queryMetadata.name=`. Global, because a line
 * may carry the key more than once (`name="a" name="b"`); every occurrence is
 * validated (see below), so which one the tag parser ultimately keeps does not
 * matter -- an unsafe value in ANY position is rejected.
 */
const QUOTED_NAME_VALUE_PATTERN = /(?<![.\w])name\s*=\s*(["'])(.*?)\1/g;

/**
 * The only shape a persist name may hold once its outer quotes are stripped:
 * dot-separated segments of `[A-Za-z0-9_-]` (letters, digits, underscore,
 * hyphen). This value is inlined into `CREATE OR REPLACE TABLE <path>` /
 * `DROP TABLE IF EXISTS <path>` DDL on the storage path, so a value carrying an
 * embedded quote/backtick (which closes the identifier), a `;`, or whitespace is
 * an injection vector and is rejected at publish.
 *
 * The character set intentionally MIRRORS the control plane's physical-name
 * grammar (`PhysicalTableName`: container segments `[A-Za-z0-9_-]+` joined by
 * dots, sanitized via `replaceAll("[^A-Za-z0-9_-]", "_")`). The two services must
 * agree: a name Publisher rejects here but the control plane would accept (or
 * vice versa) is a cross-service contract split. In particular hyphens and
 * leading digits are allowed because a dialect container path can be a
 * hyphenated BigQuery project id (e.g. `my-proj.mydataset.engaged_events`).
 */
const SAFE_NAME_PATH = /^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$/;

/**
 * Reject `#@ persist name=<value>` annotations whose name is unusable or unsafe.
 *
 * Two failure classes, both fatal at publish/load (HTTP 424):
 *
 *  - Unquoted. The persist build plan requires a quoted name -- a dialect-style
 *    table path (`name="engaged_events"`, or `name="my_dataset.engaged_events"`).
 *    A bare value is dropped from the build plan, so the source publishes and
 *    serves but is never materialized, with no error anywhere.
 *  - Quoted but not a plain identifier path. The value is author-controlled and
 *    is inlined verbatim into the storage-path DDL, so a value whose content
 *    carries an embedded quote/backtick (or a `;`, or whitespace) can close the
 *    identifier and append arbitrary statements. Only {@link SAFE_NAME_PATH} --
 *    dot-separated `[A-Za-z0-9_-]` segments -- is inlined safely.
 *
 * Throwing here fails the load with a clear, actionable message -- the same
 * hard-stop `Model.validateRenderTags` applies to a misconfigured render tag.
 *
 * Scans the raw model source line-by-line rather than the compiled annotation
 * objects: the check must fire regardless of how the compiler attaches the
 * annotation, and the raw text is the ground truth for whether the author
 * quoted the value (the tag parser discards quote information once parsed).
 * EVERY `name=` occurrence on a line is validated, not just the first: a line
 * may repeat the key, and the tag parser keeps only one -- validating all of them
 * rejects an unsafe value in any position regardless of which the parser keeps.
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
      // Validate every quoted `name=` value on the line. A line with no standalone
      // `name=` key (e.g. only `realization=`) yields no matches and is fine; the
      // unquoted pattern above already handled the bare-value case.
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
