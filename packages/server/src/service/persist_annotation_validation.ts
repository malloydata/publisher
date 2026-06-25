import { ModelCompilationError } from "../errors";

/** A line whose first non-whitespace content is a `#@ persist` directive. */
const PERSIST_LINE_PATTERN = /^\s*#@\s+persist\b/;
/**
 * A `name=` key whose value is NOT immediately opened by a single or double
 * quote — i.e. a bare `name=engaged_events` (whitespace around `=` tolerated).
 * `name="..."` and `name='...'` pass. The `\bname` word boundary requires a
 * standalone key, so neighbours like `tablename=` / `realization_name=` are
 * never mistaken for it.
 */
const UNQUOTED_NAME_PATTERN = /\bname\s*=\s*(?!["'])/;

/**
 * Reject `#@ persist name=<value>` annotations whose name is unquoted.
 *
 * The persist build plan requires a quoted name — a dialect-style table path
 * (`name="engaged_events"`, or `name="my_dataset.engaged_events"`). A bare
 * value is dropped from the build plan, so the source publishes and serves but
 * is never materialized, with no error anywhere. Throwing a
 * `ModelCompilationError` (HTTP 424) fails the publish/load with a clear,
 * actionable message — the same hard-stop `Model.validateRenderTags` applies to
 * a misconfigured render tag.
 *
 * Scans the raw model source line-by-line rather than the compiled annotation
 * objects: the check must fire regardless of how the compiler attaches the
 * annotation, and the raw text is the ground truth for whether the author
 * quoted the value (the tag parser discards quote information once parsed).
 *
 * @throws {ModelCompilationError} listing every offending annotation.
 */
export function assertPersistNamesQuoted(
   modelSource: string,
   modelPath: string,
): void {
   const offenders: string[] = [];
   for (const rawLine of modelSource.split("\n")) {
      if (!PERSIST_LINE_PATTERN.test(rawLine)) continue;
      if (UNQUOTED_NAME_PATTERN.test(rawLine)) {
         offenders.push(rawLine.trim());
      }
   }
   if (offenders.length > 0) {
      throw new ModelCompilationError({
         message:
            `${modelPath}: persist annotation name must be quoted. Write a quoted ` +
            `value like name="engaged_events" (or a dialect table path such as ` +
            `name="my_dataset.engaged_events"), not a bare value — an unquoted ` +
            `persist name is dropped from the build plan, so the source would ` +
            `publish but never materialize. Offending annotation(s): ` +
            `${offenders.join("; ")}.`,
      });
   }
}
