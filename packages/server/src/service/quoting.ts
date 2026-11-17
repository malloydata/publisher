// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The segment after the last dot of a table path (`s.t` -> `t`). Not a rename
 * target on its own: whether a bare target stays in its container is
 * dialect-specific, which {@link quoteRenameTarget} decides.
 */
export function bareTableName(tableName: string): string {
   const lastDot = tableName.lastIndexOf(".");
   return lastDot >= 0 ? tableName.substring(lastDot + 1) : tableName;
}

// Keyed by Malloy `dialectName`; everything else uses double quotes. The quote
// character agrees with the control plane's `PhysicalTableName.BACKTICK_TYPES`
// (keyed by connection type). The BigQuery escape does not: the control plane
// still doubles a backtick there. It never writes one, since it admits only
// `[A-Za-z0-9_-]` names, so the two agree on every name it sends. The
// conformance table in quoting.spec.ts pins the quote character and the escapes.
const BACKTICK_DIALECTS = new Set(["standardsql", "mysql", "databricks"]);

/** Thrown for a name part a dialect's quoted identifier cannot hold. */
export class UnquotableIdentifierError extends Error {
   constructor(identifier: string, dialect: string, reason: string) {
      super(
         `${dialect} cannot quote the identifier ${JSON.stringify(identifier)}: ${reason}`,
      );
      this.name = "UnquotableIdentifierError";
   }
}

// C0 controls and DEL. BigQuery's quoted identifier, like its quoted string,
// cannot hold a raw newline, and no BigQuery name holds a control character,
// so one is refused rather than encoded.
// eslint-disable-next-line no-control-regex
const CONTROL_CHARACTER = /[\u0000-\u001f\u007f]/;

/**
 * Quote a single SQL identifier for {@code dialect}, so it reads back as
 * exactly `identifier` and ends at its closing quote.
 *
 * - BigQuery (`standardsql`): backticks, with `\` as the escape character, as
 *   BigQuery's lexer reads a quoted identifier (the escape sequences of a
 *   string literal): `\` becomes `\\` and a backtick `` \` ``. An empty
 *   name, one holding a control character (a newline among them), and one
 *   holding a `.` are refused with {@link UnquotableIdentifierError}: BigQuery
 *   reads a dot inside backticks as a path separator (`` `a.b` `` is the path
 *   `a`.`b`), so a dotted name part would name another table. Elsewhere a
 *   quoted part may hold a dot (`"my.schema"."t"` on Postgres).
 * - MySQL and Databricks: backticks, an embedded backtick doubled; a
 *   backslash is an ordinary character in their quoted identifiers.
 * - Everything else: double quotes, an embedded double quote doubled.
 *
 * NUL is refused in every dialect: MySQL and Postgres do not allow it in an
 * identifier.
 */
export function quoteIdentifier(identifier: string, dialect: string): string {
   if (identifier.includes("\0")) {
      throw new UnquotableIdentifierError(
         identifier,
         dialect,
         "an identifier cannot contain NUL",
      );
   }
   if (dialect === "standardsql") {
      if (identifier === "") {
         throw new UnquotableIdentifierError(
            identifier,
            dialect,
            "a quoted identifier cannot be empty",
         );
      }
      if (CONTROL_CHARACTER.test(identifier)) {
         throw new UnquotableIdentifierError(
            identifier,
            dialect,
            "a quoted identifier cannot contain a newline or other control character",
         );
      }
      if (identifier.includes(".")) {
         throw new UnquotableIdentifierError(
            identifier,
            dialect,
            "a quoted identifier cannot contain a dot, which BigQuery reads as a path separator",
         );
      }
      return "`" + identifier.replace(/[\\`]/g, (c) => "\\" + c) + "`";
   }
   if (BACKTICK_DIALECTS.has(dialect)) {
      return "`" + identifier.replace(/`/g, "``") + "`";
   }
   return '"' + identifier.replace(/"/g, '""') + '"';
}

/**
 * `sql` with each of the dialect's identifier escapes decoded where it differs
 * from the quote-doubling every other dialect reads: on BigQuery a backslash
 * escape (`` \` ``, `\\`) becomes the character it escapes. For a reader that
 * compares identifiers in SQL text the warehouse spelled back.
 */
export function decodeBackslashEscapes(sql: string, dialect: string): string {
   return dialect === "standardsql" ? sql.replace(/\\(.)/g, "$1") : sql;
}

/**
 * Dialect-quote a table path for DDL, segment by segment, so names that need
 * quoting (e.g. hyphenated BigQuery project ids) work at any depth. The control
 * plane sends `physicalTableName` unquoted; quoting is the publisher's job.
 */
export function quoteTablePath(tableName: string, dialect: string): string {
   return quotePathParts(tableName.split("."), dialect);
}

/** Dialect-quote each part of a table path and join them: {@link quoteTablePath}'s rule. */
export function quotePathParts(
   parts: readonly string[],
   dialect: string,
): string {
   return parts.map((part) => quoteIdentifier(part, dialect)).join(".");
}

/**
 * Dialects whose `ALTER TABLE ... RENAME TO` target must be QUALIFIED, because a
 * bare one resolves (on some or all backends) against the session's container,
 * moving the table. Safe to widen, costly to leave narrow: the failure it
 * prevents is a silent write to the wrong container.
 *
 * Measured, one table per dialect, by renaming `s1.t_staging` to a bare `t` with
 * the session pointed at `s2`:
 *
 * | dialect   | bare target        | qualified target      |
 * | --------- | ------------------ | --------------------- |
 * | snowflake | moves to s2        | accepted, correct     |
 * | mysql     | moves to s2        | accepted, correct     |
 * | postgres  | stays in s1        | syntax error          |
 * | duckdb    | stays in s1        | parser error          |
 * | bigquery  | stays in s1        | "cannot contain dot"  |
 * | trino     | stays in s1 (*)    | accepted, correct     |
 *
 * (*) Trino was measured against the `memory` connector, which ignores the
 * target's container. That does not generalize: the engine's `RenameTableTask`
 * fills a missing schema from the session, and connectors differ on whether they
 * permit a cross-schema rename at all. Since Trino accepts a qualified target and
 * a real deployment never runs on `memory`, it is qualified here rather than
 * relying on one connector's behaviour.
 *
 * The three that reject a qualified target are why this cannot be unconditional.
 * Databricks is not covered by the sweep and stays out, keeping its current
 * behaviour; add it once measured.
 */
const QUALIFIED_RENAME_TARGET_DIALECTS = new Set([
   "snowflake",
   "mysql",
   "trino",
]);

/**
 * Dialect-quote the TARGET of an `ALTER TABLE ... RENAME TO`: the full path on
 * dialects that resolve a bare target against the session, the bare name
 * (within the existing container) on those that require it.
 */
export function quoteRenameTarget(tableName: string, dialect: string): string {
   return QUALIFIED_RENAME_TARGET_DIALECTS.has(dialect)
      ? quoteTablePath(tableName, dialect)
      : quoteIdentifier(bareTableName(tableName), dialect);
}

/**
 * Whether a table path carries a quote character, i.e. was written as SQL
 * (a legacy manifest's entry, or an author's own `manifestLocation`
 * manifest) and must be parsed, not split, before it is quoted.
 */
export function isQuotedIdentifierPath(tableName: string): boolean {
   return tableName.includes('"') || tableName.includes("`");
}

/** One identifier of a table path written as SQL, as the dialect reads it. */
interface TablePathPart {
   /** The identifier, its quotes and escapes removed; unquoted, not folded. */
   value: string;
   /** Whether it was quoted, so its case is kept as written. */
   quoted: boolean;
}

/**
 * The identifier parts of a table path, quoted or not: the inverse of
 * {@link quoteTablePath}, and the one parser of a bound table name.
 * Undefined when the path does not parse, or holds a part
 * {@link quoteIdentifier} would refuse in `dialect`, so a path that cannot be
 * read and one that cannot be re-quoted are one refusal.
 *
 * - An unquoted part runs to the next dot and holds no quote character.
 * - A part in double quotes or backticks runs to its closing quote, and a dot
 *   inside it is part of the name (`"my.schema"."t"` is two parts; BigQuery
 *   cannot quote such a part, so on BigQuery the path is refused). Inside
 *   it, a doubled quote is one quote character, except in a BigQuery
 *   (`standardsql`) backtick part, where `\` escapes the next character, as
 *   BigQuery reads one (`` \` ``, `\\`; `\n`, `\r` and `\t` read as those
 *   controls, which BigQuery then refuses).
 * - On BigQuery one backtick pair around a dotted path (`` `proj.ds.t` ``) is
 *   the whole path, as BigQuery reads it, so its dots still separate parts.
 *
 * `dialect` "" reads with no dialect's rules but these: a display, or a
 * check every dialect must pass ({@link isUsableTableName}'s).
 */
export function tablePathParts(
   path: string,
   dialect: string,
): string[] | undefined {
   return parseTablePath(path, dialect)?.map((p) => p.value);
}

/**
 * {@link tablePathParts}, each unquoted part folded as `dialect` folds it
 * ({@link foldUnquoted}): the identifiers as the warehouse's catalog stores
 * them.
 */
export function foldedTablePathParts(
   path: string,
   dialect: string,
): string[] | undefined {
   return parseTablePath(path, dialect)?.map((p) =>
      p.quoted ? p.value : foldUnquoted(p.value, dialect),
   );
}

/** {@link tablePathParts}, with whether each part was quoted. */
function parseTablePath(
   path: string,
   dialect: string,
   { dottedParts = false }: { dottedParts?: boolean } = {},
): TablePathPart[] | undefined {
   const bigQuery = dialect === "standardsql";
   const parts: TablePathPart[] = [];
   let i = 0;
   for (;;) {
      const q = path[i];
      if (q === '"' || q === "`") {
         const backslashes = bigQuery && q === "`";
         let value = "";
         let j = i + 1;
         for (;;) {
            if (j >= path.length) return undefined;
            const c = path[j];
            if (backslashes && c === "\\") {
               if (j + 1 >= path.length) return undefined;
               const e = path[j + 1];
               value +=
                  e === "n" ? "\n" : e === "r" ? "\r" : e === "t" ? "\t" : e;
               j += 2;
               continue;
            }
            if (c === q) {
               if (!backslashes && path[j + 1] === q) {
                  value += q;
                  j += 2;
                  continue;
               }
               break;
            }
            value += c;
            j++;
         }
         if (backslashes && value.includes(".")) {
            for (const v of value.split(".")) {
               parts.push({ value: v, quoted: true });
            }
         } else {
            parts.push({ value, quoted: true });
         }
         i = j + 1;
      } else {
         const end = path.indexOf(".", i);
         const value = end < 0 ? path.slice(i) : path.slice(i, end);
         if (value.includes('"') || value.includes("`")) return undefined;
         parts.push({ value, quoted: false });
         i = end < 0 ? path.length : end;
      }
      if (i >= path.length) break;
      if (path[i] !== ".") return undefined;
      i++;
      if (i >= path.length) return undefined;
   }
   return parts.every((p) => quotable(p.value, dialect, dottedParts))
      ? parts
      : undefined;
}

/**
 * Whether a table name from outside this server (a host, a manifest) parses
 * the same in every dialect, so the controller, the manifest loader, the
 * build and the bind accept or refuse it alike: no backslash and no control
 * character anywhere, and a path {@link tablePathParts} reads both with no
 * dialect and as BigQuery (the one dialect whose reading differs: its
 * backslash escapes and its whole-path backticks).
 *
 * One exception, by design: a quoted part holding a dot (`"my.schema"."t"`)
 * is usable, since it names one table on Postgres and the rest, but BigQuery
 * cannot quote it, so a BigQuery build or bind refuses that name alone.
 */
export function isUsableTableName(name: string): boolean {
   if (name.includes("\\") || CONTROL_CHARACTER.test(name)) return false;
   return (
      parseTablePath(name, "") !== undefined &&
      parseTablePath(name, "standardsql", { dottedParts: true }) !== undefined
   );
}

/**
 * Whether {@link quoteIdentifier} quotes `identifier` rather than refusing it;
 * with `dottedParts`, a dot is let through on BigQuery too
 * ({@link isUsableTableName}'s every-dialect check).
 */
function quotable(
   identifier: string,
   dialect: string,
   dottedParts = false,
): boolean {
   if (identifier === "" || identifier.includes("\0")) return false;
   if (dialect !== "standardsql") return true;
   if (CONTROL_CHARACTER.test(identifier)) return false;
   return dottedParts || !identifier.includes(".");
}

/**
 * How `dialect` folds an unquoted identifier, ASCII letters only, as the
 * engine does: Postgres `A`-`Z` to lower case (its `downcase_identifier`
 * leaves a multibyte character alone), Snowflake `a`-`z` to UPPER.
 */
export function foldUnquoted(identifier: string, dialect: string): string {
   if (dialect === "postgres") {
      return identifier.replace(/[A-Z]/g, (c) => c.toLowerCase());
   }
   if (dialect === "snowflake") {
      return identifier.replace(/[a-z]/g, (c) => c.toUpperCase());
   }
   return identifier;
}

/**
 * Quote a physical table path for a manifest entry. The single quoting
 * authority for both `FROM` bind sites (serve-side
 * {@link Package.quoteBoundTableNames} and the build-side chained-persist
 * manifest), and the DDL that writes and drops the table.
 *
 * - A plain dotted path (every name Publisher and the control plane write) is
 *   {@link quoteTablePath}: each segment quoted, its case kept, so a
 *   case-folding engine (Snowflake) resolves the table the builder wrote.
 * - A path written as SQL (it holds a quote character: a legacy manifest's
 *   entry, or an author's own `manifestLocation` manifest) is parsed by
 *   {@link tablePathParts} and each part re-quoted, an unquoted part folded
 *   as the dialect folds it, so it names the same table it named as written.
 *   It is never inlined as given.
 *
 * Throws {@link UnquotableIdentifierError} for a path that does not parse or
 * holds a part the dialect cannot quote.
 */
export function quoteManifestTablePath(
   tableName: string,
   dialect: string,
): string {
   if (!isQuotedIdentifierPath(tableName)) {
      return quoteTablePath(tableName, dialect);
   }
   const parts = parseTablePath(tableName, dialect);
   if (parts === undefined) {
      throw new UnquotableIdentifierError(
         tableName,
         dialect,
         "it is not a table path the dialect can read and quote",
      );
   }
   return parts
      .map((p) =>
         quoteIdentifier(
            p.quoted ? p.value : foldUnquoted(p.value, dialect),
            dialect,
         ),
      )
      .join(".");
}
