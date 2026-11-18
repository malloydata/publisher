// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The marks Publisher reads in a warehouse or a storage destination, and how
 * they are read back: the ownership mark on a table an auto-run built at a v1
 * `#@ persist name=` (table_ownership.ts), and the mark on a `name=` view an
 * earlier release made over a `#(materialize)` table (leftover_name_views.ts),
 * which Publisher no longer makes but still recognizes, so it can drop this
 * package's own and never touch anyone else's.
 *
 * Pure. The SQL that probes and writes the marks differs by dialect and is in
 * warehouse_sql.ts.
 */

import { decodeBackslashEscapes } from "../../quoting";

/** The prefix of the comment on a `name=` view an earlier release made. */
const NAME_VIEW_MARK = "malloy-publisher:name-view";

/**
 * What a leftover `name=` view's comment says: the table it reads (`target`)
 * and the package that made it (`owner`, `<environment>/<package>`).
 */
interface NameViewMark {
   target: string;
   owner: string;
}

/** The `owner` a mark carries for a package: `<environment>/<package>`. */
export function packageOwner(
   environmentName: string,
   packageName: string,
): string {
   return `${environmentName}/${packageName}`;
}

/**
 * The mark read back from a view's comment, or undefined when the comment is
 * not Publisher's mark: exactly the prefix, then `owner=` and `target=` once
 * each and nothing else. Accepts BigQuery's TABLE_OPTIONS spelling, a
 * double-quoted literal.
 */
export function parseNameViewComment(
   comment: unknown,
): NameViewMark | undefined {
   if (typeof comment !== "string") return undefined;
   let text = comment.trim();
   if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
      text = text.slice(1, -1);
   }
   const [prefix, ...fields] = text.split(" ");
   if (prefix !== NAME_VIEW_MARK || fields.length !== 2) return undefined;
   const values = new Map<string, string>();
   for (const field of fields) {
      const eq = field.indexOf("=");
      if (eq <= 0) return undefined;
      const key = field.slice(0, eq);
      const encoded = field.slice(eq + 1);
      if (encoded === "" || values.has(key)) return undefined;
      try {
         values.set(key, decodeURIComponent(encoded));
      } catch {
         return undefined;
      }
   }
   const target = values.get("target");
   const owner = values.get("owner");
   return target !== undefined && owner !== undefined
      ? { target, owner }
      : undefined;
}

/**
 * The prefix of the ownership mark on a table a build wrote
 * (table_ownership.ts).
 */
export const OWNED_TABLE_MARK = "malloy-publisher:table";

/**
 * What a table's ownership mark says: the package that built it (`owner`,
 * `<environment>/<package>`) and, from 0.16.0, where the table is (`table`,
 * the fully qualified location the warehouse's catalog reports for it). A
 * mark an earlier release wrote has no `table`.
 */
interface OwnedTableMark {
   owner: string;
   table?: string;
}

/**
 * The mark read back from a table's comment, or undefined when the comment
 * is not one: exactly the prefix, then `owner=`, then optionally `table=`,
 * each once and nothing else. Accepts BigQuery's double-quoted
 * TABLE_OPTIONS spelling.
 */
export function parseOwnedTableMark(
   comment: unknown,
): OwnedTableMark | undefined {
   if (typeof comment !== "string") return undefined;
   let text = comment.trim();
   if (text.length >= 2 && text.startsWith('"') && text.endsWith('"')) {
      text = text.slice(1, -1);
   }
   const fields = text.split(" ");
   if (fields.length < 2 || fields.length > 3) return undefined;
   if (fields[0] !== OWNED_TABLE_MARK) return undefined;
   const decode = (field: string, key: string): string | undefined => {
      if (!field.startsWith(`${key}=`)) return undefined;
      const encoded = field.slice(key.length + 1);
      if (encoded === "") return undefined;
      try {
         return decodeURIComponent(encoded);
      } catch {
         return undefined;
      }
   };
   const owner = decode(fields[1], "owner");
   if (owner === undefined) return undefined;
   if (fields.length === 2) return { owner };
   const table = decode(fields[2], "table");
   return table === undefined ? undefined : { owner, table };
}

/**
 * The owner a table's ownership mark names (`<environment>/<package>`), with
 * or without the table it names, or undefined when the comment is not one.
 */
export function parseOwnedTableComment(comment: unknown): string | undefined {
   return parseOwnedTableMark(comment)?.owner;
}

/** The unquoted segments of a dotted table path. */
export function pathSegments(path: string): string[] {
   return path.split(".");
}

/**
 * Whether a view's definition reads exactly `target` (`SELECT * FROM
 * <target>`, as the warehouse may spell it back: columns expanded,
 * identifiers unquoted or re-cased): one FROM, no join. A name with no
 * schema, on either side, is read in `viewSchema`, the schema the view lives
 * in (where its own unqualified names resolve, and where earlier releases
 * put a bare `name=` view beside its table), so `s2.g1` is not `g1`. A view
 * reading anything else, or more than one relation, is not. A backslash
 * escape in the definition is decoded first where `dialect` reads one
 * (BigQuery), as it reads a quoted identifier.
 */
export function viewDefinitionReadsOnly(
   definition: unknown,
   target: string,
   viewSchema?: unknown,
   dialect?: string,
): boolean {
   if (typeof definition !== "string") return false;
   const decoded =
      dialect === undefined
         ? definition
         : decodeBackslashEscapes(definition, dialect);
   const text = decoded
      .toLowerCase()
      .replace(/["`[\]]/g, "")
      .replace(/\s+/g, " ");
   // A comma after the FROM target is a second relation.
   const froms = [...text.matchAll(/\bfrom ([^\s;),]+)\s*(,?)/g)];
   if (froms.length !== 1 || froms[0][2] || /\bjoin\b/.test(text)) {
      return false;
   }
   const schema =
      typeof viewSchema === "string" && viewSchema !== ""
         ? viewSchema.toLowerCase().replace(/["`[\]]/g, "")
         : undefined;
   const qualify = (segments: string[]) =>
      segments.length === 1 && schema ? [schema, ...segments] : segments;
   const read = qualify(froms[0][1].split("."));
   const want = qualify(
      target
         .toLowerCase()
         .replace(/["`[\]]/g, "")
         .split("."),
   );
   const [shorter, longer] =
      read.length <= want.length ? [read, want] : [want, read];
   return shorter.every(
      (segment, i) => segment === longer[longer.length - shorter.length + i],
   );
}
