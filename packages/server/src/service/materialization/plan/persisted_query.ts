// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#@ persist` on a top-level `query:`: reading the declarations.
 *
 *     #@ persist
 *     query: top_carriers is flights -> { group_by: carrier; aggregate: n }
 *
 * Malloy's own persistence planner walks sources only, so a named query never
 * reaches its build plan. Every reader here keys a declaration by the file that
 * declares the query (its `location.url`) and the query's name there, and reads
 * only the notes the `query:` statement itself wrote (never one a query it
 * derives from carries).
 *
 * The planning and the build companion are in `persisted_view.ts`, beside
 * persisted views'.
 */

import { Annotations } from "@malloydata/malloy";
import {
   byCodepoint,
   persistAnnotationFindings,
   routeOfNote,
   type PackageAnnotationFinding,
} from "../../annotation_reader";
import { ownLevelNotes, type AnnotationNote } from "../../annotations";
import { declares } from "./declaration";

/** The package warning for a persisted query that is not built, or not served. */
export const PERSIST_QUERY_NOT_BUILT = "persist-query-not-built";

/** A named query a model file declares with `#@ persist` on its own statement. */
export interface DeclaredPersistQuery {
   /** The query's name in its declaring file. */
   name: string;
   /** The declaring file's URL. */
   declaringURL: string;
   /** The compiled `NamedQueryDef`. */
   def: NamedQueryLike;
   /** The `#@` notes the `query:` statement wrote, in order. */
   notes: AnnotationNote[];
}

/** What the readers use of a compiled `NamedQueryDef`. */
interface NamedQueryLike {
   type?: unknown;
   name?: unknown;
   structRef?: unknown;
   pipeline?: unknown;
   location?: { url?: unknown };
   annotations?: Parameters<typeof ownLevelNotes>[0];
}

/**
 * The named queries in a compiled model whose own statement declares
 * `#@ persist`, read as Malloy reads the line (presence, any spelling a parser
 * accepts, an `@env.` line included). With `declaredHere`, only those whose
 * declaring file it admits; every query is listed once, under the name its
 * declaring file gives it. Fail-open: an unreadable entry is skipped.
 */
export function declaredPersistQueries(
   modelDef: unknown,
   declaredHere?: (declaringURL: string) => boolean,
): DeclaredPersistQuery[] {
   const contents = (modelDef as { contents?: Record<string, unknown> })
      ?.contents;
   const out = new Map<string, DeclaredPersistQuery>();
   for (const [key, raw] of Object.entries(contents ?? {})) {
      const def = raw as NamedQueryLike;
      if (!def || def.type !== "query") continue;
      const url = def.location?.url;
      if (typeof url !== "string") continue;
      if (declaredHere && !declaredHere(url)) continue;
      const name = typeof def.name === "string" ? def.name : key;
      const notes = ownLevelNotes(def.annotations).filter(
         (n) => routeOfNote(n.text) === "@",
      );
      if (notes.length === 0) continue;
      let persisted = false;
      try {
         persisted = declares(
            new Annotations({ blockNotes: notes }),
            "@",
            "persist",
         );
      } catch {
         continue;
      }
      if (!persisted) continue;
      const id = `${name}@${url}`;
      if (!out.has(id)) out.set(id, { name, declaringURL: url, def, notes });
   }
   return [...out.values()].sort((a, b) => byCodepoint(a.name, b.name));
}

/**
 * The key findings on a persisted query's `#@` line: an unknown key (a typo,
 * which does nothing) and a retired one, as a persist source's are reported.
 */
export function persistQueryKeyFindings(
   query: DeclaredPersistQuery,
   model: string,
): PackageAnnotationFinding[] {
   try {
      return persistAnnotationFindings(
         new Annotations({ blockNotes: query.notes }),
         query.name,
         model,
         { subjectKind: "query" },
      );
   } catch {
      return [];
   }
}

/** The workaround a not-built persisted query's warning names. */
function workaround(name: string): string {
   return (
      `To store its result today, persist a source over it instead ` +
      `(\`#@ persist\` on \`source: ${name}_result is ${name}\`) and query ` +
      `that source.`
   );
}

/**
 * A persisted query that is not built, as a package finding (unservable: the
 * query is answered live). `reason` says why; absent, the feature is off.
 */
export function persistQueryNotBuiltFinding(
   name: string,
   model: string,
   reason?: string,
): PackageAnnotationFinding {
   return {
      route: "@",
      tier: "unservable",
      code: PERSIST_QUERY_NOT_BUILT,
      message:
         reason === undefined
            ? `#@ persist on query "${name}" is not built: persisted named ` +
              `queries are built only with PUBLISHER_MATERIALIZE on, so the ` +
              `query is answered live. ${workaround(name)}`
            : `#@ persist on query "${name}" is not built, so the query is ` +
              `answered live: ${reason}. ${workaround(name)}`,
      subject: name,
      model,
   };
}
