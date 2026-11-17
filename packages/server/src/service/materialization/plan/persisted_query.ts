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
   PERSIST_QUERY_INVALID_CODE,
   PERSIST_QUERY_UNPARSEABLE_CODE,
   persistAnnotationFindings,
   routeOfNote,
   type PackageAnnotationFinding,
   type RefusalKey,
} from "../../annotation_reader";
import { ownLevelNotes, type AnnotationNote } from "../../annotations";
import { declares, readNotes, readNotesIfAny } from "./declaration";
import { persistDeclarationProblems } from "./persist_annotation_validation";

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
   /** Why its `#@ persist` line does not parse, when it does not. */
   unparseable?: string;
}

/** A `#@ persist` line, told from its text when it cannot be parsed. */
const PERSIST_LINE = /^\s*#@\s*persist\b/;

/**
 * Why `#@` notes do not parse, or undefined when they do. A line that reads
 * the environment is dropped before parsing, so it is not reported here.
 */
export function unparseableNotes(
   notes: readonly unknown[],
): string | undefined {
   try {
      const errors = readNotes(notes, "@")
         .log.filter((entry) => entry.severity === "error")
         .map((entry) => entry.message);
      return errors.length > 0 ? errors.join("; ") : undefined;
   } catch (error) {
      return error instanceof Error ? error.message : String(error);
   }
}

/** Whether any of `notes` is written as a `#@ persist` line. */
export function writesPersistLine(notes: readonly { text: string }[]): boolean {
   return notes.some((n) => PERSIST_LINE.test(n.text));
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
      // A `#@ persist` line that does not parse is an invalid declaration,
      // reported by the planner, never dropped silently.
      const unparseable = writesPersistLine(notes)
         ? unparseableNotes(notes)
         : undefined;
      let persisted = unparseable !== undefined;
      if (!persisted) {
         try {
            persisted = declares(
               new Annotations({ blockNotes: notes }),
               "@",
               "persist",
            );
         } catch {
            continue;
         }
      }
      if (!persisted) continue;
      const id = `${name}@${url}`;
      if (!out.has(id)) {
         out.set(id, {
            name,
            declaringURL: url,
            def,
            notes,
            ...(unparseable !== undefined ? { unparseable } : {}),
         });
      }
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
   // A line that does not parse has no keys to read; the planner reports it.
   if (query.unparseable !== undefined) return [];
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

/** `#@ persist` keys that make a build incremental: a result rebuilds whole. */
export const INCREMENTAL_KEYS = ["watermark", "merge_key"] as const;

/**
 * Why a persisted query's `#@ persist` declaration is invalid, beyond its
 * keys ({@link persistQueryKeyFindings}), each with its code: a line that does
 * not parse, one that reads `@env.`, an incremental declaration (a query's
 * result is rebuilt whole, so `refresh="incremental"` and the incremental
 * keys are refused alike), a `storage=` that names nothing, or an unusable
 * `name=`. Empty when valid.
 */
export function persistQueryProblems(
   query: DeclaredPersistQuery,
): { problem: string; code: string }[] {
   if (query.unparseable !== undefined) {
      return [
         {
            problem: `its \`#@\` line does not parse (${query.unparseable})`,
            code: PERSIST_QUERY_UNPARSEABLE_CODE,
         },
      ];
   }
   const invalid = (problem: string) => ({
      problem,
      code: PERSIST_QUERY_INVALID_CODE,
   });
   const problems = persistDeclarationProblems(
      new Annotations({ blockNotes: query.notes }),
   );
   if (problems.length > 0) return problems.map(invalid);
   const tag = readNotesIfAny(query.notes, "@");
   const incremental = [
      ...(tag?.text("persist", "refresh") === "incremental" ||
      tag?.text("refresh") === "incremental"
         ? ['refresh="incremental"']
         : []),
      ...INCREMENTAL_KEYS.filter((k) => tag?.has("persist", k) || tag?.has(k)),
   ];
   return incremental.length > 0
      ? [
           invalid(
              `a query's result is rebuilt whole, so it takes no incremental key (${incremental.join(", ")})`,
           ),
        ]
      : [];
}

/** {@link persistQueryProblems} as findings, which fail the publish and the load. */
export function persistQueryInvalidFindings(
   query: DeclaredPersistQuery,
   model: string,
): PackageAnnotationFinding[] {
   return persistQueryProblems(query).map(({ problem, code }) => ({
      route: "@",
      tier: "malformed",
      code,
      message: `#@ persist on query "${query.name}" is invalid: ${problem}`,
      subject: query.name,
      model,
   }));
}

/** The workaround a not-built persisted query's warning names. */
function workaround(name: string): string {
   return (
      `To store its result, persist a source over it instead ` +
      `(\`#@ persist\` on \`source: ${name}_result is ${name}\`) and query ` +
      `that source.`
   );
}

/**
 * A persisted query that is not built, as a package finding (unservable: the
 * query is answered live) carrying its refusal record. `reason` says why;
 * absent, the feature is off.
 */
export function persistQueryNotBuiltFinding(
   name: string,
   model: string,
   refused: RefusalKey,
   reason?: string,
): PackageAnnotationFinding {
   return {
      route: "@",
      tier: "unservable",
      code: PERSIST_QUERY_NOT_BUILT,
      message:
         reason === undefined
            ? `#@ persist on query "${name}" is not built: persisted named ` +
              `queries are built only with PUBLISHER_MATERIALIZE_ENABLED on, so the ` +
              `query is answered live. ${workaround(name)}`
            : `#@ persist on query "${name}" is not built, so the query is ` +
              `answered live: ${reason}. ${workaround(name)}`,
      subject: name,
      model,
      refused,
   };
}
