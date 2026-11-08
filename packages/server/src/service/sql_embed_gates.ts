// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Refuse, at load, a `sql()` source whose text embeds (`%{ ... }`) a gated
 * source.
 *
 * `conn.sql("""select ... from (%{ locked -> {...} })""")` compiles the
 * embedded query into the SQL text at load. Query-time enforcement walks the
 * run target's own and inherited gates (`collectEntryPointGates`), and a
 * `sql_select` has none: it neither extends nor derives from what it embeds.
 * So the embedded read of `locked` was answered for every caller, `#(authorize)`
 * and `#(access_filter)` alike. A gate the author reads as enforced must not
 * be bypassable by the author's own construct, and Publisher cannot graft a
 * row filter into opaque SQL, so the model does not load.
 *
 * IR shape relied on (pinned in `sql_embed_gates.spec.ts`): a `sql_select`
 * carries `selectSegments`, each either `{ sql }` text, an embedded source
 * (`%{ src }`, a `sql_select` or `query_source` def), or an embedded query
 * (`%{ src -> {...} }` or `%{ named_query }`, a `Query` with `structRef` and,
 * over a composite, `compositeResolvedSourceDef`). Embedded structs are inlined
 * by value, so a nested embed (`%{ s1 }` where `s1` itself embeds) is a
 * `sql_select` deeper in the same tree; the walk below reaches every one,
 * including one inline in a join or a named query.
 *
 * Fail closed: a segment of any other shape, a `structRef` naming nothing in
 * the model, or a gate walk that throws refuses the load.
 *
 * Worker-safe (no logging): runs inside `extractCheckedModel`, on both load
 * paths.
 */
import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";
import { collectEntryPointGates } from "./gate_classification";

/** One embedded gated (or unreadable) source in one model entry's `sql()`. */
export interface SqlEmbedFinding {
   /** The `modelDef.contents` entry (source or query) carrying the `sql()`. */
   embeddingSource: string;
   /** The `sql()` source the embed is written in, when it is not `embeddingSource` itself. */
   via?: string;
   /** The embedded source, as the author named it. */
   embeddedSource: string;
   /** Gate routes in force on it, or empty when the embed could not be read. */
   routes: string[];
   /** Why the embed could not be read (fail closed); absent for a gated embed. */
   unreadable?: string;
}

type Obj = Record<string, unknown>;

const isObj = (value: unknown): value is Obj =>
   typeof value === "object" && value !== null;

/** What `via` holds for a `sql()` written inline with no name (e.g. in a query). */
export const INLINE_SQL = "<inline sql()>";

const labelOf = (struct: Obj): string => {
   const label = (struct.as as string | undefined) ?? struct.name;
   if (typeof label !== "string") return "<unnamed>";
   // An unnamed `conn.sql(...)` is named by Malloy's `sql://conn/<uuid>` key.
   return label.startsWith("sql://") ? INLINE_SQL : label;
};

/** Every `sql_select` with embeds reachable from `root`, the root included. */
function sqlSelectsWithin(root: unknown): Obj[] {
   const found: Obj[] = [];
   const seen = new Set<Obj>();
   const stack: unknown[] = [root];
   while (stack.length > 0) {
      const node = stack.pop();
      if (Array.isArray(node)) {
         stack.push(...node);
         continue;
      }
      if (!isObj(node) || seen.has(node)) continue;
      seen.add(node);
      if (node.type === "sql_select" && Array.isArray(node.selectSegments)) {
         found.push(node);
      }
      stack.push(...Object.values(node));
   }
   return found;
}

/**
 * The source struct(s) one non-text segment reads, or a reason it cannot be
 * read. An embedded query reads its `structRef` and, over a composite, the
 * member Malloy resolved.
 */
function embeddedStructs(
   segment: Obj,
   modelDef: ModelDef,
): { structs: SourceDef[] } | { unreadable: string; label: string } {
   if (segment.type === "query") {
      const ref = segment.structRef;
      let base: unknown = ref;
      if (typeof ref === "string") base = modelDef.contents[ref];
      if (!isObj(base) || !isSourceDef(base as unknown as SourceDef)) {
         return {
            unreadable: "its source reference does not resolve in the model",
            label: typeof ref === "string" ? ref : "<embedded query>",
         };
      }
      const structs = [base as unknown as SourceDef];
      const resolved = segment.compositeResolvedSourceDef;
      if (isObj(resolved) && isSourceDef(resolved as unknown as SourceDef)) {
         structs.push(resolved as unknown as SourceDef);
      }
      return { structs };
   }
   if (isSourceDef(segment as unknown as SourceDef)) {
      return { structs: [segment as unknown as SourceDef] };
   }
   return {
      unreadable: `its segment shape (type ${JSON.stringify(segment.type)}) is not one Publisher reads`,
      label: labelOf(segment),
   };
}

/**
 * Every `%{ ... }` embed of a gated source in any `sql()` reachable from a
 * model entry, plus every embed that cannot be read (fail closed). Uses
 * `collectEntryPointGates`, the walk query-time enforcement uses, so an
 * embedded source is gated here exactly when querying it would be.
 */
export function findSqlEmbedsOfGatedSources(
   modelDef: ModelDef,
): SqlEmbedFinding[] {
   const findings: SqlEmbedFinding[] = [];
   const reported = new Set<string>();
   const report = (finding: SqlEmbedFinding) => {
      const key = `${finding.embeddingSource}\0${finding.via ?? ""}\0${finding.embeddedSource}`;
      if (reported.has(key)) return;
      reported.add(key);
      findings.push(finding);
   };
   for (const [entryName, entry] of Object.entries(modelDef.contents)) {
      for (const sqlSelect of sqlSelectsWithin(entry)) {
         const sqlLabel = labelOf(sqlSelect);
         const via = sqlSelect === (entry as unknown) ? undefined : sqlLabel;
         for (const segment of sqlSelect.selectSegments as unknown[]) {
            if (!isObj(segment)) {
               report({
                  embeddingSource: entryName,
                  via,
                  embeddedSource: "<unknown>",
                  routes: [],
                  unreadable: "its segment is not an object",
               });
               continue;
            }
            // Plain SQL text (`isSegmentSQL`).
            if ("sql" in segment && segment.type === undefined) continue;
            const embedded = embeddedStructs(segment, modelDef);
            if ("unreadable" in embedded) {
               report({
                  embeddingSource: entryName,
                  via,
                  embeddedSource: embedded.label,
                  routes: [],
                  unreadable: embedded.unreadable,
               });
               continue;
            }
            for (const struct of embedded.structs) {
               let routes: string[];
               try {
                  routes = [
                     ...new Set(
                        collectEntryPointGates(struct, modelDef).map(
                           (gate) => gate.route,
                        ),
                     ),
                  ];
               } catch (err) {
                  report({
                     embeddingSource: entryName,
                     via,
                     embeddedSource: labelOf(struct as unknown as Obj),
                     routes: [],
                     unreadable: `its gates could not be read (${(err as Error).message})`,
                  });
                  continue;
               }
               if (routes.length === 0) continue;
               report({
                  embeddingSource: entryName,
                  via,
                  embeddedSource: labelOf(struct as unknown as Obj),
                  routes: routes.sort(),
               });
            }
         }
      }
   }
   return findings;
}

function describeFinding(finding: SqlEmbedFinding): string {
   const through =
      finding.via === INLINE_SQL
         ? "an inline sql()"
         : `the sql() source "${finding.via}"`;
   const where = finding.via
      ? `"${finding.embeddingSource}" (through ${through})`
      : `"${finding.embeddingSource}"`;
   if (finding.unreadable) {
      return `${where} embeds "${finding.embeddedSource}", and ${finding.unreadable}, so whether it is gated cannot be decided`;
   }
   const gates = finding.routes.map((route) => `#(${route})`).join(" and ");
   return `${where} embeds "${finding.embeddedSource}", which carries ${gates}`;
}

/**
 * Refuse the load when any `sql()` embeds a gated source (see the module doc).
 * Throws `ModelCompilationError` naming every finding at once, the same class
 * (and so the same status and `loadErrors` entry) as a gate that does not
 * parse.
 */
export function assertNoSqlEmbedsOfGatedSources(modelDef: ModelDef): void {
   const findings = findSqlEmbedsOfGatedSources(modelDef);
   if (findings.length === 0) return;
   throw new ModelCompilationError({
      message:
         `A sql() source embeds a gated source with %{ ... }:\n` +
         `${findings.map((f) => `  - ${describeFinding(f)}`).join("\n")}\n` +
         `Malloy compiles the embedded read into the SQL text, where no ` +
         `#(authorize) or #(access_filter) gate can be enforced, so every ` +
         `caller would read the gated rows. Query the gated source through ` +
         `Malloy instead (derive from it with \`->\`, or extend it), so its ` +
         `gate applies, or remove the gate if the source is meant to be open.`,
   });
}
