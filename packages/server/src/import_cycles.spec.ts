// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

/**
 * No import cycles in the server's source, except the ones named below.
 *
 * The graph is every non-spec module under src/ (test helpers and the
 * generated src/api.ts excepted) and every relative edge between them: an
 * `import`, an `export … from`, a dynamic `import()` and an `import("x").T`
 * type. Each edge is a VALUE edge (it loads the module at runtime) or a
 * TYPE edge (`import type`, an import whose every name is `type`, or an
 * `import("x").T`), which the compiler erases.
 *
 * Two allowlists, both exact (a stale entry fails too, so they only shrink):
 *
 *  - RUNTIME_CYCLES: strongly connected groups of the VALUE graph, the cycles
 *    that decide module evaluation order. Each is a hazard (a module can meet
 *    another's exports uninitialized) and needs a reason it stays. A group is
 *    named by its sorted members, so a file joining it fails here too.
 *  - TYPE_BACK_EDGES: type-only edges that close a cycle, each a module typed
 *    against the one that calls it. Without them the whole graph has no
 *    cycle but the RUNTIME_CYCLES. An entry must still be a type-only edge
 *    and must still close a cycle.
 *
 * A new cycle: move the shared piece to a leaf module, or (for a type) declare
 * the narrow interface the callee reads. Do not extend a list without a reason.
 */

/** Empty: the value graph has no cycle. Keep it so. */
const RUNTIME_CYCLES: Array<{ files: string[]; why: string }> = [];

const REGISTRY_READERS =
   "RetrievalRegistry owns retrieval's state as typed fields (one per module " +
   "that reads it through retrievalRegistry()); it names each field's type. " +
   "The classes it builds come from leaves, so the cycle is types only.";

const TYPE_BACK_EDGES: Array<{ from: string; to: string; why: string }> = [
   ...[
      "retrieval/index/engine_state.ts",
      "retrieval/index/index_build.ts",
      "retrieval/index/source_summaries.ts",
      "retrieval/index/values_index.ts",
      "retrieval/model/catalog.ts",
      "retrieval/search/routes.ts",
      "retrieval/search/values.ts",
   ].map((to) => ({
      from: "retrieval/registry.ts",
      to,
      why: REGISTRY_READERS,
   })),
   {
      from: "retrieval/index/schema_index_caches.ts",
      to: "retrieval/index/schema_index.ts",
      why: "the registry's schema caches hold schema_index's cache entry types",
   },
   {
      from: "retrieval/search/held_results.ts",
      to: "retrieval/search/response.ts",
      why: "the registry's held results hold response's SourceCard",
   },
   {
      from: "service/package_load_gates.ts",
      to: "service/package.ts",
      why: "the load gates Package runs are typed against Package",
   },
   {
      from: "service/legacy_report_views.ts",
      to: "service/package.ts",
      why: "the deprecated report projections are typed against Package",
   },
];

const SRC_DIR = __dirname;

function sourceFiles(): string[] {
   return fs
      .readdirSync(SRC_DIR, { recursive: true, encoding: "utf8" })
      .map((f) => f.split(path.sep).join("/"))
      .filter(
         (f) =>
            f.endsWith(".ts") &&
            !f.endsWith(".spec.ts") &&
            !f.endsWith(".d.ts") &&
            f !== "api.ts" &&
            !f.startsWith("test_helpers/"),
      )
      .sort();
}

type Edge = { from: string; to: string; type: boolean };

// Its exports-only package.json is not resolvable under this tsconfig's
// moduleResolution; require keeps tsc from asking for its declarations.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const { parse } = require("@typescript-eslint/parser") as {
   parse(code: string, options: { sourceType: "module" }): unknown;
};

// The ESTree shapes read below, as @typescript-eslint/parser (the parser
// `bun run lint` uses) produces them.
type Node = {
   type: string;
   importKind?: string;
   exportKind?: string;
   source?: { type: string; value?: unknown } | null;
   specifiers?: Node[];
   argument?: Node & { literal?: { type: string; value?: unknown } };
   value?: unknown;
};

/** Every import edge of one file, judged as the import-direction rule judges it. */
function edgesOf(file: string, known: Set<string>): Edge[] {
   const text = fs.readFileSync(path.join(SRC_DIR, file), "utf8");
   const edges: Edge[] = [];
   const add = (specifier: unknown, type: boolean) => {
      if (typeof specifier !== "string" || !specifier.startsWith(".")) return;
      const base = path.posix
         .normalize(path.posix.join(path.posix.dirname(file), specifier))
         .replace(/\.(ts|js)$/, "");
      const to = [`${base}.ts`, `${base}/index.ts`].find((c) => known.has(c));
      if (to) edges.push({ from: file, to, type });
   };
   const allType = (node: Node, kind: "importKind" | "exportKind") =>
      node[kind] === "type" ||
      ((node.specifiers ?? []).length > 0 &&
         node.specifiers!.every((s) => s[kind] === "type"));
   const visit = (node: Node): void => {
      switch (node.type) {
         case "ImportDeclaration":
            add(node.source?.value, allType(node, "importKind"));
            break;
         case "ExportNamedDeclaration":
            if (node.source)
               add(node.source.value, allType(node, "exportKind"));
            break;
         case "ExportAllDeclaration":
            add(node.source?.value, node.exportKind === "type");
            break;
         case "ImportExpression":
            if (node.source?.type === "Literal") add(node.source.value, false);
            break;
         case "TSImportType": {
            const arg = node.argument;
            const lit = arg?.type === "TSLiteralType" ? arg.literal : arg;
            if (lit?.type === "Literal") add(lit.value, true);
            break;
         }
      }
      for (const [key, child] of Object.entries(node)) {
         if (key === "parent" || !child || typeof child !== "object") continue;
         for (const c of Array.isArray(child) ? child : [child]) {
            if (c && typeof c === "object" && typeof c.type === "string")
               visit(c as Node);
         }
      }
   };
   visit(parse(text, { sourceType: "module" }) as Node);
   return edges;
}

type Graph = Map<string, Set<string>>;

function graphOf(files: string[], edges: Edge[]): Graph {
   const graph: Graph = new Map(files.map((f) => [f, new Set<string>()]));
   for (const e of edges) graph.get(e.from)!.add(e.to);
   return graph;
}

/** Strongly connected groups of two or more modules (Tarjan). */
function cycles(graph: Graph): string[][] {
   let next = 0;
   const index = new Map<string, number>();
   const low = new Map<string, number>();
   const stack: string[] = [];
   const onStack = new Set<string>();
   const out: string[][] = [];
   const connect = (v: string): void => {
      index.set(v, next);
      low.set(v, next);
      next++;
      stack.push(v);
      onStack.add(v);
      for (const w of graph.get(v) ?? []) {
         if (!index.has(w)) {
            connect(w);
            low.set(v, Math.min(low.get(v)!, low.get(w)!));
         } else if (onStack.has(w)) {
            low.set(v, Math.min(low.get(v)!, index.get(w)!));
         }
      }
      if (low.get(v) === index.get(v)) {
         const group: string[] = [];
         let w: string;
         do {
            w = stack.pop()!;
            onStack.delete(w);
            group.push(w);
         } while (w !== v);
         if (group.length > 1) out.push(group.sort());
      }
   };
   for (const v of graph.keys()) if (!index.has(v)) connect(v);
   return out;
}

function reaches(graph: Graph, from: string, to: string): boolean {
   const seen = new Set([from]);
   const todo = [from];
   while (todo.length > 0) {
      const v = todo.pop()!;
      if (v === to) return true;
      for (const w of graph.get(v) ?? []) {
         if (!seen.has(w)) {
            seen.add(w);
            todo.push(w);
         }
      }
   }
   return false;
}

const groupKey = (files: string[]) => [...files].sort().join(" ");
const edgeKey = (e: { from: string; to: string }) => `${e.from} -> ${e.to}`;

describe("import cycles", () => {
   const files = sourceFiles();
   const known = new Set(files);
   const edges = files.flatMap((f) => edgesOf(f, known));
   const runtime = graphOf(
      files,
      edges.filter((e) => !e.type),
   );
   const allowedBack = new Set(TYPE_BACK_EDGES.map(edgeKey));
   const reduced = graphOf(
      files,
      edges.filter((e) => !allowedBack.has(edgeKey(e))),
   );
   const runtimeListed = RUNTIME_CYCLES.map((c) => groupKey(c.files)).sort();

   it("reads the graph it judges", () => {
      // A parser that found no edges would pass every check below.
      expect(runtime.get("service/package.ts")).toContain(
         "service/package_load_gates.ts",
      );
      expect(
         edges.some(
            (e) =>
               e.type &&
               e.from === "service/package_load_gates.ts" &&
               e.to === "service/package.ts",
         ),
      ).toBe(true);
   });

   it("has no runtime cycle outside RUNTIME_CYCLES", () => {
      const found = cycles(runtime).map(groupKey).sort();
      // A new cycle: break it (move the shared piece to a leaf).
      expect(found.filter((k) => !runtimeListed.includes(k))).toEqual([]);
      // Stale: the cycle is gone. Delete the entry.
      expect(runtimeListed.filter((k) => !found.includes(k))).toEqual([]);
   });

   it("has no other cycle once TYPE_BACK_EDGES are set aside", () => {
      const found = cycles(reduced).map(groupKey).sort();
      expect(found).toEqual(runtimeListed);
   });

   it("lists only type-only back edges that still close a cycle", () => {
      for (const allowed of TYPE_BACK_EDGES) {
         const matching = edges.filter(
            (e) => e.from === allowed.from && e.to === allowed.to,
         );
         // Stale: the import is gone. Delete the entry.
         expect({
            edge: edgeKey(allowed),
            present: matching.length > 0,
         }).toEqual({ edge: edgeKey(allowed), present: true });
         // A value import is a runtime cycle, which this list cannot excuse.
         expect({
            edge: edgeKey(allowed),
            typeOnly: matching.every((e) => e.type),
         }).toEqual({ edge: edgeKey(allowed), typeOnly: true });
         // Stale: nothing leads back any more. Delete the entry.
         expect({
            edge: edgeKey(allowed),
            closesCycle: reaches(reduced, allowed.to, allowed.from),
         }).toEqual({ edge: edgeKey(allowed), closesCycle: true });
         expect(allowed.why.trim()).not.toBe("");
      }
      for (const c of RUNTIME_CYCLES) expect(c.why.trim()).not.toBe("");
   });
});
