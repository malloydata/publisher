// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `resolveEntryPointPartitions` against REAL compiled IR, not hand-built
// stubs — the shapes it reads (annotation copy-by-reference on an
// annotation-free `extend {}`/rename, and the `.inherits` demotion once the
// deriving statement carries any annotation of its own) are compiler
// behavior this test pins, the same posture
// `authorize_gate_walk.spec.ts`/`build_plan_gate_classification.spec.ts` take
// for the identical authorize walk. The one link real Malloy input cannot
// reach — the `sourceRegistry` fallback — gets its own synthetic-IR test
// below, same idiom as `authorize_gate_walk.spec.ts`.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   resolveEntryPointPartitions,
   type PartitionPair,
} from "./gate_classification";
import { PartitionAnnotationError } from "./partition_annotation";

const ROOT = "file:///partition-resolve/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

async function compileModel(model: string): Promise<ModelDef> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const materializer = runtime.loadModel(new URL(`${ROOT}m.malloy`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await materializer.getModel();
   /* eslint-disable @typescript-eslint/no-explicit-any */
   return (compiled as any)._modelDef as ModelDef;
   /* eslint-enable @typescript-eslint/no-explicit-any */
}

function source(modelDef: ModelDef, name: string): SourceDef {
   const found = modelDef.contents[name];
   if (!found) throw new Error(`no source named ${name} in compiled model`);
   return found as SourceDef;
}

function resolve(modelDef: ModelDef, name: string): PartitionPair[] {
   return resolveEntryPointPartitions(source(modelDef, name), modelDef);
}

describe("resolveEntryPointPartitions — inheritance", () => {
   it("own marker on an extend wins over the parent's", async () => {
      const modelDef = await compileModel(`
#(partition) org_id = $ORG
source: parent is duckdb.sql("select 1 as org_id, 2 as tenant_id") extend {
  measure: c is count()
}

#(partition) tenant_id = $TENANT
source: child is parent extend {}
`);
      expect(resolve(modelDef, "child")).toEqual([
         { column: "tenant_id", given: "TENANT" },
      ]);
      // The parent keeps its own — an override on the child must not mutate it.
      expect(resolve(modelDef, "parent")).toEqual([
         { column: "org_id", given: "ORG" },
      ]);
   });

   it("a child with no marker of its own inherits the parent's via the annotation copy", async () => {
      const modelDef = await compileModel(`
#(partition) org_id = $ORG
source: parent is duckdb.sql("select 1 as org_id, 2 as tenant_id") extend {
  measure: c is count()
}

source: child is parent extend {
  measure: c2 is count()
}
`);
      expect(resolve(modelDef, "child")).toEqual([
         { column: "org_id", given: "ORG" },
      ]);
   });

   it("a child whose OWN extend carries an unrelated annotation still inherits, via the .inherits chain", async () => {
      // Any annotation on the deriving statement — not just an authorize/
      // partition one — demotes the base's own notes onto `annotations.inherits`
      // rather than leaving them at top level, so this exercises the ancestor
      // walk itself rather than the copy-by-reference shortcut the case above
      // resolves through.
      const modelDef = await compileModel(`
#(partition) org_id = $ORG
source: parent is duckdb.sql("select 1 as org_id, 2 as tenant_id") extend {
  measure: c is count()
}

# some_unrelated_render_tag
source: child is parent extend {
  measure: c2 is count()
}
`);
      expect(resolve(modelDef, "child")).toEqual([
         { column: "org_id", given: "ORG" },
      ]);
   });

   it("a plain rename (no extend at all) still resolves the base's marker", async () => {
      // This is the case `getFilters`/`filterMap` in model.ts gets wrong — it
      // keys on literal per-source annotations, so `alias` would resolve to no
      // partition even though it serves the same rows as `tenant_orders`. (For
      // this exact shape Malloy copies the base's own annotation note objects
      // onto `alias`'s OWN annotations by reference, so this resolves through
      // the "own" check — the `sourceRegistry` fallback below covers the IR
      // shape where that copy does not happen.)
      const modelDef = await compileModel(`
#(partition) tenant_id = $TENANT
source: tenant_orders is duckdb.sql("select 1 as tenant_id")

source: alias is tenant_orders
`);
      expect(resolve(modelDef, "alias")).toEqual([
         { column: "tenant_id", given: "TENANT" },
      ]);
   });

   it("resolves a source with no partition marker anywhere in its chain to []", async () => {
      const modelDef = await compileModel(`
source: open_src is duckdb.sql("select 1 as x")
`);
      expect(resolve(modelDef, "open_src")).toEqual([]);
   });
});

describe("resolveEntryPointPartitions — grammar", () => {
   it("supports a dotted multi-hop join path on the entry point's own surface", async () => {
      const modelDef = await compileModel(`
source: reports is duckdb.sql("select 1 as report_id")

#(partition) report_ref.report_id = $REPORT
source: orders is duckdb.sql("select 1 as x") extend {
  join_one: report_ref is reports on true
  measure: c is count()
}
`);
      expect(resolve(modelDef, "orders")).toEqual([
         { column: "report_ref.report_id", given: "REPORT" },
      ]);
   });

   it("returns every marker when a source scopes on two independent axes", async () => {
      const modelDef = await compileModel(`
#(partition) org_id = $ORG
#(partition) list_id = $LIST
source: two_axis is duckdb.sql("select 1 as org_id, 2 as list_id") extend {
  measure: c is count()
}
`);
      expect(resolve(modelDef, "two_axis")).toEqual([
         { column: "org_id", given: "ORG" },
         { column: "list_id", given: "LIST" },
      ]);
   });

   it("throws when two markers on one source name the same given", async () => {
      const modelDef = await compileModel(`
#(partition) org_id = $TENANT
#(partition) alt_org_id = $TENANT
source: dup is duckdb.sql("select 1 as org_id, 2 as alt_org_id") extend {
  measure: c is count()
}
`);
      expect(() => resolve(modelDef, "dup")).toThrow(PartitionAnnotationError);
   });

   it("does NOT trace joins into other sources looking for a partition tag", async () => {
      // Q16 posture, mirrored from authorize: a joined source's own
      // `#(partition)` does not apply to the entry point that joins it.
      const modelDef = await compileModel(`
#(partition) tenant_id = $TENANT
source: locked_joined is duckdb.sql("select 1 as tenant_id")

source: orders is duckdb.sql("select 1 as x") extend {
  join_one: locked_joined is locked_joined on true
  measure: c is count()
}
`);
      expect(resolve(modelDef, "orders")).toEqual([]);
   });
});

describe("resolveEntryPointPartitions — sourceRegistry fallback (synthetic IR)", () => {
   // Real Malloy input for a plain rename copies the base's annotation note
   // OBJECTS onto the renaming struct's own annotations by reference (see the
   // "plain rename" test above), so the `resolveDeclaredSource` fallback in
   // `resolveEntryPointPartitions` is unreached by any shape the compiler
   // produces today — same situation `authorize_gate_walk.spec.ts` documents
   // for the identical fallback in `ancestorGateExprs`. A synthetic `ModelDef`
   // is the only way to drive a struct that has NEITHER an own annotation NOR
   // an `.inherits` link, only a `sourceRegistry` reference to its base.
   function tableSource(name: string, extra: object = {}): SourceDef {
      return {
         type: "table",
         name,
         dialect: "duckdb",
         tablePath: name,
         connection: "duckdb",
         fields: [],
         ...extra,
      } as unknown as SourceDef;
   }

   it("follows a sourceRegistry reference to a base with no annotation/inherits link", () => {
      const base = tableSource("base", {
         annotations: {
            blockNotes: [{ text: "#(partition) tenant_id = $TENANT\n" }],
         },
      });
      const renamed = tableSource("renamed", { sourceID: "sid-1" });
      const modelDef = {
         name: "synthetic",
         contents: { base, renamed },
         annotations: {},
         sourceRegistry: {
            "sid-1": {
               entry: { type: "source_registry_reference", name: "base" },
            },
         },
      } as unknown as ModelDef;

      expect(resolveEntryPointPartitions(renamed, modelDef)).toEqual([
         { column: "tenant_id", given: "TENANT" },
      ]);
   });
});
