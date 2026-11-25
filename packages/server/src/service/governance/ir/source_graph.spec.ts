// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type ModelDef,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { fieldNameReads, sourceBaseEdge, sourceDelta } from "./source_graph";

// File-backed: db-duckdb's in-memory database is shared across spec files.
const dbDir = fs.mkdtempSync(path.join(os.tmpdir(), "source-graph-spec-"));
const duckdb = new DuckDBConnection("duckdb", path.join(dbDir, "db.duckdb"));

async function compile(
   files: Record<string, string>,
   entry: string,
): Promise<ModelDef> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map(
            Object.entries(files).map(([name, text]) => [
               `file:///pkg/${name}`,
               text,
            ]),
         ),
      ),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   return (await runtime.loadModel(new URL(`file:///pkg/${entry}`)).getModel())
      ._modelDef;
}

beforeAll(async () => {
   await duckdb.runSQL(
      "CREATE TABLE ev AS SELECT 1 AS id, 'e' AS region, 1.0 AS amt",
   );
});

afterAll(async () => {
   await duckdb.close();
   fs.rmSync(dbDir, { recursive: true, force: true });
});

const A = `##! experimental.access_modifiers
source: orders is duckdb.table('ev') extend { dimension: x is 1 }
#(materialize)
source: stored is orders extend { dimension: y is 2; where: id > -5 }
source: stored_ext is stored extend { dimension: z is 3 }
source: ren is stored
source: ren_ext is ren extend { dimension: rz is 1 }
#(materialize)
source: ren_own is stored
source: ren_own_ext is ren_own extend { dimension: ro is 1 }
#(materialize)
source: annotated is stored_ext extend { dimension: an is 1 }
source: exc is stored extend { except: x }
source: priv is stored include { private: x; * }
source: wh is stored extend { where: id > 0 }
source: wh_ext is wh extend { dimension: we is 1 }
source: renaming is stored extend { rename: amount is amt }
source: renaming_ext is renaming extend { dimension: re is 1 }
source: noop is stored extend {}
source: noop_ext is noop extend { dimension: ne is 1 }
source: raw is duckdb.table('ev')
source: q is orders -> { group_by: region }
source: qe is q extend { dimension: k is 1 }
source: qr is qe
`;

const B = `import "a.malloy"
source: imp_ext is stored extend { dimension: v is 9 }
source: imp_ext2 is imp_ext extend { dimension: v2 is 9 }
`;

describe("sourceBaseEdge", () => {
   let a: ModelDef;
   let b: ModelDef;
   const base = (model: ModelDef, name: string) => {
      const edge = sourceBaseEdge(model.contents[name], model);
      return edge && [edge.hop, edge.sourceID?.split("@")[0]];
   };
   beforeAll(async () => {
      const files = { "a.malloy": A, "b.malloy": B };
      a = await compile(files, "a.malloy");
      b = await compile(files, "b.malloy");
   });

   it("finds the base of an extension of a table-backed source, which Malloy records no edge for", () => {
      expect(a.contents["stored_ext"]).not.toHaveProperty("extends");
      expect(base(a, "stored")).toEqual(["extends", "orders"]);
      expect(base(a, "stored_ext")).toEqual(["extends", "stored"]);
      // One that declares its own annotation reads its base through `inherits`.
      expect(base(a, "annotated")).toEqual(["extends", "stored_ext"]);
      // A base whose filters it extends.
      expect(base(a, "wh")).toEqual(["extends", "stored"]);
      expect(base(a, "wh_ext")).toEqual(["extends", "wh"]);
      // A `rename:` copies a field under a new name, at its own location.
      expect(base(a, "renaming")).toEqual(["extends", "stored"]);
      expect(base(a, "renaming_ext")).toEqual(["extends", "renaming"]);
   });

   it("reads a rename by its referenceID, before the extends it copies from its referent", () => {
      expect(base(a, "ren")).toEqual(["rename", "stored"]);
      // `qr is qe` carries qe's own `extends: q`: the grandparent.
      expect((a.contents["qr"] as { extends?: string }).extends).toMatch(/^q@/);
      expect(base(a, "qr")).toEqual(["rename", "qe"]);
      expect(base(a, "qe")).toEqual(["extends", "q"]);
      expect(base(a, "q")).toEqual(["query", "orders"]);
   });

   it("takes the source alike candidates are built from, unless one declares its own annotation", () => {
      // `ren` and `noop` add nothing to `stored`, so their extensions read as
      // `stored`'s: every table they could reach is `stored`'s or above.
      expect(base(a, "ren_ext")).toEqual(["extends", "stored"]);
      expect(base(a, "noop_ext")).toEqual(["extends", "stored"]);
      // A rename with its own `#(materialize)` holds an annotations object of
      // its own, which its extension shares: told apart by identity.
      expect(base(a, "ren_own_ext")).toEqual(["extends", "ren_own"]);
   });

   it("gives no edge where a field changed or a table has no base", () => {
      // `except:` drops a field, and an access modifier changes it: no source
      // holds the extension's fields, so its base is unresolved, as before.
      expect(base(a, "exc")).toBeUndefined();
      expect(base(a, "priv")).toBeUndefined();
      expect(base(a, "orders")).toBeUndefined();
      expect(base(a, "raw")).toBeUndefined();
   });

   it("finds a base declared in an imported file", () => {
      expect(base(b, "imp_ext")).toEqual(["extends", "stored"]);
      expect(base(b, "imp_ext2")).toEqual(["extends", "imp_ext"]);
      // The importing file's sources are never the imported file's bases.
      expect(base(b, "stored_ext")).toEqual(["extends", "stored"]);
   });

   it("never takes a look-alike for a base: another source on the same table, a sibling, or one in another file", async () => {
      const files = {
         "x.malloy":
            "source: a is duckdb.table('ev') extend { dimension: d is 1 }",
         "w.malloy":
            "source: a2 is duckdb.table('ev') extend { dimension: d is 1 }",
         "z.malloy": `import "x.malloy"
import "w.malloy"
source: c is a extend { dimension: e is 2 }
source: twin is duckdb.table('ev') extend { dimension: d is 1 }
source: twin_ext is twin extend { dimension: t is 3 }
source: s1 is a extend { dimension: p is 1 }
source: s2 is a extend { dimension: p is 1 }
source: l1 is duckdb.table('ev'); source: l2 is duckdb.table('ev') extend { dimension: k is 1 }
source: copied is duckdb.table('ev') extend { dimension: d is 1; dimension: e is 2 }
`,
      };
      const z = await compile(files, "z.malloy");
      // The one it extends, not the look-alike in the other imported file.
      expect(base(z, "c")).toEqual(["extends", "a"]);
      // Two sources on one table, with the same fields written alike, are
      // not each other's: a table's columns carry the call that read them.
      expect(base(z, "twin")).toBeUndefined();
      expect(base(z, "twin_ext")).toEqual(["extends", "twin"]);
      // A sibling that adds the same field is not the base.
      expect(base(z, "s2")).toEqual(["extends", "a"]);
      // Two table calls on one line.
      expect(base(z, "l2")).toBeUndefined();
      // Every field another source has, written again, is no extension of it.
      expect(base(z, "copied")).toBeUndefined();
   });

   it("gives no edge where the compiled model cannot say which file came first", async () => {
      // `z`'s model lists only its own import of `y`, not `y`'s of `x`: in it,
      // `b`'s base in `x` is of unknown order, so `b` gets no edge there.
      const files = {
         "x.malloy":
            "source: a is duckdb.table('ev') extend { dimension: d1 is 1 }",
         "y.malloy":
            'import "x.malloy"\nsource: b is a extend { dimension: d2 is 2 }',
         "z.malloy":
            'import "y.malloy"\nimport "x.malloy"\nsource: c is b extend { dimension: d3 is 3 }',
      };
      const z = await compile(files, "z.malloy");
      const y = await compile(files, "y.malloy");
      expect(base(z, "c")).toEqual(["extends", "b"]);
      expect(base(y, "b")).toEqual(["extends", "a"]);
      expect(base(z, "b")).toBeUndefined();
   });
});

describe("sourceDelta and fieldNameReads", () => {
   it("reads what an extension adds over its base, across hops", async () => {
      const m = await compile(
         {
            "m.malloy": `source: base is duckdb.sql("SELECT 1 AS a, 'x' AS b") extend {
  measure: n is count()
}
source: one is base extend { dimension: lb is lower(b) }
source: two is one extend { dimension: lb2 is concat(lb, a) }
source: filtered is base extend { where: a > 0 }
source: renamed is base extend { rename: bee is b }
`,
         },
         "m.malloy",
      );
      const def = (n: string) => m.contents[n];
      const names = (fs: unknown[]) =>
         fs.map((f) => (f as { name: string }).name);
      const two = sourceDelta(def("two"), def("base"));
      expect(two.addsFilter).toBe(false);
      expect(two.changed).toEqual([]);
      expect(names(two.added)).toEqual(["lb", "lb2"]);
      expect(two.added.map((f) => fieldNameReads(f))).toEqual([
         ["b"],
         ["lb", "a"],
      ]);
      expect(sourceDelta(def("filtered"), def("base")).addsFilter).toBe(true);
      expect(sourceDelta(def("base"), def("base"))).toEqual({
         addsFilter: false,
         changed: [],
         added: [],
      });
      // A rename takes the field away under its old name, and adds it anew.
      const renamed = sourceDelta(def("renamed"), def("base"));
      expect(renamed.added.length).toBe(1);
   });

   it("reads nothing it cannot: a path through a join, or no record", () => {
      expect(fieldNameReads({})).toBeUndefined();
      expect(
         fieldNameReads({ refSummary: { fieldUsage: [{ path: ["j", "x"] }] } }),
      ).toBeUndefined();
      expect(
         fieldNameReads({
            refSummary: {
               fieldUsage: [{ path: [] }, { path: ["a"] }, { path: ["a"] }],
            },
         }),
      ).toEqual(["a"]);
   });
});
