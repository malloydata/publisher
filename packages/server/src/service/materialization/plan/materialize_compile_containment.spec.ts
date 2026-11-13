// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A failure in one `#(materialize)` source's generated text is contained to
 * that source (materialize_compile.ts): the rest of the model's companion
 * still compiles and builds. Found in the review of #1678.
 */

import { afterAll, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import { tryCompileMaterializeCompanion } from "./materialize_compile";

const dirs: string[] = [];
const connections: DuckDBConnection[] = [];

afterAll(async () => {
   for (const c of connections) await c.close().catch(() => {});
   for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

const SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'east', 10.5, 'r1'), (2, 'west', 20.0, 'r2'), (3, 'east', 5.5, 'r3')
    ) AS t(id, region, amt, rid)`,
   `CREATE TABLE rg AS SELECT * FROM (VALUES
      ('r1', 'east', 'North'), ('r2', 'west', 'South'), ('r3', 'east', 'Mid')
    ) AS t(id, region, label)`,
];

/**
 * Compile `text` as `m.malloy` and plan its companion, as a package does.
 * `corrupt` may rewrite a generated file before it compiles, standing in for
 * a generator bug in one source's text.
 */
async function companionOf(
   text: string,
   corrupt?: (url: string, text: string) => string,
) {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-mat-cc-"));
   dirs.push(dir);
   const duckdb = new DuckDBConnection("duckdb", path.join(dir, "duckdb.db"));
   connections.push(duckdb);
   for (const sql of SEED) await duckdb.runSQL(sql);
   fs.writeFileSync(path.join(dir, "m.malloy"), text);
   const url = pathToFileURL(path.join(dir, "m.malloy")).href;
   const root = new URL(`${pathToFileURL(dir).href}/`);
   const conns = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const runtimeOver = (overlay: Map<string, string>) =>
      new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, text], ...overlay])),
         connections: conns,
      });
   const model = await runtimeOver(new Map())
      .loadModel(new URL(url), { importBaseURL: root })
      .getModel();
   const def = model._modelDef;
   return tryCompileMaterializeCompanion({
      packagePath: dir,
      modelPath: "m.malloy",
      contents: def.contents as Record<string, unknown>,
      givens: (def as { givens?: Record<string, unknown> }).givens ?? {},
      getRuntime: async (overlay) => ({
         runtime: runtimeOver(
            new Map(
               [...overlay].map(([u, t]) => [u, corrupt ? corrupt(u, t) : t]),
            ),
         ),
         importBaseURL: root,
      }),
   });
}

const inlineGivenModel = (given: string) => `##! experimental.givens
given: REGION :: string${given}

source: regions is duckdb.table('rg') extend {
  primary_key: id
  where: region = $REGION
}

#(materialize) storage="lake" name="wide_t" inline=["r"]
source: wide is duckdb.table('ev') extend {
  join_one: r is regions on rid = r.id
  measure: total is amt.sum()
}

#(materialize) storage="lake" name="plain_t"
source: plain is duckdb.table('ev') extend {
  measure: n is count()
}
`;

describe("an inline= join whose rows read a given", () => {
   // Without a default, the uniqueness check's reading of the given failed the
   // whole companion (`plain` went unbuilt); with one, the build stored the
   // default caller's joined rows for everyone.
   for (const [label, given] of [
      ["a given with no default", ""],
      ["a given with a default", " is 'east'"],
   ] as const) {
      it(`refuses that source alone, with the reason P9 gives, and plans the rest: ${label}`, async () => {
         const { generated, refused } = await companionOf(
            inlineGivenModel(given),
         );
         expect(generated?.plans.map((p) => p.sourceName)).toEqual(["plain"]);
         expect(refused).toEqual([
            {
               sourceName: "wide",
               sourceID: expect.stringMatching(/^wide@/),
               reason:
                  "inline= names `r`, through which a given is read, so its " +
                  "stored columns would hold one caller's values; reference " +
                  "the join instead",
            },
         ]);
         expect(generated?.text).not.toContain("wide__mat");
      });
   }

   it("refuses a join whose joined source's own join reads a given", async () => {
      const { generated, refused } = await companionOf(`##! experimental.givens
given: REGION :: string

source: regions is duckdb.table('rg') extend {
  primary_key: id
  where: region = $REGION
}

source: ev2 is duckdb.table('ev') extend {
  primary_key: id
  join_one: r is regions on rid = r.id
}

#(materialize) storage="lake" name="wide_t" inline=["e"]
source: wide is duckdb.table('ev') extend {
  join_one: e is ev2 on id = e.id
}

#(materialize) storage="lake" name="plain_t"
source: plain is duckdb.table('ev')
`);
      expect(generated?.plans.map((p) => p.sourceName)).toEqual(["plain"]);
      expect(refused.map((r) => [r.sourceName, r.reason])).toEqual([
         ["wide", expect.stringContaining("inline= names `e`, through which")],
      ]);
   });

   it("still inlines a join that reads no given, beside a referenced one that does", async () => {
      const { generated, refused } = await companionOf(`##! experimental.givens
given: REGION :: string

source: regions is duckdb.table('rg') extend {
  primary_key: id
  where: region = $REGION
}

source: labels is duckdb.table('rg') extend { primary_key: id }

#(materialize) storage="lake" name="wide_t" inline=["l"]
source: wide is duckdb.table('ev') extend {
  join_one: r is regions on rid = r.id
  join_one: l is labels on rid = l.id
}
`);
      expect(refused).toEqual([]);
      expect(generated?.plans.map((p) => p.sourceName)).toEqual(["wide"]);
      expect(generated?.text).toContain("l__label is l.label");
   });
});

describe("a generated build that does not compile", () => {
   const model = `#(materialize) storage="lake" name="one_t"
source: one is duckdb.table('ev') extend {
  measure: n is count()
}

#(materialize) storage="lake" name="two_t"
source: two is duckdb.table('ev') extend {
  dimension: twice is amt * 2
}

#(materialize) storage="lake" name="three_t"
source: three is duckdb.table('rg')
`;

   it("costs only the source whose text it is in", async () => {
      const { generated, refused } = await companionOf(model, (url, text) =>
         url.endsWith(".materialize.malloy")
            ? text.replace("    twice\n", "    no_such_field\n")
            : text,
      );
      expect(generated?.plans.map((p) => p.sourceName)).toEqual([
         "one",
         "three",
      ]);
      expect(refused).toEqual([
         {
            sourceName: "two",
            sourceID: expect.stringMatching(/^two@/),
            reason: expect.stringMatching(
               /^its generated build did not compile, so it is not built \(a Publisher bug; please report it\): .*no_such_field/,
            ),
         },
      ]);
   });

   it("compiles every source when nothing is wrong", async () => {
      const { generated, refused } = await companionOf(model);
      expect(refused).toEqual([]);
      expect(generated?.plans.map((p) => p.sourceName)).toEqual([
         "one",
         "three",
         "two",
      ]);
   });
});

describe("extension tables whose chained builds do not all compile", () => {
   // `b` reads `r`, which `base` declares above `a`: over `a`'s build source
   // (which carries no join) it does not compile. `c` does.
   const model = `source: regions is duckdb.table('rg') extend { primary_key: id }

source: base is duckdb.table('ev') extend {
  join_one: r is regions on rid = r.id
}

#(materialize) storage="lake" name="a_t"
source: a is base extend {
  measure: total is amt.sum()
}

#(materialize) storage="lake" name="b_t"
source: b is a extend {
  dimension: lbl is r.label
}

#(materialize) storage="lake" name="c_t"
source: c is a extend {
  dimension: dbl is amt * 2
}
`;

   it("sends the failing one alone to the raw chain; the other keeps its parent's table", async () => {
      const { generated, refused } = await companionOf(model);
      expect(refused).toEqual([]);
      const over = Object.fromEntries(
         (generated?.plans ?? []).map((p) => [
            p.sourceName,
            p.buildsOver?.sourceName ?? null,
         ]),
      );
      expect(over).toEqual({ a: null, b: null, c: "a" });
      // `c` still carries the text it builds from over `a`'s stored table.
      const c = generated?.plans.find((p) => p.sourceName === "c");
      expect(c?.buildsOver?.overStored?.parentBuildSourceName).toBe("a__mat");
      // Its build copy leaves out `b`, empty, rather than lose the chain.
      expect(c?.buildsOver?.omit?.length).toBe(1);
      expect(generated?.buildCopies?.get(c?.sourceID ?? "")).toContain(
         "b is a__mat extend { except: id, region, amt, rid }",
      );
   });
});
