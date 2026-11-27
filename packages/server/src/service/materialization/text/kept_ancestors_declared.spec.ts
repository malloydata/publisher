// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type ModelDef } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import {
   confirmedAncestors,
   keptAncestorChain,
   keptAncestorChainOrDeclared,
} from "./kept_ancestors";
import { SourceMap } from "./source_map";

/**
 * F-08: the base a table-backed extension's declaration names, for one the
 * compiled model records no base for (an `except:` drops a field of its
 * base, so the source map infers none). The base is taken only when it is on
 * the same table and holds every field the source inherits, and the step is
 * then confirmed against the text as any kept base is.
 */

const mz = materializationFixture();
const ROOT = "file:///pkg/";

beforeAll(async () => {
   for (const table of ["ev", "ev2"]) {
      await mz.duckdb.runSQL(
         `CREATE TABLE ${table} AS SELECT 1 AS id, 'acme' AS org, 1.0 AS amt, 'x' AS mark`,
      );
   }
});

const measured = (name: string, table = "ev") =>
   `source: ${name} is duckdb.table('${table}') extend {
  measure: n_rows is count()
  measure: amt_total is amt.sum()
}
`;

interface Walk {
   chain: string[];
   confirmed: [string, string | undefined][];
}

/** The walk for source `s`, with the files' text (or `text` instead). */
async function walk(
   files: Record<string, string>,
   text: (url: string) => string | undefined = (url) =>
      files[url.replace(ROOT, "")],
): Promise<Walk> {
   const model: ModelDef = await mz.compile(files);
   const s = model.contents["s"] as unknown as {
      sourceID: string;
      location: { url: string; range: never };
   };
   const map = SourceMap.of({ contents: model.contents } as never);
   // The compiled model records no base for it: the case this reads.
   expect(keptAncestorChain(map, s.sourceID, s.location.url)).toEqual([]);
   const chain = keptAncestorChainOrDeclared(
      map,
      s.sourceID,
      s.location.url,
      text,
   );
   const name = (id: string) => id.split("@")[0];
   return {
      chain: chain.map((a) => name(a.sourceID)),
      confirmed: confirmedAncestors(
         text,
         s.location.url,
         s.location.range,
         chain,
      ).map((a) => [name(a.sourceID), a.runText]),
   };
}

const RUN = `extend {
  measure: n_rows is count()
  measure: amt_total is amt.sum()
}`;

describe("keptAncestorChainOrDeclared", () => {
   it("takes the base the declaration names, in the same file", async () => {
      const got = await walk({
         "m.malloy": `${measured("ev_measured")}
source: s is ev_measured extend { except: mark }
`,
      });
      expect(got.chain).toEqual(["ev_measured"]);
      expect(got.confirmed).toEqual([["ev_measured", RUN]]);
   });

   it("takes a base in a file imported whole", async () => {
      const got = await walk({
         "m.malloy": `import "base.malloy"
source: s is ev_measured extend { except: mark }
`,
         "base.malloy": measured("ev_measured"),
      });
      expect(got.chain).toEqual(["ev_measured"]);
      expect(got.confirmed).toEqual([["ev_measured", RUN]]);
   });

   it("walks on through a bare rename the declaration names", async () => {
      const got = await walk({
         "m.malloy": `${measured("ev_measured")}
source: b2 is ev_measured
source: s is b2 extend { except: mark }
`,
      });
      expect(got.chain[0]).toBe("b2");
      // The rename has no block of its own; its referent's is kept.
      expect(got.confirmed.at(-1)).toEqual(["ev_measured", RUN]);
      expect(
         got.confirmed.every(([, run]) => run === undefined || run === RUN),
      ).toBe(true);
   });

   it("never takes the model's namesake for the declaring file's name", async () => {
      // The model's namespace holds the entry file's `ev_measured` (on
      // `ev2`), but `mid.malloy` reads the one it imports, which this model
      // (its contents alone) does not hold: no base is taken, and the source
      // falls back as before. With the package's other models the walk finds
      // the right one (below).
      const got = await walk({
         "e.malloy": `import "mid.malloy"
${measured("ev_measured", "ev2")}`,
         "mid.malloy": `import "base.malloy"
source: s is ev_measured extend { except: mark }
`,
         "base.malloy": measured("ev_measured"),
      });
      expect(got.chain).toEqual([]);
      expect(got.confirmed).toEqual([]);
   });

   it("falls back for a base imported under another name", async () => {
      const got = await walk({
         "m.malloy": `import { em is ev_measured } from "base.malloy"
source: s is em extend { except: mark }
`,
         "base.malloy": measured("ev_measured"),
      });
      expect(got.confirmed).toEqual([]);
   });

   it("takes nothing from text it cannot read or parse", async () => {
      const files = {
         "m.malloy": `${measured("ev_measured")}
source: s is ev_measured extend { except: mark }
`,
      };
      expect(await walk(files, () => undefined)).toEqual({
         chain: [],
         confirmed: [],
      });
      expect(await walk(files, () => "source: ((( is")).toEqual({
         chain: [],
         confirmed: [],
      });
   });
});

/**
 * N-06: a model that imports a `#(materialize)` source's file does not hold
 * the source's base when the base is declared in a file the declaring file
 * imports (Malloy imports are not transitive), so the walk reads each
 * declaration the model lacks from the package model that holds it
 * (`elsewhere`), and each name in the scope of the file that wrote it.
 */
describe("keptAncestorChainOrDeclared, past the model's own declarations", () => {
   const MID_RUN = `extend {
  measure: top_amt is amt.max()
}`;
   const files = (entryExtra = "") => ({
      "e.malloy": `import "m.malloy"
${entryExtra}`,
      "m.malloy": `import "mid.malloy"
source: s is ev_mid extend { where: amt > 0 }
`,
      "mid.malloy": `import "base.malloy"
source: ev_mid is ev_measured ${MID_RUN}
`,
      "base.malloy": measured("ev_measured"),
   });

   /** Every file compiled as its own model, as a package compiles each. */
   async function walkFromEntry(all: Record<string, string>) {
      const maps = await Promise.all(
         Object.keys(all).map(async (entry) =>
            SourceMap.of(await mz.compile(all, { entry })),
         ),
      );
      const [map] = maps;
      const s = (map.model() as ModelDef).contents["s"] as unknown as {
         sourceID: string;
         location: { url: string; range: never };
      };
      const text = (url: string) =>
         all[url.slice(url.lastIndexOf("/") + 1)] as string | undefined;
      const elsewhere = (id: string) =>
         maps.map((m) => m.node(id)?.def).find((d) => d !== undefined);
      const of = (lookup?: (id: string) => unknown) => {
         const chain = keptAncestorChainOrDeclared(
            map,
            s.sourceID,
            s.location.url,
            text,
            lookup,
         );
         return {
            chain: chain.map((a) => a.sourceID),
            confirmed: confirmedAncestors(
               text,
               s.location.url,
               s.location.range,
               chain,
            ).map((a) => [a.sourceID, a.runText]),
         };
      };
      return { s, of: () => of(elsewhere), alone: () => of() };
   }

   const idIn = (s: { sourceID: string }, name: string, file: string) =>
      `${name}@${s.sourceID.slice(s.sourceID.indexOf("@") + 1).replace(/[^/]*$/, file)}`;

   it("follows the base two files from the model, and its base in turn", async () => {
      const { s, of, alone } = await walkFromEntry(files());
      // The entry's model holds neither base: nothing to walk on its own.
      expect(alone().chain).toEqual([]);
      expect(of()).toEqual({
         chain: [
            idIn(s, "ev_mid", "mid.malloy"),
            idIn(s, "ev_measured", "base.malloy"),
         ],
         confirmed: [
            [idIn(s, "ev_mid", "mid.malloy"), MID_RUN],
            [idIn(s, "ev_measured", "base.malloy"), RUN],
         ],
      });
   });

   it("takes the namesake the declaring file imports, not the model's own", async () => {
      // The entry declares an `ev_measured` and an `ev_mid` of its own, on the
      // same table with the same fields: only the declaring files' scopes
      // tell them apart.
      const { s, of } = await walkFromEntry(
         files(`${measured("ev_measured")}
source: ev_mid is ev_measured ${MID_RUN}
`),
      );
      expect(of().chain).toEqual([
         idIn(s, "ev_mid", "mid.malloy"),
         idIn(s, "ev_measured", "base.malloy"),
      ]);
   });

   it("confirms nothing for a base imported under another name", async () => {
      const all = {
         ...files(),
         "m.malloy": `import { ev_mid is ev_measured } from "base.malloy"
source: s is ev_mid extend { where: amt > 0 }
`,
      };
      const { of } = await walkFromEntry(all);
      expect(of().confirmed).toEqual([]);
   });
});
