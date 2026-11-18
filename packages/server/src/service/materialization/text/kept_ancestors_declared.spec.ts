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

   it("rejects a namesake on a different table with the same field names", async () => {
      // The model's namespace holds the entry file's `ev_measured` (on `ev2`),
      // not the one `mid.malloy` imports (on `ev`): not the source's table, so
      // no base is taken and the source falls back as before.
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
