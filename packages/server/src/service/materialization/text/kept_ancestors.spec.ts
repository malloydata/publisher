// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type ModelDef } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import { confirmedAncestors, keptAncestorChain } from "./kept_ancestors";
import { SourceMap } from "./source_map";

const mz = materializationFixture();
const URL_ = "file:///pkg/m.malloy";

beforeAll(async () => {
   await mz.duckdb.runSQL(
      "CREATE TABLE ev AS SELECT 1 AS id, 'acme' AS org, 1.0 AS amt",
   );
});

// Over a table, Malloy records no base edge for an extension; the walk
// infers it, through renames and past an extension's own `where:` or join.
const TEXT = `source: ev is duckdb.table('ev')
source: base is ev extend { where: org = 'acme'; dimension: a is 1 }
source: b2 is base
source: b3 is b2
source: renamed_ext is b3 extend { measure: total is amt.sum() }
source: mid is b2 extend { where: amt > 0; join_one: p is ev on p.id = id }
source: mid_ext is mid extend { measure: total is amt.sum() }
`;

describe("the kept walk over a table-backed base", () => {
   let model: ModelDef;
   const names = (chain: { sourceID: string }[]) =>
      chain.map((a) => a.sourceID.split("@")[0]);
   const idOf = (name: string) =>
      (model.contents[name] as unknown as { sourceID: string }).sourceID;
   const own = (name: string) =>
      (model.contents[name] as unknown as { location: { range: never } })
         .location.range;
   beforeAll(async () => {
      model = await mz.compile(TEXT);
   });

   for (const [label, read] of [
      // The materialize planner reads a model's contents alone.
      ["its contents alone", () => ({ contents: model.contents })],
      ["the whole model", () => model],
   ] as const) {
      it(`walks through renames and past an own where or join, reading ${label}`, () => {
         const map = SourceMap.of(read() as never);
         const chain = keptAncestorChain(map, idOf("renamed_ext"), URL_);
         expect(names(chain)).toEqual(["base", "ev"]);
         expect(chain[0].aliases?.map((a) => a.sourceID.split("@")[0])).toEqual(
            ["b2", "b3"],
         );
         expect(
            names(
               confirmedAncestors(() => TEXT, URL_, own("renamed_ext"), chain),
            ),
         ).toEqual(["base", "ev"]);
         const mid = keptAncestorChain(map, idOf("mid_ext"), URL_);
         expect(names(mid)).toEqual(["mid", "base", "ev"]);
         expect(
            names(confirmedAncestors(() => TEXT, URL_, own("mid_ext"), mid)),
         ).toEqual(["mid", "base", "ev"]);
      });
   }

   it("confirms a rename only where the text declares it bare, naming the next", () => {
      const map = SourceMap.of({ contents: model.contents } as never);
      const chain = keptAncestorChain(map, idOf("renamed_ext"), URL_);
      // An alias whose declaration is not a bare rename (it extends): never
      // confirmed, though it names the next alias.
      const at = (name: string) => ({
         sourceID: idOf(name),
         url: URL_,
         range: own(name),
      });
      const forged = [
         {
            ...at("base"),
            aliases: [at("mid"), at("b2")],
         },
      ];
      expect(
         confirmedAncestors(() => TEXT, URL_, own("mid_ext"), forged),
      ).toEqual([]);
      // An alias naming a source that is not the one it renames.
      const misnamed = TEXT.replace("source: b3 is b2\n", "source: b3 is ev\n");
      expect(
         confirmedAncestors(() => misnamed, URL_, own("renamed_ext"), chain),
      ).toEqual([]);
      // A child naming a source that is no alias of the ancestor.
      const other = TEXT.replace(
         "source: renamed_ext is b3 extend",
         "source: renamed_ext is ev extend",
      );
      expect(
         confirmedAncestors(() => other, URL_, own("renamed_ext"), chain),
      ).toEqual([]);
   });
});

describe("carrying an ancestor's blocks from another file", () => {
   const BASE = "file:///pkg/base.malloy";
   const CHILD = "file:///pkg/child.malloy";
   /** `snippet`'s range in `text`, 0-based lines and characters. */
   const rangeOf = (text: string, snippet: string) => {
      const at = text.indexOf(snippet);
      const pos = (offset: number) => {
         const before = text.slice(0, offset).split("\n");
         return {
            line: before.length - 1,
            character: before[before.length - 1].length,
         };
      };
      return { start: pos(at), end: pos(at + snippet.length) } as never;
   };
   const baseDecl = (lookup: string) =>
      `source: lk is trips extend { join_one: L is ${lookup} on cat = L.cat }`;
   const baseText = (lookup: string) =>
      `source: trips is duckdb.table('t')\nsource: lookup is duckdb.table('c')\n${baseDecl(lookup)}\n`;
   const childDecl = "source: lk_other is lk extend { where: amt > 0 }";
   const confirmed = (
      base: string,
      child: string,
      others: Record<string, string> = {},
   ) =>
      confirmedAncestors(
         (url) => (url === BASE ? base : url === CHILD ? child : others[url]),
         CHILD,
         rangeOf(child, childDecl),
         [
            {
               sourceID: `lk@${BASE}`,
               url: BASE,
               range: rangeOf(
                  base,
                  baseDecl(base.includes("`lookup`") ? "`lookup`" : "lookup"),
               ),
            },
         ] as never,
      );

   it("carries them through a whole import", () => {
      const child = `import "base.malloy"\n${childDecl}\n`;
      expect(confirmed(baseText("lookup"), child)).toHaveLength(1);
   });

   it("not when the importing file declares a name the blocks read, backticked or not", () => {
      const child = `import "base.malloy"\nsource: lookup is duckdb.table('evil')\n${childDecl}\n`;
      expect(confirmed(baseText("lookup"), child)).toEqual([]);
      expect(confirmed(baseText("`lookup`"), child)).toEqual([]);
      const backtickedChild = `import "base.malloy"\nsource: \`lookup\` is duckdb.table('evil')\n${childDecl}\n`;
      expect(confirmed(baseText("lookup"), backtickedChild)).toEqual([]);
   });

   it("through a selective import, importing each name the blocks read that the file does not bind", () => {
      const child = `import { lk } from "base.malloy"\n${childDecl}\n`;
      const kept = confirmed(baseText("lookup"), child);
      expect(kept).toHaveLength(1);
      expect(kept[0].imports).toEqual(['import { lookup } from "base.malloy"']);
   });

   it("not when a selective import binds a name the blocks read to another declaration", () => {
      const OTHER = "file:///pkg/other.malloy";
      const child = `import { lk } from "base.malloy"\nimport { lookup } from "other.malloy"\n${childDecl}\n`;
      expect(
         confirmed(baseText("lookup"), child, {
            [OTHER]: "source: lookup is duckdb.table('evil')\n",
         }),
      ).toEqual([]);
      const renamed = `import { lk, lookup is trips } from "base.malloy"\n${childDecl}\n`;
      expect(confirmed(baseText("lookup"), renamed)).toEqual([]);
   });

   it("imports nothing through a whole import, which binds every name the file declares", () => {
      const child = `import "base.malloy"\n${childDecl}\n`;
      expect(confirmed(baseText("lookup"), child)[0].imports).toBeUndefined();
   });

   describe("when the ancestor's file imports another file", () => {
      const RAW = "file:///pkg/raw.malloy";
      const DEEP = "file:///pkg/deep.malloy";
      const child = `import "base.malloy"\n${childDecl}\n`;
      // base.malloy declares trips, lookup and lk, and imports raw.malloy.
      const importing = (line: string) => `${line}\n${baseText("lookup")}`;

      it("carries them when the blocks read no name it imports (P2-22)", () => {
         const raw = "source: stations is duckdb.table('s')\n";
         expect(
            confirmed(importing(`import "raw.malloy"`), child, { [RAW]: raw }),
         ).toHaveLength(1);
         expect(
            confirmed(
               importing(`import { stations } from "raw.malloy"`),
               child,
               {
                  [RAW]: raw,
               },
            ),
         ).toHaveLength(1);
      });

      it("imports a name the blocks read that it imports, which the importing file does not see (Malloy imports are not transitive)", () => {
         // `lookup`, which the blocks read, is now raw.malloy's, which the child
         // does not see through its whole import of base.malloy.
         const base = `import "raw.malloy"\nsource: trips is duckdb.table('t')\n${baseDecl("lookup")}\n`;
         const raw = "source: lookup is duckdb.table('c')\n";
         expect(confirmed(base, child, { [RAW]: raw })[0].imports).toEqual([
            'import { lookup } from "raw.malloy"',
         ]);
         const selective = `import { lookup } from "raw.malloy"\nsource: trips is duckdb.table('t')\n${baseDecl("lookup")}\n`;
         expect(confirmed(selective, child, { [RAW]: raw })[0].imports).toEqual(
            ['import { lookup } from "raw.malloy"'],
         );
         const renamed = `import { lookup is cats } from "raw.malloy"\nsource: trips is duckdb.table('t')\n${baseDecl("lookup")}\n`;
         expect(
            confirmed(renamed, child, {
               [RAW]: "source: cats is duckdb.table('c')\n",
            })[0].imports,
         ).toEqual(['import { lookup is cats } from "raw.malloy"']);
      });

      it("imports nothing when the importing file binds that name to the same declaration", () => {
         const base = `import "raw.malloy"\nsource: trips is duckdb.table('t')\n${baseDecl("lookup")}\n`;
         const raw = "source: lookup is duckdb.table('c')\n";
         const both = `import "base.malloy"\nimport "raw.malloy"\n${childDecl}\n`;
         const kept = confirmed(base, both, { [RAW]: raw });
         expect(kept).toHaveLength(1);
         expect(kept[0].imports).toBeUndefined();
      });

      it("not when the importing file binds that name to another declaration", () => {
         const base = `import "raw.malloy"\nsource: trips is duckdb.table('t')\n${baseDecl("lookup")}\n`;
         const raw = "source: lookup is duckdb.table('c')\n";
         const own = `import "base.malloy"\nsource: lookup is duckdb.table('evil')\n${childDecl}\n`;
         expect(confirmed(base, own, { [RAW]: raw })).toEqual([]);
         const viaDeep = `import "base.malloy"\nimport "deep.malloy"\n${childDecl}\n`;
         expect(confirmed(base, viaDeep, { [RAW]: raw, [DEEP]: raw })).toEqual(
            [],
         );
      });

      it("not when the importing file declares a name the blocks read as a field", () => {
         const own = `import "base.malloy"\nsource: cat is duckdb.table('evil')\n${childDecl}\n`;
         expect(
            confirmed(importing(`import "raw.malloy"`), own, {
               [RAW]: "source: stations is duckdb.table('s')\n",
            }),
         ).toEqual([]);
      });

      it("not when an imported file cannot be read", () => {
         expect(confirmed(importing(`import "raw.malloy"`), child)).toEqual([]);
      });
   });
});
