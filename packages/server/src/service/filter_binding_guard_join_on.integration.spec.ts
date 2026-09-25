// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A joined-field `#(access_filter)` gate (`child.org_id in $GROUPS`) reads
 * more than the path it names: the join's ON (or `with`) reads columns of the
 * struct that declares the join, and the joined source's own `where:` reads
 * columns of the joined struct. Rebinding either one, in the entry point or in
 * a swapped-in joined source, changes which child row each parent row reaches
 * without touching `child.org_id` itself. These pin that the filter-binding
 * check compares those columns too, through the real `Model.create` /
 * `getQueryResults` path.
 *
 * Seed: parent ids 1,2 join children in org 1 and ids 3,4 children in org 2,
 * so GROUPS [1] admits exactly parent ids 1,2. `active` is true only for
 * children 1,2.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection, type GivenValue } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
import { Model } from "./model";

const SEED_SQL = `
CREATE OR REPLACE TABLE orgtable (id INTEGER, org_id INTEGER);
INSERT INTO orgtable VALUES (1, 1), (2, 1), (3, 2), (4, 2);

CREATE OR REPLACE TABLE childtable (id INTEGER, org_id INTEGER, active BOOLEAN);
INSERT INTO childtable VALUES (1, 1, true), (2, 1, true), (3, 2, false), (4, 2, false);

CREATE OR REPLACE TABLE midtable (id INTEGER, grand_id INTEGER);
INSERT INTO midtable VALUES (1, 1), (2, 2), (3, 3), (4, 4);

CREATE OR REPLACE TABLE grandtable (id INTEGER, org_id INTEGER);
INSERT INTO grandtable VALUES (1, 1), (2, 1), (3, 2), (4, 2);
`;

const MODEL_TEXT = `##! experimental { givens parameters }

given:
  GROUPS :: number[]

source: child_all is duckdb.table('childtable') extend { primary_key: id }
source: child_active is duckdb.table('childtable') extend {
   primary_key: id
   where: active
}
source: grand_all is duckdb.table('grandtable') extend { primary_key: id }
source: mid_src is duckdb.table('midtable') extend {
   primary_key: id
   join_one: g is grand_all on grand_id = g.id
}

source: child_fake_active is duckdb.table('childtable') extend {
   primary_key: id
   rename: raw_active is active
   dimension: active is true
   where: active
}
source: child_shifted_id is duckdb.table('childtable') extend {
   rename: raw_id is id
   dimension: id is 5 - raw_id
   primary_key: id
}
source: child_excepted_id is duckdb.table('childtable') extend {
   except: id
   dimension: id is org_id + 2
}
source: mid_shifted is duckdb.table('midtable') extend {
   primary_key: id
   rename: raw_grand is grand_id
   dimension: grand_id is 5 - raw_grand
   join_one: g is grand_all on grand_id = g.id
}
source: child_from(minid::number) is duckdb.table('childtable') extend {
   primary_key: id
   where: id >= minid
}

#(access_filter) child.org_id in $GROUPS
source: gated_plain is duckdb.table('orgtable') extend {
   join_one: child is child_all on id = child.id
   measure: n is count()
}

#(access_filter) child.org_id in $GROUPS
source: gated_with is duckdb.table('orgtable') extend {
   join_one: child is child_all with id
   measure: n is count()
}

#(access_filter) child.org_id in $GROUPS
source: gated_active is duckdb.table('orgtable') extend {
   join_one: child is child_active on id = child.id
   measure: n is count()
}

#(access_filter) mid.g.org_id in $GROUPS
source: gated_two is duckdb.table('orgtable') extend {
   join_one: mid is mid_src on id = mid.id
   measure: n is count()
}

#(access_filter) child.org_id in $GROUPS
source: gated_from is duckdb.table('orgtable') extend {
   join_one: child is child_from(minid is 3) on id = child.id
   measure: n is count()
}

source: gated_plain_ext is gated_plain extend { dimension: doubled is id * 2 }
`;

async function newDuckdb(): Promise<DuckDBConnection> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   for (const stmt of SEED_SQL.trim()
      .split(";")
      .filter((s) => s.trim())) {
      await duckdb.runSQL(stmt.trim() + ";");
   }
   return duckdb;
}

async function withModel(
   run: (model: Model) => Promise<void>,
   text = MODEL_TEXT,
): Promise<void> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fbg-join-on-"));
   try {
      fs.writeFileSync(path.join(dir, "m.malloy"), text);
      const model = await Model.create(
         "test-pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      expect(
         (model as unknown as { compilationError?: Error }).compilationError,
      ).toBeUndefined();
      await run(model);
   } finally {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

async function rows(
   model: Model,
   queryText: string,
   givens: Record<string, GivenValue>,
): Promise<Record<string, unknown>[]> {
   const result = await model.getQueryResults(
      undefined,
      undefined,
      queryText,
      {},
      true,
      givens,
   );
   return result.compactResult as unknown as Record<string, unknown>[];
}

/** Denied, and never served: a served result fails with the rows it returned. */
async function expectDenied(
   model: Model,
   queryText: string,
   givens: Record<string, GivenValue>,
): Promise<void> {
   let served: Record<string, unknown>[];
   try {
      served = await rows(model, queryText, givens);
   } catch (err) {
      expect(err).toBeInstanceOf(AccessDeniedError);
      return;
   }
   throw new Error(`expected a denial, served ${JSON.stringify(served)}`);
}

const IDS = " -> { group_by: id; order_by: id }";

describe("filter binding guard — the columns a gated join's ON reads", () => {
   it("the unmodified gated sources serve only the rows the gate admits (negative)", async () => {
      await withModel(async (model) => {
         for (const target of ["gated_plain", "gated_with", "gated_two"]) {
            expect(
               await rows(model, `run: ${target}${IDS}`, { GROUPS: [1] }),
            ).toEqual([{ id: 1 }, { id: 2 }]);
         }
         expect(
            await rows(model, `run: gated_active${IDS}`, { GROUPS: [1, 2] }),
         ).toEqual([{ id: 1 }, { id: 2 }]);
         expect(
            await rows(model, `run: gated_from${IDS}`, { GROUPS: [1, 2] }),
         ).toEqual([{ id: 3 }, { id: 4 }]);
      });
   });

   it.each([
      [
         "rename + dimension",
         "gated_plain",
         "rename: real_id is id; dimension: id is 1",
      ],
      ["except + dimension", "gated_plain", "except: id; dimension: id is 1"],
      [
         "a with: join over the redefined key",
         "gated_with",
         "rename: real_id is id; dimension: id is 1",
      ],
      [
         "two joins down",
         "gated_two",
         "rename: real_id is id; dimension: id is 1",
      ],
      [
         "a joined source with its own where:",
         "gated_active",
         "rename: real_id is id; dimension: id is 1",
      ],
   ])(
      "the entry point redefining the column the join's ON reads is denied (%s)",
      async (_label, target, extendBody) => {
         await withModel(async (model) => {
            await expectDenied(
               model,
               `run: ${target} extend { ${extendBody} } -> { aggregate: n is count() }`,
               { GROUPS: [1] },
            );
         });
      },
   );
});

describe("filter binding guard — a swapped-in joined source that rebinds what the join reads", () => {
   it.each([
      [
         "a fresh sibling with the same where: text over a redefined field",
         "gated_active",
         "except: child; join_one: child is child_fake_active on id = child.id",
         [1, 2],
      ],
      [
         "an inline extend with the same where: text over a redefined field",
         "gated_active",
         "except: child; join_one: child is child_all extend { rename: raw_active is active; dimension: active is true; where: active } on id = child.id",
         [1, 2],
      ],
      [
         "a sibling redefining the ON column (rename + dimension)",
         "gated_plain",
         "except: child; join_one: child is child_shifted_id on id = child.id",
         [1],
      ],
      [
         "a sibling redefining the ON column (except + dimension)",
         "gated_plain",
         "except: child; join_one: child is child_excepted_id on id = child.id",
         [1],
      ],
      [
         "two joins down, a middle sibling redefining the second join's ON column",
         "gated_two",
         "except: mid; join_one: mid is mid_shifted on id = mid.id",
         [1],
      ],
      [
         "a with: sibling whose primary key is a redefined dimension",
         "gated_with",
         "except: child; join_one: child is child_shifted_id with id",
         [1],
      ],
      [
         "a parameterized sibling bound to a different argument",
         "gated_from",
         "except: child; join_one: child is child_from(minid is 1) on id = child.id",
         [1, 2],
      ],
   ])("is denied (%s)", async (_label, target, extendBody, groups) => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            `run: ${target} extend { ${extendBody} }${IDS}`,
            { GROUPS: groups },
         );
      });
   });
});

describe("filter binding guard — shapes that leave the join's inputs alone still serve", () => {
   it("a model-declared extend of the gated source", async () => {
      await withModel(async (model) => {
         expect(
            await rows(model, `run: gated_plain_ext${IDS}`, { GROUPS: [1] }),
         ).toEqual([{ id: 1 }, { id: 2 }]);
      });
   });

   it("a caller extend off the join's path", async () => {
      await withModel(async (model) => {
         expect(
            await rows(
               model,
               `run: gated_plain extend { dimension: doubled is id * 2; rename: org is org_id }${IDS}`,
               { GROUPS: [1] },
            ),
         ).toEqual([{ id: 1 }, { id: 2 }]);
      });
   });

   it("a query-level where: on the joined field", async () => {
      await withModel(async (model) => {
         expect(
            await rows(
               model,
               "run: gated_plain -> { where: child.id = 2; group_by: id }",
               { GROUPS: [1] },
            ),
         ).toEqual([{ id: 2 }]);
      });
   });

   it("a joined source whose own where: reads a given", async () => {
      await withModel(
         async (model) => {
            expect(
               await rows(model, `run: gated_min${IDS}`, {
                  GROUPS: [1, 2],
                  MINID: 2,
               }),
            ).toEqual([{ id: 2 }, { id: 3 }, { id: 4 }]);
         },
         MODEL_TEXT.replace(
            "  GROUPS :: number[]\n",
            "  GROUPS :: number[]\n  MINID :: number\n",
         ) +
            `
source: child_min is duckdb.table('childtable') extend {
   primary_key: id
   where: id >= $MINID
}
#(access_filter) child.org_id in $GROUPS
source: gated_min is duckdb.table('orgtable') extend {
   join_one: child is child_min on id = child.id
   measure: n is count()
}
`,
      );
   });

   it("a notebook cell over a gated source whose joined source has its own where:", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fbg-join-on-nb-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

source: child_active is duckdb.table('childtable') extend {
   primary_key: id
   where: active
}

#(access_filter) child.org_id in $GROUPS
source: gated_active is duckdb.table('orgtable') extend {
   join_one: child is child_active on id = child.id
   measure: n is count()
}

>>>malloy
run: gated_active -> { group_by: id, child.org_id; order_by: id }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const result = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1, 2],
         });
         const served = (
            JSON.parse(result.result!) as { data?: { array_value?: unknown[] } }
         ).data?.array_value?.length;
         expect(served).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
