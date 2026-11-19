// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which sources an access decision reads: every dependency, at any depth, of
 * a definition that carries a gate or reads a given, by every edge a compiled
 * definition records (extends, `->`, joins, composite members).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { collectAccessReads } from "./access_closure";

let connection: DuckDBConnection;
let runtime: SingleConnectionRuntime;

beforeAll(() => {
   connection = new DuckDBConnection("duckdb", ":memory:");
   runtime = new SingleConnectionRuntime({ connection });
});

afterAll(async () => {
   await connection.close();
});

/** The names (before `@`) collectAccessReads finds in `text`. */
async function accessReads(text: string): Promise<string[]> {
   const model = await runtime.loadModel(text).getModel();
   const out = new Set<string>();
   collectAccessReads(
      (model as unknown as { _modelDef: { contents: unknown } })._modelDef
         .contents,
      out,
   );
   return [...out].map((id) => id.split("@")[0]).sort();
}

const HEAD = `##! experimental { givens composite_sources }
given: USER :: number
source: raw is duckdb.sql("SELECT 10 AS id, 1 AS user_id")
source: base is raw -> { select: * }
source: other is raw -> { select: id }
`;

describe("collectAccessReads", () => {
   it("follows an extension, a -> derivation, a join, a composite and the chain behind each", async () => {
      expect(
         await accessReads(`${HEAD}
source: mid is base extend {}
#(access_filter) user_id = $USER
source: by_extend is mid extend {}
`),
      ).toEqual(expect.arrayContaining(["base", "mid"]));
      expect(
         await accessReads(`${HEAD}
#(access_filter) user_id = $USER
source: by_query is base -> { select: * }
`),
      ).toEqual(expect.arrayContaining(["base"]));
      expect(
         await accessReads(`${HEAD}
#(access_filter) b.user_id = $USER
source: by_join is raw extend { join_one: b is base on b.id = id }
`),
      ).toEqual(expect.arrayContaining(["base"]));
      expect(
         await accessReads(`${HEAD}
source: c is compose(base extend { dimension: k is 1 }, base)
source: by_composite is c extend { where: user_id = $USER }
`),
      ).toEqual(expect.arrayContaining(["base"]));
   });

   it("counts a source that reads a given, gated or not", async () => {
      expect(
         await accessReads(`${HEAD}
source: by_given is raw extend {
  join_one: b is base on b.id = id
  dimension: mine is b.user_id = $USER
}
`),
      ).toEqual(expect.arrayContaining(["base"]));
   });

   it("names nothing no access decision reads", async () => {
      const reads = await accessReads(`${HEAD}
source: plain is other extend { dimension: k is 1 }
`);
      expect(reads).not.toContain("other");
      expect(reads).not.toContain("base");
   });
});
