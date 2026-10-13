// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
// A gated source joined by an author inside a diamond import: the file that
// declares the gate is compiled once per import path, so the join embeds one
// parse's note while the namespace holds the other's. The misplaced-annotation
// check must read that as Malloy's copy of the gate, not as a tag typed on the
// join line — and must still catch a tag that WAS typed there.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   CacheManager,
   FixedConnectionMap,
   InMemoryModelCache,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { extractSourcesFromModelDef } from "./source_extraction";

const T =
   "duckdb.sql(\"select * from (values (1,'a','US'),(2,'b','EU')) v(id,name,region)\")";
const DIMS = `##! experimental { givens }
given:
  GROUPS :: string[]
#(authorize) false
source: dim_attr is ${T} extend { rename: attr_id is id; primary_key: attr_id; dimension: attr_name is name }
`;
const FACTS = `import "dims.malloy"
##! experimental { givens }
#(access_filter) region in $GROUPS
source: fact_base is ${T} extend { join_one: field is dim_attr on id = field.attr_id }
#(access_filter) region in $GROUPS
source: fact is fact_base -> { select: id, region, nm is field.attr_name } extend { primary_key: id }
`;
const SECURITY = `import "dims.malloy"
import "facts.malloy"
##! experimental { givens }
#(authorize) true
#(access_filter) region in $GROUPS
source: secure_attr is dim_attr extend { join_many: values is fact on attr_id = values.id }
`;
// The genuine mistake inside the diamond: the gate typed on the join line of
// a file that itself declares gated sources, so the file's URL is in the
// own-note set and only the position tells the notes apart.
const FACTS_TYPED = `import "dims.malloy"
##! experimental { givens }
#(access_filter) region in $GROUPS
source: fact_base is ${T} extend {
  #(authorize) false
  join_one: field is dim_attr on id = field.attr_id
}
#(access_filter) region in $GROUPS
source: fact is fact_base -> { select: id, region, nm is field.attr_name } extend { primary_key: id }
`;
// The genuine mistake in a file that declares no gated source of its own.
const TYPED_ON_JOIN = `import "dims.malloy"
##! experimental { givens }
source: joiner is ${T} extend {
  #(authorize) false
  join_one: field is dim_attr on id = field.attr_id
}
`;

let duck: DuckDBConnection;
beforeAll(() => {
   duck = new DuckDBConnection("duckdb", ":memory:");
});
afterAll(async () => {
   await duck.close();
});

async function compile(
   files: Record<string, string>,
   entry: string,
   cached: boolean,
) {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map(Object.entries(files).map(([k, v]) => [`file:///${k}`, v])),
      ),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duck]]),
         "duckdb",
      ),
      ...(cached
         ? { cacheManager: new CacheManager(new InMemoryModelCache()) }
         : {}),
   });
   const model = await runtime
      .loadModel(new URL(`file:///${entry}`))
      .getModel();
   return extractSourcesFromModelDef(model._modelDef, undefined)
      .misplacedAuthorize;
}

describe("a gated source joined inside a diamond import", () => {
   const files = {
      "dims.malloy": DIMS,
      "facts.malloy": FACTS,
      "security.malloy": SECURITY,
   };
   for (const cached of [false, true]) {
      it(`is Malloy's copied gate, not a misplaced annotation (cache ${cached ? "on" : "off"})`, async () => {
         expect(await compile(files, "security.malloy", cached)).toEqual([]);
      });
   }
   it("the joiner's own file still reports nothing", async () => {
      expect(await compile(files, "facts.malloy", false)).toEqual([]);
   });
});

describe("a gate typed on a join line", () => {
   it("inside the diamond, in a file whose URL holds gated sources, is still reported", async () => {
      const found = await compile(
         {
            "dims.malloy": DIMS,
            "facts.malloy": FACTS_TYPED,
            "security.malloy": SECURITY,
         },
         "security.malloy",
         false,
      );
      expect(found).toEqual([
         {
            kind: "field",
            name: "fact_base",
            fieldName: "field",
            route: "authorize",
         },
      ]);
   });

   it("is still reported as misplaced", async () => {
      const found = await compile(
         { "dims.malloy": DIMS, "joiner.malloy": TYPED_ON_JOIN },
         "joiner.malloy",
         false,
      );
      expect(found).toEqual([
         {
            kind: "field",
            name: "joiner",
            fieldName: "field",
            route: "authorize",
         },
      ]);
   });
});
