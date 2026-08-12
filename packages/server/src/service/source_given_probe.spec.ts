// Contract test for where a *source-level* `where:` referencing a given ends up
// in the compiled model, and for `readDashboardModelFacts` reading it.
//
// Scoping a source once — `source: scoped is orders extend { where: category ~
// $CATEGORY }` — is the idiomatic way to apply a given to everything built on
// it, and it is what lets a composite dashboard have a control row at all: a
// composite has no query of its own, so the filtering it applies lives in what
// it composes (docs/malloyyo-dashboards-design.md). The reference lands in the source's
// `filterList`, *not* in any view's pipeline, so a walk of the pipeline alone
// misses it — and the symptom is silent: the dashboard renders filtered results
// with no control to change them. Pinned here against the real compiler, since
// a Malloy change to where that filter lives would reintroduce exactly that.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import type { ModelDef } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";

import { readDashboardModelFacts } from "./dashboard";

const ROOT = "file:///probe/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

const MODEL = `
##! experimental.givens

given: CATEGORY :: filter<string> is f''
given: BRAND :: filter<string> is f''

source: orders is duckdb.sql("""
  select 'Tops' as category, 'Nike' as brand, 10 as amount
""") extend {
  measure: total_amount is sum(amount)

  view: by_brand is {
    group_by: brand
    aggregate: total_amount
  }
}

// The given is applied once, on the source. Every view below inherits it.
source: scoped is orders extend {
  where: category ~ $CATEGORY

  view: by_category is {
    group_by: category
    aggregate: total_amount
  }

  // Adds a second given in the view itself, so the two collection paths have
  // to union rather than either one winning.
  view: filtered_brands is {
    where: brand ~ $BRAND
    group_by: brand
    aggregate: total_amount
  }
}
`;

async function facts() {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, MODEL]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const model = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const def = (model as any)._modelDef as ModelDef;
   return readDashboardModelFacts("m.malloy", def, ["CATEGORY", "BRAND"]);
}

describe("givens applied on a source reach that source's views", () => {
   it("credits a source-level where: to every view of the source", async () => {
      const { viewGivens } = await facts();
      expect(viewGivens.get("scoped -> by_category")).toEqual(["CATEGORY"]);
   });

   it("unions the source's givens with the view's own", async () => {
      const { viewGivens } = await facts();
      expect(
         [...(viewGivens.get("scoped -> filtered_brands") ?? [])].sort(),
      ).toEqual(["BRAND", "CATEGORY"]);
   });

   it("leaves an unscoped source's views alone", async () => {
      const { viewGivens } = await facts();
      expect(viewGivens.get("orders -> by_brand")).toEqual([]);
   });
});
