// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package compiled by the real Malloy compiler whose joins exercise what
 * get_context's join handling has to get right: one source reachable from two
 * roots, a source joined twice under different names (role-playing), and a
 * chain three joins deep. The SourceInfos and the ModelDef come from the
 * compiler, not from hand-written stand-ins, so a test built on it checks the
 * IR the server actually reads.
 *
 * Join graph (arrow = join, written alias):
 *
 *    ord --buyer--> cust --origin--> country
 *    ord --seller-> cust
 *    ord --region_r--> reg --c--> cust
 *    inv --customer--> cust
 *
 * From ord, `country_code` is reached as buyer.origin, seller.origin and
 * region_r.c.origin, the last one three joins deep.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   modelDefToModelInfo,
   Runtime,
   type Connection,
   type ModelDef,
} from "@malloydata/malloy";

export const JOIN_FIXTURE_MODEL_PATH = "m.malloy";

const ROOT = "file:///join-fixture/";

const MODEL_TEXT = `
source: country is duckdb.sql("select 1 as id, 'US' as code") extend {
  #(doc) ISO code of the country.
  dimension: country_code is code
}

source: cust is duckdb.sql("select 1 as id, 1 as country_id, 'a' as nm") extend {
  #(doc) Name of the customer.
  dimension: name is nm
  #(doc) Number of customers.
  measure: customer_count is count()
  join_one: origin is country on origin.id = country_id
}

source: reg is duckdb.sql("select 1 as id, 1 as cust_id, 'r' as rn") extend {
  #(doc) Sales region.
  dimension: region is rn
  join_many: c is cust on c.id = cust_id
}

source: ord is duckdb.sql("select 1 as id, 1 as buyer_id, 1 as seller_id, 1 as reg_id, 's' as st") extend {
  #(doc) Order status.
  dimension: status is st
  #(doc) Total revenue.
  measure: revenue is count()
  join_one: buyer is cust on buyer.id = buyer_id
  join_one: seller is cust on seller.id = seller_id
  join_one: region_r is reg on region_r.id = reg_id
}

source: inv is duckdb.sql("select 1 as id, 1 as cust_id, 'i' as rf") extend {
  #(doc) Invoice reference.
  dimension: ref is rf
  join_one: customer is cust on customer.id = cust_id
}
`;

/** Compile the fixture and return a Package-shaped stand-in over it. */
export async function compileJoinFixture(
   modelPath: string = JOIN_FIXTURE_MODEL_PATH,
): Promise<{ pkg: unknown; modelDef: ModelDef }> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map([[`${ROOT}${modelPath}`, MODEL_TEXT]]),
      ),
      connections: new FixedConnectionMap(
         new Map<string, Connection>([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const compiled = await runtime
      .loadModel(new URL(`${ROOT}${modelPath}`), {
         importBaseURL: new URL(ROOT),
      })
      .getModel();
   const modelDef = (compiled as unknown as { _modelDef: ModelDef })._modelDef;
   const sourceInfos = modelDefToModelInfo(modelDef).entries.filter(
      (entry) => entry.kind === "source",
   );
   const model = {
      getSourceInfos: () => sourceInfos,
      getQueries: () => [],
      getModelDef: () => modelDef,
   };
   await duckdb.close();
   return {
      pkg: {
         listModels: async () => [{ path: modelPath }],
         getModel: (path: string) => (path === modelPath ? model : undefined),
      },
      modelDef,
   };
}

/**
 * Compile several model files with the real compiler and return a
 * Package-shaped stand-in that serves all of them. For a test that needs the
 * same source name defined in two files, or a join the single fixture does
 * not have.
 */
export async function compileModelFiles(
   files: Record<string, string>,
): Promise<{ pkg: unknown; modelDefs: Record<string, ModelDef> }> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map(
            Object.entries(files).map(([path, text]) => [
               `${ROOT}${path}`,
               text,
            ]),
         ),
      ),
      connections: new FixedConnectionMap(
         new Map<string, Connection>([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const models = new Map<
      string,
      {
         getSourceInfos: () => unknown[];
         getQueries: () => never[];
         getModelDef: () => ModelDef;
      }
   >();
   const modelDefs: Record<string, ModelDef> = {};
   for (const path of Object.keys(files)) {
      const compiled = await runtime
         .loadModel(new URL(`${ROOT}${path}`), {
            importBaseURL: new URL(ROOT),
         })
         .getModel();
      const modelDef = (compiled as unknown as { _modelDef: ModelDef })
         ._modelDef;
      const sourceInfos = modelDefToModelInfo(modelDef).entries.filter(
         (entry) => entry.kind === "source",
      );
      modelDefs[path] = modelDef;
      models.set(path, {
         getSourceInfos: () => sourceInfos,
         getQueries: () => [],
         getModelDef: () => modelDef,
      });
   }
   await duckdb.close();
   return {
      pkg: {
         listModels: async () => Object.keys(files).map((path) => ({ path })),
         getModel: (path: string) => models.get(path),
      },
      modelDefs,
   };
}

/** An EnvironmentStore stand-in that serves one package. */
export function storeServing(
   pkg: unknown,
   extra: Record<string, unknown> = {},
): never {
   return {
      getEnvironment: async () => ({
         getPackage: async () => pkg,
         getStaleCompileErrors: () => new Map(),
      }),
      ...extra,
   } as never;
}
