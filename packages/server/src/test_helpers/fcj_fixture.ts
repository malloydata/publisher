// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package compiled by the real Malloy compiler for the find, connect, judge
 * engine's tests. It holds the cases the engine has to get right:
 *
 * - three `start_date` fields in three islands: races, orders, flights (no
 *   join between them);
 * - "flights to Denver": `cities.city` stores "Denver" exactly, but no flights
 *   measure is in its reach; `flights.arrives_at` stores
 *   "Denver International (DIA)" beside the measure;
 * - a question with three measures, one per island;
 * - a twin in two model files: `orders` is defined in shop.malloy and resolves
 *   in reports.malloy too, which imports it;
 * - an extension that hides a field: `orders_public` is `orders` without
 *   `customer_email`;
 * - a dependent measure: `net_sales is sales { where: is_fulfilled }`;
 * - role-played joins (flights joins airports as origin and as arrival) and a
 *   chain three joins deep (legs -> flights -> airports -> regions).
 */

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import type { PackageIndex } from "../mcp/tools/get_context_tool";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   type PackageRetrievalSettings,
} from "../service/package_retrieval";
import { getPackageIndex } from "../mcp/tools/get_context_tool";
import {
   buildCatalog,
   catalogInputOf,
   type Catalog,
} from "../mcp/context/catalog";
import { compileModelFiles, storeServing } from "./get_context_join_fixture";

export const FCJ_FILES: Record<string, string> = {
   "shop.malloy": `
#(doc) One row per order placed in the web shop.
source: orders is duckdb.sql("select 1 as id, 10.0 as amount, 'fulfilled' as status, date '2024-01-01' as start_date, 'a@b.c' as customer_email") extend {
  #(doc) Order amount in dollars. Exclude test orders when summing.
  measure: sales is amount.sum()
  #(doc) The order has shipped.
  dimension: is_fulfilled is status = 'fulfilled'
  measure: net_sales is sales { where: is_fulfilled }
  view: fulfilled_sales is { group_by: is_fulfilled aggregate: net_sales }
}

#(doc) Orders without personal data.
source: orders_public is orders extend {
  except: customer_email
  #(doc) The order came through a reseller.
  dimension: is_reseller is status = 'reseller'
}
`,
   "reports.malloy": `
import "shop.malloy"

#(doc) One row per running race.
source: races is duckdb.sql("select 1 as id, date '2024-01-01' as start_date, 'Boston' as city") extend {
  #(doc) Number of races.
  measure: race_count is count()
}
`,
   "travel.malloy": `
#(doc) One row per region.
source: regions is duckdb.sql("select 1 as id, 'Mountain' as name") extend {
  dimension: region_name is name
}

#(doc) One row per airport.
source: airports is duckdb.sql("select 'DIA' as code, 'Denver International' as airport_name, 1 as region_id") extend {
  join_one: region is regions on region.id = region_id
}

#(doc) One row per city in the gazetteer.
source: cities is duckdb.sql("select 1 as id, 'Denver' as city_name, 'CO' as state") extend {
  #(index)
  #(doc) City name.
  dimension: city is city_name
  #(doc) Number of cities.
  measure: city_count is count()
}

#(doc) One row per scheduled flight.
source: flights is duckdb.sql("select 1 as id, 'Denver International (DIA)' as destination, 'DIA' as origin_code, 'DIA' as arrival_code, date '2024-01-01' as start_date") extend {
  #(index)
  #(doc) Airport the flight lands at.
  dimension: arrives_at is destination
  #(doc) Number of flights.
  measure: flight_count is count()
  join_one: origin is airports on origin.code = origin_code
  join_one: arrival is airports on arrival.code = arrival_code
}

#(doc) One row per leg of a multi-leg trip.
source: legs is duckdb.sql("select 1 as id, 1 as flight_id") extend {
  join_one: flight is flights on flight.id = flight_id
}
`,
};

/**
 * The shape of a fee question over payments: a fact table (`payments`, one
 * row per payment) that reaches a dimension table (`merchants`, one row per
 * merchant) through the join `merchant_profile`. Both are documented, so a
 * card that carries the wrong one's doc says the wrong grain.
 *
 * The joins the catalog keeps as fields of their own: `networks`, a SQL table
 * with a documented field added in the join, and `labelled`, which adds a
 * field to `merchants` (`merchants extend {...}`). And two extensions of
 * `payments`: `eur_payments` documents itself, `plain_payments` does not (the
 * compiler then hands it its parent's notes as its own).
 */
export const FCJ_PAYMENTS_FILES: Record<string, string> = {
   "payments.malloy": `
#(doc) One row per merchant, 30 in all, carrying the account characteristics.
source: merchants is duckdb.sql("select 'm1' as merchant, 'R' as account_type") extend {
  #(doc) Account type code: R retail, D digital.
  dimension: account_kind is account_type
}

#(doc) One row per payment transaction.
source: payments is duckdb.sql("select 'm1' as merchant, 10.0 as eur_amount, 'VISA' as scheme") extend {
  #(doc) The merchant's account profile.
  join_one: merchant_profile is merchants on merchant_profile.merchant = merchant
  #(doc) Card networks, one row per network. Join on the scheme code only.
  join_one: networks is duckdb.sql("select 'VISA' as code, 'Visa' as label") extend {
    #(doc) Network name as printed on the card.
    dimension: network_name is label
  } on networks.code = scheme
  #(doc) Merchants with a display name.
  join_one: labelled is merchants extend {
    #(doc) Merchant name in capitals.
    dimension: loud_name is upper(merchant)
  } on labelled.merchant = merchant
  #(doc) Total transaction value in euros.
  measure: total_volume_eur is eur_amount.sum()
}

#(doc) Payments settled in euros. One row per payment.
source: eur_payments is payments extend {
  #(doc) Number of euro payments.
  measure: eur_count is count()
}

source: plain_payments is payments extend {
  dimension: is_large is eur_amount > 100
}
`,
};

/**
 * The shape of a list-mode loss: the best candidate for "total revenue" sits
 * on a narrow derived source (`campaign_summary`, one row per campaign) and
 * the field the question wants on the main one (`sales_mix`, which reaches
 * `venues` through the join `venue`). No join links the two.
 */
export const FCJ_PEERS_FILES: Record<string, string> = {
   "bars.malloy": `
#(doc) One row per venue.
source: venues is duckdb.sql("select 1 as id, 'Denver' as city_name") extend {
  #(doc) City the venue is in.
  dimension: venue_city is city_name
}

#(doc) One row per item line sold, at every venue on every day.
source: sales_mix is duckdb.sql("select 1 as id, 1 as venue_id, 12.0 as amount") extend {
  #(doc) Revenue of the item lines sold.
  measure: total_revenue is amount.sum()
  join_one: venue is venues on venue.id = venue_id
}

#(doc) One row per campaign, over its own window only.
source: campaign_summary is duckdb.sql("select 1 as id, 3.0 as lift") extend {
  #(doc) Total revenue lift of the campaign.
  measure: total_revenue_lift is lift.sum()
}
`,
};

/** A throwaway DuckDB file, with `create` run on it, and its cleanup. */
export async function openTempDb(
   create: (db: DuckDBConnection) => Promise<void>,
): Promise<{ db: DuckDBConnection; close: () => Promise<void> }> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fcj-spec-"));
   const db = new DuckDBConnection(path.join(dir, "test.db"));
   await db.initialize();
   await create(db);
   return {
      db,
      close: async () => {
         await db.close();
         fs.rmSync(dir, { recursive: true, force: true });
      },
   };
}

export const FCJ_ENVIRONMENT = "env";
export const FCJ_PACKAGE = "fixture";

export interface FcjFixture {
   /** The Package stand-in; `retrieval.engine` is `fcj` unless the caller says otherwise. */
   pkg: unknown;
   pkgIndex: PackageIndex;
   catalog: Catalog;
}

/**
 * Sources a served model resolves because it imports them. The compiler
 * fixture lists only the sources a file defines; Publisher's own models list
 * the imported ones too (that is why one source can be a card per file), so
 * the fixture adds them the same way.
 */
export const FCJ_IMPORTS: Record<
   string,
   Array<{ from: string; source: string }>
> = {
   "reports.malloy": [{ from: "shop.malloy", source: "orders" }],
};

type ModelStandIn = {
   getSourceInfos: () => Array<{ name?: string }>;
   getQueries: () => never[];
   getModelDef: () => unknown;
};

/** Compile the fixture, build the shipped index over it, and the catalog from that. */
export async function compileFcjFixture(
   files: Record<string, string> = FCJ_FILES,
   imports: Record<
      string,
      Array<{ from: string; source: string }>
   > = FCJ_IMPORTS,
   /** What the package's publisher.json `retrieval` block resolves to. */
   retrieval: PackageRetrievalSettings = {
      ...DEFAULT_PACKAGE_RETRIEVAL,
      engine: "fcj",
   },
): Promise<FcjFixture> {
   const compiled = await compileModelFiles(files);
   const inner = compiled.pkg as {
      listModels: () => Promise<Array<{ path: string }>>;
      getModel: (path: string) => ModelStandIn | undefined;
   };
   const pkg = {
      getPackageName: () => FCJ_PACKAGE,
      getRetrievalSettings: () => retrieval,
      listModels: inner.listModels,
      getModel: (path: string) => {
         const model = inner.getModel(path);
         const extra = imports[path] ?? [];
         if (!model || extra.length === 0) return model;
         return {
            ...model,
            getSourceInfos: () => [
               ...model.getSourceInfos(),
               ...extra.flatMap(
                  ({ from, source }) =>
                     inner
                        .getModel(from)
                        ?.getSourceInfos()
                        .filter((s) => s.name === source) ?? [],
               ),
            ],
         };
      },
   };
   const pkgIndex = await getPackageIndex(
      storeServing(pkg),
      FCJ_ENVIRONMENT,
      FCJ_PACKAGE,
   );
   const catalog = buildCatalog(await catalogInputOf(pkgIndex));
   return { pkg, pkgIndex, catalog };
}
