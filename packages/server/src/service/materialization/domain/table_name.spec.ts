// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   tableNamesMeet,
   authorModelPath,
   COMPANION_SUFFIXES,
   fileSegment,
   fitPrefix,
   fitReadable,
   leafBudget,
   namesBefore,
   readableSegment,
   readableTableName,
   readableVersion,
   sourceLabel,
   type TableIdentity,
} from "./table_name";

/**
 * The control plane's prefix of a readable name, as its own naming builds it:
 * the hosted tail is `__g<3 digits>__<12 hex>`, 20 characters, after an
 * optional `__v<ver>`, and the readable parts are fitted to what the budget
 * leaves. Only the prefix is shared; each side's tail is its own.
 */
function controlPlanePrefix(c: {
   pkg: string;
   modelPath: string;
   label: string;
   version?: string;
   dialect: string | undefined;
   inDestination?: boolean;
}): string {
   const scope =
      c.version === undefined ? "" : `__${readableVersion(c.version)}`;
   const room = Math.max(
      1,
      leafBudget(c.dialect, c.inDestination ?? false) - (scope.length + 20),
   );
   return (
      fitReadable(
         readableSegment(c.pkg),
         fileSegment(c.modelPath),
         readableSegment(c.label),
         room,
      ) + scope
   );
}

/**
 * The shared prefix, conformed byte for byte to the control plane's worked
 * examples (its readable table-name test, which pins the examples both sides
 * agreed on). Each row is that test's input and the name it pins, less the
 * hosted tail. A change here that the control plane does not make too splits
 * how one table reads in the two deployments.
 */
const CONFORMANCE: {
   name: string;
   pkg: string;
   modelPath: string;
   label: string;
   version?: string;
   dialect: string | undefined;
   inDestination?: boolean;
   prefix: string;
}[] = [
   {
      name: "normal Postgres keeps the whole source and cuts the package",
      pkg: "ecommerce",
      modelPath: "models/orders.malloy",
      label: "orders_summary",
      dialect: "postgres",
      prefix: "ecomme__orders__orders_summary",
   },
   {
      name: "a generous dialect cuts nothing",
      pkg: "ecommerce",
      modelPath: "models/orders.malloy",
      label: "orders_summary",
      dialect: "snowflake",
      prefix: "ecommerce__orders__orders_summary",
   },
   {
      name: "a deep path and a long source on Postgres",
      pkg: "finance_reporting",
      modelPath: "models/finance/q4/revenue_recognition.malloy",
      label: "revenue_by_region_and_product_line",
      dialect: "postgres",
      prefix: "financ__revenu__revenue_by_reg",
   },
   {
      name: "BigQuery keeps every part (a container is outside the budget)",
      pkg: "finance_reporting",
      modelPath: "models/finance/q4/revenue_recognition.malloy",
      label: "revenue_by_region_and_product_line",
      dialect: "standardsql",
      prefix:
         "finance_reporting__revenue_recognition__revenue_by_region_and_product_line",
   },
   {
      name: "a version shows its release digits",
      pkg: "ecommerce",
      modelPath: "orders.malloy",
      label: "orders",
      version: "1.2.0",
      dialect: "postgres",
      prefix: "ecomme__orders__orders__v1_2_0",
   },
   {
      name: "a prerelease or build suffix is not shown",
      pkg: "ecommerce",
      modelPath: "orders.malloy",
      label: "orders",
      version: "1.2.0-rc.1+b7",
      dialect: "postgres",
      prefix: "ecomme__orders__orders__v1_2_0",
   },
   {
      name: "a rollup reads base_by_grain",
      pkg: "ecommerce",
      modelPath: "orders.malloy",
      label: sourceLabel("orders__rollup__category__10880159", {
         baseSourceName: "orders",
         grainDimensions: ["category"],
      }),
      dialect: "postgres",
      prefix: "ecomme__orders__orders_by_cate",
   },
   {
      name: "awkward characters and a reserved word",
      pkg: "Sales-Ops",
      modelPath: "Q3 Plan/Big Model.v2.malloy",
      label: "`select`",
      dialect: "postgres",
      prefix: "sales_ops__big_model_v__select",
   },
   {
      name: "a storage destination takes the generous budget",
      pkg: "ecommerce",
      modelPath: "models/orders.malloy",
      label: "orders_summary",
      dialect: "postgres",
      inDestination: true,
      prefix: "ecommerce__orders__orders_summary",
   },
   {
      name: "a segment starting with a digit takes a t",
      pkg: "2024_metrics",
      modelPath: "kpis.malloy",
      label: "revenue",
      dialect: "postgres",
      prefix: "t2024_metrics__kpis__revenue",
   },
];

describe("the prefix shared with the control plane", () => {
   for (const row of CONFORMANCE) {
      it(row.name, () => {
         expect(controlPlanePrefix(row)).toBe(row.prefix);
      });
   }

   it("a name= sets the prefix: its last segment, sanitized, then cut to the room", () => {
      // The control plane's room for a 1.2.0 version on Postgres: 50 less
      // `__v1_2_0` and its 20-character tail.
      expect(fitPrefix("summary_t", 22)).toBe("summary_t");
      expect(fitPrefix("Big-Orders_Summary_Daily_Rollup_Table", 22)).toBe(
         "big_orders_summary_dai",
      );
      // A cut that ends on `_` drops it.
      expect(fitPrefix("orders_summary_x", 7)).toBe("orders");
      expect(fitPrefix("2024 Totals", 50)).toBe("t2024_totals");
   });

   it("a persisted view reads source_view", () => {
      expect(sourceLabel("orders__by_month__view")).toBe("orders__by_month");
      expect(readableSegment("orders__by_month")).toBe("orders_by_month");
   });

   it("a persisted named query reads as the query; any other name ending __query keeps it", () => {
      expect(
         sourceLabel("top__query", undefined, { persistedQuery: true }),
      ).toBe("top");
      expect(
         sourceLabel("top_query__query", undefined, { persistedQuery: true }),
      ).toBe("top_query");
      expect(sourceLabel("mine__query")).toBe("mine__query");
   });

   it("budgets by dialect", () => {
      for (const d of ["postgres", "mysql", "trino", "presto", undefined]) {
         expect(leafBudget(d, false)).toBe(50);
      }
      expect(leafBudget("sqlserver", false)).toBe(115);
      expect(leafBudget("DuckDB", false)).toBe(120);
      expect(leafBudget("unknown_dialect", false)).toBe(50);
      for (const d of ["duckdb", "snowflake", "standardsql", "databricks"]) {
         expect(leafBudget(d, false)).toBe(120);
      }
      expect(leafBudget("postgres", true)).toBe(120);
   });

   it("the file segment is the author's base name", () => {
      expect(fileSegment("models/sales/orders.malloy")).toBe("orders");
      expect(fileSegment("orders.malloynb")).toBe("orders");
      expect(fileSegment("models/orders.malloy.persist-view.malloy")).toBe(
         "orders",
      );
      expect(fileSegment("orders.malloy.materialize.malloy")).toBe("orders");
      expect(fileSegment(undefined)).toBe("x");
   });

   it("maps exactly the seven companion suffixes the control plane maps to the author's file", () => {
      // Pinned on both sides: a suffix added here fails until the control
      // plane's list has it too, and the other way round.
      expect([...COMPANION_SUFFIXES]).toEqual([
         ".materialize.malloy",
         ".materialize-build.malloy",
         ".materialize-level.malloy",
         ".persist-view.malloy",
         ".preaggregate.malloy",
         ".view-author.malloy",
         ".rollup-author.malloy",
      ]);
      for (const suffix of COMPANION_SUFFIXES) {
         expect(fileSegment(`/tmp/pkg/models/a.malloy${suffix}`)).toBe("a");
         expect(authorModelPath(`models/a.malloy${suffix}`)).toBe(
            "models/a.malloy",
         );
      }
      // Only a suffix after the author's `.malloy` is a companion's.
      expect(fileSegment("a_preaggregate.malloy")).toBe("a_preaggregate");
      expect(authorModelPath("a_preaggregate.malloy")).toBe(
         "a_preaggregate.malloy",
      );
   });

   it("no segment contains a double underscore", () => {
      expect(readableSegment("__A__b..c__")).toBe("a_b_c");
      expect(readableSegment("…")).toBe("x");
   });

   it("cuts every name of one source alike", () => {
      expect(
         fitReadable(
            "finance_reporting",
            "revenue_recognition",
            "revenue_by_region_and_product_line",
            30,
         ),
      ).toBe("financ__revenu__revenue_by_reg");
   });

   it("the last resort drops file, then package", () => {
      expect(fitReadable("pkgxxxxxxx", "filexxxxxx", "revenue_by", 13)).toBe(
         "pkgx__revenue",
      );
      expect(fitReadable("pkgxxxxxxx", "filexxxxxx", "revenue_by", 8)).toBe(
         "revenue",
      );
   });

   it("a non-semantic version is sanitized and cut to 10", () => {
      expect(readableVersion("2024-11-15-nightly")).toBe("v2024_11_15");
      expect(readableVersion("Beta Two")).toBe("vbeta_two");
   });
});

const identity = (over: Partial<TableIdentity> = {}): TableIdentity => ({
   environmentName: "prod",
   packageName: "ecommerce",
   modelPath: "orders.malloy",
   sourceName: "orders_summary",
   ...over,
});

describe("the standalone name", () => {
   const name = (
      over: Partial<TableIdentity> = {},
      dialect: string | undefined = "postgres",
      extra: {
         container?: string;
         inDestination?: boolean;
         label?: string;
      } = {},
   ) =>
      readableTableName({
         identity: identity(over),
         label: extra.label ?? over.sourceName ?? "orders_summary",
         dialect,
         inDestination: extra.inDestination ?? false,
         ...(extra.container ? { container: extra.container } : {}),
      });

   it("is the shared prefix and 8 hex of the identity's hash", () => {
      expect(name()).toBe("ecommerce__orders__orders_summary__p881bfc66");
      expect(
         name({
            environmentName: "examples",
            packageName: "storefront",
            modelPath: "storefront.malloy",
            sourceName: "order_items",
         }),
      ).toBe("storefront__storefront__order_items__p55ad30e3");
   });

   it("fits the dialect's budget, cutting the package and file first", () => {
      const deep = {
         packageName: "finance_reporting",
         modelPath: "models/finance/q4/revenue_recognition.malloy",
         sourceName: "revenue_by_region",
      };
      const pg = name(deep);
      expect(pg.length).toBeLessThanOrEqual(50);
      expect(pg).toMatch(
         /^finance_r__revenue_r__revenue_by_region__p[0-9a-f]{8}$/,
      );
      expect(name(deep, "standardsql")).toMatch(
         /^finance_reporting__revenue_recognition__revenue_by_region__p[0-9a-f]{8}$/,
      );
   });

   it("shows a scope: version table's release, and hashes the whole version", () => {
      const v = name({ sourceName: "orders", version: "1.2.0" });
      const rc = name({ sourceName: "orders", version: "1.2.0-rc.1" });
      expect(v).toMatch(/^ecommerce__orders__orders__v1_2_0__p[0-9a-f]{8}$/);
      expect(rc).toMatch(/^ecommerce__orders__orders__v1_2_0__p[0-9a-f]{8}$/);
      expect(rc).not.toBe(v);
   });

   it("is placed under its container, which is outside the budget", () => {
      expect(name({}, "standardsql", { container: "analytics" })).toBe(
         `analytics.${name({}, "standardsql")}`,
      );
   });

   it("tells apart what reads alike: same-base-name files, environments, sanitizing aliases", () => {
      const us = name({ modelPath: "us/orders.malloy", sourceName: "orders" });
      const eu = name({ modelPath: "eu/orders.malloy", sourceName: "orders" });
      const staging = name({
         environmentName: "staging",
         modelPath: "us/orders.malloy",
         sourceName: "orders",
      });
      expect(us.replace(/__p.*$/, "")).toBe(eu.replace(/__p.*$/, ""));
      expect(new Set([us, eu, staging]).size).toBe(3);
      expect(name({ packageName: "sales-ops" })).not.toBe(
         name({ packageName: "sales_ops" }),
      );
   });

   it("every name ends in __p<8 hex>, a version's included", () => {
      for (const n of [name(), name({ version: "9.9.9" })]) {
         expect(n).toMatch(/__p[0-9a-f]{8}$/);
      }
   });
});

describe("which source a shared table is named after", () => {
   it("the smallest model path, then the smallest source", () => {
      expect(
         namesBefore(
            { modelPath: "a.malloy", label: "z" },
            { modelPath: "b.malloy", label: "a" },
         ),
      ).toBe(true);
      expect(
         namesBefore(
            { modelPath: "a.malloy", label: "b" },
            { modelPath: "a.malloy", label: "a" },
         ),
      ).toBe(false);
   });
});

describe("tableNamesMeet", () => {
   it("meets on one table segment with the same container, or none on either", () => {
      expect(tableNamesMeet("t", "t")).toBe(true);
      expect(tableNamesMeet("main.t", "t")).toBe(true);
      expect(tableNamesMeet("t", "main.t")).toBe(true);
      expect(tableNamesMeet("a.b.t", "a.b.t")).toBe(true);
      expect(tableNamesMeet("s1.t", "s2.t")).toBe(false);
      expect(tableNamesMeet("main.t", "main.u")).toBe(false);
   });

   it("meets when one name's containers end the other's: a shorter name lands in the connection's defaults", () => {
      expect(tableNamesMeet("proj.ds.t", "ds.t")).toBe(true);
      expect(tableNamesMeet("ds.t", "proj.ds.t")).toBe(true);
      expect(tableNamesMeet("DB.S.T", "S.T")).toBe(true);
      expect(tableNamesMeet("proj.ds.t", "other.t")).toBe(false);
      expect(tableNamesMeet("p1.ds.t", "p2.ds.t")).toBe(false);
   });
});
