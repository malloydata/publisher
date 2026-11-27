// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The text step of a served copy reading a managed connection's tables
 * through a storage destination over the same catalog: which paths
 * it places, as the managed connection resolves them, and what it refuses to
 * move. The end-to-end case is storage_same_catalog_join.component.spec.ts.
 */

import { MalloyConfig } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { MAIN_ONLY, managedSchemas } from "../../connection_type";
import {
   destinationTablePath,
   readThroughDestination,
} from "./same_catalog_reads";
import { servedCopyAlias, servedCopyConfig } from "./served_copy_config";

const MOVE = {
   connection: "credible",
   alias: "__alias",
   destination: "credible",
   schemas: managedSchemas(["stripe"]),
};

describe("a managed connection's table path, read on the destination", () => {
   it.each([
      ["customers", "credible.main.customers"],
      ["main.customers", "credible.main.customers"],
      ["MAIN.customers", "credible.MAIN.customers"],
      ["credible.customers", "credible.main.customers"],
      ["credible.main.customers", "credible.main.customers"],
      ["stripe.charges", "credible.stripe.charges"],
      ["credible.stripe.charges", "credible.stripe.charges"],
   ])("places %s as %s", (path, placed) => {
      expect(destinationTablePath(path, MOVE)).toBe(placed);
   });

   it.each([
      ["a schema it may not read", "materialized.kept"],
      ["another catalog", "other.main.t"],
      ["a quoted name", '"main".customers'],
      ["a file", "/data/x.parquet"],
      ["four parts", "credible.main.x.y"],
   ])("places no path naming %s", (_what, path) => {
      expect(destinationTablePath(path, MOVE)).toBe(undefined);
   });

   it("places no `<connection>.t` when a schema of that name makes it ambiguous", () => {
      expect(
         destinationTablePath("credible.t", {
            ...MOVE,
            schemas: managedSchemas(["credible"]),
         }),
      ).toBe(undefined);
   });

   it("qualifies with the destination's name, not the connection's", () => {
      expect(
         destinationTablePath("customers", {
            connection: "crm",
            destination: "lake",
            schemas: MAIN_ONLY,
         }),
      ).toBe("lake.main.customers");
      expect(
         destinationTablePath("crm.customers", {
            connection: "crm",
            destination: "lake",
            schemas: MAIN_ONLY,
         }),
      ).toBe("lake.main.customers");
   });
});

describe("a copy's text, its connection's tables read through the destination", () => {
   it("rewrites each .table() on the connection, and nothing else", () => {
      const text = [
         "source: customer is credible.table('customers') extend {",
         "  primary_key: customer_id",
         "  dimension: credible_name is name",
         "}",
         'source: charges is `credible`.table("stripe.charges")',
         "source: other is wh.table('public.t')",
         "source: k is __alias.virtual('h')::k_t extend { join_one: customer with customer_id }",
      ].join("\n");
      const read = readThroughDestination(text, MOVE);
      expect(read?.paths).toEqual(["customers", "stripe.charges"]);
      expect(read?.text).toBe(
         [
            "source: customer is __alias.table('credible.main.customers') extend {",
            "  primary_key: customer_id",
            "  dimension: credible_name is name",
            "}",
            "source: charges is __alias.table('credible.stripe.charges')",
            "source: other is wh.table('public.t')",
            "source: k is __alias.virtual('h')::k_t extend { join_one: customer with customer_id }",
         ].join("\n"),
      );
   });

   it("leaves a text that names no table on the connection as it is", () => {
      const text = "source: o is wh.table('public.t')\n";
      expect(readThroughDestination(text, MOVE)).toEqual({ text, paths: [] });
   });

   it.each([
      ["an .sql() source", `source: s is credible.sql("SELECT 1 AS n")`],
      ["a virtual() source", "source: s is credible.virtual('h')::t"],
      [
         "a path it cannot place",
         "source: s is credible.table('materialized.t')",
      ],
      ["a literal with an escape", "source: s is credible.table('ma\\'in.t')"],
      ["a path built some other way", "source: s is credible.table(x)"],
   ])("moves nothing when the connection is named in %s", (_what, text) => {
      expect(readThroughDestination(text, MOVE)).toBe(undefined);
   });
});

describe("a destination looked up under its alias", () => {
   it("names the alias, and so does every table schema it fetches", async () => {
      const alias = servedCopyAlias("same-catalog-spec-dest");
      const destination = {
         name: "same-catalog-spec-dest",
         dialectName: "duckdb",
         calls: 0,
         fetchSchemaForTables(tables: Record<string, string>) {
            this.calls++;
            return Promise.resolve({
               schemas: Object.fromEntries(
                  Object.entries(tables).map(([key, tablePath]) => [
                     key,
                     {
                        type: "table",
                        name: key,
                        tablePath,
                        connection: this.name,
                        dialect: "duckdb",
                        fields: [],
                     },
                  ]),
               ),
               errors: {},
            });
         },
      };
      const destinations = new MalloyConfig({ connections: {} });
      destinations.wrapConnections(() => ({
         lookupConnection: async () => destination as never,
      }));
      const allowed = new Set(["same-catalog-spec-dest"]);
      const config = (underAlias: ReadonlySet<string>) =>
         servedCopyConfig({} as never, destinations, allowed, underAlias);

      // As before, without it: the connection itself.
      expect(await config(new Set()).connections.lookupConnection(alias)).toBe(
         destination as never,
      );
      const view = await config(allowed).connections.lookupConnection(alias);
      expect(view.name).toBe(alias);
      // One view per alias, so a lookup is stable.
      expect(await config(allowed).connections.lookupConnection(alias)).toBe(
         view,
      );
      const fetched = await view.fetchSchemaForTables(
         { k: "same-catalog-spec-dest.main.t" },
         {},
      );
      expect(fetched.schemas.k.connection).toBe(alias);
      expect(fetched.schemas.k.tablePath).toBe("same-catalog-spec-dest.main.t");
      // Called on the connection itself, which keeps its own name.
      expect(destination.calls).toBe(1);
      expect(destination.name).toBe("same-catalog-spec-dest");
   });
});
