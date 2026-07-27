import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { materializationConfigWarnings } from "./materialization_config_validation";

const source = (
   name: string,
   queryMetadata: Record<string, string> | null,
): components["schemas"]["PersistSourcePlan"] => ({
   name,
   sourceID: `${name}@m`,
   connectionName: "duckdb",
   sourceEntityId: "bid",
   sql: "SELECT 1",
   columns: [],
   queryMetadata,
});

describe("materializationConfigWarnings", () => {
   it("says nothing about a clean config", () => {
      expect(
         materializationConfigWarnings({
            packageMaterialization: {
               schedule: null,
               freshness: null,
               queryMetadata: { team: "finance" },
            },
            sources: [source("orders", { team: "finance" })],
         }),
      ).toEqual([]);
   });

   it("passes manifest deprecations through", () => {
      expect(
         materializationConfigWarnings({
            manifestWarnings: ['"scope" at the manifest root is deprecated'],
         }),
      ).toEqual([{ message: '"scope" at the manifest root is deprecated' }]);
   });

   it("reports a package-level property that violates the contract", () => {
      const warnings = materializationConfigWarnings({
         packageMaterialization: {
            schedule: null,
            freshness: null,
            queryMetadata: { "team.name": "finance" },
         },
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].message).toContain("materialization.queryMetadata");
      expect(warnings[0].message).toContain("team.name");
      expect(warnings[0].target).toBeUndefined();
   });

   it("reports the offending property at the source that resolves it", () => {
      const warnings = materializationConfigWarnings({
         sources: [source("orders", { "team.name": "finance" })],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].target).toBe("orders");
      expect(warnings[0].message).toContain("#@ persist queryMetadata");
   });

   it("reports a BigQuery-dropped name as a warning, not silence", () => {
      const warnings = materializationConfigWarnings({
         sources: [source("orders", { _team: "finance" })],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].message).toMatch(/BigQuery/);
   });

   it("reports the problem at every level that resolves it", () => {
      // The package declares it and both sources inherit it. Reporting each
      // level is what lets an author find the one they can edit; identical
      // messages are collapsed, different levels are not.
      const warnings = materializationConfigWarnings({
         packageMaterialization: {
            schedule: null,
            freshness: null,
            queryMetadata: { "team.name": "finance" },
         },
         sources: [
            source("orders", { "team.name": "finance" }),
            source("returns", { "team.name": "finance" }),
         ],
      });
      expect(warnings).toHaveLength(3);
      expect(warnings.map((w) => w.target)).toEqual([
         undefined,
         "orders",
         "returns",
      ]);
   });

   it("ignores sources with no metadata", () => {
      expect(
         materializationConfigWarnings({ sources: [source("orders", null)] }),
      ).toEqual([]);
   });
});
