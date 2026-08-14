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
      expect(warnings[0].subject).toBeUndefined();
   });

   it("reports the offending property at the source that resolves it", () => {
      const warnings = materializationConfigWarnings({
         sources: [source("orders", { "team.name": "finance" })],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].subject).toBe("orders");
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
      expect(warnings.map((w) => w.subject)).toEqual([
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

   it("warns about a reserved name, naming the level that declared it", () => {
      // A context name is dropped rather than attached, so declaring one
      // publishes clean and then vanishes on every statement with nothing but a
      // counter tick behind it. The level is what makes the message actionable.
      const warnings = materializationConfigWarnings({
         packageMaterialization: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            queryMetadata: { model: "marts" } as any,
         },
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].message).toContain("materialization.queryMetadata");
      expect(warnings[0].message).toContain("'model'");
      expect(warnings[0].message).toContain("dropped rather than attached");
   });

   it("checks a model file that persists nothing", () => {
      // The case this level exists for. A `##` declaration in a file with no
      // persist source has no source to surface under, and still rides every
      // query served from that file.
      const warnings = materializationConfigWarnings({
         modelDeclarations: [
            {
               modelPath: "marts.malloy",
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               queryMetadata: { run_id: "x" } as any,
            },
         ],
         sources: [],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].subject).toBe("marts.malloy");
      expect(warnings[0].message).toContain("## materialization.queryMetadata");
   });
});
