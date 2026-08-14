import { describe, expect, it } from "bun:test";
import { materializationConfigWarnings } from "./materialization_config_validation";

describe("materializationConfigWarnings", () => {
   it("says nothing about a clean set of declarations", () => {
      expect(
         materializationConfigWarnings({
            declarations: [
               { level: "package", queryMetadata: { team: "finance" } },
               {
                  level: "source",
                  subject: "orders",
                  queryMetadata: { team: "finance" },
               },
            ],
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
         declarations: [
            { level: "package", queryMetadata: { "team.name": "finance" } },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].message).toContain("queryMetadata");
      expect(warnings[0].message).toContain("team.name");
      expect(warnings[0].subject).toBeUndefined();
   });

   it("reports a source-level property at the source that declared it", () => {
      const warnings = materializationConfigWarnings({
         declarations: [
            {
               level: "source",
               subject: "orders",
               queryMetadata: { "team.name": "finance" },
            },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].subject).toBe("orders");
      expect(warnings[0].message).toContain("#@ queryMetadata");
   });

   it("reports a BigQuery-dropped name as a warning, not silence", () => {
      const warnings = materializationConfigWarnings({
         declarations: [
            {
               level: "source",
               subject: "orders",
               queryMetadata: { _team: "finance" },
            },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].message).toMatch(/BigQuery/);
   });

   it("reports once per DECLARATION, not once per source that inherits it", () => {
      // The point of checking declarations rather than resolved bags. A package
      // property is one mistake on one line; reporting it again at every source
      // that inherits it sends the author to lines that do not contain it.
      const warnings = materializationConfigWarnings({
         declarations: [
            { level: "package", queryMetadata: { "team.name": "finance" } },
            {
               level: "source",
               subject: "orders",
               queryMetadata: { workload: "marts" },
            },
            {
               level: "source",
               subject: "returns",
               queryMetadata: { workload: "marts" },
            },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].subject).toBeUndefined();
   });

   it("ignores a level that declares nothing", () => {
      expect(materializationConfigWarnings({ declarations: [] })).toEqual([]);
      expect(materializationConfigWarnings({})).toEqual([]);
   });

   it("warns about a reserved name, naming the level that declared it", () => {
      // A context name is dropped rather than attached, so declaring one
      // publishes clean and then vanishes on every statement with nothing but a
      // counter tick behind it. The level is what makes the message actionable.
      const warnings = materializationConfigWarnings({
         declarations: [
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            { level: "package", queryMetadata: { model: "marts" } as any },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].message).toContain("queryMetadata");
      expect(warnings[0].message).toContain("'model'");
      expect(warnings[0].message).toContain("dropped rather than attached");
   });

   it("names each level in the spelling an author should write", () => {
      // The label is the actionable half of the message, so it has to be the
      // canonical spelling rather than the deprecated `materialization.` one.
      const warnings = materializationConfigWarnings({
         declarations: [
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            {
               level: "model",
               subject: "marts.malloy",
               queryMetadata: { run_id: "x" } as any,
            },
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            {
               level: "source",
               subject: "orders_live",
               queryMetadata: { run_id: "x" } as any,
            },
         ],
      });
      expect(warnings).toHaveLength(2);
      expect(warnings[0].subject).toBe("marts.malloy");
      expect(warnings[0].message).toContain("## queryMetadata");
      expect(warnings[1].subject).toBe("orders_live");
      expect(warnings[1].message).toContain("#@ queryMetadata");
   });

   it("checks a source whether or not it persists", () => {
      // `queryMetadata` is a sibling of `persist` in the `#@` namespace, so a
      // source that is never materialized declares tags the same way and they
      // ride every query against it. Nothing about the build plan enters into
      // whether its declaration is checked.
      const warnings = materializationConfigWarnings({
         declarations: [
            {
               level: "source",
               subject: "orders_live",
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               queryMetadata: { run_id: "x" } as any,
            },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].subject).toBe("orders_live");
      expect(warnings[0].message).toContain("dropped rather than attached");
   });
});
