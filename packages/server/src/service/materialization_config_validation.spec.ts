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
            { level: "package", queryMetadata: { model: "marts" } },
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
            {
               level: "model",
               modelPath: "marts.malloy",
               queryMetadata: { run_id: "x" },
            },
            {
               level: "source",
               subject: "orders_live",
               modelPath: "marts.malloy",
               queryMetadata: { run_id: "x" },
            },
         ],
      });
      expect(warnings).toHaveLength(2);
      expect(warnings[0].model).toBe("marts.malloy");
      expect(warnings[0].message).toContain("## queryMetadata");
      expect(warnings[1].subject).toBe("orders_live");
      expect(warnings[1].message).toContain("#@ queryMetadata");
   });

   it("puts a model path in `model`, never in `subject`", () => {
      // The wire schema keeps the two apart: `model` is a package-relative model
      // path and `subject` is a source / query / view. A model path in `subject`
      // was invisible to a client filtering findings by model, and rendered a
      // file name where a reader expects a source name.
      const warnings = materializationConfigWarnings({
         declarations: [
            {
               level: "model",
               modelPath: "marts.malloy",
               queryMetadata: { "team.name": "finance" },
            },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].model).toBe("marts.malloy");
      expect(warnings[0].subject).toBeUndefined();
   });

   it("locates a source finding by BOTH its model path and its name", () => {
      // A source name is unique within a model, not within a package. Without
      // the path, two models declaring a same-named source produced identical
      // findings that deduped to one message naming neither file.
      const warnings = materializationConfigWarnings({
         declarations: [
            {
               level: "source",
               subject: "orders",
               modelPath: "a.malloy",
               queryMetadata: { run_id: "x" },
            },
            {
               level: "source",
               subject: "orders",
               modelPath: "b.malloy",
               queryMetadata: { run_id: "x" },
            },
         ],
      });
      expect(warnings).toHaveLength(2);
      expect(warnings.map((w) => w.model)).toEqual(["a.malloy", "b.malloy"]);
   });

   it("checks the MERGED bag against the budget, which no declaration can", () => {
      // The author budget is MAX_PROPERTIES minus the context the server adds.
      // Every layer here is individually under it while the merge is over, so
      // checking declarations alone let a package publish clean and then shed
      // context properties on every statement — visible afterwards only as a
      // metric. No level label: no single line is at fault.
      const six = (prefix: string) =>
         Object.fromEntries(
            Array.from({ length: 6 }, (_, i) => [`${prefix}${i}`, "v"]),
         );
      const warnings = materializationConfigWarnings({
         declarations: [
            { level: "package", queryMetadata: six("p") },
            {
               level: "model",
               modelPath: "marts.malloy",
               queryMetadata: six("m"),
            },
         ],
         effectiveMerges: [
            {
               modelPath: "marts.malloy",
               floorSize: 12,
               sources: [{ subject: "orders", size: 12 }],
            },
         ],
      });
      const merged = warnings.filter((w) => w.message.includes("merged"));
      expect(merged).toHaveLength(1);
      expect(merged[0].model).toBe("marts.malloy");
   });

   it("catches a package+model overflow when NO source declares anything", () => {
      // The case the merge check was added for and then missed: sizing only the
      // declaring sources meant a package and a model file that overflow
      // together published clean whenever no source in the file happened to add
      // a tag of its own — and every query against every source in that file
      // then shed context properties.
      const warnings = materializationConfigWarnings({
         effectiveMerges: [
            { modelPath: "marts.malloy", floorSize: 12, sources: [] },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].model).toBe("marts.malloy");
      expect(warnings[0].subject).toBeUndefined();
      expect(warnings[0].message).toContain("package + model file, merged");
   });

   it("reports an overflowing floor ONCE, not once per source under it", () => {
      // Every source in the file carries the floor, so naming each would be N
      // messages for one mistake — and the sources are not where the fix goes.
      const warnings = materializationConfigWarnings({
         effectiveMerges: [
            {
               modelPath: "marts.malloy",
               floorSize: 12,
               sources: [
                  { subject: "orders", size: 13 },
                  { subject: "returns", size: 14 },
               ],
            },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].subject).toBeUndefined();
   });

   it("names the individual sources when the floor itself is fine", () => {
      // Floor under budget, so each source that tips its own merge over is a
      // separate mistake in a separate place and gets its own message.
      const warnings = materializationConfigWarnings({
         effectiveMerges: [
            {
               modelPath: "marts.malloy",
               floorSize: 5,
               sources: [
                  { subject: "orders", size: 12 },
                  { subject: "fine", size: 6 },
                  { subject: "returns", size: 13 },
               ],
            },
         ],
      });
      expect(warnings).toHaveLength(2);
      expect(warnings.map((w) => w.subject)).toEqual(["orders", "returns"]);
      expect(warnings[0].message).toContain(
         "package + model file + source, merged",
      );
   });

   it("says nothing about merges inside the budget", () => {
      expect(
         materializationConfigWarnings({
            effectiveMerges: [
               {
                  modelPath: "marts.malloy",
                  floorSize: 2,
                  sources: [{ subject: "orders", size: 3 }],
               },
            ],
         }),
      ).toEqual([]);
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
               queryMetadata: { run_id: "x" },
            },
         ],
      });
      expect(warnings).toHaveLength(1);
      expect(warnings[0].subject).toBe("orders_live");
      expect(warnings[0].message).toContain("dropped rather than attached");
   });
});
