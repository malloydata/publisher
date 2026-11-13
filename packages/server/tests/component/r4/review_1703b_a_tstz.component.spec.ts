// Review probe: which column keeps a colocated `wide` source off the served copy.
import { expect, it } from "bun:test";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";

const variant = (
   cols: string,
   select: string,
) => `##! experimental { persistence }
source: wide_raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, ${cols})) AS t(id, x)
""")

#@ persist name="wide"
source: wide is wide_raw -> { select: ${select} }
`;
for (const [name, cols] of [
   ["int", "5::INTEGER"],
   ["bigint", "5::BIGINT"],
   ["decimal", "0.5::DECIMAL(38,9)"],
   ["double", "0.5::DOUBLE"],
   ["tstz", "TIMESTAMPTZ '2026-01-15 23:30:00+00'"],
   ["varchar", "'a'"],
]) {
   it(
      name,
      async () => {
         await freshLoadPool();
         const restore = withSwitch("cutover");
         const loader = await loadFixture({
            "w.malloy": variant(cols, "id, x"),
         });
         try {
            const f = await loader.bind();
            const out = await f.run(
               "w.malloy",
               "run: wide -> { group_by: id, x }",
            );
            // Served from the table today (off); must stay so under the cutover.
            expect("answer" in out && out.answer.servedBy).toBe("materialized");
         } finally {
            restore();
            await loader.dispose();
         }
      },
      120_000,
   );
}

it("tstz with the build's captured schema", async () => {
   await freshLoadPool();
   const restore = withSwitch("cutover");
   const loader = await loadFixture({
      "w.malloy": variant("TIMESTAMPTZ '2026-01-15 23:30:00+00'", "id, x"),
   });
   try {
      const Q =
         "run: wide -> { timezone: 'America/Los_Angeles'; group_by: id, x, d is x.day }";
      const live = await loader.run("w.malloy", Q);
      const f = await loader.bind();
      const wh = (await f.pkg.getMalloyConnection("duckdb")) as unknown as {
         runSQL(s: string, o?: unknown): Promise<{ rows: unknown[] }>;
      };
      const entries: Record<string, unknown> = {};
      for (const s of Object.values(f.pkg.getBuildPlan()?.sources ?? {})) {
         const table = (s.annotationFields?.name || s.name).trim();
         const d = await wh.runSQL(`DESCRIBE "${table}"`, { rowLimit: 1000 });
         entries[s.sourceEntityId] = {
            tableName: table,
            connectionName: s.connectionName ?? "duckdb",
            schema: (
               d.rows as { column_name: string; column_type: string }[]
            ).map((r) => ({ name: r.column_name, type: r.column_type })),
         };
      }
      f.pkg.bindColocatedServeManifest({});
      f.pkg.bindColocatedServeManifest(entries as never);
      for (const p of f.pkg.getModelPaths())
         await f.pkg.getModel(p)?.prepareServedCopy();
      const served = await f.run("w.malloy", Q);
      if (!("answer" in live) || !("answer" in served))
         throw new Error("errored");
      expect(served.answer.via).toBe("served_copy");
      // The result types must be live's (today's off path keeps timestamptz).
      // A number's subtype is G6, accepted; compared without it.
      const kinds = (fs: { type: unknown }[]) =>
         fs.map((f) =>
            JSON.stringify(f.type).replace(/,"subtype":"[a-z]+"/g, ""),
         );
      expect(kinds(served.answer.fields)).toEqual(kinds(live.answer.fields));
   } finally {
      restore();
      await loader.dispose();
   }
}, 120_000);
