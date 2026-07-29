import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Environment } from "./environment";

/**
 * The fragment-checking advice in malloy_compile's tool description, pinned
 * against the REAL compiler.
 *
 * That description tells an agent how to validate part of a source: send a view
 * body as a top-level `query:`, or wrap a field in a throwaway `extend`, and do
 * not resubmit the source being edited. None of that is enforced by code, so
 * nothing but a test stops it from quietly becoming false. Asserting it against
 * a hand-written checker would be worthless: only Malloy decides what compiles.
 */

const PUBLISHER_JSON = JSON.stringify({
   name: "pkg",
   description: "fragments",
});

// `secret` is private on the base source, so nothing downstream may read it.
const MODEL = `##! experimental.access_modifiers
source: base is duckdb.sql("SELECT 1.5 as price, 90 as points, 'shh' as secret") include {
  public: price, points
  private: secret
}

source: sales is base extend {
  measure: record_count is count()
  measure: avg_price is avg(price)
  view: overview is { aggregate: record_count }
}
`;

describe("malloy_compile: checking part of a source", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-frag-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            PUBLISHER_JSON,
         );
         await fs.writeFile(path.join(stagingPath, "model.malloy"), MODEL);
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   /** Error-severity messages from compiling `source` against the fixture. */
   async function errorsFor(source: string): Promise<string[]> {
      const result = await env.compileSource("pkg", "model.malloy", source);
      return (result.problems ?? [])
         .filter((p) => p.severity === "error")
         .map((p) => p.message);
   }

   describe("the two failures the description exists to explain", () => {
      it("rejects a bare view: fragment, naming only the symptom", async () => {
         const errors = await errorsFor(
            "view: by_pts is { group_by: points, aggregate: record_count }",
         );
         expect(errors.join(" ")).toContain("view:");
      });

      it("rejects resubmitting the source being edited", async () => {
         // The natural move when validating an edit, and it fails for a reason
         // that has nothing to do with the edit.
         const errors = await errorsFor(`source: sales is base extend {
  measure: record_count is count()
  view: by_pts is { group_by: points }
}`);
         expect(errors.join(" ")).toContain("Cannot redefine 'sales'");
      });
   });

   describe("the top-level query: form", () => {
      it("compiles a view body with no wrapper at all", async () => {
         expect(
            await errorsFor(
               "query: check is sales -> { group_by: points, aggregate: record_count }",
            ),
         ).toEqual([]);
      });

      it("still resolves measures the source inherits", async () => {
         expect(
            await errorsFor(
               "query: check is sales -> { aggregate: avg_price }",
            ),
         ).toEqual([]);
      });
   });

   describe("the throwaway extend form", () => {
      const wrap = (fragment: string, into = "sales") =>
         `source: check is ${into} extend {\n${fragment}\n}`;

      it("compiles bare view, measure, and dimension fragments", async () => {
         expect(
            await errorsFor(
               wrap(
                  "view: by_pts is { group_by: points, aggregate: record_count }",
               ),
            ),
         ).toEqual([]);
         expect(
            await errorsFor(wrap("measure: max_price is max(price)")),
         ).toEqual([]);
         expect(
            await errorsFor(wrap("dimension: pricey is price > 1")),
         ).toEqual([]);
      });

      it("resolves measures the source inherits", async () => {
         expect(
            await errorsFor(wrap("view: v is { aggregate: avg_price }")),
         ).toEqual([]);
      });

      it("hides a private field exactly as an in-place extension does", async () => {
         // The fidelity claim in the description: the wrapper must not widen or
         // narrow visibility versus the edit it stands in for.
         const wrapped = await errorsFor(wrap("dimension: s is secret"));
         const inPlace = await errorsFor(
            "source: probe is base extend { dimension: s is secret }",
         );
         expect(wrapped.join(" ")).toContain("private");
         expect(inPlace.join(" ")).toContain("private");
      });

      it("reports a redefinition when the fragment reuses an existing name", async () => {
         // The documented caveat: an extension adds to the namespace rather
         // than overriding it, so checking an EDIT needs the fragment renamed.
         expect(
            await errorsFor(wrap("view: overview is { aggregate: avg_price }")),
         ).toEqual(["Cannot redefine 'overview'"]);

         expect(
            await errorsFor(
               wrap("view: overview__check is { aggregate: avg_price }"),
            ),
         ).toEqual([]);
      });
   });
});
