/**
 * Integration test: exercise `Package.create` with the package-load
 * worker pool enabled (PACKAGE_LOAD_WORKERS=1).
 *
 * Validates that the worker-load path:
 *   - reads the manifest, probes embedded databases, and compiles
 *     every model in a single off-thread job
 *   - produces a live `Package` whose `Model`s have populated
 *     `modelDef` / `sources` / `queries`
 *   - hydrates the `ModelMaterializer` from `modelDef` on first
 *     query (no recompile) — verified end-to-end by running a
 *     query through the resulting Model and getting a result
 *
 * Kept separate from `package.spec.ts` so the existing tests keep
 * running on the in-process path without paying worker startup cost.
 *
 * Pool reuse strategy: one `PackageLoadPool` shared across all
 * cases in this file. Spawning a fresh worker per test crashes Bun
 * (segfault) because DuckDB's native bindings don't tolerate being
 * loaded concurrently into multiple worker isolates of the same Bun
 * process. Production uses one pool; this matches.
 */
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { logger } from "../logger";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

describe("Package.create via worker pool", () => {
   let tempDir: string;
   let pool: PackageLoadPool;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      // Wire our pool into the module-level singleton so Package.create
      // picks it up via getPackageLoadPool().
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) {
         delete process.env.PACKAGE_LOAD_WORKERS;
      } else {
         process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
      }
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-pkg-worker-"));
   });

   afterEach(() => {
      if (tempDir) {
         // tempDir gets wiped by Package.create on failure (it's the
         // staging-cleanup path); ignore ENOENT here.
         try {
            fs.rmSync(tempDir, { recursive: true, force: true });
         } catch {
            /* already gone */
         }
         tempDir = "";
      }
   });

   async function makeMalloyConfig(): Promise<{
      malloyConfig: import("@malloydata/malloy").MalloyConfig;
      duckdb: { close: () => Promise<void> };
   }> {
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);
      return { malloyConfig, duckdb };
   }

   function writeManifest(): void {
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package" }),
      );
   }

   it("loads a package end-to-end and serves a query through the hydrated materializer", async () => {
      writeManifest();
      // Define `total_v` as a *view* on the source so the query
      // builder's `run: nums -> total_v` form resolves. (Top-level
      // queries take the `run: total_q` form — orthogonal path that
      // the in-process tests already cover.)
      fs.writeFileSync(
         path.join(tempDir, "trivial.malloy"),
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  view: total_v is { aggregate: total }
}`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg).toBeInstanceOf(Package);
         expect(pkg.getModelPaths()).toEqual(["trivial.malloy"]);

         const model = pkg.getModel("trivial.malloy");
         expect(model).toBeDefined();
         const apiModel = (await model!.getModel()) as {
            modelDef?: string;
            sources?: { name?: string }[];
         };
         expect(apiModel.modelDef).toBeDefined();
         expect(apiModel.sources?.[0]?.name).toBe("nums");

         // First query against the package — hydrates the
         // ModelMaterializer from the worker's modelDef without a
         // recompile, then runs the SQL against the *main thread's*
         // DuckDB connection (the only one with the in-memory `nums`
         // source loaded via duckdb.sql()).
         const { result } = await model!.getQueryResults(
            "nums",
            "total_v",
            undefined,
         );
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("propagates a per-model compile failure as a thrown error from Package.create", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "broken.malloy"),
         `source: bad is duckdb.sql("select 1 as a") extend {
  measure: oops is THIS_FUNC_DOES_NOT_EXIST(a)
}`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(Error);
      } finally {
         await duckdb.close();
      }
   });

   it("validates and surfaces a valid #(authorize) model through the worker", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "gated.malloy"),
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("gated.malloy");
         const apiModel = (await model!.getModel()) as {
            sources?: { name?: string; authorize?: string[] }[];
         };
         // The worker compiled the authorize probe (no throw) and surfaced the
         // effective expression list — proves worker-path validation runs.
         expect(apiModel.sources?.[0]?.authorize).toEqual([
            "$ROLE = 'analyst'",
         ]);
         expect(model!.getAuthorize("gated")).toEqual(["$ROLE = 'analyst'"]);
      } finally {
         await duckdb.close();
      }
   });

   it("rejects a package whose #(authorize) references an unknown given (worker validation)", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "badgate.malloy"),
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$NOPE = 'x'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { ModelCompilationError } = await import("../errors");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         // Must surface as a 424 ModelCompilationError across the worker
         // boundary, not a generic 500 — the worker serializes it with
         // isCompilationError so the main thread re-wraps it.
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(ModelCompilationError);
      } finally {
         await duckdb.close();
      }
   });

   it("validates a valid #(authorize) source in a .malloynb notebook through the worker", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "gated.malloynb"),
         `>>>markdown
# Gated notebook

>>>malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("gated.malloynb");
         // compileNotebookModel ran authorize validation (no throw) and
         // surfaced the gate — the notebook compile path was previously
         // unexercised by tests.
         expect(model!.getAuthorize("gated")).toEqual(["$ROLE = 'analyst'"]);
      } finally {
         await duckdb.close();
      }
   });

   it("rejects a .malloynb notebook whose #(authorize) references an unknown given", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "badgate.malloynb"),
         `>>>malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) "$NOPE = 'x'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { ModelCompilationError } = await import("../errors");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(ModelCompilationError);
      } finally {
         await duckdb.close();
      }
   });

   it("logs a warning for a package whose view carries an invalid renderer tag", async () => {
      writeManifest();
      // `# big_value { sparkline=... }` is a child-only renderer config placed on
      // the view itself, with no activating big_value. The renderer declines to
      // match it. Render-tag validation runs on the main thread after the worker
      // hydrates the model. The misconfiguration is logged as a warning naming
      // the offending view; it does not fail the package load.
      fs.writeFileSync(
         path.join(tempDir, "bad_render.malloy"),
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # big_value { sparkline=trend }
  view: card is {
    aggregate: total
    nest:
      # line_chart { size=spark }
      trend is { group_by: a; aggregate: total }
  }
}`,
      );

      const warnSpy = spyOn(logger, "warn");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg.getModelPaths()).toEqual(["bad_render.malloy"]);
         const warnings = warnSpy.mock.calls.map((c) => String(c[0]));
         expect(
            warnings.some(
               (m) =>
                  m.includes("Invalid renderer configuration") &&
                  m.includes("nums -> card"),
            ),
         ).toBe(true);
         // The same finding rides the package response (non-fatal) so the
         // control plane / UI can surface it without scraping logs.
         const responseWarnings = pkg.getPackageMetadata().warnings ?? [];
         expect(
            responseWarnings.some(
               (w) =>
                  w.model === "bad_render.malloy" &&
                  w.target === "nums -> card" &&
                  w.severity === "error",
            ),
         ).toBe(true);
      } finally {
         warnSpy.mockRestore();
         await duckdb.close();
      }
   });

   it("loads a package whose view renderer tags are valid", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "good_render.malloy"),
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # bar_chart
  view: chart is { group_by: a; aggregate: total }
}`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg.getModelPaths()).toEqual(["good_render.malloy"]);
         // A clean package omits the warnings field entirely.
         expect(pkg.getPackageMetadata().warnings).toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });

   it("logs a warning for a backtick-quoted (hyphenated) source with a bad view render tag", async () => {
      writeManifest();
      // A source whose name needs Malloy backtick-quoting (here, a hyphen). The
      // validation target must quote the identifier; otherwise `run: bad-source
      // -> card` fails to lex, loadQuery throws, the catch swallows it, and the
      // misconfigured view is silently skipped -- which would defeat the feature
      // for any quote-needing name (the repo already has such a source,
      // `gated-source`, in authorize_integration.spec.ts).
      fs.writeFileSync(
         path.join(tempDir, "hyphen_render.malloy"),
         `source: \`bad-source\` is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # big_value { sparkline=trend }
  view: card is {
    aggregate: total
    nest:
      # line_chart { size=spark }
      trend is { group_by: a; aggregate: total }
  }
}`,
      );

      const warnSpy = spyOn(logger, "warn");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg.getModelPaths()).toEqual(["hyphen_render.malloy"]);
         const warnings = warnSpy.mock.calls.map((c) => String(c[0]));
         expect(
            warnings.some(
               (m) =>
                  m.includes("Invalid renderer configuration") &&
                  m.includes("bad-source -> card"),
            ),
         ).toBe(true);
      } finally {
         warnSpy.mockRestore();
         await duckdb.close();
      }
   });

   it("logs a warning for a source name containing an escaped backtick with a bad view render tag", async () => {
      writeManifest();
      // Source name with a literal backtick (written escaped in Malloy as
      // `wei\`rd`). The validation target must re-escape it; a bare wrap would
      // produce `run: `wei`rd` -> ...`, where the embedded backtick closes the
      // quote early, fails to lex, and the bad view is silently skipped. Mirrors
      // malloy's own identifierCode escaping.
      fs.writeFileSync(
         path.join(tempDir, "backtick_render.malloy"),
         `source: \`wei\\\`rd\` is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # big_value { sparkline=trend }
  view: card is {
    aggregate: total
    nest:
      # line_chart { size=spark }
      trend is { group_by: a; aggregate: total }
  }
}`,
      );

      const warnSpy = spyOn(logger, "warn");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg.getModelPaths()).toEqual(["backtick_render.malloy"]);
         const warnings = warnSpy.mock.calls.map((c) => String(c[0]));
         expect(
            warnings.some(
               (m) =>
                  m.includes("Invalid renderer configuration") &&
                  m.includes("card"),
            ),
         ).toBe(true);
      } finally {
         warnSpy.mockRestore();
         await duckdb.close();
      }
   });

   it("logs a warning for a .malloynb notebook whose source view carries an invalid renderer tag", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "bad_render.malloynb"),
         `>>>malloy
source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # big_value { sparkline=trend }
  view: card is {
    aggregate: total
    nest:
      # line_chart { size=spark }
      trend is { group_by: a; aggregate: total }
  }
}`,
      );

      const warnSpy = spyOn(logger, "warn");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg.getModelPaths()).toEqual(["bad_render.malloynb"]);
         const warnings = warnSpy.mock.calls.map((c) => String(c[0]));
         expect(
            warnings.some((m) => m.includes("Invalid renderer configuration")),
         ).toBe(true);
         // The notebook finding also rides the package response (not just logs).
         const responseWarnings = pkg.getPackageMetadata().warnings ?? [];
         expect(
            responseWarnings.some(
               (w) =>
                  w.model === "bad_render.malloynb" && w.severity === "error",
            ),
         ).toBe(true);
      } finally {
         warnSpy.mockRestore();
         await duckdb.close();
      }
   });

   it("re-validates renderer tags on reload, logging a warning for a model that develops a bad tag", async () => {
      writeManifest();
      // Start valid so Package.create succeeds.
      fs.writeFileSync(
         path.join(tempDir, "m.malloy"),
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # bar_chart
  view: chart is { group_by: a; aggregate: total }
}`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      const warnSpy = spyOn(logger, "warn");
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg.getModel("m.malloy")).toBeDefined();
         warnSpy.mockClear();

         // The model now develops a misconfigured render tag. Reload re-validates
         // (not just Package.create) and logs a warning for it, without aborting
         // the reload or failing the model.
         fs.writeFileSync(
            path.join(tempDir, "m.malloy"),
            `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # big_value { sparkline=trend }
  view: card is {
    aggregate: total
    nest:
      # line_chart { size=spark }
      trend is { group_by: a; aggregate: total }
  }
}`,
         );
         await pkg.reloadAllModels({});

         const reloaded = pkg.getModel("m.malloy");
         expect(reloaded).toBeDefined();
         await expect(reloaded!.getModel()).resolves.toBeDefined();
         const warnings = warnSpy.mock.calls.map((c) => String(c[0]));
         expect(
            warnings.some((m) => m.includes("Invalid renderer configuration")),
         ).toBe(true);
         // Reload refreshes the response-level warnings too.
         const responseWarnings = pkg.getPackageMetadata().warnings ?? [];
         expect(responseWarnings.some((w) => w.model === "m.malloy")).toBe(
            true,
         );
      } finally {
         warnSpy.mockRestore();
         await duckdb.close();
      }
   });

   it("clears render-tag warnings on reload when a bad tag is fixed (bad -> clean)", async () => {
      writeManifest();
      // Start BAD so the package loads with a warning.
      fs.writeFileSync(
         path.join(tempDir, "m.malloy"),
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # big_value { sparkline=trend }
  view: card is {
    aggregate: total
    nest:
      # line_chart { size=spark }
      trend is { group_by: a; aggregate: total }
  }
}`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(
            (pkg.getPackageMetadata().warnings ?? []).some(
               (w) => w.model === "m.malloy",
            ),
         ).toBe(true);

         // Fix the tag, then reload: renderTagWarnings is rebuilt from scratch,
         // so the stale finding must disappear (field omitted when empty).
         fs.writeFileSync(
            path.join(tempDir, "m.malloy"),
            `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # bar_chart
  view: chart is { group_by: a; aggregate: total }
}`,
         );
         await pkg.reloadAllModels({});

         expect(pkg.getPackageMetadata().warnings).toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });

   it("aggregates render-tag warnings from multiple models, each tagged with its own path", async () => {
      writeManifest();
      const badModel = (view: string) =>
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  # big_value { sparkline=trend }
  view: ${view} is {
    aggregate: total
    nest:
      # line_chart { size=spark }
      trend is { group_by: a; aggregate: total }
  }
}`;
      fs.writeFileSync(path.join(tempDir, "a.malloy"), badModel("card_a"));
      fs.writeFileSync(path.join(tempDir, "b.malloy"), badModel("card_b"));

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const responseWarnings = pkg.getPackageMetadata().warnings ?? [];
         const models = new Set(responseWarnings.map((w) => w.model));
         expect(models.has("a.malloy")).toBe(true);
         expect(models.has("b.malloy")).toBe(true);
         expect(models.size).toBe(2);
         // The message field is carried through, not just model/target/severity.
         expect(
            responseWarnings.every((w) => (w.message ?? "").length > 0),
         ).toBe(true);
      } finally {
         await duckdb.close();
      }
   });

   // NB: kept last in this describe — swapping the singleton for a
   // pre-shutdown pool also tears down the shared `pool` (the swap
   // implementation shuts down the outgoing singleton). Subsequent
   // tests in this describe would see a dead pool. afterAll only
   // resets the singleton to null, so this is safe at the tail.
   it("rewraps pool-infrastructure failures as ServiceUnavailableError (HTTP 503)", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "trivial.malloy"),
         `source: nums is duckdb.sql("select 1 as a")`,
      );

      const deadPool = new PackageLoadPool(1);
      await deadPool.shutdown();
      await __setPackageLoadPoolForTests(deadPool);

      const { ServiceUnavailableError } = await import("../errors");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(ServiceUnavailableError);
      } finally {
         await duckdb.close();
         // Replace the singleton with a fresh, live pool immediately. The
         // outer afterAll also resets to null, but restoring here ensures the
         // shut-down pool never outlives this test -- otherwise, under serial
         // execution, a later spec file that lazily reuses the singleton via
         // getPackageLoadPool() could observe the dead pool before afterAll
         // runs and fail with "PackageLoadPool is shutting down".
         pool = new PackageLoadPool(1);
         await __setPackageLoadPoolForTests(pool);
      }
   });
});
