// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Integration test: Query Boundary (`queryableSources`).
 *
 * Drives `Package.create` through the package-load worker pool and verifies the
 * rule "queryable == discoverable": under the default `queryableSources:
 * "declared"`, only the discovery surface (`explores` files + their `export {}`
 * closure) is a valid top-level query target. Everything else still compiles,
 * imports, joins, and extends, but is denied a direct query with a generic 404
 * (`NotQueryableError`). `"all"` decouples the axes (the prior behavior), and a
 * package with no `explores` is unaffected in either mode.
 *
 * Enforcement is two-step (mirroring the authorize gate): an early
 * pre-compilation gate that positively denies what it can resolve (schema-
 * oracle defense), and a compiled backstop that settles everything else
 * against the run target Malloy actually executes (read off the prepared
 * query's structRef — inspecting compiler output, never altering compilation).
 * Ad-hoc derivation over a curated source is admitted via the same alias walk
 * filters use; an unresolvable target fails closed.
 *
 * This is the *what* axis; `#(authorize)` (the *who* axis) is orthogonal and
 * tested in explore_visibility.spec.ts / the authorize specs. Notebooks are
 * always public and never gated.
 */
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { NotQueryableError } from "../errors";
import type { Model } from "./model";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

describe("Query Boundary (queryableSources) via worker pool", () => {
   let tempDir: string;
   let pool: PackageLoadPool;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-qbound-"));
   });

   afterEach(() => {
      if (tempDir) {
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

   function writeManifest(extra: Record<string, unknown> = {}): void {
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package", ...extra }),
      );
   }

   // base.malloy (a building block, never an explore) + index.malloy (the
   // explore) which imports/joins it, exports only `customers`, and keeps a
   // local `helper` source it does NOT export. Each runnable source carries a
   // view so it can actually be queried.
   function writeLayeredModels(): void {
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id, 5 as n") extend {
  measure: total_n is n.sum()
  view: v is { aggregate: total_n }
}`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "base.malloy"
source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: hv is { aggregate: c }
}
source: customers is duckdb.sql("select 1 as id, 100 as amt") extend {
  join_one: b is base_source on id = b.id
  measure: total is amt.sum()
  view: v is { aggregate: total }
}
export { customers }`,
      );
      fs.writeFileSync(
         path.join(tempDir, "report.malloynb"),
         `>>>markdown\n# Report\nAlways public.`,
      );
   }

   /**
    * Assert a named (`sourceName`/`queryName`) request is refused and returns
    * nothing. Deliberately does NOT pin the error class: these fields are
    * quoted as identifiers, so an injected statement is refused at the lexer as
    * an unresolvable/unlexable NAME (a Malloy compile error) rather than by the
    * boundary (`NotQueryableError`) — and which one fires depends only on which
    * gate the shape happens to reach first. What must hold is that it threw and
    * no rows came back. Reports the leaked row count on failure, since a
    * success here IS the leak.
    */
   async function expectNamedRejected(
      model: Model,
      sourceName: string | undefined,
      queryName: string | undefined,
   ): Promise<void> {
      let leakedRows: number | undefined;
      try {
         const { compactResult } = await model.getQueryResults(
            sourceName,
            queryName,
         );
         leakedRows = (compactResult as unknown[] | undefined)?.length ?? 0;
      } catch {
         return;
      }
      throw new Error(
         `Expected the named request { sourceName: ${JSON.stringify(sourceName)}, ` +
            `queryName: ${JSON.stringify(queryName)} } to be rejected, but it ` +
            `succeeded and returned ${leakedRows} rows (INJECTION / LEAK).`,
      );
   }

   // -- default mode ("declared") -----------------------------------------

   it("declared: exported source in an explores file IS queryable (incl. join-through)", async () => {
      writeManifest({ explores: ["index.malloy"] }); // queryableSources defaults to "declared"
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const { result } = await pkg
            .getModel("index.malloy")!
            .getQueryResults("customers", "v", undefined);
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a non-explores model file is not queryable (file-level, 404)", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         // base.malloy is hidden (not an explore) → not a query entry point.
         await expect(
            pkg
               .getModel("base.malloy")!
               .getQueryResults("base_source", "v", undefined),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a non-exported source inside an explores file is not queryable", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         // `helper` compiles and is joinable, but is not in index's export{}.
         await expect(
            pkg.getModel("index.malloy")!.getQueryResults("helper", "hv"),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: ad-hoc query is admitted for a curated source, denied for a hidden one", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         const { result } = await model.getQueryResults(
            undefined,
            undefined,
            "run: customers -> { aggregate: total }",
         );
         expect(result.data).toBeDefined();

         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: helper -> { aggregate: c }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   // The package-wide union admits a name only when the requesting model
   // resolves it to the same DECLARATION a listed model exported. Keying the
   // union on bare names instead let a name curated anywhere clear the gate
   // everywhere, while Malloy still resolved the declaration in the requested
   // model's namespace — so a same-named hidden source was served through a
   // model that never exported it. These pin both axes of that, plus the
   // legitimate re-export the identity check must NOT break.
   describe("declared: package-wide union admits by declaration, not by name", () => {
      // a.malloy (listed) exports its OWN `customers` (1 row) and its own named
      // query `top`. b.malloy (listed) imports a hidden file declaring a
      // DIFFERENT `customers` (3 rows) plus its own private `top` over it, and
      // exports neither. Nothing in b is curated but `safe`.
      function writeCollidingModels(): void {
         fs.writeFileSync(
            path.join(tempDir, "hidden.malloy"),
            `source: customers is duckdb.sql("select 1 as id union all select 2 union all select 3") extend {
  measure: c is count()
  view: v is { aggregate: c }
}`,
         );
         fs.writeFileSync(
            path.join(tempDir, "a.malloy"),
            `source: customers is duckdb.sql("select 99 as id") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
query: top is customers -> { aggregate: c }
export { customers, top }`,
         );
         fs.writeFileSync(
            path.join(tempDir, "b.malloy"),
            `import "hidden.malloy"
source: safe is duckdb.sql("select 1 as id") extend { measure: c is count() }
query: top is customers -> { aggregate: c }
export { safe }`,
         );
      }

      it("denies a hidden source that merely shares a name with a sibling explore's export", async () => {
         writeManifest({
            explores: ["a.malloy", "b.malloy"],
            queryableSources: "declared",
         });
         writeCollidingModels();
         const { malloyConfig, duckdb } = await makeMalloyConfig();
         try {
            const pkg = await Package.create(
               "env",
               "pkg",
               tempDir,
               malloyConfig,
            );
            const b = pkg.getModel("b.malloy")!;
            // Ad-hoc, explicit sourceName, and the named-view form all resolve
            // `customers` to hidden.malloy's declaration in b's namespace.
            await expect(
               b.getQueryResults(
                  undefined,
                  undefined,
                  "run: customers -> { aggregate: c }",
               ),
            ).rejects.toBeInstanceOf(NotQueryableError);
            await expect(
               b.getQueryResults("customers", "v"),
            ).rejects.toBeInstanceOf(NotQueryableError);
            // a.malloy's own export is untouched, and returns ITS row count.
            const viaOwner = await pkg
               .getModel("a.malloy")!
               .getQueryResults(
                  undefined,
                  undefined,
                  "run: customers -> { aggregate: c }",
               );
            expect(
               (viaOwner.compactResult as { c: number }[] | undefined)?.[0].c,
            ).toBe(1);
         } finally {
            await duckdb.close();
         }
      });

      it("denies a private named query that shares a name with a sibling explore's exported query", async () => {
         // Worse than the source axis: the pure-queryName path clears without
         // ever reaching the compiled backstop, so a name-keyed union ran
         // b.malloy's unexported `top` against its hidden source.
         writeManifest({
            explores: ["a.malloy", "b.malloy"],
            queryableSources: "declared",
         });
         writeCollidingModels();
         const { malloyConfig, duckdb } = await makeMalloyConfig();
         try {
            const pkg = await Package.create(
               "env",
               "pkg",
               tempDir,
               malloyConfig,
            );
            await expect(
               pkg.getModel("b.malloy")!.getQueryResults(undefined, "top"),
            ).rejects.toBeInstanceOf(NotQueryableError);
            // The genuine export still runs, over a.malloy's 1-row source.
            const owned = await pkg
               .getModel("a.malloy")!
               .getQueryResults(undefined, "top");
            expect(
               (owned.compactResult as { c: number }[] | undefined)?.[0].c,
            ).toBe(1);
         } finally {
            await duckdb.close();
         }
      });

      it("still admits a source RE-EXPORTED by a listed file from a hidden one", async () => {
         // The identity check keys on the declaration, which lives in the
         // hidden file — so this must still pass, through the re-exporting
         // model AND through a sibling explore that imports the same file.
         writeManifest({
            explores: ["reexport.malloy", "other.malloy"],
            queryableSources: "declared",
         });
         fs.writeFileSync(
            path.join(tempDir, "hidden.malloy"),
            `source: shared is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: v is { aggregate: c }
}`,
         );
         fs.writeFileSync(
            path.join(tempDir, "reexport.malloy"),
            `import "hidden.malloy"
export { shared }`,
         );
         fs.writeFileSync(
            path.join(tempDir, "other.malloy"),
            `import "hidden.malloy"
source: own is duckdb.sql("select 1 as id") extend { measure: c is count() }
export { own }`,
         );
         const { malloyConfig, duckdb } = await makeMalloyConfig();
         try {
            const pkg = await Package.create(
               "env",
               "pkg",
               tempDir,
               malloyConfig,
            );
            const viaExporter = await pkg
               .getModel("reexport.malloy")!
               .getQueryResults("shared", "v");
            expect(viaExporter.result.data).toBeDefined();
            // Same declaration reached through a sibling explore: the union's
            // whole purpose (CR-5), and it survives the identity check.
            const viaSibling = await pkg
               .getModel("other.malloy")!
               .getQueryResults(
                  undefined,
                  undefined,
                  "run: shared -> { aggregate: c }",
               );
            expect(viaSibling.result.data).toBeDefined();
         } finally {
            await duckdb.close();
         }
      });
   });

   it("declared: no-export files listed in explores keep every source queryable, including base files imported by a sibling explore", async () => {
      // The exact shape a QA session shipped and watched break on
      // server 0.0.243 (HANDOFF CR-5): four model files, every one listed in
      // explores, queryableSources declared, and NO export{} statement
      // anywhere. Three base files are imported by the fourth. Observed then:
      // only the importing file stayed queryable; the three base files 404'd
      // on query and compile while exploresWarnings said none. The documented
      // rule ("a file with no export exposes all of its own top-level
      // sources") requires all four to work.
      writeManifest({
         explores: [
            "track_analysis.malloy",
            "tracks.malloy",
            "artists.malloy",
            "decade_trends.malloy",
         ],
         queryableSources: "declared",
      });
      const base = (name: string) =>
         `source: ${name} is duckdb.sql("select 1 as id, 5 as n") extend {
  measure: c is count()
  view: v is { aggregate: c }
}`;
      fs.writeFileSync(path.join(tempDir, "tracks.malloy"), base("tracks"));
      fs.writeFileSync(path.join(tempDir, "artists.malloy"), base("artists"));
      fs.writeFileSync(
         path.join(tempDir, "decade_trends.malloy"),
         base("decade_trends"),
      );
      fs.writeFileSync(
         path.join(tempDir, "track_analysis.malloy"),
         `import "tracks.malloy"
import "artists.malloy"
import "decade_trends.malloy"
source: track_analysis is tracks extend {
  join_one: a is artists on id = a.id
  measure: total is n.sum()
  view: tv is { aggregate: total }
}`,
      );
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const cases: Array<[string, string]> = [
            ["tracks.malloy", "tracks"],
            ["artists.malloy", "artists"],
            ["decade_trends.malloy", "decade_trends"],
            ["track_analysis.malloy", "track_analysis"],
         ];
         for (const [modelPath, sourceName] of cases) {
            const model = pkg.getModel(modelPath);
            expect(model).toBeDefined();
            // Discoverable: the no-export file exposes its own sources.
            expect(model!.getSources()?.map((s) => s.name)).toContain(
               sourceName,
            );
            // Queryable: named view and ad-hoc both clear the boundary.
            const named = await model!.getQueryResults(
               sourceName,
               sourceName === "track_analysis" ? "tv" : "v",
            );
            expect(named.result.data).toBeDefined();
            const adhoc = await model!.getQueryResults(
               undefined,
               undefined,
               `run: ${sourceName} -> { aggregate: c is count() }`,
            );
            expect(adhoc.result.data).toBeDefined();
         }

         // The shape that actually broke (root cause of the CR-5 404s): every
         // query posted through ONE model path — the importer. `tracks` is
         // declared queryable by its own listed file, so addressing it via
         // track_analysis.malloy must clear too; the per-model closure used to
         // deny it here with `No queryable source "tracks"`, which admitted
         // nothing (it was queryable via tracks.malloy) and broke any client
         // that pins one model path for all queries.
         const importer = pkg.getModel("track_analysis.malloy")!;
         const viaImporterAdhoc = await importer.getQueryResults(
            undefined,
            undefined,
            "run: tracks -> { aggregate: c is count() }",
         );
         expect(viaImporterAdhoc.result.data).toBeDefined();
         const viaImporterNamed = await importer.getQueryResults("tracks", "v");
         expect(viaImporterNamed.result.data).toBeDefined();
         // Derivation over a package-curated source clears the backstop too.
         const viaImporterDerived = await importer.getQueryResults(
            undefined,
            undefined,
            "source: t2 is artists extend { measure: m is n.sum() }\nrun: t2 -> { aggregate: m }",
         );
         expect(viaImporterDerived.result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: multi-statement is settled by the COMPILED run target (last statement wins)", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // Curated decoy first, hidden real target last. The early gate only
         // sees the first `run:` (curated → defers); the compiled backstop
         // resolves the LAST statement — the one Malloy actually executes —
         // and denies it. This is the case the structRef read exists for.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: customers -> { aggregate: total }\nrun: helper -> { aggregate: c }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);

         // Hidden target FIRST is positively denied by the early gate, before
         // compilation — its compile errors can't be used as a schema oracle.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: helper -> { aggregate: c }\nrun: customers -> { aggregate: total }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);

         // All-curated multi-statement is legitimate and admitted (no false
         // denial from the old fail-closed-on-shape heuristic).
         const { result } = await model.getQueryResults(
            undefined,
            undefined,
            "run: customers -> { aggregate: total }\nrun: customers -> { group_by: id }",
         );
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a newline in queryName cannot smuggle a second run: statement (issue #964)", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // Positive control: the legitimate named request works.
         const before = await model.getQueryResults("customers", "v");
         expect(before.result.data).toBeDefined();

         // The reported repro. `sourceName` is curated, so the early gate
         // returns "cleared" and the compiled backstop is deliberately skipped
         // — nothing ever re-checks the statement the newline opened, and
         // Malloy runs the LAST `run:`. Quoting `queryName` is what stops it
         // becoming a second statement at all.
         await expectNamedRejected(
            model,
            "customers",
            "v\nrun: helper -> { aggregate: c }",
         );

         // The reporter's verified variant table. Each of these is already
         // blocked (an exact-string curated lookup that misses fails closed
         // with a 404), so these pin the matrix against future drift.
         //
         // Injection through `sourceName`, swallowing `queryName` into the
         // smuggled statement…
         await expectNamedRejected(model, "customers -> v\nrun: helper", "hv");
         // …and with the smuggled statement self-contained.
         await expectNamedRejected(
            model,
            "customers\nrun: helper -> { aggregate: c }",
            "v",
         );
         // `queryName` alone (no source prefix): misses the curated-QUERY set.
         await expectNamedRejected(
            model,
            undefined,
            "v\nrun: helper -> { aggregate: c }",
         );
         // The escape itself, which is what the whole fix rests on: a payload
         // carrying its own BACKTICK, trying to close the quote we opened and
         // resume as syntax. quoteMalloyIdentifier escapes `\` before `` ` ``
         // (the same order, and the same function body, as malloy's internal
         // escapeIdentifier), so the backtick becomes ``\` `` inside the
         // identifier instead of terminating it. Escaping in the other order
         // would let `\` + `` ` `` reopen the hole.
         await expectNamedRejected(
            model,
            "customers",
            "v` -> `hv`\nrun: `helper",
         );
         await expectNamedRejected(
            model,
            "customers` -> `v`\nrun: `helper",
            "hv",
         );
         // A backslash-escape payload: ``` is a backtick in JSON-style
         // unescaping, and malloy's decoder (ParseUtil.parseString) honors
         // \uXXXX inside the quotes. Escaping the backslash FIRST is what keeps
         // it a literal six-character name rather than a closing quote.
         await expectNamedRejected(model, "customers", "v\\u0060 -> \\u0060hv");
         // The ad-hoc equivalent: settled by the compiled backstop instead.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: customers -> v\nrun: helper -> { aggregate: c }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);

         // The hidden source is still refused when named honestly, i.e. the
         // boundary itself is intact and the rejections above are the injection
         // being refused, not the boundary firing twice.
         await expect(
            model.getQueryResults("helper", "hv"),
         ).rejects.toBeInstanceOf(NotQueryableError);

         // …and the legitimate request still works after all of it.
         const after = await model.getQueryResults("customers", "v");
         expect(after.result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a name that needs Malloy quoting is queryable on the named path", async () => {
      // The named path composes `run: <source> -> <view>`; unquoted, a name
      // that REQUIRES Malloy quoting (here a hyphen) does not even lex, so it
      // was unreachable through `sourceName`/`queryName`. The caller sends the
      // bare names the model's `sources` listing returns — quoting is the
      // server's job.
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `source: \`customer-orders\` is duckdb.sql("select 1 as id, 100 as amt") extend {
  measure: total is amt.sum()
  view: \`by-total\` is { aggregate: total }
}
export { \`customer-orders\` }`,
      );
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const { result } = await pkg
            .getModel("index.malloy")!
            .getQueryResults("customer-orders", "by-total");
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: ad-hoc derivation over a curated source is queryable; over a hidden one is not", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // Composing over a queryable source is itself queryable: the compiled
         // structRef names the ad-hoc alias `x`, and the derivation walk maps
         // x → customers (curated).
         const { result } = await model.getQueryResults(
            undefined,
            undefined,
            "source: x is customers extend { measure: m is count() }\nrun: x -> { aggregate: m }",
         );
         expect(result.data).toBeDefined();

         // Laundering a hidden source through an ad-hoc alias is denied: the
         // chain y → helper never reaches the curated surface.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "source: y is helper extend { measure: m is count() }\nrun: y -> { aggregate: m }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: an exported named query reading a hidden source is admitted by name", async () => {
      // Exporting a query is the author's deliberate exposure of a result,
      // even when the source it reads stays hidden. The explicit queryName
      // request is cleared by the early gate and the compiled backstop is
      // skipped — re-deriving the underlying (hidden) source must not re-deny
      // the author's chosen entry point.
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
}
source: customers is duckdb.sql("select 1 as id, 100 as amt") extend {
  measure: total is amt.sum()
}
query: helper_stats is helper -> { aggregate: c }
export { customers, helper_stats }`,
      );
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         const { result } = await model.getQueryResults(
            undefined,
            "helper_stats",
            undefined,
         );
         expect(result.data).toBeDefined();

         // …while the hidden source itself stays non-queryable directly.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: helper -> { aggregate: c }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a hidden source is not queryable via a view whose name collides with an exported query (regression)", async () => {
      // Guards the early-gate fast-path. An exported top-level query named
      // `stats` must admit the pure named query `run: stats`, but must NOT clear
      // `run: helper->stats` where `helper` is hidden and `stats` is also a view
      // on it. Clearing on the bare queryName (ignoring the source prefix) would
      // skip the compiled backstop and let the hidden source be read through the
      // name collision — so the source-prefixed form is gated on the *source*
      // being curated, not on the query name.
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: stats is { aggregate: c }
}
source: customers is duckdb.sql("select 1 as id, 100 as amt") extend {
  measure: total is amt.sum()
}
query: stats is customers -> { aggregate: total }
export { customers, stats }`,
      );
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // The exported top-level query is reachable on its own name.
         const { result } = await model.getQueryResults(
            undefined,
            "stats",
            undefined,
         );
         expect(result.data).toBeDefined();

         // …but the hidden source is NOT reachable through the colliding view
         // name when explicitly targeted as the run source.
         await expect(
            model.getQueryResults("helper", "stats", undefined),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: ad-hoc derivation over a backtick-quoted exported source is queryable", async () => {
      // A source whose name needs backticks (a hyphen) must still be walkable
      // by the derivation alias map — otherwise composing over it returns a
      // false 404. Guards buildAliasMap's quoted-identifier handling.
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `source: \`customer-orders\` is duckdb.sql("select 1 as id, 100 as amt") extend {
  measure: total is amt.sum()
}
export { \`customer-orders\` }`,
      );
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         const { result } = await model.getQueryResults(
            undefined,
            undefined,
            "source: x is `customer-orders` extend { measure: m is count() }\nrun: x -> { aggregate: m }",
         );
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a notebook is exempt from the boundary (always public)", async () => {
      // The /compile path runs assertQueryBoundaryEarly against the target
      // model; a notebook is never in `explores`, so without an explicit
      // exemption it would 404 — contradicting "notebooks are always public".
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const notebook = pkg.getModel("report.malloynb");
         expect(notebook).toBeDefined();
         // Boundary is inert for notebooks even though it's not an explore.
         expect(
            notebook!.assertQueryBoundaryEarly(undefined, undefined, "run: x"),
         ).toBe("cleared");
         expect(() =>
            notebook!.assertQueryBoundaryCompiled("anything", "run: x"),
         ).not.toThrow();
      } finally {
         await duckdb.close();
      }
   });

   // -- "all" mode (decoupled) --------------------------------------------

   it("all: explores gates discovery only — hidden file/source stay queryable", async () => {
      writeManifest({ explores: ["index.malloy"], queryableSources: "all" });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);

         // Listing is still curated to the explore.
         const listed = (await pkg.listModels()).map((m) => m.path);
         expect(listed).toEqual(["index.malloy"]);

         // …but the non-explores file and the non-exported source are queryable.
         const base = await pkg
            .getModel("base.malloy")!
            .getQueryResults("base_source", "v", undefined);
         expect(base.result.data).toBeDefined();
         const helper = await pkg
            .getModel("index.malloy")!
            .getQueryResults("helper", "hv", undefined);
         expect(helper.result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   // -- no explores declared (boundary inert) -----------------------------

   it("declared default + no explores: everything stays queryable (backward compatible)", async () => {
      writeManifest(); // no explores → no curated surface to enforce
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const base = await pkg
            .getModel("base.malloy")!
            .getQueryResults("base_source", "v", undefined);
         expect(base.result.data).toBeDefined();
         // `helper` is non-exported, but with no explores there is no boundary.
         const helper = await pkg
            .getModel("index.malloy")!
            .getQueryResults("helper", "hv", undefined);
         expect(helper.result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared default + no explores: a newline in queryName still cannot smuggle a statement", async () => {
      // The default deployment shape: no `explores`, so the boundary is inert
      // and quoting the caller's names is the only thing holding the line.
      // Nothing here is about hiding `helper` — it is legitimately queryable in
      // this mode — it is that a request naming ONE source must not run a
      // statement naming another.
      writeManifest(); // no explores → boundary inert
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // Control: with no explores, `helper` really is queryable by name…
         const helper = await model.getQueryResults("helper", "hv");
         expect(helper.result.data).toBeDefined();

         // …so these rejections are the injection being refused, not a boundary.
         await expectNamedRejected(
            model,
            "customers",
            "v\nrun: helper -> { aggregate: c }",
         );
         await expectNamedRejected(model, "customers -> v\nrun: helper", "hv");
      } finally {
         await duckdb.close();
      }
   });
});
