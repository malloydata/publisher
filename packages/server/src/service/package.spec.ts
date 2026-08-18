import { DuckDBConnection } from "@malloydata/db-duckdb";
import "@malloydata/db-duckdb/native";
import { MalloyConfig } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { Stats } from "fs";
import fs from "fs/promises";
import { join, resolve } from "path";
import sinon from "sinon";
import { PackageNotFoundError } from "../errors";
import { Model } from "./model";
import { Package } from "./package";

// Minimal partial types for mocking
type PartialModel = Pick<Model, "getPath">;

describe("service/package", () => {
   const testPackageDirectory = resolve("testPackage");

   beforeEach(async () => {
      await fs.mkdir(testPackageDirectory, { recursive: true });
      await fs.writeFile(join(testPackageDirectory, "model1.model"), "");
      // Create a simple parquet file with schema "name: string, value: int"
      const parquetBuffer = Buffer.from("Name,Value\nJohn,10\nJane,20\nJim,30");

      await fs.writeFile(
         join(testPackageDirectory, "database.csv"),
         parquetBuffer,
      );
      const publisherContent = JSON.stringify({ description: "Test package" });
      await fs.writeFile(
         join(testPackageDirectory, "publisher.json"),
         publisherContent,
      );
   });

   afterEach(async () => {
      sinon.restore();
      // On Windows, DuckDB connections may still have file handles open,
      // causing EBUSY errors. Retry deletion with exponential backoff.
      const maxRetries = 3;
      const delay = 50; // Start with 50ms
      let lastError: unknown;

      for (let attempt = 0; attempt < maxRetries; attempt++) {
         try {
            await fs.rm(testPackageDirectory, { recursive: true, force: true });
            return; // Success, exit retry loop
         } catch (error) {
            lastError = error;
            const errnoError = error as NodeJS.ErrnoException;
            // Only retry on EBUSY errors (Windows file handle still open)
            if (errnoError.code !== "EBUSY") {
               throw error; // Non-EBUSY errors should be thrown immediately
            }
            // Exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms
            if (attempt < maxRetries - 1) {
               await new Promise((resolve) => setTimeout(resolve, delay));
            }
         }
      }
      // If we've exhausted retries, ignore EBUSY errors as they don't affect test results
      // and the files will be cleaned up eventually
      if ((lastError as NodeJS.ErrnoException).code !== "EBUSY") {
         throw lastError;
      }
   });

   it("should create a package instance", async () => {
      // Using 'as any' for simplified mock Map value in test
      const pkg = new Package(
         "testProject",
         "testPackage",
         testPackageDirectory,
         { name: "testPackage", description: "Test package" },
         [],
         new Map([
            [
               "model1.malloy",
               {
                  getPath: () => "model1.malloy",
                  setDiscoveryCuration: () => {},
                  setQueryBoundary: () => {},
               } as unknown as Model,
            ],
            [
               "model2.malloynb",
               {
                  getPath: () => "model2.malloynb",
                  setDiscoveryCuration: () => {},
                  setQueryBoundary: () => {},
               } as unknown as Model,
            ],
         ]),
      );

      expect(pkg).toBeInstanceOf(Package);
   });

   describe("per-package sandbox extension pinning", () => {
      const originalPolicy = process.env.EXTENSION_FETCH_POLICY;
      afterEach(() => {
         if (originalPolicy === undefined) {
            delete process.env.EXTENSION_FETCH_POLICY;
         } else {
            process.env.EXTENSION_FETCH_POLICY = originalPolicy;
         }
      });

      it(
         "pins autoinstall off on the :memory: sandbox under local-only",
         async () => {
            // Regression guard for the sandbox bypass: the per-package sandbox
            // has no attached databases, so the environment attach paths never
            // touch it. Under local-only its session must still be pinned
            // against DuckDB's implicit auto-install, or a sandbox query
            // referencing an unbaked extension could silently fetch from the
            // CDN. Uses a spy (not a stub) so real DuckDB runs the pragma.
            process.env.EXTENSION_FETCH_POLICY = "local-only";
            const spy = sinon.spy(DuckDBConnection.prototype, "runSQL");
            await Package.create(
               "testProject",
               "testPackage",
               testPackageDirectory,
               new Map(),
            );
            const issued = spy.getCalls().map((c) => String(c.args[0]));
            expect(
               issued.some((s) =>
                  /autoinstall_known_extensions\s*=\s*false/i.test(s),
               ),
            ).toBe(true);
         },
         { timeout: 30000 },
      );
   });

   describe("instance methods", () => {
      describe("create", () => {
         it("should throw PackageNotFoundError if the package manifest does not exist", async () => {
            await fs.rm(join(testPackageDirectory, "publisher.json"));
            sinon.stub(fs, "stat").rejects(new Error("File not found"));

            await expect(
               Package.create(
                  "testProject",
                  "testPackage",
                  testPackageDirectory,
                  new Map(),
               ),
            ).rejects.toBeInstanceOf(PackageNotFoundError);
         });
         it(
            "should return a Package object if the package exists",
            async () => {
               sinon.stub(fs, "stat").resolves();
               const readFileStub = sinon
                  .stub(fs, "readFile")
                  .resolves(
                     Buffer.from(
                        JSON.stringify({ description: "Test package" }),
                     ),
                  );

               // Still use Partial<Model> for the stub resolution type
               sinon
                  .stub(Model, "create")
                  // @ts-expect-error PartialModel is a partial type
                  .resolves({ getPath: () => "model1.model" } as PartialModel);

               readFileStub.restore();
               readFileStub.resolves(Buffer.from(JSON.stringify([])));

               const packageInstance = await Package.create(
                  "testProject",
                  "testPackage",
                  testPackageDirectory,
                  new Map(),
               );

               expect(packageInstance).toBeInstanceOf(Package);
               expect(packageInstance.getPackageName()).toBe("testPackage");
               expect(packageInstance.getPackageMetadata().description).toBe(
                  "Test package",
               );
               expect(packageInstance.listDatabases()).toEqual([
                  {
                     path: "database.csv",
                     type: "embedded",
                     info: {
                        name: "database.csv",
                        columns: [
                           { name: "Name", type: "string" },
                           { name: "Value", type: "number" },
                        ],
                        rowCount: 3,
                     },
                  },
               ]);
               expect(packageInstance.listModels()).toBeEmpty();
            },
            { timeout: 20000 },
         );

         it(
            "uses package-root-relative DuckDB file paths for nested models",
            async () => {
               await fs.mkdir(join(testPackageDirectory, "data"), {
                  recursive: true,
               });
               await fs.mkdir(join(testPackageDirectory, "models"), {
                  recursive: true,
               });
               await fs.writeFile(
                  join(testPackageDirectory, "data", "root.csv"),
                  "name,value\nalpha,1\nbeta,2\n",
               );
               await fs.writeFile(
                  join(testPackageDirectory, "models", "root_read.malloy"),
                  [
                     "source: rows is duckdb.table('data/root.csv')",
                     "query: row_count is rows -> { aggregate: c is count() }",
                  ].join("\n"),
               );

               const absolutePackageDirectory = resolve(testPackageDirectory);
               const packageInstance = await Package.create(
                  "testProject",
                  "testPackage",
                  absolutePackageDirectory,
                  new Map(),
               );
               try {
                  const rootModel = packageInstance.getModel(
                     "models/root_read.malloy",
                  );
                  expect(rootModel).toBeDefined();
                  const rootResults = await rootModel!.getQueryResults(
                     undefined,
                     "row_count",
                  );
                  expect(
                     (rootResults.compactResult as { c: number }[])[0]?.c,
                  ).toBe(2);
               } finally {
                  await packageInstance.getMalloyConfig().releaseConnections();
               }
            },
            { timeout: 20000 },
         );

         it(
            "does not resolve DuckDB file paths relative to the model directory",
            async () => {
               await fs.mkdir(join(testPackageDirectory, "models"), {
                  recursive: true,
               });
               await fs.writeFile(
                  join(testPackageDirectory, "models", "sibling.csv"),
                  "name,value\nnested,3\n",
               );
               await fs.writeFile(
                  join(testPackageDirectory, "models", "sibling_read.malloy"),
                  [
                     "source: rows is duckdb.table('./sibling.csv')",
                     "query: row_count is rows -> { aggregate: c is count() }",
                  ].join("\n"),
               );

               await expect(
                  Package.create(
                     "testProject",
                     "testPackage",
                     resolve(testPackageDirectory),
                     new Map(),
                  ),
               ).rejects.toThrow();
            },
            { timeout: 20000 },
         );

         it(
            "does not treat package-root paths as filesystem isolation",
            async () => {
               const outsidePath = resolve(
                  testPackageDirectory,
                  "..",
                  "outside-package.csv",
               );
               await fs.mkdir(join(testPackageDirectory, "models"), {
                  recursive: true,
               });
               await fs.writeFile(outsidePath, "name,value\noutside,9\n");
               await fs.writeFile(
                  join(testPackageDirectory, "models", "outside_read.malloy"),
                  [
                     "source: rows is duckdb.table('../outside-package.csv')",
                     "query: row_count is rows -> { aggregate: c is count() }",
                  ].join("\n"),
               );

               let packageInstance: Package | undefined;
               try {
                  packageInstance = await Package.create(
                     "testProject",
                     "testPackage",
                     resolve(testPackageDirectory),
                     new Map(),
                  );
                  const outsideModel = packageInstance.getModel(
                     "models/outside_read.malloy",
                  );
                  expect(outsideModel).toBeDefined();
                  const outsideResults = await outsideModel!.getQueryResults(
                     undefined,
                     "row_count",
                  );
                  expect(
                     (outsideResults.compactResult as { c: number }[])[0]?.c,
                  ).toBe(1);
               } finally {
                  await packageInstance?.getMalloyConfig().releaseConnections();
                  await fs.rm(outsidePath, { force: true });
               }
            },
            { timeout: 20000 },
         );
      });

      describe("listModels", () => {
         it("should return a list of models with their paths and types", async () => {
            // Using 'as any' for simplified mock Map value in test
            const packageInstance = new Package(
               "testProject",
               "testPackage",
               testPackageDirectory,
               { name: "testPackage", description: "Test package" },
               [],
               new Map([
                  [
                     "model1.malloy",
                     {
                        getPath: () => "model1.malloy",
                        getModel: () => "foo",
                        setDiscoveryCuration: () => {},
                        setQueryBoundary: () => {},
                     } as unknown as Model,
                  ],
                  [
                     "model2.malloynb",
                     {
                        getPath: () => "model2.malloynb",
                        setDiscoveryCuration: () => {},
                        getNotebookError: () => {
                           return {
                              message: "This is the error",
                           };
                        },
                        // A notebook that failed to compile has no annotations
                        // and no cells, so it resolves no title or description
                        // and lists as its path.
                        getNotebookListing: () => ({}),
                        setQueryBoundary: () => {},
                     } as unknown as Model,
                  ],
               ]),
            );

            const models = await packageInstance.listModels();
            expect(models).toEqual([
               {
                  // @ts-expect-error TODO: Fix missing projectName type in API
                  environmentName: "testProject",
                  packageName: "testPackage",
                  path: "model1.malloy",
                  error: undefined,
               },
            ]);

            const notebooks = await packageInstance.listNotebooks();
            // No `@ts-expect-error` here, unlike listModels above: the
            // `Notebook` schema now declares `environmentName`, which the
            // response has always sent (issue #979).
            expect(notebooks).toEqual([
               {
                  environmentName: "testProject",
                  packageName: "testPackage",
                  path: "model2.malloynb",
                  error: "This is the error",
               },
            ]);
         });
      });

      describe("getDatabaseInfo", () => {
         it("should return the size of the database file", async () => {
            sinon.stub(fs, "stat").resolves({ size: 13 } as Stats);

            // `getDatabaseInfo` now requires the caller to pass in the
            // shared DuckDB connection (resolved once by `readDatabases`
            // off the package's MalloyConfig). For this isolated unit
            // test we mint a fresh ephemeral one — production paths
            // reuse a single connection per package via `Package.create`.
            const conn = new DuckDBConnection("duckdb");

            // @ts-expect-error Accessing private static method for testing
            const info = await Package.getDatabaseInfo(
               testPackageDirectory,
               "database.csv",
               conn,
            );

            expect(info).toEqual({
               name: "database.csv",
               columns: [
                  { name: "Name", type: "string" },
                  { name: "Value", type: "number" },
               ],
               rowCount: 3,
            });
         });

         it("should probe an xlsx database file", async () => {
            // Real committed fixture: DuckDB's excel extension reads it via the
            // same duckdb.table() replacement scan the csv path uses.
            await fs.copyFile(
               join("tests", "fixtures", "xlsx", "database.xlsx"),
               join(testPackageDirectory, "database.xlsx"),
            );
            const conn = new DuckDBConnection("duckdb");

            // Unlike parquet/csv (statically linked), excel is downloaded from
            // extensions.duckdb.org on demand. Some CI runners (notably the
            // hosted macOS fleet, per bake-duckdb-extensions.js) cannot reach
            // that CDN, so skip the read there rather than fail; the Linux e2e
            // and the extension-independent specs still cover this path.
            try {
               await conn.runSQL("INSTALL excel; LOAD excel;");
            } catch {
               console.warn(
                  "Skipping xlsx probe test: excel extension could not be installed (offline runner)",
               );
               await conn.close();
               return;
            }

            // @ts-expect-error Accessing private static method for testing
            const info = await Package.getDatabaseInfo(
               testPackageDirectory,
               "database.xlsx",
               conn,
            );

            expect(info).toEqual({
               name: "database.xlsx",
               columns: [
                  { name: "Name", type: "string" },
                  // xlsx stores every number as a double, so numeric columns
                  // always surface as Malloy `number`.
                  { name: "Value", type: "number" },
               ],
               rowCount: 3,
            });
         });
      });

      describe("getDatabasePaths", () => {
         it("should list xlsx files alongside parquet and csv", async () => {
            await fs.copyFile(
               join("tests", "fixtures", "xlsx", "database.xlsx"),
               join(testPackageDirectory, "database.xlsx"),
            );

            // @ts-expect-error Accessing private static method for testing
            const paths = await Package.getDatabasePaths(testPackageDirectory);

            expect(paths.sort()).toEqual(["database.csv", "database.xlsx"]);
         });

         it("should skip Excel owner files (~$name.xlsx)", async () => {
            // Excel creates this sibling while a workbook is open; it is not a
            // valid zip, so probing it would throw and drop the package.
            await fs.writeFile(
               join(testPackageDirectory, "~$database.xlsx"),
               "not a real xlsx",
            );

            // @ts-expect-error Accessing private static method for testing
            const paths = await Package.getDatabasePaths(testPackageDirectory);

            expect(paths).toEqual(["database.csv"]);
         });
      });

      describe("readDatabases", () => {
         it("should report an unreadable database file instead of failing the package", async () => {
            // A corrupt or partial spreadsheet must not take the whole package
            // down, but it must not vanish silently either: it stays in the
            // listing carrying `error` instead of `info`, the same way a model
            // that fails to compile is reported. Extension-independent:
            // broken.xlsx throws whether excel loads or not, so this holds on
            // runners without the CDN too.
            await fs.writeFile(
               join(testPackageDirectory, "broken.xlsx"),
               "not a real xlsx",
            );
            const conn = new DuckDBConnection("duckdb");
            const malloyConfig = {
               connections: { lookupConnection: async () => conn },
            } as unknown as MalloyConfig;

            const databases =
               // @ts-expect-error Accessing private static method for testing
               (await Package.readDatabases(
                  testPackageDirectory,
                  malloyConfig,
               )) as {
                  path: string;
                  info?: unknown;
                  error?: string;
               }[];

            // The valid file still loads with a full probe.
            const good = databases.find((d) => d.path === "database.csv");
            expect(good?.info).toBeDefined();
            expect(good?.error).toBeUndefined();

            // The broken one is visible over the API, with a reason.
            const bad = databases.find((d) => d.path === "broken.xlsx");
            expect(bad).toBeDefined();
            expect(bad?.info).toBeUndefined();
            expect(bad?.error).toBeTruthy();
         });
      });
   });

   // A host is authoritative about WHICH TABLE backs a source, never about
   // WHETHER the source may be served frozen — that is decided by compiling the
   // model. These assert the gate that enforces it, because the failure is silent:
   // an admitted binding for a `given`-filtered source serves one caller's rows to
   // everyone, and no error is raised. Scenarios
   // `host-binding-honors-row-level-access` and `host-binding-of-unplanned-source`
   // prove it end to end; these keep it from being refactored away.
   describe("bindStorageServeBindings eligibility gate", () => {
      /** A structurally complete manifest entry — the shape a host sends. */
      const entry = (sourceName: string, table: string) => ({
         sourceEntityId: `eid-${sourceName}`,
         sourceName,
         physicalTableName: table,
         storageDestinationName: "lake",
         schema: [{ name: "region", type: "VARCHAR" }],
      });

      const packageWith = (
         eligibility:
            | { eligible: string[]; refused: Record<string, string> }
            | undefined,
      ) => {
         const pkg = new Package(
            "testProject",
            "testPackage",
            testPackageDirectory,
            { name: "testPackage" },
            [],
            new Map(),
         );
         // Private by design: its only writer is the load path, which needs a
         // compiled package. Set directly to isolate the filter.
         (
            pkg as unknown as { sourceEligibility: typeof eligibility }
         ).sourceEligibility = eligibility;
         return pkg;
      };

      const boundSources = (pkg: Package) =>
         (pkg.getPackageMetadata().storageServeBindings ?? []).map(
            (b) => b.sourceName,
         );

      it("drops a refused source's binding and keeps its eligible neighbour", () => {
         const pkg = packageWith({
            eligible: ["all_rollup"],
            refused: { scoped_rollup: "references a given" },
         });

         pkg.bindStorageServeBindings({
            a: entry("all_rollup", "t_all"),
            b: entry("scoped_rollup", "t_all"),
         });

         expect(boundSources(pkg)).toEqual(["all_rollup"]);
      });

      it("refuses every binding when the build plan could not be computed", () => {
         // The plan compute does live schema RPCs, so a warehouse blip during load
         // is the ordinary way this happens — and the package still serves. An
         // unexamined binding must not read as an eligible one.
         const pkg = packageWith(undefined);

         pkg.bindStorageServeBindings({ a: entry("all_rollup", "t_all") });

         expect(boundSources(pkg)).toEqual([]);
      });

      it("refuses a source the plan never contained", () => {
         // Malloy admits only query-shaped sources as build roots, so `#@ persist`
         // on a filtered pass-through produces no plan entry — neither eligible nor
         // refused. Absence is not eligibility.
         const pkg = packageWith({ eligible: ["all_rows"], refused: {} });

         pkg.bindStorageServeBindings({
            a: entry("all_rows", "t_all"),
            b: entry("scoped", "t_all"),
         });

         expect(boundSources(pkg)).toEqual(["all_rows"]);
      });
   });

   describe("getDeclaredQueryMetadata", () => {
      const pkgWith = (metadata: Record<string, unknown>) =>
         new Package(
            "testProject",
            "testPackage",
            testPackageDirectory,
            { name: "testPackage", ...metadata } as never,
            [],
            new Map(),
         );

      it("reads the canonical top-level field", () => {
         expect(
            pkgWith({
               queryMetadata: { team: "finance" },
            }).getDeclaredQueryMetadata(),
         ).toEqual({ team: "finance" });
      });

      it("falls back to the deprecated materialization block", () => {
         // An un-migrated client PATCHes the block and nothing else, so dropping
         // the fallback would silently stop tagging that package's queries.
         expect(
            pkgWith({
               materialization: { queryMetadata: { team: "finance" } },
            }).getDeclaredQueryMetadata(),
         ).toEqual({ team: "finance" });
      });

      it("prefers the canonical field when both are present", () => {
         // Both homes are populated on the way out, so a client that sets only
         // the new one must not have a stale block win over it.
         expect(
            pkgWith({
               queryMetadata: { team: "finance" },
               materialization: { queryMetadata: { team: "stale" } },
            }).getDeclaredQueryMetadata(),
         ).toEqual({ team: "finance" });
      });

      it("is null when neither home carries a bag", () => {
         expect(pkgWith({}).getDeclaredQueryMetadata()).toBeNull();
         expect(
            pkgWith({
               materialization: { schedule: null },
            }).getDeclaredQueryMetadata(),
         ).toBeNull();
      });
   });
});
