/**
 * The property this whole split exists for: a storage destination is
 * reachable from the serve path and from nowhere a tenant can author.
 *
 * Driven through a REAL Package (the worker-pool harness the other package
 * integration specs use) loaded against a REAL Environment, wired exactly as
 * `Environment.loadPackage` wires it — the package resolves connections through
 * `getEnvironmentMalloyConfig`, and the serve shape through the destination
 * config. A stub of either side would prove nothing here, because the bug being
 * defended against is precisely those two being the same thing.
 *
 * How a name in scope is told apart from one that is not, with no live warehouse:
 * Malloy answers an unknown name with `No connection named "…" found in config`
 * before any I/O, while a name it does hold gets past resolution and fails trying
 * to reach the warehouse. The destinations here point at a catalog that does not
 * exist, so "resolved, then could not connect" is the observable proof that a name
 * IS in a namespace. The end-to-end pair — a real catalog, a real materialized
 * table, a real routed query — lives in the hammer scenarios.
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

import { components } from "../api";
import { ConnectionNotFoundError } from "../errors";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import {
   buildEnvironmentMalloyConfig,
   restrictMalloyConfigToConnections,
} from "./connection";
import { Environment } from "./environment";
import { Package } from "./package";

type ApiConnection = components["schemas"]["Connection"];

/**
 * A destination with no connection of the same name — the managed-tier case, and
 * the one the compile assertions use. A name shared with a connection is a
 * different question (a tenant reaching their OWN warehouse is legitimate) and
 * has its own test at the bottom.
 */
const DESTINATION_NAME = "managed";
/** A name deliberately held by BOTH lists. */
const SHARED_NAME = "shared";

/** What Malloy says when a name is not in the config it was handed. */
const NOT_IN_CONFIG = `No connection named "${DESTINATION_NAME}" found in config`;

/** A DuckLake destination whose catalog is not reachable. */
function ducklakeDestination(name: string): ApiConnection {
   return {
      name,
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "127.0.0.1",
               port: 5,
               databaseName: "no_such_catalog",
               userName: "publisher",
               password: "catalog-secret",
            },
         },
         storage: { bucketUrl: "/tmp/no-such-bucket" },
      },
   };
}

const ORIGINAL_WORKERS = process.env.PACKAGE_LOAD_WORKERS;

describe("a storage destination is not in the namespace a tenant authors in", () => {
   let tempDir: string;
   let envDir: string;
   let pool: PackageLoadPool;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_WORKERS === undefined)
         delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_WORKERS;
   });

   beforeEach(() => {
      envDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-dest-env-"));
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-dest-pkg-"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
   });

   afterEach(() => {
      for (const dir of [tempDir, envDir]) {
         try {
            fs.rmSync(dir, { recursive: true, force: true });
         } catch {
            /* already gone */
         }
      }
   });

   /**
    * An Environment holding two destinations — one the connection list has no
    * name for, one it deliberately shares a name with — plus the tenant's own
    * connection under that shared name.
    */
   function makeEnvironment(): Environment {
      const tenantConnection: ApiConnection = {
         name: SHARED_NAME,
         type: "postgres",
         postgresConnection: {
            host: "tenant.example.com",
            port: 5432,
            databaseName: "analytics",
            userName: "tenant",
            password: "tenant-secret",
         },
      };
      const malloyConfig = buildEnvironmentMalloyConfig(
         [tenantConnection],
         envDir,
      );
      return new Environment(
         "env",
         envDir,
         malloyConfig,
         malloyConfig.apiConnections,
         [
            ducklakeDestination(DESTINATION_NAME),
            ducklakeDestination(SHARED_NAME),
         ],
      );
   }

   function writeModel(name: string, text: string): void {
      fs.writeFileSync(path.join(tempDir, name), text);
   }

   /**
    * Why the given model does not work, as a message. Loading the package is what
    * fails when a model cannot resolve a connection — a package with such a model
    * does not serve at all — but a per-model or per-cell error is caught too, so
    * the assertion does not depend on which layer refuses first.
    */
   async function whyItFails(
      environment: Environment,
      modelPath: string,
   ): Promise<string> {
      let pkg: Package;
      try {
         pkg = await Package.create("env", "pkg", tempDir, () =>
            environment.getEnvironmentMalloyConfig(),
         );
      } catch (error) {
         return String((error as Error).message);
      }
      const model = pkg.getModel(modelPath);
      if (!model) return "the package loaded without the model";
      const recorded = model.getNotebookError();
      if (recorded) return String(recorded.message);
      try {
         if (modelPath.endsWith(".malloynb")) await model.getNotebook();
         else await model.getModel();
      } catch (error) {
         return String((error as Error).message);
      }
      return "";
   }

   it("fails to compile a model file whose source names the destination", async () => {
      // Hole (2)'s first authoring surface. The package connection lookup passes
      // any name but `duckdb` to the environment config, so before the lists were
      // split this resolved on the worker with no control-plane round-trip at all.
      const environment = makeEnvironment();
      writeModel(
         "names_destination.malloy",
         `source: stolen is ${DESTINATION_NAME}.table('org_abc.orders__g000')`,
      );

      const why = await whyItFails(environment, "names_destination.malloy");

      // A `table()` reference dies in the schema fetch, which reports the
      // unresolvable connection as an import-reference failure rather than
      // repeating Malloy's config message — so this one pins the refusal and the
      // line it refused, and the `sql()` form below pins the reason.
      expect(why).toContain("import reference failure");
      expect(why).toContain(
         `${DESTINATION_NAME}.table('org_abc.orders__g000')`,
      );
      expect(why).not.toContain("catalog-secret");
   });

   it("fails to compile a model file that embeds SQL against the destination", async () => {
      const environment = makeEnvironment();
      writeModel(
         "sql_destination.malloy",
         `source: stolen is ${DESTINATION_NAME}.sql('SELECT * FROM org_abc.orders__g000')`,
      );

      const why = await whyItFails(environment, "sql_destination.malloy");

      expect(why).toContain(NOT_IN_CONFIG);
   });

   it("fails to compile a notebook cell that names the destination", async () => {
      // The second authoring surface, and a distinct compile: notebook cells go
      // through `loadQuery`, not the restricted variant a query string gets, so
      // restricted mode is not what stops this one.
      const environment = makeEnvironment();
      writeModel(
         "names_destination.malloynb",
         [
            ">>>markdown",
            "# Reaching for the destination",
            "",
            ">>>malloy",
            `run: ${DESTINATION_NAME}.sql('SELECT 1 AS x') -> { select: x }`,
         ].join("\n"),
      );

      const why = await whyItFails(environment, "names_destination.malloynb");

      expect(why).toContain(NOT_IN_CONFIG);
   });

   it("rejects an ad-hoc query naming the destination", async () => {
      // Nearly free next to the two above, and it must not be skipped on the
      // strength of restricted mode rejecting `connection.sql(...)`: that is a
      // neighbouring layer's guarantee, and the disjoint list should be what holds
      // if it is ever relaxed. Asserts the failure, not the reason.
      const environment = makeEnvironment();
      writeModel("plain.malloy", `source: nums is duckdb.sql("SELECT 1 AS a")`);

      const pkg = await Package.create("env", "pkg", tempDir, () =>
         environment.getEnvironmentMalloyConfig(),
      );

      await expect(
         pkg
            .getModel("plain.malloy")!
            .getQueryResults(
               undefined,
               undefined,
               `run: ${DESTINATION_NAME}.sql('SELECT 1 AS x') -> { select: x }`,
            ),
      ).rejects.toThrow();
   });

   it("compiles the serve shape against the destinations, not the package's own connections", async () => {
      // What the Environment EXPOSES being disjoint is not the same claim as the
      // serve path being wired to the right one of the two. Without this, handing
      // `getEnvironmentMalloyConfig()` to the serve path — the whole bug — passes
      // every other test in this file.
      //
      // Told apart by which failure comes back for a binding onto the
      // destination: wired correctly, the name resolves and the unreachable
      // catalog is what fails; wired to the package's connections, the name is not
      // there at all.
      const environment = makeEnvironment();
      const packageDir = path.join(envDir, "pkg");
      fs.mkdirSync(packageDir, { recursive: true });
      fs.writeFileSync(
         path.join(packageDir, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
      fs.writeFileSync(
         path.join(packageDir, "plain.malloy"),
         `source: nums is duckdb.sql("SELECT 1 AS a")`,
      );

      // The real production wiring: the environment's own load path is what
      // attaches the serve config.
      const pkg = await environment.getPackage("pkg");
      const model = pkg.getModel("plain.malloy")!;
      model.setServeBindings([
         {
            sourceName: "nums",
            destinationName: DESTINATION_NAME,
            virtualHandle: "h",
            tablePath: `${DESTINATION_NAME}.mz_nums`,
            schema: [{ name: "a", type: "BIGINT" }],
         },
      ]);

      // The load handed the model a provider, and it yields the DESTINATION
      // config — not the one the package's own models fall through to. Asserted by
      // identity because that is the actual question: Malloy reports an
      // unresolvable connection and an unreachable one with the same
      // "cannot determine dialect" text, so no error message can tell these two
      // wirings apart.
      const serveProvider = (
         model as unknown as {
            serveDestinationConfig?: () => unknown;
         }
      ).serveDestinationConfig;
      expect(serveProvider).toBeDefined();
      expect(serveProvider!()).toBe(
         environment.getStorageDestinationMalloyConfig(),
      );
      expect(serveProvider!()).not.toBe(
         environment.getEnvironmentMalloyConfig(),
      );

      // And it is live wiring, not a captured snapshot: replacing the destination
      // list is reflected without reloading the package.
      const before = serveProvider!();
      environment.setStorageDestinations([
         ducklakeDestination(DESTINATION_NAME),
      ]);
      expect(serveProvider!()).not.toBe(before);
      expect(serveProvider!()).toBe(
         environment.getStorageDestinationMalloyConfig(),
      );

      // A memoized serve shape holds the connections of the generation it compiled
      // against. Replacing the destination list retires those, so the memo has to
      // be dropped — its key is the BINDING set, which a destination change does
      // not touch, so nothing else would invalidate it and every routed query for
      // this package would fail over to live until the package reloaded.
      const cacheOf = () =>
         (model as unknown as { serveShapeCache?: unknown }).serveShapeCache;
      try {
         await (
            model as unknown as {
               loadServeShapeQuery: (query: string) => Promise<unknown>;
            }
         ).loadServeShapeQuery("run: nums -> { select: a }");
      } catch {
         // The catalog is unreachable, but the compile still memoizes a shape.
      }
      expect(cacheOf()).toBeDefined();
      environment.setStorageDestinations([
         ducklakeDestination(DESTINATION_NAME),
         ducklakeDestination(SHARED_NAME),
         ducklakeDestination("added_later"),
      ]);
      expect(cacheOf()).toBeUndefined();

      // A reload rebuilds every model, so it has to re-apply the wiring — or serve
      // routing would quietly stop at the first reload.
      await pkg.reloadAllModels({});
      const reloaded = pkg.getModel("plain.malloy")!;
      const reloadedProvider = (
         reloaded as unknown as { serveDestinationConfig?: () => unknown }
      ).serveDestinationConfig;
      expect(reloadedProvider).toBeDefined();
      expect(reloadedProvider!()).toBe(
         environment.getStorageDestinationMalloyConfig(),
      );

      // Behaviourally, the shape does reach for the destination: it gets as far as
      // needing that connection's dialect, which it can only ask for after the
      // name has resolved.
      model.setServeBindings([
         {
            sourceName: "nums",
            destinationName: DESTINATION_NAME,
            virtualHandle: "h",
            tablePath: `${DESTINATION_NAME}.mz_nums`,
            schema: [{ name: "a", type: "BIGINT" }],
         },
      ]);
      let why = "";
      try {
         await (
            model as unknown as {
               loadServeShapeQuery: (query: string) => Promise<unknown>;
            }
         ).loadServeShapeQuery("run: nums -> { select: a }");
      } catch (error) {
         why = String((error as Error).message);
      }
      expect(why).toContain(
         `Cannot determine dialect for connection '${DESTINATION_NAME}'`,
      );
   });

   it("keeps the two compiles' connection namespaces disjoint", async () => {
      // The guard against a green suite that means nothing: if the serve config
      // and the config a package falls through to were the same object, the tests
      // above would fail — and if the serve config simply had no destinations,
      // they would pass while serve routing was silently dead. Both halves are
      // asserted here, on one Environment.
      const environment = makeEnvironment();

      expect(environment.getStorageDestinationMalloyConfig()).not.toBe(
         environment.getEnvironmentMalloyConfig(),
      );

      // The tenant-facing namespace does not hold the name at all: refused at
      // resolution, before any connection is attempted.
      let userSide = "";
      try {
         await environment
            .getEnvironmentMalloyConfig()
            .connections.lookupConnection(DESTINATION_NAME);
      } catch (error) {
         userSide = String((error as Error).message);
      }
      expect(userSide).toContain(NOT_IN_CONFIG);

      // The serve-side namespace does hold it: resolution gets through, and the
      // failure is the unreachable catalog behind it.
      let serveSide = "";
      try {
         await environment
            .getStorageDestinationMalloyConfig()
            .connections.lookupConnection(DESTINATION_NAME);
      } catch (error) {
         serveSide = String((error as Error).message);
      }
      expect(serveSide).not.toContain(NOT_IN_CONFIG);
   });

   it("lets a serve shape reach only the destinations its own bindings name", async () => {
      // The narrowing is derived from the binding set rather than handing over the
      // whole destination list, so a shape cannot reach a sibling destination it
      // has no binding for — including one belonging to another package.
      const environment = makeEnvironment();
      const destinations = environment.getStorageDestinationMalloyConfig();
      const narrowed = restrictMalloyConfigToConnections(
         destinations,
         new Set([DESTINATION_NAME]),
      );

      let boundError = "";
      try {
         await narrowed.connections.lookupConnection(DESTINATION_NAME);
      } catch (error) {
         boundError = String((error as Error).message);
      }
      // Delegated: the failure is the unreachable catalog, not the narrowing.
      expect(boundError).not.toContain("not available to a materialization");

      // The sibling destination is configured on this environment and still out of
      // reach, because no binding named it.
      await expect(
         narrowed.connections.lookupConnection(SHARED_NAME),
      ).rejects.toThrow(ConnectionNotFoundError);
   });

   it("keeps a user connection resolvable under a name a destination also holds", async () => {
      // The reverse direction: splitting the lists must not cost the tenant the
      // connection they own, even when a destination shares its name. A model
      // naming it reaches THEIR warehouse — which is why the compile assertions
      // above use a name only the destination list holds.
      const environment = makeEnvironment();

      expect(environment.getApiConnection(SHARED_NAME).type).toBe("postgres");
      expect(environment.getStorageDestination(SHARED_NAME).type).toBe(
         "ducklake",
      );
   });
});
