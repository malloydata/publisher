import { beforeEach, afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import { components } from "../api";
import { getProcessedPublisherConfig } from "../config";
import { PUBLISHER_CONFIG_NAME } from "../constants";
import {
   ConnectionNotFoundError,
   DestinationNotFoundError,
   internalErrorToHttpError,
} from "../errors";
import { ConnectionController } from "../controller/connection.controller";
import { buildEnvironmentMalloyConfig } from "./connection";
import {
   assembleEnvironmentConnections,
   MATERIALIZATION_DESTINATIONS_DIR,
   materializationDestinationRoot,
   processMaterializationDestinations,
} from "./connection_config";
import { Environment } from "./environment";
import type { EnvironmentStore } from "./environment_store";

type ApiConnection = components["schemas"]["Connection"];

/**
 * A materialization destination is a warehouse the build path writes to and the
 * serve path reads from, and it must be reachable from neither of the two
 * namespaces a tenant can name: the connection endpoints and a model's
 * connection references. These tests pin the first half of that — the lists are
 * disjoint, in both directions, and a name in one says nothing about the other.
 */
function ducklakeDestination(
   name: string,
   bucketPath: string,
   password = "catalog-secret",
): ApiConnection {
   return {
      name,
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "catalog.internal",
               port: 5432,
               databaseName: "ducklake",
               userName: "publisher",
               password,
            },
         },
         storage: { bucketUrl: `gs://managed-tier/${bucketPath}` },
      },
   };
}

function tenantPostgresConnection(name: string): ApiConnection {
   return {
      name,
      type: "postgres",
      postgresConnection: {
         host: "tenant.example.com",
         port: 5432,
         databaseName: "analytics",
         userName: "tenant",
         password: "tenant-secret",
      },
   };
}

describe("processMaterializationDestinations", () => {
   it("accepts a well-formed DuckLake destination and drops its resource path", () => {
      const accepted = processMaterializationDestinations([
         {
            ...ducklakeDestination("managed", "org_a"),
            resource: "/api/v0/connections/managed",
         },
      ]);

      expect(accepted).toHaveLength(1);
      expect(accepted[0].name).toBe("managed");
      expect(accepted[0].type).toBe("ducklake");
      expect(accepted[0].resource).toBeUndefined();
      expect(accepted[0].ducklakeConnection?.storage?.bucketUrl).toBe(
         "gs://managed-tier/org_a",
      );
   });

   it("drops a type outside the supported destination set", () => {
      expect(
         processMaterializationDestinations([
            tenantPostgresConnection("managed"),
         ]),
      ).toEqual([]);
   });

   it("drops a DuckLake destination missing the fields its attach needs", () => {
      const noBucket = ducklakeDestination("managed", "org_a");
      delete noBucket.ducklakeConnection!.storage;

      expect(processMaterializationDestinations([noBucket])).toEqual([]);
   });

   it("keeps the first of two destinations sharing a name", () => {
      const accepted = processMaterializationDestinations([
         ducklakeDestination("managed", "org_a"),
         ducklakeDestination("managed", "org_b"),
      ]);

      expect(accepted).toHaveLength(1);
      expect(accepted[0].ducklakeConnection?.storage?.bucketUrl).toBe(
         "gs://managed-tier/org_a",
      );
   });

   it("drops a destination named duckdb, which the package sandbox would shadow", () => {
      expect(
         processMaterializationDestinations([
            ducklakeDestination("duckdb", "org_a"),
         ]),
      ).toEqual([]);
   });

   it("drops entries that are not objects at all", () => {
      expect(
         processMaterializationDestinations([
            null,
            "managed",
         ] as unknown as ApiConnection[]),
      ).toEqual([]);
   });
});

describe("Environment: connections and materialization destinations are disjoint", () => {
   let envPath: string;

   beforeEach(() => {
      envPath = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-destinations-"),
      );
   });

   afterEach(() => {
      fs.rmSync(envPath, { recursive: true, force: true });
      sinon.restore();
   });

   function makeEnvironment(
      connections: ApiConnection[],
      destinations: ApiConnection[],
   ): Environment {
      const malloyConfig = buildEnvironmentMalloyConfig(connections, envPath);
      return new Environment(
         "test-env",
         envPath,
         malloyConfig,
         malloyConfig.apiConnections,
         destinations,
      );
   }

   it("omits destinations from the connection list", () => {
      const environment = makeEnvironment(
         [tenantPostgresConnection("warehouse")],
         [ducklakeDestination("managed", "org_a")],
      );

      expect(environment.listApiConnections().map((c) => c.name)).toEqual([
         "warehouse",
      ]);
      expect(environment.listMaterializationDestinations()).toHaveLength(1);
   });

   it("answers getApiConnection for a destination exactly as for a name that does not exist", () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      let destinationError: unknown;
      try {
         environment.getApiConnection("managed");
      } catch (error) {
         destinationError = error;
      }
      let unknownError: unknown;
      try {
         environment.getApiConnection("no_such_connection");
      } catch (error) {
         unknownError = error;
      }

      expect(destinationError).toBeInstanceOf(ConnectionNotFoundError);
      expect(unknownError).toBeInstanceOf(ConnectionNotFoundError);
      // Same class, same 404, and a message that differs only by the name the
      // caller asked for: nothing here tells a prober that "managed" exists.
      expect((destinationError as Error).message).toBe(
         "Connection managed not found",
      );
      expect(internalErrorToHttpError(destinationError as Error).status).toBe(
         internalErrorToHttpError(unknownError as Error).status,
      );
   });

   it("refuses to resolve a connection name as a destination", () => {
      const environment = makeEnvironment(
         [tenantPostgresConnection("warehouse")],
         [],
      );

      expect(() =>
         environment.getMaterializationDestination("warehouse"),
      ).toThrow(DestinationNotFoundError);
      expect(environment.hasMaterializationDestination("warehouse")).toBe(
         false,
      );
   });

   it("maps a missing destination to 422, not to a connection 404", () => {
      const environment = makeEnvironment([], []);

      let error: unknown;
      try {
         environment.getMaterializationDestination("managed");
      } catch (caught) {
         error = caught;
      }

      expect(internalErrorToHttpError(error as Error).status).toBe(422);
   });

   it("keeps a connection and a destination that share a name independent", () => {
      const environment = makeEnvironment(
         [tenantPostgresConnection("credible")],
         [ducklakeDestination("credible", "org_a")],
      );

      const connection = environment.getApiConnection("credible");
      const destination = environment.getMaterializationDestination("credible");

      expect(connection.type).toBe("postgres");
      expect(destination.type).toBe("ducklake");
      expect(connection.postgresConnection?.host).toBe("tenant.example.com");
      expect(
         destination.ducklakeConnection?.catalog?.postgresConnection?.host,
      ).toBe("catalog.internal");
   });

   it("derives a destination's DuckDB file apart from a same-named connection's", () => {
      // A pooled DuckDB instance is keyed on `databasePath` + `workingDirectory`
      // and NOT on the connection name, while both lists derive that path from
      // `<root>/<name>`. Same root plus the same name would be one shared
      // instance, and the attach is `ATTACH OR REPLACE … AS <name>` — so one
      // would replace the other's, across two namespaces that are allowed to
      // reuse a name. Different roots is what makes that unreachable.
      const connectionSide = assembleEnvironmentConnections(
         [ducklakeDestination("credible", "tenant_lake")],
         envPath,
      );
      const destinationSide = assembleEnvironmentConnections(
         [ducklakeDestination("credible", "org_a")],
         materializationDestinationRoot(envPath),
      );

      const connectionMeta = connectionSide.metadata.get("credible")!;
      const destinationMeta = destinationSide.metadata.get("credible")!;

      expect(destinationMeta.databasePath).not.toBe(
         connectionMeta.databasePath,
      );
      expect(destinationMeta.workingDirectory).not.toBe(
         connectionMeta.workingDirectory,
      );
      expect(destinationMeta.databasePath).toContain(
         MATERIALIZATION_DESTINATIONS_DIR,
      );
      expect(connectionMeta.databasePath).not.toContain(
         MATERIALIZATION_DESTINATIONS_DIR,
      );
   });

   it("opens two different database files for one name held by both lists", async () => {
      // The filesystem consequence of the derivation above, through a real
      // Environment: resolving the name on each side opens its own DuckDB file.
      // Both lookups fail — neither catalog exists — but the local database each
      // one opened is what the instance pool keys on, so two files is two
      // instances.
      const environment = makeEnvironment(
         [ducklakeDestination("credible", "tenant_lake")],
         [ducklakeDestination("credible", "org_a")],
      );

      for (const config of [
         environment.getEnvironmentMalloyConfig(),
         environment.getMaterializationDestinationMalloyConfig(),
      ]) {
         try {
            await config.connections.lookupConnection("credible");
         } catch {
            // Expected: the catalogs behind both are unreachable.
         }
      }

      expect(
         fs.existsSync(path.join(envPath, "credible_ducklake.duckdb")),
      ).toBe(true);
      expect(
         fs.existsSync(
            path.join(
               materializationDestinationRoot(envPath),
               "credible_ducklake.duckdb",
            ),
         ),
      ).toBe(true);
   });

   it("reports a destination as name and type, and never its config", async () => {
      const environment = makeEnvironment(
         [tenantPostgresConnection("warehouse")],
         [ducklakeDestination("managed", "org_a")],
      );

      const serialized = await environment.serialize();

      expect(serialized.materializationDestinations).toEqual([
         { name: "managed", type: "ducklake" },
      ]);
      const payload = JSON.stringify(serialized);
      expect(payload).not.toContain("catalog-secret");
      expect(payload).not.toContain("catalog.internal");
      expect(payload).not.toContain("managed-tier");
   });

   it("reports an empty list when no destination is configured", async () => {
      const environment = makeEnvironment([], []);

      expect(
         (await environment.serialize()).materializationDestinations,
      ).toEqual([]);
   });

   it("keeps stored configs when an update echoes back what a read reported", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );
      const reported = (await environment.serialize())
         .materializationDestinations;

      await environment.update({
         name: "test-env",
         materializationDestinations: reported,
      });

      expect(
         environment.getMaterializationDestination("managed").ducklakeConnection
            ?.storage?.bucketUrl,
      ).toBe("gs://managed-tier/org_a");
   });

   it("does not admit a config-less destination for a name it does not hold", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      await environment.update({
         name: "test-env",
         materializationDestinations: [
            { name: "managed", type: "ducklake" },
            { name: "invented", type: "ducklake" },
         ],
      });

      expect(
         environment.listMaterializationDestinations().map((d) => d.name),
      ).toEqual(["managed"]);
   });

   it("leaves destinations alone when an update carries only connections", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      await environment.update({
         name: "test-env",
         connections: [tenantPostgresConnection("warehouse")],
      });

      expect(environment.listApiConnections().map((c) => c.name)).toEqual([
         "warehouse",
      ]);
      expect(environment.getMaterializationDestination("managed").type).toBe(
         "ducklake",
      );
   });

   it("replaces the destination list when an update carries one", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      await environment.update({
         name: "test-env",
         materializationDestinations: [ducklakeDestination("other", "org_b")],
      });

      expect(environment.hasMaterializationDestination("managed")).toBe(false);
      expect(environment.hasMaterializationDestination("other")).toBe(true);
   });

   it("validates destinations that arrive over the API, not just from config", async () => {
      const environment = makeEnvironment([], []);

      await environment.update({
         name: "test-env",
         materializationDestinations: [
            tenantPostgresConnection("managed"),
            ducklakeDestination("also_managed", "org_b"),
         ],
      });

      expect(
         environment.listMaterializationDestinations().map((d) => d.name),
      ).toEqual(["also_managed"]);
   });

   it("validates destinations handed to the constructor", () => {
      const environment = makeEnvironment(
         [],
         [
            tenantPostgresConnection("managed"),
            ducklakeDestination("ok", "org_a"),
         ],
      );

      expect(
         environment.listMaterializationDestinations().map((d) => d.name),
      ).toEqual(["ok"]);
   });

   it("does not list or fetch a destination through the connection endpoints", async () => {
      const environment = makeEnvironment(
         [tenantPostgresConnection("warehouse")],
         [ducklakeDestination("managed", "org_a")],
      );
      const store = {
         getEnvironment: sinon.stub().resolves(environment),
      } as unknown as EnvironmentStore;
      const controller = new ConnectionController(store);

      const listed = await controller.listConnections("test-env");
      expect(listed.map((c) => c.name)).toEqual(["warehouse"]);

      let error: unknown;
      try {
         await controller.getConnection("test-env", "managed");
      } catch (caught) {
         error = caught;
      }
      expect(error).toBeInstanceOf(ConnectionNotFoundError);
      expect(internalErrorToHttpError(error as Error).status).toBe(404);
   });
});

describe("publisher config round-trip", () => {
   let serverRoot: string;

   beforeEach(() => {
      serverRoot = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-destinations-config-"),
      );
   });

   afterEach(() => {
      fs.rmSync(serverRoot, { recursive: true, force: true });
   });

   function writeConfig(config: unknown): void {
      fs.writeFileSync(
         path.join(serverRoot, PUBLISHER_CONFIG_NAME),
         JSON.stringify(config),
      );
   }

   it("carries materializationDestinations through, including a name a connection already uses", () => {
      writeConfig({
         frozenConfig: false,
         environments: [
            {
               name: "examples",
               packages: [{ name: "storefront", location: "./storefront" }],
               connections: [tenantPostgresConnection("credible")],
               materializationDestinations: [
                  ducklakeDestination("credible", "org_a"),
               ],
            },
         ],
      });

      const processed = getProcessedPublisherConfig(serverRoot);
      const environment = processed.environments[0];

      expect(environment.connections.map((c) => c.name)).toEqual(["credible"]);
      expect(
         environment.materializationDestinations.map((d) => d.name),
      ).toEqual(["credible"]);
   });

   it("reports no destinations when the key is absent or not a list", () => {
      writeConfig({
         frozenConfig: false,
         environments: [
            {
               name: "examples",
               packages: [{ name: "storefront", location: "./storefront" }],
            },
            {
               name: "other",
               packages: [{ name: "storefront", location: "./storefront" }],
               materializationDestinations: "credible",
            },
         ],
      });

      const processed = getProcessedPublisherConfig(serverRoot);

      expect(processed.environments[0].materializationDestinations).toEqual([]);
      expect(processed.environments[1].materializationDestinations).toEqual([]);
   });
});
