import { beforeEach, afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import { components } from "../api";
import { getProcessedPublisherConfig } from "../config";
import { PUBLISHER_CONFIG_NAME } from "../constants";
import {
   BadRequestError,
   ConnectionNotFoundError,
   DestinationNotFoundError,
   internalErrorToHttpError,
} from "../errors";
import { ConnectionController } from "../controller/connection.controller";
import { logger } from "../logger";
import { buildEnvironmentMalloyConfig } from "./connection";
import {
   assembleEnvironmentConnections,
   STORAGE_DESTINATIONS_DIR,
   storageDestinationRoot,
   processStorageDestinations,
   storageDestinationsEqual,
} from "./connection_config";
import { Environment } from "./environment";
import type { EnvironmentStore } from "./environment_store";

type ApiConnection = components["schemas"]["Connection"];

/**
 * A storage destination is a warehouse the build path writes to and the
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

/**
 * The same destination with its keys emitted in a different order, which is what
 * an equivalent config assembled or parsed somewhere else looks like.
 */
function withReorderedKeys(destination: ApiConnection): ApiConnection {
   const ducklake = destination.ducklakeConnection!;
   return {
      ducklakeConnection: {
         storage: ducklake.storage,
         catalog: ducklake.catalog,
      },
      type: destination.type,
      name: destination.name,
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

describe("processStorageDestinations", () => {
   it("accepts a well-formed DuckLake destination and drops its resource path", () => {
      const accepted = processStorageDestinations([
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
         processStorageDestinations([tenantPostgresConnection("managed")]),
      ).toEqual([]);
   });

   it("drops a DuckLake destination missing the fields its attach needs", () => {
      // A config that arrived without its storage block. The schema marks the
      // field required, which is exactly why the validator has to check it: a
      // request body and a config file are not type-checked on the way in.
      const noBucket = ducklakeDestination("managed", "org_a");
      delete (noBucket.ducklakeConnection as unknown as Record<string, unknown>)
         .storage;

      expect(processStorageDestinations([noBucket])).toEqual([]);
   });

   it("keeps the first of two destinations sharing a name", () => {
      const accepted = processStorageDestinations([
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
         processStorageDestinations([ducklakeDestination("duckdb", "org_a")]),
      ).toEqual([]);
   });

   it("inherits every rule the connection validator applies to a DuckLake", () => {
      // Destinations go through `validateConnectionShape`, the same function a
      // connection goes through, so a rule added there covers both lists with no
      // change here. Pinned because that inheritance is invisible at this call
      // site: a rule reached only by connections would leave destinations as the
      // one unvalidated way to define a warehouse. `metadataSchema` is the live
      // example — it must be a plain identifier, since it reaches the ATTACH as a
      // literal and the catalog-format preflight as an identifier.
      const destination = ducklakeDestination("managed", "org_a");
      destination.ducklakeConnection!.catalog.metadataSchema =
         'lake"; drop schema public; --';

      expect(processStorageDestinations([destination])).toEqual([]);
   });

   it("keeps a destination naming its own metadata schema", () => {
      const destination = ducklakeDestination("managed", "org_a");
      destination.ducklakeConnection!.catalog.metadataSchema = "org_a_catalog";

      expect(
         processStorageDestinations([destination])[0]?.ducklakeConnection
            ?.catalog.metadataSchema,
      ).toBe("org_a_catalog");
   });

   it("drops entries that are not objects at all", () => {
      expect(
         processStorageDestinations([
            null,
            "managed",
         ] as unknown as ApiConnection[]),
      ).toEqual([]);
   });
});

describe("storageDestinationsEqual", () => {
   const managed = ducklakeDestination("managed", "org_a");
   const archive = ducklakeDestination("archive", "org_b");

   it("ignores the order the destinations are listed in", () => {
      expect(
         storageDestinationsEqual([managed, archive], [archive, managed]),
      ).toBe(true);
   });

   it("ignores the key order within a config", () => {
      expect(
         storageDestinationsEqual([managed], [withReorderedKeys(managed)]),
      ).toBe(true);
   });

   it("reports a changed bucket, a changed catalog and a rotated password", () => {
      expect(
         storageDestinationsEqual(
            [managed],
            [ducklakeDestination("managed", "org_b")],
         ),
      ).toBe(false);

      const rehosted = ducklakeDestination("managed", "org_a");
      rehosted.ducklakeConnection!.catalog!.postgresConnection!.host =
         "other.internal";
      expect(storageDestinationsEqual([managed], [rehosted])).toBe(false);

      expect(
         storageDestinationsEqual(
            [managed],
            [ducklakeDestination("managed", "org_a", "rotated-secret")],
         ),
      ).toBe(false);
   });

   it("reports a destination added, removed or renamed", () => {
      expect(storageDestinationsEqual([managed], [managed, archive])).toBe(
         false,
      );
      expect(storageDestinationsEqual([managed], [])).toBe(false);
      expect(
         storageDestinationsEqual(
            [managed],
            [ducklakeDestination("renamed", "org_a")],
         ),
      ).toBe(false);
   });

   it("does not distinguish a field set to undefined from one left out", () => {
      // Not a correctness requirement either way — recorded because it is the one
      // place the comparison is coarser than a deep equality: `JSON.stringify`
      // omits an undefined value, so the two read as equal. That is the harmless
      // direction (an attach reads both as absent), and it is not the direction a
      // config change arrives from.
      expect(
         storageDestinationsEqual(
            [managed],
            [{ ...managed, fingerprint: undefined }],
         ),
      ).toBe(true);
   });
});

describe("Environment: connections and storage destinations are disjoint", () => {
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

   it("never writes a destination's catalog password to a log line", async () => {
      // A destination carries warehouse credentials, and the paths that handle one
      // log freely: the list is logged when it is set, an unusable entry is logged
      // with the validator's reason, and the DuckLake attach echoes the whole
      // catalog DSN into its own log line. That last one is why this is a test and
      // not a code reading — `redactPgSecrets` covers it today, and nothing else
      // proves it keeps covering a DESTINATION's DSN rather than only a
      // connection's.
      const levels = ["info", "warn", "error", "debug"] as const;
      const captured: unknown[][] = [];
      for (const level of levels) {
         sinon.stub(logger, level).callsFake((...args: unknown[]) => {
            captured.push(args);
            return logger as never;
         });
      }

      const environment = makeEnvironment(
         [],
         [
            ducklakeDestination("managed", "org_a", "sup3r-secret-catalog-pw"),
            // An entry that fails validation, so the reject path logs too.
            { name: "broken", type: "ducklake" },
         ],
      );
      await environment.serialize();
      try {
         await environment
            .getStorageDestinationMalloyConfig()
            .connections.lookupConnection("managed");
      } catch {
         // The catalog is not reachable; the attach attempt is what logs.
      }

      const logged = JSON.stringify(
         captured.map((args) =>
            args.map((arg) =>
               arg instanceof Error ? `${arg.message}\n${arg.stack}` : arg,
            ),
         ),
      );
      expect(captured.length).toBeGreaterThan(0);
      expect(logged).not.toContain("sup3r-secret-catalog-pw");
      // The name is not a secret and stays visible — an operator has to be able to
      // tell WHICH destination a line is about.
      expect(logged).toContain("managed");
   });

   it("omits destinations from the connection list", () => {
      const environment = makeEnvironment(
         [tenantPostgresConnection("warehouse")],
         [ducklakeDestination("managed", "org_a")],
      );

      expect(environment.listApiConnections().map((c) => c.name)).toEqual([
         "warehouse",
      ]);
      expect(environment.listStorageDestinations()).toHaveLength(1);
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

      expect(() => environment.getStorageDestination("warehouse")).toThrow(
         DestinationNotFoundError,
      );
      expect(environment.hasStorageDestination("warehouse")).toBe(false);
   });

   it("maps a missing destination to 422, not to a connection 404", () => {
      const environment = makeEnvironment([], []);

      let error: unknown;
      try {
         environment.getStorageDestination("managed");
      } catch (caught) {
         error = caught;
      }

      expect(internalErrorToHttpError(error as Error).status).toBe(422);
   });

   it("keeps a connection and a destination that share a name independent", () => {
      const environment = makeEnvironment(
         [tenantPostgresConnection("shared")],
         [ducklakeDestination("shared", "org_a")],
      );

      const connection = environment.getApiConnection("shared");
      const destination = environment.getStorageDestination("shared");

      expect(connection.type).toBe("postgres");
      expect(destination.type).toBe("ducklake");
      expect(connection.postgresConnection?.host).toBe("tenant.example.com");
      expect(
         destination.ducklakeConnection?.catalog?.postgresConnection?.host,
      ).toBe("catalog.internal");
   });

   it("does not re-assemble the destinations when the list is unchanged", () => {
      // A caller that reconciles by re-pushing its whole desired state does that
      // on a loop. Re-assembling each time would re-attach every destination and
      // retire the connections its serve shapes were compiled against, every
      // cycle, for no change at all.
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );
      const first = environment.getStorageDestinationMalloyConfig();

      environment.setStorageDestinations([
         ducklakeDestination("managed", "org_a"),
      ]);
      expect(environment.getStorageDestinationMalloyConfig()).toBe(first);

      // A real change still swaps it.
      environment.setStorageDestinations([
         ducklakeDestination("managed", "org_b"),
      ]);
      expect(environment.getStorageDestinationMalloyConfig()).not.toBe(first);
   });

   it("treats a re-push in a different order as unchanged", () => {
      // Two callers can describe the same destinations differently: the list is a
      // set keyed by name, and a config is JSON whose key order depends on how it
      // was assembled or parsed. Reading either as a change would re-attach and
      // retire on a reconcile loop exactly as an unchanged re-push would.
      const environment = makeEnvironment(
         [],
         [
            ducklakeDestination("managed", "org_a"),
            ducklakeDestination("archive", "org_b"),
         ],
      );
      const first = environment.getStorageDestinationMalloyConfig();

      // The same two destinations, listed the other way round and with one
      // config's own keys in a different order.
      environment.setStorageDestinations([
         withReorderedKeys(ducklakeDestination("archive", "org_b")),
         ducklakeDestination("managed", "org_a"),
      ]);

      expect(environment.getStorageDestinationMalloyConfig()).toBe(first);
   });

   it("assembles a destination config for an environment that has none", async () => {
      // The overwhelmingly common case, and the one the no-op check can swallow:
      // the constructor's list and the list it is given are both empty, so
      // "nothing changed" is true while nothing has been built yet. What that
      // costs is not confined to the storage tier — every shutdown of every
      // deployment logs an error, and the serve path throws a TypeError instead
      // of the ConnectionNotFoundError a caller can act on.
      const environment = makeEnvironment([], []);

      expect(() =>
         environment.getStorageDestinationMalloyConfig(),
      ).not.toThrow();

      const errors: unknown[][] = [];
      sinon.stub(logger, "error").callsFake((...args: unknown[]) => {
         errors.push(args);
         return logger;
      });
      await environment.closeAllConnections();

      expect(errors).toEqual([]);
   });

   it("clears the destination list when the environment is torn down", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      await environment.closeAllConnections();

      expect(environment.listStorageDestinations()).toEqual([]);
      // And the empty list is not mistaken for a desired state afterwards.
      expect(environment.hasAuthoritativeStorageDestinations()).toBe(false);
   });

   it("derives a destination's DuckDB file apart from a same-named connection's", () => {
      // A pooled DuckDB instance is keyed on `databasePath` + `workingDirectory`
      // and NOT on the connection name, while both lists derive that path from
      // `<root>/<name>`. Same root plus the same name would be one shared
      // instance, and the attach is `ATTACH OR REPLACE … AS <name>` — so one
      // would replace the other's, across two namespaces that are allowed to
      // reuse a name. Different roots is what makes that unreachable.
      const connectionSide = assembleEnvironmentConnections(
         [ducklakeDestination("shared", "tenant_lake")],
         envPath,
      );
      const destinationSide = assembleEnvironmentConnections(
         [ducklakeDestination("shared", "org_a")],
         storageDestinationRoot(envPath),
      );

      const connectionMeta = connectionSide.metadata.get("shared")!;
      const destinationMeta = destinationSide.metadata.get("shared")!;

      expect(destinationMeta.databasePath).not.toBe(
         connectionMeta.databasePath,
      );
      expect(destinationMeta.workingDirectory).not.toBe(
         connectionMeta.workingDirectory,
      );
      expect(destinationMeta.databasePath).toContain(STORAGE_DESTINATIONS_DIR);
      expect(connectionMeta.databasePath).not.toContain(
         STORAGE_DESTINATIONS_DIR,
      );
   });

   it("opens two different database files for one name held by both lists", async () => {
      // The filesystem consequence of the derivation above, through a real
      // Environment: resolving the name on each side opens its own DuckDB file.
      // Both lookups fail — neither catalog exists — but the local database each
      // one opened is what the instance pool keys on, so two files is two
      // instances.
      const environment = makeEnvironment(
         [ducklakeDestination("shared", "tenant_lake")],
         [ducklakeDestination("shared", "org_a")],
      );

      for (const config of [
         environment.getEnvironmentMalloyConfig(),
         environment.getStorageDestinationMalloyConfig(),
      ]) {
         try {
            await config.connections.lookupConnection("shared");
         } catch {
            // Expected: the catalogs behind both are unreachable.
         }
      }

      expect(fs.existsSync(path.join(envPath, "shared_ducklake.duckdb"))).toBe(
         true,
      );
      expect(
         fs.existsSync(
            path.join(
               storageDestinationRoot(envPath),
               "shared_ducklake.duckdb",
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

      expect(serialized.storageDestinations).toEqual([
         { name: "managed", type: "ducklake" },
      ]);
      const payload = JSON.stringify(serialized);
      expect(payload).not.toContain("catalog-secret");
      expect(payload).not.toContain("catalog.internal");
      expect(payload).not.toContain("managed-tier");
   });

   it("reports an empty list when no destination is configured", async () => {
      const environment = makeEnvironment([], []);

      expect((await environment.serialize()).storageDestinations).toEqual([]);
   });

   it("keeps stored configs when an update echoes back what a read reported", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );
      const reported = (await environment.serialize()).storageDestinations;

      await environment.update({
         name: "test-env",
         storageDestinations: reported,
      });

      expect(
         environment.getStorageDestination("managed").ducklakeConnection
            ?.storage?.bucketUrl,
      ).toBe("gs://managed-tier/org_a");
   });

   it("does not admit a config-less destination for a name it does not hold", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      // "managed" resolves to the stored destination; "invented" has no config to
      // resolve to and none of its own, so the update is refused whole.
      await expect(
         environment.update({
            name: "test-env",
            storageDestinations: [
               { name: "managed", type: "ducklake" },
               { name: "invented", type: "ducklake" },
            ],
         }),
      ).rejects.toThrow(/invented/);

      expect(environment.listStorageDestinations().map((d) => d.name)).toEqual([
         "managed",
      ]);
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
      expect(environment.getStorageDestination("managed").type).toBe(
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
         storageDestinations: [ducklakeDestination("other", "org_b")],
      });

      expect(environment.hasStorageDestination("managed")).toBe(false);
      expect(environment.hasStorageDestination("other")).toBe(true);
   });

   it("refuses an update carrying an unusable destination, and applies none of it", async () => {
      // A partial application would be the quiet kind of data loss: the accepted
      // entries become the authoritative set, so the entry we could not read is
      // un-registered — and its stored row pruned — by an update the caller was
      // told succeeded.
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      await expect(
         environment.update({
            name: "test-env",
            storageDestinations: [
               tenantPostgresConnection("wrong_type"),
               ducklakeDestination("also_managed", "org_b"),
            ],
         }),
      ).rejects.toThrow(BadRequestError);

      expect(environment.listStorageDestinations().map((d) => d.name)).toEqual([
         "managed",
      ]);
   });

   it("reports the validator's own reason when it refuses an update", async () => {
      // The refusal carries whatever `validateConnectionShape` objected to, so a
      // rule this file never mentions still reaches the caller.
      const environment = makeEnvironment([], []);
      const destination = ducklakeDestination("managed", "org_a");
      destination.ducklakeConnection!.catalog.metadataSchema =
         "not an identifier";

      await expect(
         environment.update({
            name: "test-env",
            storageDestinations: [destination],
         }),
      ).rejects.toThrow(/metadataSchema/);
   });

   it("refuses an update whose destination list is not a list at all", async () => {
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      await expect(
         environment.update({
            name: "test-env",
            storageDestinations: "lake" as unknown as ApiConnection[],
         }),
      ).rejects.toThrow(BadRequestError);

      expect(environment.listStorageDestinations().map((d) => d.name)).toEqual([
         "managed",
      ]);
      // Still the desired state, so the store sync that follows an update has no
      // reason to prune the rows the refused body failed to describe.
      expect(environment.hasAuthoritativeStorageDestinations()).toBe(true);
   });

   it("names every defect in a refusal, so one round trip is enough to fix it", async () => {
      const environment = makeEnvironment([], []);

      let refusal: Error | undefined;
      try {
         await environment.update({
            name: "test-env",
            storageDestinations: [
               tenantPostgresConnection("wrong_type"),
               { name: "duckdb", type: "ducklake" },
            ],
         });
      } catch (error) {
         refusal = error as Error;
      }

      expect(refusal?.message).toContain("wrong_type");
      expect(refusal?.message).toContain("duckdb");
      // And it reaches the caller as a request error rather than a server fault.
      expect(internalErrorToHttpError(refusal!).status).toBe(400);
   });

   it("clears the destination list for an explicit empty list", async () => {
      // Distinct from a shape we cannot read: an empty list is understood, and
      // means the environment should hold no destinations.
      const environment = makeEnvironment(
         [],
         [ducklakeDestination("managed", "org_a")],
      );

      await environment.update({ name: "test-env", storageDestinations: [] });

      expect(environment.listStorageDestinations()).toEqual([]);
   });

   it("drops an unusable destination from config instead of refusing to load", async () => {
      // The other half of the same decision. A config file and a restored row
      // cannot be asked to fix themselves, and one bad entry must not take a
      // tenant's whole environment offline.
      const environment = makeEnvironment([], []);

      environment.setStorageDestinations([
         tenantPostgresConnection("wrong_type"),
         ducklakeDestination("usable", "org_b"),
      ]);

      expect(environment.listStorageDestinations().map((d) => d.name)).toEqual([
         "usable",
      ]);
   });

   it("validates destinations handed to the constructor", () => {
      const environment = makeEnvironment(
         [],
         [
            tenantPostgresConnection("managed"),
            ducklakeDestination("ok", "org_a"),
         ],
      );

      expect(environment.listStorageDestinations().map((d) => d.name)).toEqual([
         "ok",
      ]);
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

   it("carries storageDestinations through, including a name a connection already uses", () => {
      writeConfig({
         frozenConfig: false,
         environments: [
            {
               name: "examples",
               packages: [{ name: "storefront", location: "./storefront" }],
               connections: [tenantPostgresConnection("shared")],
               storageDestinations: [ducklakeDestination("shared", "org_a")],
            },
         ],
      });

      const processed = getProcessedPublisherConfig(serverRoot);
      const environment = processed.environments[0];

      expect(environment.connections.map((c) => c.name)).toEqual(["shared"]);
      expect(environment.storageDestinations.map((d) => d.name)).toEqual([
         "shared",
      ]);
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
               storageDestinations: "shared",
            },
         ],
      });

      const processed = getProcessedPublisherConfig(serverRoot);

      expect(processed.environments[0].storageDestinations).toEqual([]);
      expect(processed.environments[1].storageDestinations).toEqual([]);
   });
});
