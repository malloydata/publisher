// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";
import { components } from "../api";
import { ConnectionNotFoundError, FrozenConfigError } from "../errors";
import {
   startFakePublisher,
   type FakePublisher,
} from "../test_helpers/fake_publisher";
import {
   buildEnvironmentMalloyConfig,
   isPublisherTokenOnlyUpdate,
} from "./connection";
import { ConnectionService } from "./connection_service";
import { Environment } from "./environment";
import { EnvironmentStore } from "./environment_store";

type ApiConnection = components["schemas"]["Connection"];

describe("service/connection_service", () => {
   let connectionService: ConnectionService;
   let mockEnvironmentStore: Record<string, unknown>;
   let mockRepository: Record<string, unknown>;

   beforeEach(() => {
      mockRepository = {
         getEnvironmentByName: sinon.stub(),
         getConnectionByName: sinon.stub(),
         deleteConnection: sinon.stub(),
      };

      mockEnvironmentStore = {
         finishedInitialization: Promise.resolve(),
         publisherConfigIsFrozen: false,
         getEnvironment: sinon.stub(),
         updateConnection: sinon.stub(),
         addConnection: sinon.stub(),
         storageManager: {
            getRepository: sinon.stub().returns(mockRepository),
         },
      };

      connectionService = new ConnectionService(
         mockEnvironmentStore as unknown as EnvironmentStore,
      );
   });

   afterEach(() => {
      sinon.restore();
   });

   describe("getConnection", () => {
      it("should return connection, project, and repository", async () => {
         const mockDbProject = {
            id: "project-123",
            name: "test-project",
            path: "/test/path",
            createdAt: new Date(),
            updatedAt: new Date(),
         };

         const mockDbConnection = {
            id: "conn-123",
            environmentId: "project-123",
            name: "test-connection",
            type: "postgres" as const,
            config: {},
            createdAt: new Date(),
            updatedAt: new Date(),
            postgresConnection: {
               host: "localhost",
               port: 5432,
               databaseName: "test-db",
               userName: "user",
               password: "pass",
            },
         };

         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(
            mockDbConnection,
         );

         const result = await connectionService.getConnection(
            "test-project",
            "test-connection",
         );

         expect(result.dbEnvironment).toEqual(mockDbProject);
         expect(result.dbConnection).toEqual(mockDbConnection);
         expect(result.repository).toBeDefined();
      });

      it("should throw error when project not found", async () => {
         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves(
            null,
         );

         await expect(
            connectionService.getConnection("non-existent", "test-connection"),
         ).rejects.toThrow('Environment "non-existent" not found in database');
      });

      it("should throw ConnectionNotFoundError when connection not found", async () => {
         const mockDbProject = {
            id: "project-123",
            name: "test-project",
            path: "/test/path",
            createdAt: new Date(),
            updatedAt: new Date(),
         };

         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(null);

         await expect(
            connectionService.getConnection("test-project", "non-existent"),
         ).rejects.toThrow(ConnectionNotFoundError);
      });
   });

   describe("addConnection", () => {
      it("should add a new connection successfully", async () => {
         const mockDbProject = {
            id: "project-123",
            name: "test-project",
            path: "/test/path",
            createdAt: new Date(),
            updatedAt: new Date(),
         };

         const newConnection: ApiConnection = {
            name: "new-connection",
            type: "postgres",
            postgresConnection: {
               host: "localhost",
               port: 5432,
               databaseName: "test-db",
               userName: "user",
               password: "pass",
            },
         };

         const mockProject = {
            listApiConnections: sinon.stub().returns([]),
            updateConnections: sinon.stub(),
            metadata: { location: "/test/path" },
         };

         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(null);
         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         await connectionService.addConnection(
            "test-project",
            "new-connection",
            newConnection,
         );

         expect(
            (mockEnvironmentStore.addConnection as sinon.SinonStub).called,
         ).toBe(true);
         expect(mockProject.updateConnections.called).toBe(true);
      });

      it("should add a connection whose name a storage destination also holds", async () => {
         // The two namespaces are independent, so a tenant naming their connection
         // after the tier's destination is legal and must not read as a duplicate.
         // Driven against a REAL Environment: a stub would answer whatever it was
         // told, and the claim here is about which list the service consults.
         const envPath = fs.mkdtempSync(
            path.join(os.tmpdir(), "publisher-conn-service-"),
         );
         try {
            const destination: ApiConnection = {
               name: "shared",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: {
                        host: "catalog.internal",
                        databaseName: "ducklake",
                        userName: "publisher",
                        password: "catalog-secret",
                     },
                  },
                  storage: { bucketUrl: "gs://managed-tier/org_a" },
               },
            };
            const malloyConfig = buildEnvironmentMalloyConfig([], envPath);
            const environment = new Environment(
               "test-project",
               envPath,
               malloyConfig,
               malloyConfig.apiConnections,
               [destination],
            );

            (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves({
               id: "project-123",
               name: "test-project",
               path: envPath,
               createdAt: new Date(),
               updatedAt: new Date(),
            });
            (mockRepository.getConnectionByName as sinon.SinonStub).resolves(
               null,
            );
            (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
               environment,
            );

            await connectionService.addConnection("test-project", "shared", {
               name: "shared",
               type: "postgres",
               postgresConnection: {
                  host: "tenant.example.com",
                  port: 5432,
                  databaseName: "analytics",
                  userName: "tenant",
                  password: "tenant-secret",
               },
            } as ApiConnection);

            // The connection landed, and the destination of the same name is
            // untouched — still a ducklake, still pointed at its own catalog.
            expect(environment.getApiConnection("shared").type).toBe(
               "postgres",
            );
            expect(
               environment.listStorageDestinations().map((d) => d.name),
            ).toEqual(["shared"]);
            expect(
               environment.getStorageDestination("shared").ducklakeConnection
                  ?.storage?.bucketUrl,
            ).toBe("gs://managed-tier/org_a");
         } finally {
            fs.rmSync(envPath, { recursive: true, force: true });
         }
      });

      it("should throw FrozenConfigError when config is frozen", async () => {
         mockEnvironmentStore.publisherConfigIsFrozen = true;

         await expect(
            connectionService.addConnection("test-project", "new-connection", {
               name: "new-connection",
               type: "postgres",
            } as ApiConnection),
         ).rejects.toThrow(FrozenConfigError);
      });

      it("should throw error when project not found", async () => {
         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves(
            null,
         );

         await expect(
            connectionService.addConnection("non-existent", "new-connection", {
               name: "new-connection",
               type: "postgres",
            } as ApiConnection),
         ).rejects.toThrow('Environment "non-existent" not found in database');
      });

      it("should throw error when connection already exists", async () => {
         const mockDbProject = {
            id: "project-123",
            name: "test-project",
            path: "/test/path",
            createdAt: new Date(),
            updatedAt: new Date(),
         };

         const existingConnection = {
            id: "conn-123",
            name: "existing-connection",
         };

         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(
            existingConnection,
         );

         await expect(
            connectionService.addConnection(
               "test-project",
               "existing-connection",
               {
                  name: "existing-connection",
                  type: "postgres",
               } as ApiConnection,
            ),
         ).rejects.toThrow(
            'Connection "existing-connection" already exists in environment "test-project"',
         );
      });

      it("should add connection to existing connections list", async () => {
         const mockDbProject = {
            id: "project-123",
            name: "test-project",
            path: "/test/path",
            createdAt: new Date(),
            updatedAt: new Date(),
         };

         const existingConnection: ApiConnection = {
            name: "existing-connection",
            type: "postgres",
            postgresConnection: {
               host: "existing-host",
               port: 5432,
               databaseName: "existing-db",
               userName: "user",
               password: "pass",
            },
         };

         const newConnection: ApiConnection = {
            name: "new-connection",
            type: "bigquery",
            bigqueryConnection: {
               defaultProjectId: "my-project",
            },
         };

         const mockProject = {
            listApiConnections: sinon.stub().returns([existingConnection]),
            updateConnections: sinon.stub(),
            metadata: { location: "/test/path" },
         };

         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(null);
         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         await connectionService.addConnection(
            "test-project",
            "new-connection",
            newConnection,
         );

         expect(mockProject.updateConnections.called).toBe(true);
      });
   });

   describe("updateConnection", () => {
      it("should update a connection successfully", async () => {
         const mockDbProject = {
            id: "project-123",
            name: "test-project",
            metadata: { location: "/test/path" },
         };

         const mockDbConnection: ApiConnection = {
            name: "test-connection",
            type: "postgres",
            postgresConnection: {
               host: "localhost",
               port: 5432,
               databaseName: "test-db",
               userName: "user",
               password: "pass",
            },
         };

         const mockProject = {
            listApiConnections: sinon.stub().returns([
               mockDbConnection,
               {
                  name: "other-connection",
                  type: "postgres",
                  postgresConnection: {
                     host: "other-host",
                     port: 5432,
                     databaseName: "other-db",
                     userName: "user",
                     password: "pass",
                  },
               },
            ]),
            updateConnections: sinon.stub(),
            metadata: { location: "/test/path" },
         };

         // ✅ Fix: Use proper stub pattern
         const getConnectionStub = sinon.stub(
            connectionService,
            "getConnection",
         );
         getConnectionStub.resolves({
            dbEnvironment: mockDbProject,
            dbConnection: mockDbConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         const updates: Partial<ApiConnection> = {
            type: "postgres",
            postgresConnection: {
               host: "new-host",
               port: 5432,
               databaseName: "test-db",
               userName: "user",
               password: "pass",
            },
         };

         await connectionService.updateConnection(
            "test-project",
            "test-connection",
            updates,
         );

         expect(
            getConnectionStub.calledWith("test-project", "test-connection"),
         ).toBe(true);

         expect(
            (mockEnvironmentStore.getEnvironment as sinon.SinonStub).calledWith(
               "test-project",
               false,
            ),
         ).toBe(true);

         expect(mockProject.updateConnections.called).toBe(true);

         expect(
            (mockEnvironmentStore.updateConnection as sinon.SinonStub).called,
         ).toBe(true);
         const updateCall = (
            mockEnvironmentStore.updateConnection as sinon.SinonStub
         ).getCall(0);
         expect(updateCall.args[0].name).toBe("test-connection");
         expect(updateCall.args[1]).toBe("project-123");
      });

      it("should throw FrozenConfigError when config is frozen", async () => {
         mockEnvironmentStore.publisherConfigIsFrozen = true;

         await expect(
            connectionService.updateConnection(
               "test-project",
               "test-connection",
               {},
            ),
         ).rejects.toThrow(FrozenConfigError);
      });

      it("should preserve other connections when updating one", async () => {
         const connection1: ApiConnection = {
            name: "conn-1",
            type: "postgres",
            postgresConnection: {
               host: "host-1",
               port: 5432,
               databaseName: "db-1",
               userName: "user",
               password: "pass",
            },
         };

         const connection2: ApiConnection = {
            name: "conn-2",
            type: "postgres",
            postgresConnection: {
               host: "host-2",
               port: 3306,
               databaseName: "db-2",
               userName: "user",
               password: "pass",
            },
         };

         const mockDbProject = {
            id: "project-123",
            metadata: { location: "/test" },
         };

         const mockProject = {
            listApiConnections: sinon
               .stub()
               .returns([connection1, connection2]),
            updateConnections: sinon.stub(),
            metadata: { location: "/test" },
         };

         const getConnectionStub = sinon.stub(
            connectionService,
            "getConnection",
         );
         getConnectionStub.resolves({
            dbEnvironment: mockDbProject,
            dbConnection: connection1,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         await connectionService.updateConnection("test-project", "conn-1", {
            type: "postgres",
            postgresConnection: {
               host: "updated-host",
               port: 5432,
               databaseName: "db-1",
               userName: "user",
               password: "pass",
            },
         });

         expect(mockProject.updateConnections.called).toBe(true);

         const updateCall = mockProject.updateConnections.getCall(0);
         const apiConnections = updateCall.args[1];

         expect(apiConnections).toHaveLength(2);
      });

      it("should merge partial updates with existing connection", async () => {
         const existingConnection: ApiConnection = {
            name: "test-conn",
            type: "postgres",
            postgresConnection: {
               host: "old-host",
               port: 5432,
               databaseName: "test-db",
               userName: "user",
               password: "old-pass",
            },
         };

         const mockDbProject = {
            id: "project-123",
            metadata: { location: "/test" },
         };

         const mockProject = {
            listApiConnections: sinon.stub().returns([existingConnection]),
            updateConnections: sinon.stub(),
            metadata: { location: "/test" },
         };

         const getConnectionStub = sinon.stub(
            connectionService,
            "getConnection",
         );
         getConnectionStub.resolves({
            dbEnvironment: mockDbProject,
            dbConnection: existingConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         const partialUpdate: Partial<ApiConnection> = {
            type: "postgres",
            postgresConnection: {
               host: "new-host",
               port: 5432,
               databaseName: "test-db",
               userName: "user",
               password: "old-pass",
            },
         };

         await connectionService.updateConnection(
            "test-project",
            "test-conn",
            partialUpdate,
         );

         expect(
            (mockEnvironmentStore.updateConnection as sinon.SinonStub).called,
         ).toBe(true);
         const updateCall = (
            mockEnvironmentStore.updateConnection as sinon.SinonStub
         ).getCall(0);
         const updatedConn = updateCall.args[0];

         expect(updatedConn.name).toBe("test-conn");
         expect(updatedConn.type).toBe("postgres");
         expect(updatedConn.postgresConnection.host).toBe("new-host");
         expect(updatedConn.postgresConnection.port).toBe(5432);
         expect(updatedConn.postgresConnection.databaseName).toBe("test-db");
      });

      it("should use dbConnection from getConnection instead of searching again", async () => {
         const dbConnection: ApiConnection = {
            name: "test-conn",
            type: "postgres",
            postgresConnection: {
               host: "db-host",
               port: 5432,
               databaseName: "db-name",
               userName: "user",
               password: "pass",
            },
         };

         const mockDbProject = {
            id: "project-123",
            metadata: { location: "/test" },
         };

         const mockProject = {
            listApiConnections: sinon.stub().returns([dbConnection]),
            updateConnections: sinon.stub(),
            metadata: { location: "/test" },
         };

         const getConnectionStub = sinon.stub(
            connectionService,
            "getConnection",
         );
         getConnectionStub.resolves({
            dbEnvironment: mockDbProject,
            dbConnection: dbConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         await connectionService.updateConnection("test-project", "test-conn", {
            type: "postgres",
            postgresConnection: {
               host: "updated-host",
               port: 5432,
               databaseName: "db-name",
               userName: "user",
               password: "pass",
            },
         });

         expect(getConnectionStub.calledOnce).toBe(true);

         const updateCall = (
            mockEnvironmentStore.updateConnection as sinon.SinonStub
         ).getCall(0);
         expect(updateCall.args[0].name).toBe("test-conn");
         expect(updateCall.args[0].type).toBe("postgres");
      });
   });

   describe("updateConnection on a publisher connection", () => {
      let envPath: string;
      let remote: FakePublisher;
      let environment: Environment;
      let stored: ApiConnection;
      const priorFlag = process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;

      const publisherConnection = (
         connectionUri: string,
         accessToken: string,
      ): ApiConnection => ({
         name: "remote",
         type: "publisher",
         publisherConnection: { connectionUri, accessToken },
      });

      beforeEach(async () => {
         process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = "true";
         envPath = fs.mkdtempSync(
            path.join(os.tmpdir(), "publisher-conn-token-"),
         );
         remote = startFakePublisher();
         stored = publisherConnection(remote.connectionUri("remote"), "old");
         const malloyConfig = buildEnvironmentMalloyConfig([stored], envPath);
         environment = new Environment(
            "test-project",
            envPath,
            malloyConfig,
            malloyConfig.apiConnections,
         );
         (mockRepository.getEnvironmentByName as sinon.SinonStub).resolves({
            id: "project-123",
            name: "test-project",
         });
         (mockRepository.getConnectionByName as sinon.SinonStub).callsFake(
            async () => ({ config: stored }),
         );
         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            environment,
         );
      });

      afterEach(async () => {
         await environment.getEnvironmentMalloyConfig().shutdown("close");
         remote.stop();
         fs.rmSync(envPath, { recursive: true, force: true });
         if (priorFlag === undefined) {
            delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
         } else {
            process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = priorFlag;
         }
      });

      const retiredCount = () =>
         (
            environment as unknown as {
               retiredConnectionGenerations: Set<unknown>;
            }
         ).retiredConnectionGenerations.size;

      it("keeps the live instance on a token-only patch and serves the new token", async () => {
         const live = await environment.getMalloyConnection("remote");
         const config = environment.getEnvironmentMalloyConfig();
         await live.runSQL("select 1");
         expect(remote.authorizations.at(-1)).toBe("Bearer old");

         await connectionService.updateConnection("test-project", "remote", {
            publisherConnection: {
               connectionUri: remote.connectionUri("remote"),
               accessToken: "new",
            },
         });

         // Same object: the generation and the worker schema-cache keys hang off it.
         expect(await environment.getMalloyConnection("remote")).toBe(live);
         expect(environment.getEnvironmentMalloyConfig()).toBe(config);
         expect(retiredCount()).toBe(0);
         await live.runSQL("select 1");
         expect(remote.authorizations.at(-1)).toBe("Bearer new");
         expect(
            environment.getApiConnection("remote").publisherConnection
               ?.accessToken,
         ).toBe("new");
         expect(environment.listApiConnections().map((c) => c.name)).toEqual([
            "remote",
         ]);
         const persisted = (
            mockEnvironmentStore.updateConnection as sinon.SinonStub
         ).getCall(0).args[0] as ApiConnection;
         expect(persisted.publisherConnection?.accessToken).toBe("new");
      });

      it("rebuilds and retires when a non-credential field changes", async () => {
         const live = await environment.getMalloyConnection("remote");
         const other = startFakePublisher();
         try {
            await connectionService.updateConnection("test-project", "remote", {
               publisherConnection: {
                  connectionUri: other.connectionUri("remote"),
                  accessToken: "new",
               },
            });
            expect(retiredCount()).toBe(1);
            const rebuilt = await environment.getMalloyConnection("remote");
            expect(rebuilt).not.toBe(live);
            await rebuilt.runSQL("select 1");
            expect(other.authorizations.at(-1)).toBe("Bearer new");
         } finally {
            other.stop();
         }
      });

      it("rebuilds when no live instance exists yet to rebind", async () => {
         await connectionService.updateConnection("test-project", "remote", {
            publisherConnection: {
               connectionUri: remote.connectionUri("remote"),
               accessToken: "new",
            },
         });
         expect(retiredCount()).toBe(1);
         const created = await environment.getMalloyConnection("remote");
         await created.runSQL("select 1");
         expect(remote.authorizations.at(-1)).toBe("Bearer new");
      });
   });

   describe("isPublisherTokenOnlyUpdate", () => {
      const publisher = (uri: string, token: string): ApiConnection => ({
         name: "remote",
         type: "publisher",
         publisherConnection: { connectionUri: uri, accessToken: token },
      });
      const postgres = (host: string, password: string): ApiConnection => ({
         name: "pg",
         type: "postgres",
         postgresConnection: { host, password },
      });

      it("accepts only a publisher bearer token change", () => {
         expect(
            isPublisherTokenOnlyUpdate(
               publisher("https://a/x", "old"),
               publisher("https://a/x", "new"),
            ),
         ).toBe(true);
         expect(
            isPublisherTokenOnlyUpdate(
               publisher("https://a/x", "old"),
               publisher("https://b/x", "new"),
            ),
         ).toBe(false);
      });

      // Postgres hands pg.password to the pool when the connection is built, so
      // its live instance cannot take a new credential in place.
      it("rejects a credential-only change on every other type", () => {
         expect(
            isPublisherTokenOnlyUpdate(
               postgres("h", "old"),
               postgres("h", "new"),
            ),
         ).toBe(false);
      });
   });

   describe("deleteConnection", () => {
      it("should delete a connection successfully", async () => {
         const mockDbConnection = {
            id: "conn-123",
            name: "test-connection",
            type: "postgres",
         };

         const mockProject = {
            deleteConnection: sinon.stub(),
         };

         const getConnectionStub = sinon.stub(
            connectionService,
            "getConnection",
         );
         getConnectionStub.resolves({
            dbConnection: mockDbConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         await connectionService.deleteConnection(
            "test-project",
            "test-connection",
         );

         expect(
            mockProject.deleteConnection.calledWith("test-connection"),
         ).toBe(true);
         expect(
            (mockRepository.deleteConnection as sinon.SinonStub).calledWith(
               "conn-123",
            ),
         ).toBe(true);
      });

      it("should defer DuckDB file cleanup until the previous config release callback", async () => {
         const connectionName = "local-file";
         const duckdbConnection: ApiConnection = {
            name: connectionName,
            type: "duckdb",
            duckdbConnection: {
               attachedDatabases: [],
            },
         };
         const mockDbConnection = {
            id: "conn-123",
            name: duckdbConnection.name,
            type: "duckdb",
            config: duckdbConnection,
         };
         const mockProject = {
            listApiConnections: sinon.stub().returns([duckdbConnection]),
            getApiConnection: sinon.stub().returns(duckdbConnection),
            updateConnections: sinon.stub(),
            deleteDuckDBConnection: sinon.stub().resolves(),
            metadata: { location: "/test/path" },
         };
         const getConnectionStub = sinon.stub(
            connectionService,
            "getConnection",
         );
         getConnectionStub.resolves({
            dbConnection: mockDbConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);
         (mockEnvironmentStore.getEnvironment as sinon.SinonStub).resolves(
            mockProject,
         );

         await connectionService.deleteConnection(
            "test-project",
            connectionName,
         );

         expect(mockProject.deleteDuckDBConnection.called).toBe(false);
         const releaseCallback = mockProject.updateConnections.getCall(0)
            .args[2] as () => Promise<void>;

         expect(typeof releaseCallback).toBe("function");
         await releaseCallback();
         expect(
            mockProject.deleteDuckDBConnection.calledWith(connectionName),
         ).toBe(true);
      });

      it("should throw FrozenConfigError when config is frozen", async () => {
         mockEnvironmentStore.publisherConfigIsFrozen = true;

         await expect(
            connectionService.deleteConnection(
               "test-project",
               "test-connection",
            ),
         ).rejects.toThrow(FrozenConfigError);
      });

      it("should throw error when connection not found", async () => {
         const getConnectionStub = sinon.stub(
            connectionService,
            "getConnection",
         );
         getConnectionStub.rejects(
            new ConnectionNotFoundError(
               'Connection "non-existent" not found in environment "test-project"',
            ),
         );

         await expect(
            connectionService.deleteConnection("test-project", "non-existent"),
         ).rejects.toThrow(ConnectionNotFoundError);
      });
   });
});
