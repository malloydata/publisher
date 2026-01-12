import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import sinon from "sinon";
import { FrozenConfigError, ConnectionNotFoundError } from "../errors";
import { ConnectionService } from "./connection_service";
import { ProjectStore } from "./project_store";
import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

describe("service/connection_service", () => {
   let connectionService: ConnectionService;
   let mockProjectStore: Record<string, unknown>;
   let mockRepository: Record<string, unknown>;

   beforeEach(() => {
      mockRepository = {
         getProjectByName: sinon.stub(),
         getConnectionByName: sinon.stub(),
         deleteConnection: sinon.stub(),
      };

      mockProjectStore = {
         finishedInitialization: Promise.resolve(),
         publisherConfigIsFrozen: false,
         getProject: sinon.stub(),
         updateConnection: sinon.stub(),
         addConnection: sinon.stub(),
         storageManager: {
            getRepository: sinon.stub().returns(mockRepository),
         },
      };

      connectionService = new ConnectionService(
         mockProjectStore as unknown as ProjectStore,
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
            projectId: "project-123",
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

         (mockRepository.getProjectByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(
            mockDbConnection,
         );

         const result = await connectionService.getConnection(
            "test-project",
            "test-connection",
         );

         expect(result.dbProject).toEqual(mockDbProject);
         expect(result.dbConnection).toEqual(mockDbConnection);
         expect(result.repository).toBeDefined();
      });

      it("should throw error when project not found", async () => {
         (mockRepository.getProjectByName as sinon.SinonStub).resolves(null);

         await expect(
            connectionService.getConnection("non-existent", "test-connection"),
         ).rejects.toThrow('Project "non-existent" not found in database');
      });

      it("should throw ConnectionNotFoundError when connection not found", async () => {
         const mockDbProject = {
            id: "project-123",
            name: "test-project",
            path: "/test/path",
            createdAt: new Date(),
            updatedAt: new Date(),
         };

         (mockRepository.getProjectByName as sinon.SinonStub).resolves(
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

         (mockRepository.getProjectByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(null);
         (mockProjectStore.getProject as sinon.SinonStub).resolves(mockProject);

         await connectionService.addConnection(
            "test-project",
            "new-connection",
            newConnection,
         );

         expect(
            (mockProjectStore.addConnection as sinon.SinonStub).called,
         ).toBe(true);
         expect(mockProject.updateConnections.called).toBe(true);
      });

      it("should throw FrozenConfigError when config is frozen", async () => {
         mockProjectStore.publisherConfigIsFrozen = true;

         await expect(
            connectionService.addConnection("test-project", "new-connection", {
               name: "new-connection",
               type: "postgres",
            } as ApiConnection),
         ).rejects.toThrow(FrozenConfigError);
      });

      it("should throw error when project not found", async () => {
         (mockRepository.getProjectByName as sinon.SinonStub).resolves(null);

         await expect(
            connectionService.addConnection("non-existent", "new-connection", {
               name: "new-connection",
               type: "postgres",
            } as ApiConnection),
         ).rejects.toThrow('Project "non-existent" not found in database');
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

         (mockRepository.getProjectByName as sinon.SinonStub).resolves(
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
            'Connection "existing-connection" already exists in project "test-project"',
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

         (mockRepository.getProjectByName as sinon.SinonStub).resolves(
            mockDbProject,
         );
         (mockRepository.getConnectionByName as sinon.SinonStub).resolves(null);
         (mockProjectStore.getProject as sinon.SinonStub).resolves(mockProject);

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
            dbProject: mockDbProject,
            dbConnection: mockDbConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockProjectStore.getProject as sinon.SinonStub).resolves(mockProject);

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
            (mockProjectStore.getProject as sinon.SinonStub).calledWith(
               "test-project",
               false,
            ),
         ).toBe(true);

         expect(mockProject.updateConnections.called).toBe(true);

         expect(
            (mockProjectStore.updateConnection as sinon.SinonStub).called,
         ).toBe(true);
         const updateCall = (
            mockProjectStore.updateConnection as sinon.SinonStub
         ).getCall(0);
         expect(updateCall.args[0].name).toBe("test-connection");
         expect(updateCall.args[1]).toBe("project-123");
      });

      it("should throw FrozenConfigError when config is frozen", async () => {
         mockProjectStore.publisherConfigIsFrozen = true;

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
            dbProject: mockDbProject,
            dbConnection: connection1,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockProjectStore.getProject as sinon.SinonStub).resolves(mockProject);

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
            dbProject: mockDbProject,
            dbConnection: existingConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockProjectStore.getProject as sinon.SinonStub).resolves(mockProject);

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
            (mockProjectStore.updateConnection as sinon.SinonStub).called,
         ).toBe(true);
         const updateCall = (
            mockProjectStore.updateConnection as sinon.SinonStub
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
            dbProject: mockDbProject,
            dbConnection: dbConnection,
            repository: mockRepository,
         } as unknown as Awaited<
            ReturnType<typeof connectionService.getConnection>
         >);

         (mockProjectStore.getProject as sinon.SinonStub).resolves(mockProject);

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
            mockProjectStore.updateConnection as sinon.SinonStub
         ).getCall(0);
         expect(updateCall.args[0].name).toBe("test-conn");
         expect(updateCall.args[0].type).toBe("postgres");
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

         (mockProjectStore.getProject as sinon.SinonStub).resolves(mockProject);

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

      it("should throw FrozenConfigError when config is frozen", async () => {
         mockProjectStore.publisherConfigIsFrozen = true;

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
               'Connection "non-existent" not found in project "test-project"',
            ),
         );

         await expect(
            connectionService.deleteConnection("test-project", "non-existent"),
         ).rejects.toThrow(ConnectionNotFoundError);
      });
   });
});
