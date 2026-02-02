import { mock } from "bun:test";

/**
 * IMPORTANT:
 * Bun + ESM requires mocking the module itself.
 * This must be declared BEFORE importing the command file.
 */
mock.module("fs-extra", () => ({
  readJSON: mock(),
}));

import { describe, test, expect, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as connectionCommands from "../commands/connections";
import * as fs from "fs-extra";

describe("Connection Commands", () => {
  let consoleLogSpy: any;

  beforeEach(() => {
    consoleLogSpy = mock(() => {});
    console.log = consoleLogSpy as any;

    // Reset fs mock between tests
    (fs.readJSON as any).mockReset();
  });

  test("listConnections should call listConnections API", async () => {
    const mockClient = {
      listConnections: mock(() =>
        Promise.resolve([{ name: "conn1", type: "bigquery" }]),
      ),
    } as unknown as PublisherClient;

    await connectionCommands.listConnections(mockClient, "proj");

    expect(mockClient.listConnections).toHaveBeenCalledWith("proj");
  });

  test("listConnections should handle empty list", async () => {
    const mockClient = {
      listConnections: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await connectionCommands.listConnections(mockClient, "proj");

    expect(mockClient.listConnections).toHaveBeenCalledWith("proj");
  });

  test("getConnection should fetch and print connection JSON", async () => {
    const conn = { name: "conn1", type: "postgres" };

    const mockClient = {
      getConnection: mock(() => Promise.resolve(conn)),
    } as unknown as PublisherClient;

    await connectionCommands.getConnection(mockClient, "proj", "conn1");

    expect(mockClient.getConnection).toHaveBeenCalledWith("proj", "conn1");
    expect(consoleLogSpy).toHaveBeenCalledWith(JSON.stringify(conn, null, 2));
  });

  test("createConnection should create from JSON string", async () => {
    const mockClient = {
      createConnection: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    const json = JSON.stringify({
      name: "conn1",
      type: "mysql",
    });

    await connectionCommands.createConnection(mockClient, "proj", { json });

    expect(mockClient.createConnection).toHaveBeenCalledWith("proj", {
      name: "conn1",
      type: "mysql",
    });
  });

  test("createConnection should create from file (single connection)", async () => {
    (fs.readJSON as any).mockResolvedValue({
      name: "conn1",
      type: "postgres",
    });

    const mockClient = {
      createConnection: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await connectionCommands.createConnection(mockClient, "proj", {
      file: "conn.json",
    });

    expect(fs.readJSON).toHaveBeenCalledWith("conn.json");
    expect(mockClient.createConnection).toHaveBeenCalledWith("proj", {
      name: "conn1",
      type: "postgres",
    });
  });

  test("createConnection should bulk create from file", async () => {
    (fs.readJSON as any).mockResolvedValue({
      connections: [
        { name: "c1", type: "bigquery" },
        { name: "c2", type: "snowflake" },
      ],
    });

    const mockClient = {
      createConnection: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await connectionCommands.createConnection(mockClient, "proj", {
      file: "connections.json",
    });

    expect(mockClient.createConnection).toHaveBeenCalledTimes(2);
    expect(mockClient.createConnection).toHaveBeenNthCalledWith(1, "proj", {
      name: "c1",
      type: "bigquery",
    });
    expect(mockClient.createConnection).toHaveBeenNthCalledWith(2, "proj", {
      name: "c2",
      type: "snowflake",
    });
  });

  test("createConnection should pick named connection from file", async () => {
    (fs.readJSON as any).mockResolvedValue({
      connections: [
        { name: "c1", type: "bigquery" },
        { name: "c2", type: "snowflake" },
      ],
    });

    const mockClient = {
      createConnection: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await connectionCommands.createConnection(mockClient, "proj", {
      file: "connections.json",
      name: "c2",
    });

    expect(mockClient.createConnection).toHaveBeenCalledWith("proj", {
      name: "c2",
      type: "snowflake",
    });
  });

  test("createConnection should throw if named connection not found", async () => {
    (fs.readJSON as any).mockResolvedValue({
      connections: [{ name: "c1" }],
    });

    const mockClient = {} as PublisherClient;

    await expect(
      connectionCommands.createConnection(mockClient, "proj", {
        file: "connections.json",
        name: "missing",
      }),
    ).rejects.toThrow("Connection 'missing' not found in file");
  });

  test("createConnection should throw if neither file nor json is provided", async () => {
    const mockClient = {} as PublisherClient;

    await expect(
      connectionCommands.createConnection(mockClient, "proj", {}),
    ).rejects.toThrow("Either --file or --json is required");
  });

  test("updateConnection should update from JSON string", async () => {
    const mockClient = {
      updateConnection: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    const json = JSON.stringify({
      name: "conn1",
      type: "mysql",
    });

    await connectionCommands.updateConnection(mockClient, "proj", "conn1", {
      json,
    });

    expect(mockClient.updateConnection).toHaveBeenCalledWith("proj", "conn1", {
      name: "conn1",
      type: "mysql",
    });
  });

  test("updateConnection should update from file", async () => {
    (fs.readJSON as any).mockResolvedValue({
      name: "conn1",
      type: "postgres",
    });

    const mockClient = {
      updateConnection: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await connectionCommands.updateConnection(mockClient, "proj", "conn1", {
      file: "conn.json",
    });

    expect(mockClient.updateConnection).toHaveBeenCalledWith("proj", "conn1", {
      name: "conn1",
      type: "postgres",
    });
  });

  test("updateConnection should throw if neither file nor json is provided", async () => {
    const mockClient = {} as PublisherClient;

    await expect(
      connectionCommands.updateConnection(mockClient, "proj", "conn1", {}),
    ).rejects.toThrow("Either --file or --json is required");
  });

  test("deleteConnection should call delete API", async () => {
    const mockClient = {
      deleteConnection: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await connectionCommands.deleteConnection(mockClient, "proj", "conn1");

    expect(mockClient.deleteConnection).toHaveBeenCalledWith("proj", "conn1");
  });
});
