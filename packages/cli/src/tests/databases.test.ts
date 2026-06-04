import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as databaseCommands from "../commands/databases";

describe("Database Commands", () => {
  beforeEach(() => {
    console.log = mock(() => {}) as any;
  });

  test("listDatabases calls the API", async () => {
    const mockClient = {
      listDatabases: mock(() =>
        Promise.resolve([{ path: "data.parquet", type: "embedded" }]),
      ),
    } as unknown as PublisherClient;

    await databaseCommands.listDatabases(mockClient, "env", "pkg");

    expect(mockClient.listDatabases).toHaveBeenCalledWith("env", "pkg");
  });

  test("listDatabases handles empty list", async () => {
    const mockClient = {
      listDatabases: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await databaseCommands.listDatabases(mockClient, "env", "pkg");

    expect(mockClient.listDatabases).toHaveBeenCalledWith("env", "pkg");
  });
});
