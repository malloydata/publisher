import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as manifestCommands from "../commands/manifests";

describe("Manifest Commands", () => {
  beforeEach(() => {
    console.log = mock(() => {}) as any;
  });

  test("getManifest renders entries", async () => {
    const mockClient = {
      getManifest: mock(() =>
        Promise.resolve({ entries: { build1: { tableName: "orders" } } }),
      ),
    } as unknown as PublisherClient;

    await manifestCommands.getManifest(mockClient, "env", "pkg");

    expect(mockClient.getManifest).toHaveBeenCalledWith("env", "pkg");
  });

  test("getManifest handles empty manifest", async () => {
    const mockClient = {
      getManifest: mock(() => Promise.resolve({ entries: {} })),
    } as unknown as PublisherClient;

    await manifestCommands.getManifest(mockClient, "env", "pkg");

    expect(mockClient.getManifest).toHaveBeenCalledWith("env", "pkg");
  });

  test("reloadManifest calls reload API", async () => {
    const mockClient = {
      reloadManifest: mock(() => Promise.resolve({ entries: {} })),
    } as unknown as PublisherClient;

    await manifestCommands.reloadManifest(mockClient, "env", "pkg");

    expect(mockClient.reloadManifest).toHaveBeenCalledWith("env", "pkg");
  });
});
