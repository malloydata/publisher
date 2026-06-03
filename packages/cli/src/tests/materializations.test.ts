import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as materializationCommands from "../commands/materializations";

describe("Materialization Commands", () => {
  beforeEach(() => {
    console.log = mock(() => {}) as any;
  });

  test("listMaterializations passes pagination through", async () => {
    const mockClient = {
      listMaterializations: mock(() =>
        Promise.resolve([{ id: "m1", status: "SUCCESS" }]),
      ),
    } as unknown as PublisherClient;

    await materializationCommands.listMaterializations(
      mockClient,
      "env",
      "pkg",
      { limit: 10, offset: 5 },
    );

    expect(mockClient.listMaterializations).toHaveBeenCalledWith(
      "env",
      "pkg",
      10,
      5,
    );
  });

  test("listMaterializations handles empty list", async () => {
    const mockClient = {
      listMaterializations: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await materializationCommands.listMaterializations(
      mockClient,
      "env",
      "pkg",
    );

    expect(mockClient.listMaterializations).toHaveBeenCalledWith(
      "env",
      "pkg",
      undefined,
      undefined,
    );
  });

  test("getMaterialization fetches and prints JSON", async () => {
    const mat = { id: "m1", status: "RUNNING" };
    const mockClient = {
      getMaterialization: mock(() => Promise.resolve(mat)),
    } as unknown as PublisherClient;

    await materializationCommands.getMaterialization(
      mockClient,
      "env",
      "pkg",
      "m1",
    );

    expect(mockClient.getMaterialization).toHaveBeenCalledWith(
      "env",
      "pkg",
      "m1",
    );
  });

  test("materialize creates then starts, no poll without wait", async () => {
    const mockClient = {
      createMaterialization: mock(() => Promise.resolve({ id: "m1" })),
      materializationAction: mock(() =>
        Promise.resolve({ id: "m1", status: "RUNNING" }),
      ),
      getMaterialization: mock(() => Promise.resolve({ status: "SUCCESS" })),
    } as unknown as PublisherClient;

    await materializationCommands.materialize(mockClient, "env", "pkg", {
      forceRefresh: true,
      autoLoadManifest: true,
    });

    expect(mockClient.createMaterialization).toHaveBeenCalledWith(
      "env",
      "pkg",
      {
        forceRefresh: true,
        autoLoadManifest: true,
      },
    );
    expect(mockClient.materializationAction).toHaveBeenCalledWith(
      "env",
      "pkg",
      "m1",
      "start",
    );
    expect(mockClient.getMaterialization).not.toHaveBeenCalled();
  });

  test("materialize with wait polls until terminal", async () => {
    let calls = 0;
    const mockClient = {
      createMaterialization: mock(() => Promise.resolve({ id: "m1" })),
      materializationAction: mock(() =>
        Promise.resolve({ id: "m1", status: "RUNNING" }),
      ),
      getMaterialization: mock(() => {
        calls += 1;
        return Promise.resolve({
          id: "m1",
          status: calls >= 2 ? "SUCCESS" : "RUNNING",
        });
      }),
    } as unknown as PublisherClient;

    await materializationCommands.materialize(mockClient, "env", "pkg", {
      wait: true,
      pollIntervalMs: 1,
      timeoutMs: 1000,
    });

    expect(calls).toBeGreaterThanOrEqual(2);
  });

  test("materialize with wait throws on a FAILED build (non-zero exit)", async () => {
    const mockClient = {
      createMaterialization: mock(() => Promise.resolve({ id: "m1" })),
      materializationAction: mock(() =>
        Promise.resolve({ id: "m1", status: "RUNNING" }),
      ),
      getMaterialization: mock(() =>
        Promise.resolve({ id: "m1", status: "FAILED", error: "boom" }),
      ),
    } as unknown as PublisherClient;

    await expect(
      materializationCommands.materialize(mockClient, "env", "pkg", {
        wait: true,
        pollIntervalMs: 1,
        timeoutMs: 1000,
      }),
    ).rejects.toThrow(/FAILED/);
  });

  test("stopMaterialization calls action stop", async () => {
    const mockClient = {
      materializationAction: mock(() =>
        Promise.resolve({ id: "m1", status: "CANCELLED" }),
      ),
    } as unknown as PublisherClient;

    await materializationCommands.stopMaterialization(
      mockClient,
      "env",
      "pkg",
      "m1",
    );

    expect(mockClient.materializationAction).toHaveBeenCalledWith(
      "env",
      "pkg",
      "m1",
      "stop",
    );
  });

  test("deleteMaterialization calls delete API", async () => {
    const mockClient = {
      deleteMaterialization: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await materializationCommands.deleteMaterialization(
      mockClient,
      "env",
      "pkg",
      "m1",
    );

    expect(mockClient.deleteMaterialization).toHaveBeenCalledWith(
      "env",
      "pkg",
      "m1",
    );
  });
});
