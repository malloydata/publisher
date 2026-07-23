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

  test("listEnvironmentMaterializations passes env + pagination through", async () => {
    const mockClient = {
      listEnvironmentMaterializations: mock(() =>
        Promise.resolve([
          {
            id: "m1",
            packageName: "orders",
            status: "MANIFEST_FILE_READY",
            metadata: { trigger: "SCHEDULER" },
          },
        ]),
      ),
    } as unknown as PublisherClient;

    await materializationCommands.listEnvironmentMaterializations(
      mockClient,
      "env",
      { limit: 7, offset: 2 },
    );

    expect(mockClient.listEnvironmentMaterializations).toHaveBeenCalledWith(
      "env",
      7,
      2,
    );
  });

  test("listEnvironmentMaterializations handles empty list", async () => {
    const mockClient = {
      listEnvironmentMaterializations: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await materializationCommands.listEnvironmentMaterializations(
      mockClient,
      "env",
    );

    expect(mockClient.listEnvironmentMaterializations).toHaveBeenCalledWith(
      "env",
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

  test("materialize creates a run, no poll without wait", async () => {
    const mockClient = {
      createMaterialization: mock(() =>
        Promise.resolve({ id: "m1", status: "PENDING" }),
      ),
      materializationAction: mock(() => Promise.resolve({})),
      getMaterialization: mock(() => Promise.resolve({ status: "PENDING" })),
    } as unknown as PublisherClient;

    await materializationCommands.materialize(mockClient, "env", "pkg", {
      forceRefresh: true,
    });

    expect(mockClient.createMaterialization).toHaveBeenCalledWith(
      "env",
      "pkg",
      {
        forceRefresh: true,
      },
    );
    // Without --wait the CLI never polls.
    expect(mockClient.materializationAction).not.toHaveBeenCalled();
    expect(mockClient.getMaterialization).not.toHaveBeenCalled();
  });

  test("materialize with wait polls until MANIFEST_FILE_READY", async () => {
    let calls = 0;
    const mockClient = {
      createMaterialization: mock(() =>
        Promise.resolve({ id: "m1", status: "PENDING" }),
      ),
      getMaterialization: mock(() => {
        calls += 1;
        // The build settles only at MANIFEST_FILE_READY; keep polling until then.
        return Promise.resolve({
          id: "m1",
          status: calls >= 3 ? "MANIFEST_FILE_READY" : "PENDING",
        });
      }),
    } as unknown as PublisherClient;

    await materializationCommands.materialize(mockClient, "env", "pkg", {
      wait: true,
      pollIntervalMs: 1,
      timeoutMs: 1000,
    });

    expect(calls).toBeGreaterThanOrEqual(3);
  });

  test("materialize with wait throws on a FAILED build (non-zero exit)", async () => {
    const mockClient = {
      createMaterialization: mock(() =>
        Promise.resolve({ id: "m1", status: "PENDING" }),
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
      undefined,
    );
  });

  test("deleteMaterialization forwards dropTables", async () => {
    const mockClient = {
      deleteMaterialization: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await materializationCommands.deleteMaterialization(
      mockClient,
      "env",
      "pkg",
      "m1",
      true,
    );

    expect(mockClient.deleteMaterialization).toHaveBeenCalledWith(
      "env",
      "pkg",
      "m1",
      true,
    );
  });
});
