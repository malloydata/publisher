import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as scheduleCommands from "../commands/schedule";

describe("Schedule Commands", () => {
  beforeEach(() => {
    console.log = mock(() => {}) as any;
  });

  test("viewSchedule reads the package", async () => {
    const mockClient = {
      getPackage: mock(() =>
        Promise.resolve({
          scope: "version",
          materialization: { schedule: "0 6 * * *" },
        }),
      ),
    } as unknown as PublisherClient;

    await scheduleCommands.viewSchedule(mockClient, "env", "orders");

    expect(mockClient.getPackage).toHaveBeenCalledWith("env", "orders");
  });

  test("setSchedule PATCHes scope: version + the cron, carrying description", async () => {
    const mockClient = {
      getPackage: mock(() => Promise.resolve({ description: "keep me" })),
      updatePackage: mock(() => Promise.resolve({})),
    } as unknown as PublisherClient;

    await scheduleCommands.setSchedule(
      mockClient,
      "env",
      "orders",
      "0 6 * * *",
    );

    expect(mockClient.updatePackage).toHaveBeenCalledWith("env", "orders", {
      name: "orders",
      description: "keep me",
      scope: "version",
      materialization: { schedule: "0 6 * * *" },
    });
  });

  test("setSchedule rejects an empty cron without calling the server", async () => {
    const mockClient = {
      getPackage: mock(() => Promise.resolve({})),
      updatePackage: mock(() => Promise.resolve({})),
    } as unknown as PublisherClient;

    await expect(
      scheduleCommands.setSchedule(mockClient, "env", "orders", "   "),
    ).rejects.toThrow(/cron/);
    expect(mockClient.updatePackage).not.toHaveBeenCalled();
  });

  test("setSchedule warns but still PATCHes when replacing a freshness policy", async () => {
    const mockClient = {
      getPackage: mock(() =>
        Promise.resolve({
          description: "keep me",
          materialization: { freshness: { window: "24h" } },
        }),
      ),
      updatePackage: mock(() => Promise.resolve({})),
    } as unknown as PublisherClient;

    await scheduleCommands.setSchedule(
      mockClient,
      "env",
      "orders",
      "0 6 * * *",
    );

    // The schedule replaces the freshness policy (they cannot coexist).
    expect(mockClient.updatePackage).toHaveBeenCalledWith("env", "orders", {
      name: "orders",
      description: "keep me",
      scope: "version",
      materialization: { schedule: "0 6 * * *" },
    });
  });

  test("clearSchedule PATCHes a null schedule when one is set, carrying description", async () => {
    const mockClient = {
      getPackage: mock(() =>
        Promise.resolve({
          description: "keep me",
          materialization: { schedule: "0 6 * * *" },
        }),
      ),
      updatePackage: mock(() => Promise.resolve({})),
    } as unknown as PublisherClient;

    await scheduleCommands.clearSchedule(mockClient, "env", "orders");

    expect(mockClient.updatePackage).toHaveBeenCalledWith("env", "orders", {
      name: "orders",
      description: "keep me",
      materialization: { schedule: null },
    });
  });

  test("clearSchedule no-ops when no schedule is set (never wipes freshness)", async () => {
    const mockClient = {
      getPackage: mock(() =>
        Promise.resolve({
          description: "keep me",
          materialization: { freshness: { window: "24h" } },
        }),
      ),
      updatePackage: mock(() => Promise.resolve({})),
    } as unknown as PublisherClient;

    await scheduleCommands.clearSchedule(mockClient, "env", "orders");

    expect(mockClient.updatePackage).not.toHaveBeenCalled();
  });

  test("clearSchedule clears a stuck empty-string schedule (presence is != null, not truthiness)", async () => {
    // A package can get stuck with `schedule: ""` (the server does not yet
    // reject an empty cron at the publish gate). A truthiness guard would treat
    // "" as absent and refuse to clear it; presence is `!= null`, so the clear
    // goes through and unsticks the package.
    const mockClient = {
      getPackage: mock(() =>
        Promise.resolve({
          description: "keep me",
          materialization: { schedule: "" },
        }),
      ),
      updatePackage: mock(() => Promise.resolve({})),
    } as unknown as PublisherClient;

    await scheduleCommands.clearSchedule(mockClient, "env", "orders");

    expect(mockClient.updatePackage).toHaveBeenCalledWith("env", "orders", {
      name: "orders",
      description: "keep me",
      materialization: { schedule: null },
    });
  });

  test("setSchedule surfaces a server rejection (invalid cron)", async () => {
    const mockClient = {
      getPackage: mock(() => Promise.resolve({})),
      updatePackage: mock(() =>
        Promise.reject(new Error("400: not a valid 5-field UNIX cron")),
      ),
    } as unknown as PublisherClient;

    await expect(
      scheduleCommands.setSchedule(mockClient, "env", "orders", "bogus"),
    ).rejects.toThrow(/cron/);
  });
});
