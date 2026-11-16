// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherApiError, PublisherClient } from "../api/client";
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
  test("setSchedule adds the publisher.json form to the server's PACKAGE_IS_VERSIONED refusal", async () => {
    // A published version is immutable: the server refuses the PATCH, and the
    // CLI says where a version's schedule is declared.
    const mockClient = {
      getPackage: mock(() => Promise.resolve({ versionId: "1.0.0" })),
      updatePackage: mock(() =>
        Promise.reject(
          new PublisherApiError(
            409,
            "PACKAGE_IS_VERSIONED",
            "Publisher API Error (409): Package orders has published versions, which are immutable.",
          ),
        ),
      ),
    } as unknown as PublisherClient;

    await expect(
      scheduleCommands.setSchedule(mockClient, "env", "orders", "0 6 * * *"),
    ).rejects.toThrow(
      /published versions, which are immutable\. .*publisher\.json.*"materialization": \{ "scope": "version", "schedule"/s,
    );
    expect(mockClient.updatePackage).toHaveBeenCalled();
  });

  test("clearSchedule adds the same hint to a PACKAGE_IS_VERSIONED refusal", async () => {
    const mockClient = {
      getPackage: mock(() =>
        Promise.resolve({
          versionId: "1.0.0",
          materialization: { schedule: "0 6 * * *" },
        }),
      ),
      updatePackage: mock(() =>
        Promise.reject(
          new PublisherApiError(409, "PACKAGE_IS_VERSIONED", "immutable."),
        ),
      ),
    } as unknown as PublisherClient;

    await expect(
      scheduleCommands.clearSchedule(mockClient, "env", "orders"),
    ).rejects.toThrow(/publisher\.json/);
  });

  test("a 409 with another reason passes through unchanged", async () => {
    const refusal = new PublisherApiError(
      409,
      "PACKAGE_INSTALLED_FROM_LOCATION",
      "installed from a location",
    );
    const mockClient = {
      getPackage: mock(() => Promise.resolve({})),
      updatePackage: mock(() => Promise.reject(refusal)),
    } as unknown as PublisherClient;

    await expect(
      scheduleCommands.setSchedule(mockClient, "env", "orders", "0 6 * * *"),
    ).rejects.toBe(refusal);
  });
});
