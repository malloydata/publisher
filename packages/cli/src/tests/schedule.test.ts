// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
});
