import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as environmentCommands from "../commands/environments";

describe("Environment Commands", () => {
  let consoleLogSpy: any;

  beforeEach(() => {
    consoleLogSpy = mock(() => {});
    console.log = consoleLogSpy as any;
  });

  test("listEnvironments should call listEnvironments API", async () => {
    const mockClient = {
      getBaseURL: () => "http://localhost:4000",
      listEnvironments: mock(() =>
        Promise.resolve([
          { name: "test-environment", packages: [], connections: [] },
        ]),
      ),
    } as unknown as PublisherClient;

    await environmentCommands.listEnvironments(mockClient);

    expect(mockClient.listEnvironments).toHaveBeenCalled();
  });

  test("listEnvironments should handle empty environment list", async () => {
    const mockClient = {
      getBaseURL: () => "http://localhost:4000",
      listEnvironments: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await environmentCommands.listEnvironments(mockClient);

    expect(mockClient.listEnvironments).toHaveBeenCalled();
  });

  test("getEnvironment should fetch and print environment JSON", async () => {
    const environment = { name: "my-environment", readme: "docs" };

    const mockClient = {
      getEnvironment: mock(() => Promise.resolve(environment)),
    } as unknown as PublisherClient;

    await environmentCommands.getEnvironment(mockClient, "my-environment");

    expect(mockClient.getEnvironment).toHaveBeenCalledWith("my-environment");
    expect(consoleLogSpy).toHaveBeenCalledWith(
      JSON.stringify(environment, null, 2),
    );
  });

  test("createEnvironment should call API with environment name", async () => {
    const mockClient = {
      createEnvironment: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await environmentCommands.createEnvironment(mockClient, "new-environment");

    expect(mockClient.createEnvironment).toHaveBeenCalledWith(
      "new-environment",
    );
  });

  test("updateEnvironment should call update API with readme", async () => {
    const mockClient = {
      updateEnvironment: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await environmentCommands.updateEnvironment(mockClient, "env", {
      readme: "Updated README",
    });

    expect(mockClient.updateEnvironment).toHaveBeenCalledWith("env", {
      name: "env",
      readme: "Updated README",
    });
  });

  test("updateEnvironment should call update API with location", async () => {
    const mockClient = {
      updateEnvironment: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await environmentCommands.updateEnvironment(mockClient, "env", {
      location: "/data/environments/env",
    });

    expect(mockClient.updateEnvironment).toHaveBeenCalledWith("env", {
      name: "env",
      location: "/data/environments/env",
    });
  });

  test("updateEnvironment should not call API if no updates provided", async () => {
    const mockClient = {
      updateEnvironment: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await environmentCommands.updateEnvironment(mockClient, "env", {});

    expect(mockClient.updateEnvironment).not.toHaveBeenCalled();
  });

  test("deleteEnvironment should call delete API with environment name", async () => {
    const mockClient = {
      deleteEnvironment: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await environmentCommands.deleteEnvironment(mockClient, "old-environment");

    expect(mockClient.deleteEnvironment).toHaveBeenCalledWith(
      "old-environment",
    );
  });
});
