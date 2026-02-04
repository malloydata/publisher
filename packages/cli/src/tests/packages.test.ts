import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as packageCommands from "../commands/packages";

describe("Package Commands", () => {
  let consoleLogSpy: any;

  beforeEach(() => {
    consoleLogSpy = mock(() => {});
    console.log = consoleLogSpy as any;
  });

  test("listPackages should call listPackages API", async () => {
    const mockClient = {
      listPackages: mock(() =>
        Promise.resolve([{ name: "pkg-1", location: "/packages/pkg-1" }]),
      ),
    } as unknown as PublisherClient;

    await packageCommands.listPackages(mockClient, "test-project");

    expect(mockClient.listPackages).toHaveBeenCalledWith("test-project");
  });

  test("listPackages should handle empty package list", async () => {
    const mockClient = {
      listPackages: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await packageCommands.listPackages(mockClient, "empty-project");

    expect(mockClient.listPackages).toHaveBeenCalledWith("empty-project");
  });

  test("getPackage should fetch and print package JSON", async () => {
    const pkg = {
      name: "pkg-1",
      location: "/packages/pkg-1",
      description: "Test package",
    };

    const mockClient = {
      getPackage: mock(() => Promise.resolve(pkg)),
    } as unknown as PublisherClient;

    await packageCommands.getPackage(mockClient, "proj", "pkg-1");

    expect(mockClient.getPackage).toHaveBeenCalledWith("proj", "pkg-1");
    expect(consoleLogSpy).toHaveBeenCalledWith(JSON.stringify(pkg, null, 2));
  });

  test("createPackage should call API with correct arguments", async () => {
    const mockClient = {
      createPackage: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await packageCommands.createPackage(
      mockClient,
      "proj",
      "pkg-1",
      "/packages/pkg-1",
      "My package",
    );

    expect(mockClient.createPackage).toHaveBeenCalledWith(
      "proj",
      "pkg-1",
      "/packages/pkg-1",
      "My package",
    );
  });

  test("createPackage should work without description", async () => {
    const mockClient = {
      createPackage: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await packageCommands.createPackage(
      mockClient,
      "proj",
      "pkg-1",
      "/packages/pkg-1",
    );

    expect(mockClient.createPackage).toHaveBeenCalledWith(
      "proj",
      "pkg-1",
      "/packages/pkg-1",
      undefined,
    );
  });

  test("updatePackage should call update API with location", async () => {
    const mockClient = {
      updatePackage: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await packageCommands.updatePackage(mockClient, "proj", "pkg-1", {
      location: "/new/location",
    });

    expect(mockClient.updatePackage).toHaveBeenCalledWith("proj", "pkg-1", {
      name: "pkg-1",
      location: "/new/location",
    });
  });

  test("updatePackage should call update API with description", async () => {
    const mockClient = {
      updatePackage: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await packageCommands.updatePackage(mockClient, "proj", "pkg-1", {
      description: "Updated description",
    });

    expect(mockClient.updatePackage).toHaveBeenCalledWith("proj", "pkg-1", {
      name: "pkg-1",
      description: "Updated description",
    });
  });

  test("deletePackage should call delete API with correct args", async () => {
    const mockClient = {
      deletePackage: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await packageCommands.deletePackage(mockClient, "proj", "pkg-1");

    expect(mockClient.deletePackage).toHaveBeenCalledWith("proj", "pkg-1");
  });
});
