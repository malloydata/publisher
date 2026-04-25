import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as projectCommands from "../commands/projects";

describe("Project Commands", () => {
  let consoleLogSpy: any;

  beforeEach(() => {
    consoleLogSpy = mock(() => {});
    console.log = consoleLogSpy as any;
  });

  test("listProjects should call listProjects API", async () => {
    const mockClient = {
      getBaseURL: () => "http://localhost:4000",
      listProjects: mock(() =>
        Promise.resolve([
          { name: "test-project", packages: [], connections: [] },
        ]),
      ),
    } as unknown as PublisherClient;

    await projectCommands.listProjects(mockClient);

    expect(mockClient.listProjects).toHaveBeenCalled();
  });

  test("listProjects should handle empty project list", async () => {
    const mockClient = {
      getBaseURL: () => "http://localhost:4000",
      listProjects: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await projectCommands.listProjects(mockClient);

    expect(mockClient.listProjects).toHaveBeenCalled();
  });

  test("getProject should fetch and print project JSON", async () => {
    const project = { name: "my-project", readme: "docs" };

    const mockClient = {
      getProject: mock(() => Promise.resolve(project)),
    } as unknown as PublisherClient;

    await projectCommands.getProject(mockClient, "my-project");

    expect(mockClient.getProject).toHaveBeenCalledWith("my-project");
    expect(consoleLogSpy).toHaveBeenCalledWith(
      JSON.stringify(project, null, 2),
    );
  });

  test("createProject should call API with project name", async () => {
    const mockClient = {
      createProject: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await projectCommands.createProject(mockClient, "new-project");

    expect(mockClient.createProject).toHaveBeenCalledWith("new-project");
  });

  test("updateProject should call update API with readme", async () => {
    const mockClient = {
      updateProject: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await projectCommands.updateProject(mockClient, "proj", {
      readme: "Updated README",
    });

    expect(mockClient.updateProject).toHaveBeenCalledWith("proj", {
      name: "proj",
      readme: "Updated README",
    });
  });

  test("updateProject should call update API with location", async () => {
    const mockClient = {
      updateProject: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await projectCommands.updateProject(mockClient, "proj", {
      location: "/data/projects/proj",
    });

    expect(mockClient.updateProject).toHaveBeenCalledWith("proj", {
      name: "proj",
      location: "/data/projects/proj",
    });
  });

  test("updateProject should not call API if no updates provided", async () => {
    const mockClient = {
      updateProject: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await projectCommands.updateProject(mockClient, "proj", {});

    expect(mockClient.updateProject).not.toHaveBeenCalled();
  });

  test("deleteProject should call delete API with project name", async () => {
    const mockClient = {
      deleteProject: mock(() => Promise.resolve()),
    } as unknown as PublisherClient;

    await projectCommands.deleteProject(mockClient, "old-project");

    expect(mockClient.deleteProject).toHaveBeenCalledWith("old-project");
  });
});
