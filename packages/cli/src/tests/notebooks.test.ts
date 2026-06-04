import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as notebookCommands from "../commands/notebooks";

describe("Notebook Commands", () => {
  beforeEach(() => {
    console.log = mock(() => {}) as any;
  });

  test("listNotebooks calls the API", async () => {
    const mockClient = {
      listNotebooks: mock(() =>
        Promise.resolve([{ path: "analysis.malloynb", error: "" }]),
      ),
    } as unknown as PublisherClient;

    await notebookCommands.listNotebooks(mockClient, "env", "pkg");

    expect(mockClient.listNotebooks).toHaveBeenCalledWith("env", "pkg");
  });

  test("listNotebooks handles empty list", async () => {
    const mockClient = {
      listNotebooks: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await notebookCommands.listNotebooks(mockClient, "env", "pkg");

    expect(mockClient.listNotebooks).toHaveBeenCalledWith("env", "pkg");
  });

  test("getNotebook fetches and prints JSON", async () => {
    const notebook = { path: "analysis.malloynb", notebookCells: [] };
    const mockClient = {
      getNotebook: mock(() => Promise.resolve(notebook)),
    } as unknown as PublisherClient;

    await notebookCommands.getNotebook(
      mockClient,
      "env",
      "pkg",
      "analysis.malloynb",
    );

    expect(mockClient.getNotebook).toHaveBeenCalledWith(
      "env",
      "pkg",
      "analysis.malloynb",
    );
  });
});
