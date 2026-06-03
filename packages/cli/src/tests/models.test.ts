import { describe, test, expect, mock, beforeEach } from "bun:test";
import { PublisherClient } from "../api/client";
import * as modelCommands from "../commands/models";

describe("Model Commands", () => {
  beforeEach(() => {
    console.log = mock(() => {}) as any;
  });

  test("listModels calls the API", async () => {
    const mockClient = {
      listModels: mock(() =>
        Promise.resolve([{ path: "flights.malloy", error: "" }]),
      ),
    } as unknown as PublisherClient;

    await modelCommands.listModels(mockClient, "env", "pkg");

    expect(mockClient.listModels).toHaveBeenCalledWith("env", "pkg");
  });

  test("listModels handles empty list", async () => {
    const mockClient = {
      listModels: mock(() => Promise.resolve([])),
    } as unknown as PublisherClient;

    await modelCommands.listModels(mockClient, "env", "pkg");

    expect(mockClient.listModels).toHaveBeenCalledWith("env", "pkg");
  });

  test("getModel fetches and prints JSON", async () => {
    const model = { path: "flights.malloy", queries: [] };
    const mockClient = {
      getModel: mock(() => Promise.resolve(model)),
    } as unknown as PublisherClient;

    await modelCommands.getModel(mockClient, "env", "pkg", "flights.malloy");

    expect(mockClient.getModel).toHaveBeenCalledWith(
      "env",
      "pkg",
      "flights.malloy",
    );
  });
});
