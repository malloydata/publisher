// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { EnvironmentStore } from "../../service/environment_store";
import { registerListPackagesTool } from "./list_packages_tool";

type Result = {
   isError?: boolean;
   content: Array<{
      type: string;
      text?: string;
      resource?: { uri: string; text: string };
   }>;
};

function listPackages(store: unknown): Promise<Result> {
   let handler: (() => Promise<Result>) | undefined;
   registerListPackagesTool(
      {
         registerTool: (_n: string, _config: unknown, h: typeof handler) => {
            handler = h;
         },
      } as never,
      store as EnvironmentStore,
   );
   return handler!();
}

const parse = (r: Result) => JSON.parse(r.content[0]!.resource!.text);

describe("list_packages", () => {
   it("lists packages with their health, and an environment whose listing failed", async () => {
      const store = {
         listEnvironments: async () => [{ name: "ok" }, { name: "down" }],
         getEnvironment: async (name: string) => {
            if (name === "down") throw new Error("bucket unreachable");
            return {
               getStaleCompileErrors: () =>
                  new Map([["stale", { message: "parse error" }]]),
               listPackages: async () => [
                  { name: "sales", description: "orders" },
                  { name: "stale" },
               ],
               getFailedPackages: () => new Map([["broken", "bad manifest"]]),
            };
         },
      };
      const result = await listPackages(store);
      expect(result.isError).toBe(false);
      expect(result.content[0]!.resource!.uri).toBe(
         "malloy://environment/home#list-environments",
      );
      expect(parse(result)).toEqual({
         environments: [
            {
               name: "ok",
               packages: [
                  { name: "sales", description: "orders" },
                  { name: "stale", error: "parse error", stale: true },
                  { name: "broken", error: "bad manifest" },
               ],
            },
            { name: "down", error: "bucket unreachable", packages: [] },
         ],
      });
   });

   it("answers a failed catalog read with an error that keeps the success shape", async () => {
      const result = await listPackages({
         listEnvironments: async () => {
            throw new Error("store offline");
         },
      });
      expect(result.isError).toBe(true);
      const payload = parse(result);
      expect(payload.environments).toEqual([]);
      expect(payload.error).toBe(
         "An unexpected internal error occurred during listPackages.: store offline",
      );
      expect(result.content[1]).toMatchObject({ type: "text" });
   });
});
