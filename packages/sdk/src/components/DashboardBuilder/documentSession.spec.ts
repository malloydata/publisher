// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it, mock } from "bun:test";
import type { ApiClients } from "../ServerProvider";
import {
   resolveEditorTarget,
   withWorkspace,
   writePackageFile,
} from "./documentSession";

const clientWith = (
   updateModelSource: (...args: unknown[]) => Promise<unknown>,
) => ({ models: { updateModelSource } }) as unknown as ApiClients;

const base = {
   environmentName: "env",
   packageName: "pkg",
   modelPath: "dashboards/a.malloy",
   source: "new text",
   expectedHash: "h0",
};

/** A client whose invalidations are recorded in call order. */
const spyingClient = () => {
   const queryClient = new QueryClient();
   const calls: { queryKey: unknown; refetchType?: string }[] = [];
   queryClient.invalidateQueries = mock((filters: never) => {
      calls.push(filters);
      return Promise.resolve();
   }) as never;
   return { queryClient, calls };
};

describe("writePackageFile", () => {
   it("sends the source against the expected hash and returns the server's hash", async () => {
      const update = mock(async () => ({ data: { contentHash: "h1" } }));
      const { queryClient } = spyingClient();
      const hash = await writePackageFile({
         ...base,
         apiClients: clientWith(update),
         queryClient,
         invalidateOnError: [],
         invalidate: [],
      });
      expect(hash).toBe("h1");
      expect(update).toHaveBeenCalledWith("env", "pkg", "dashboards/a.malloy", {
         source: "new text",
         expectedHash: "h0",
      });
   });

   it("invalidates exactly the caller's list, in order, after afterWrite", async () => {
      const { queryClient, calls } = spyingClient();
      const order: string[] = [];
      await writePackageFile({
         ...base,
         apiClients: clientWith(async () => ({ data: { contentHash: "h1" } })),
         queryClient,
         invalidateOnError: [["on-error"]],
         afterWrite: (hash) => {
            order.push(`after:${hash}:${calls.length}`);
         },
         invalidate: [
            { queryKey: ["a"], wait: true },
            { queryKey: ["b"], refetchType: "none" },
         ],
      });
      expect(order).toEqual(["after:h1:0"]);
      expect(calls).toEqual([
         { queryKey: ["a"] },
         { queryKey: ["b"], refetchType: "none" },
      ]);
   });

   it("holds the result until a waited invalidation lands, and not for the rest", async () => {
      const queryClient = new QueryClient();
      let release = () => {};
      queryClient.invalidateQueries = mock((filters: { queryKey: string[] }) =>
         filters.queryKey[0] === "slow"
            ? new Promise<void>((r) => {
                 release = r;
              })
            : new Promise<void>(() => {}),
      ) as never;
      let done = false;
      const written = writePackageFile({
         ...base,
         apiClients: clientWith(async () => ({ data: { contentHash: "h1" } })),
         queryClient,
         invalidateOnError: [],
         invalidate: [
            { queryKey: ["slow"], wait: true },
            { queryKey: ["never-settles"] },
         ],
      }).then(() => {
         done = true;
      });
      await new Promise((r) => setTimeout(r, 0));
      expect(done).toBe(false);
      release();
      await written;
      expect(done).toBe(true);
   });

   it("invalidates the error list, skips success work, and throws the server's reason on a refused write", async () => {
      const { queryClient, calls } = spyingClient();
      const afterWrite = mock(() => {});
      const refused = Object.assign(new Error("Request failed"), {
         response: { data: { message: "file changed since you opened it" } },
      });
      await expect(
         writePackageFile({
            ...base,
            apiClients: clientWith(async () => {
               throw refused;
            }),
            queryClient,
            invalidateOnError: [["model"], ["other"]],
            afterWrite,
            invalidate: [{ queryKey: ["success-only"] }],
         }),
      ).rejects.toThrow("file changed since you opened it");
      expect(calls).toEqual([{ queryKey: ["model"] }, { queryKey: ["other"] }]);
      expect(afterWrite).not.toHaveBeenCalled();
   });
});

describe("resolveEditorTarget", () => {
   it("reads the environment, package and version off a resource URI", () => {
      expect(
         resolveEditorTarget({
            resourceUri:
               "publisher://environments/prod/packages/sales?versionId=v7",
         }),
      ).toEqual({
         environmentName: "prod",
         packageName: "sales",
         versionId: "v7",
         namesBoth: true,
      });
   });

   it("takes the deprecated form as given, with no version", () => {
      expect(
         resolveEditorTarget({ environmentName: "prod", packageName: "sales" }),
      ).toEqual({
         environmentName: "prod",
         packageName: "sales",
         versionId: undefined,
         namesBoth: true,
      });
   });

   it("degrades a string that is not a publisher URI instead of throwing", () => {
      expect(resolveEditorTarget({ resourceUri: "not a uri" })).toEqual({
         environmentName: "",
         packageName: "",
         versionId: undefined,
         namesBoth: false,
      });
   });

   it("says so when the URI names an environment but no package", () => {
      const target = resolveEditorTarget({
         resourceUri: "publisher://environments/prod",
      });
      expect(target.namesBoth).toBe(false);
   });
});

describe("withWorkspace", () => {
   const saved: { type: string; where: string; workspace?: string } = {
      type: "dashboard.saved",
      where: "host",
   };

   it("names the workspace on a save a workspace took", () => {
      expect(withWorkspace(saved, "records")).toEqual({
         ...saved,
         workspace: "records",
      });
   });

   it("leaves a package save alone", () => {
      const pkg = { type: "notebook.saved", where: "package" };
      expect(withWorkspace(pkg, "records")).toBe(pkg);
   });

   it("leaves other events and an unnamed workspace alone", () => {
      const refused = { type: "dashboard.save_refused", reason: "x" };
      expect(withWorkspace(refused, "records")).toBe(refused);
      expect(withWorkspace(saved, undefined)).toBe(saved);
   });
});
