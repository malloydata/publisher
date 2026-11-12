// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { createServer, type Server } from "node:net";
import { defaultContainerName, firstFreePort, portIsFree } from "./postgres";

function listen(port: number): Promise<Server> {
   return new Promise((resolve, reject) => {
      const server = createServer();
      server.once("error", reject);
      server.listen(port, () => resolve(server));
   });
}

describe("the hammer Postgres container", () => {
   it("is named for its port, so runs on different ports never share one", () => {
      expect(defaultContainerName(55432)).toBe("publisher-hammer-pg-55432");
      expect(defaultContainerName(55433)).not.toBe(defaultContainerName(55432));
   });

   it("skips a port something already listens on", async () => {
      const start = await firstFreePort(56100);
      const held = await listen(start);
      try {
         expect(await portIsFree(start)).toBe(false);
         expect(await firstFreePort(start)).toBeGreaterThan(start);
      } finally {
         await new Promise((r) => held.close(r));
      }
      expect(await portIsFree(start)).toBe(true);
   });
});
