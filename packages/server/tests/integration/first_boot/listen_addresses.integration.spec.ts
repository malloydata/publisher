// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import {
   pollUntil,
   spawnServer,
   type SpawnedServer,
} from "../../harness/spawn_server";

/**
 * The MCP endpoint is unauthenticated and exposes every tool, so it binds
 * loopback unless told otherwise, while REST keeps its own wider default. Read
 * from the listening lines a real boot prints, which name the address each
 * listener actually bound.
 */
setDefaultTimeout(120_000);

const listeningAt = (log: string, label: string) =>
   log.match(new RegExp(`${label} listening at http://([^\\s]+):\\d+`))?.[1];

describe("listener bind addresses", () => {
   let server: SpawnedServer | undefined;
   afterEach(async () => {
      await server?.stop();
      server = undefined;
   });

   async function boot(env: Record<string, string | undefined>) {
      server = await spawnServer({
         env: { PUBLISHER_HOST: undefined, MCP_HOST: undefined, ...env },
      });
      const s = server;
      const up = await pollUntil(
         () =>
            s.exited() ||
            (listeningAt(s.output(), "MCP server") !== undefined &&
               listeningAt(s.output(), "Publisher server") !== undefined),
         60_000,
      );
      expect(up && !s.exited(), s.output().slice(-2000)).toBe(true);
      return {
         rest: listeningAt(s.output(), "Publisher server"),
         mcp: listeningAt(s.output(), "MCP server"),
      };
   }

   it("binds MCP to loopback and REST to every interface by default", async () => {
      expect(await boot({})).toEqual({ rest: "0.0.0.0", mcp: "127.0.0.1" });
   });

   it("moves both listeners together with an explicit --host", async () => {
      expect(await boot({ PUBLISHER_HOST: "127.0.0.1" })).toEqual({
         rest: "127.0.0.1",
         mcp: "127.0.0.1",
      });
   });
});
