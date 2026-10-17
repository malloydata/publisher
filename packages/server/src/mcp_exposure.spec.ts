// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The MCP endpoint is unauthenticated and exposes every MCP tool, so two things
 * decide who can reach it: the address it binds, and which origins a browser
 * will hand its responses to.
 *
 * The CORS cases drive the real MCP app over a real request and read the
 * headers it emits, because the policy that matters is the one the library
 * applies, not the shape of the option object. The bind cases assert the
 * resolution rule the server applies to MCP_HOST / PUBLISHER_HOST; the
 * listeners themselves are covered by the first-boot integration spec.
 */

import { afterEach, describe, expect, it } from "bun:test";
import request from "supertest";
import { getMcpCorsOrigins } from "./config";
import { createMcpApp, resolveMcpHost } from "./routes/mcp";
import type { EnvironmentStore } from "./service/environment_store";

const ENV = "MCP_CORS_ORIGINS";

/**
 * The MCP app the server listens with. A GET is refused with 405 before the
 * store is touched, and CORS headers are applied ahead of that, so it reads the
 * policy without a tool call.
 */
function mcpApp() {
   return createMcpApp({} as EnvironmentStore);
}

describe("MCP CORS allowlist", () => {
   const saved = process.env[ENV];

   afterEach(() => {
      if (saved === undefined) {
         delete process.env[ENV];
      } else {
         process.env[ENV] = saved;
      }
   });

   const set = (value: string | undefined) => {
      if (value === undefined) {
         delete process.env[ENV];
      } else {
         process.env[ENV] = value;
      }
   };

   // The default. `cors()` with no options reflects the caller's own Origin,
   // which lets any browser page read a response from this endpoint.
   it.each([[undefined], [""], ["   "]])(
      "sends no allow-origin for a cross-origin request when the env is %p",
      async (value) => {
         set(value);
         const response = await request(mcpApp())
            .get("/mcp")
            .set("Origin", "https://evil.example");
         expect(
            response.headers["access-control-allow-origin"],
         ).toBeUndefined();
      },
   );

   it("allows a configured origin and refuses one that is not listed", async () => {
      set("https://console.example,https://ops.example");
      const allowed = await request(mcpApp())
         .get("/mcp")
         .set("Origin", "https://console.example");
      expect(allowed.headers["access-control-allow-origin"]).toBe(
         "https://console.example",
      );

      const refused = await request(mcpApp())
         .get("/mcp")
         .set("Origin", "https://evil.example");
      expect(refused.headers["access-control-allow-origin"]).toBeUndefined();
   });

   it("tolerates whitespace around a listed origin", async () => {
      set("  https://console.example ,  https://ops.example  ");
      const response = await request(mcpApp())
         .get("/mcp")
         .set("Origin", "https://ops.example");
      expect(response.headers["access-control-allow-origin"]).toBe(
         "https://ops.example",
      );
   });

   it("allows any origin only on an explicit wildcard", async () => {
      set("*");
      const response = await request(mcpApp())
         .get("/mcp")
         .set("Origin", "https://anything.example");
      expect(response.headers["access-control-allow-origin"]).toBe("*");
   });

   // A separator-only value parses to no origins. It must deny rather than be
   // read as "allow everything".
   it.each([[","], [" , "]])(
      "refuses every origin for the separator-only value %p",
      async (value) => {
         set(value);
         const response = await request(mcpApp())
            .get("/mcp")
            .set("Origin", "https://evil.example");
         expect(
            response.headers["access-control-allow-origin"],
         ).toBeUndefined();
      },
   );

   // A non-browser MCP client sends no Origin, so the allowlist must not turn
   // into a connection gate for the agents this endpoint exists to serve.
   it("still answers a request that carries no Origin", async () => {
      set(undefined);
      // A GET reaches the handler, which refuses it as stateless-only: the
      // request was not stopped by CORS.
      const response = await request(mcpApp()).get("/mcp");
      expect(response.status).toBe(405);
      expect(response.body.error.message).toBe(
         "Method Not Allowed in Stateless Mode",
      );
   });
});

describe("getMcpCorsOrigins", () => {
   const saved = process.env[ENV];

   afterEach(() => {
      if (saved === undefined) {
         delete process.env[ENV];
      } else {
         process.env[ENV] = saved;
      }
   });

   it("defaults to false, which sends no allow-origin", () => {
      delete process.env[ENV];
      expect(getMcpCorsOrigins()).toBe(false);
   });

   it("splits a comma-separated list", () => {
      process.env[ENV] = "https://a.example, https://b.example";
      expect(getMcpCorsOrigins()).toEqual([
         "https://a.example",
         "https://b.example",
      ]);
   });

   it("passes a wildcard through", () => {
      process.env[ENV] = "*";
      expect(getMcpCorsOrigins()).toBe("*");
   });
});

/**
 * The MCP bind default. Loopback unless an operator widens it, so a naive
 * deployment does not publish an unauthenticated tool endpoint on every
 * interface.
 */
describe("MCP host resolution", () => {
   const resolve_ = (MCP_HOST?: string, PUBLISHER_HOST?: string) =>
      resolveMcpHost({ MCP_HOST, PUBLISHER_HOST });

   it("binds loopback when neither host is set", () => {
      expect(resolve_(undefined, undefined)).toBe("127.0.0.1");
   });

   it("follows an explicit --host so both listeners move together", () => {
      expect(resolve_(undefined, "0.0.0.0")).toBe("0.0.0.0");
   });

   it("lets MCP_HOST override PUBLISHER_HOST", () => {
      expect(resolve_("127.0.0.1", "0.0.0.0")).toBe("127.0.0.1");
      expect(resolve_("0.0.0.0", "127.0.0.1")).toBe("0.0.0.0");
   });
});
