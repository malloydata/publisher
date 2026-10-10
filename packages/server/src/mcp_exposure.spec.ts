// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The MCP endpoint is unauthenticated and exposes every MCP tool, so two things
 * decide who can reach it: the address it binds, and which origins a browser
 * will hand its responses to.
 *
 * Both are asserted against the real thing rather than a reimplementation. The
 * CORS cases drive the actual `cors` middleware over a real request and read the
 * headers it emits, because the policy that matters is the one the library
 * applies, not the shape of the option object. The bind cases assert the
 * resolution rule that `server.ts` applies to MCP_HOST / PUBLISHER_HOST; the
 * literal `mcpApp.listen(MCP_PORT, MCP_HOST, ...)` wiring is pinned from source
 * here, since booting the module binds real ports and starts the scheduler.
 */

import { afterEach, describe, expect, it } from "bun:test";
import cors from "cors";
import express from "express";
import { readFileSync } from "fs";
import { resolve } from "path";
import request from "supertest";
import { getMcpCorsOrigins } from "./config";

const ENV = "MCP_CORS_ORIGINS";

/** A minimal app wired the way server.ts wires the MCP endpoint. */
function appWithCors() {
   const app = express();
   // codeql[js/cors-permissive-configuration]: this fixture mirrors the
   // production wiring on purpose -- asserting the default denies, and that an
   // allowlist is honoured, requires calling the real resolver the same way
   // server.ts does. Rewriting it to satisfy the scanner would stop it testing
   // the thing it exists for.
   app.use("/mcp", cors({ origin: getMcpCorsOrigins() }));
   app.post("/mcp", (_req, res) => res.status(200).json({ ok: true }));
   return app;
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
         const response = await request(appWithCors())
            .post("/mcp")
            .set("Origin", "https://evil.example");
         expect(
            response.headers["access-control-allow-origin"],
         ).toBeUndefined();
      },
   );

   it("allows a configured origin and refuses one that is not listed", async () => {
      set("https://console.example,https://ops.example");
      const allowed = await request(appWithCors())
         .post("/mcp")
         .set("Origin", "https://console.example");
      expect(allowed.headers["access-control-allow-origin"]).toBe(
         "https://console.example",
      );

      const refused = await request(appWithCors())
         .post("/mcp")
         .set("Origin", "https://evil.example");
      expect(refused.headers["access-control-allow-origin"]).toBeUndefined();
   });

   it("tolerates whitespace around a listed origin", async () => {
      set("  https://console.example ,  https://ops.example  ");
      const response = await request(appWithCors())
         .post("/mcp")
         .set("Origin", "https://ops.example");
      expect(response.headers["access-control-allow-origin"]).toBe(
         "https://ops.example",
      );
   });

   it("allows any origin only on an explicit wildcard", async () => {
      set("*");
      const response = await request(appWithCors())
         .post("/mcp")
         .set("Origin", "https://anything.example");
      expect(response.headers["access-control-allow-origin"]).toBe("*");
   });

   // A separator-only value parses to no origins. It must deny rather than be
   // read as "allow everything".
   it.each([[","], [" , "]])(
      "refuses every origin for the separator-only value %p",
      async (value) => {
         set(value);
         const response = await request(appWithCors())
            .post("/mcp")
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
      const response = await request(appWithCors()).post("/mcp");
      expect(response.status).toBe(200);
      expect(response.body).toEqual({ ok: true });
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
   // The rule server.ts applies: MCP_HOST, else an explicit PUBLISHER_HOST, else
   // loopback.
   const resolve_ = (mcpHost?: string, publisherHost?: string) =>
      mcpHost || publisherHost || "127.0.0.1";

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

   describe("as wired in server.ts", () => {
      const source = readFileSync(
         resolve(import.meta.dir, "server.ts"),
         "utf8",
      );
      // Only line comments are stripped. A block-comment regex is unsound here:
      // `server.ts` carries route-path literals containing `/*` (the `public/*`
      // and `notebooks/*` wildcards), and a lazy `/\/\*[\s\S]*?\*\//` reads one
      // of those as a comment opener and deletes ~39KB of live code up to the
      // next `*` + `/`. Matching the raw text cannot be fooled that way.
      // CRLF first: the repo pins no .gitattributes, so a Windows checkout has
      // \r\n and the multi-line needles below (which spell \n) match nothing.
      // That is a false failure on one platform for source that is correct.
      const withoutComments = source
         .replace(/\r\n/g, "\n")
         .replace(/^\s*\/\/.*$/gm, "");

      it("defaults MCP_HOST to loopback, falling back to PUBLISHER_HOST", () => {
         expect(withoutComments).toContain(
            'process.env.MCP_HOST || process.env.PUBLISHER_HOST || "127.0.0.1"',
         );
      });

      // The listener must take MCP_HOST. Passing PUBLISHER_HOST here would put
      // the endpoint back on every interface with every other test still green.
      it("binds the MCP listener to MCP_HOST", () => {
         expect(withoutComments).toContain(
            "mcpApp.listen(\n   MCP_PORT,\n   MCP_HOST,",
         );
      });

      it("leaves the REST listener on PUBLISHER_HOST", () => {
         expect(withoutComments).toContain(
            "mainServer.listen(PUBLISHER_PORT, PUBLISHER_HOST,",
         );
      });

      /**
       * The behavioural CORS cases above build their own app from
       * `getMcpCorsOrigins`, so they prove the POLICY but cannot see whether the
       * MCP endpoint actually applies it. Without this pin, reverting the
       * registration to a bare `cors()` leaves every one of them green.
       */
      it("registers the allowlist on the MCP endpoint, not a bare cors()", () => {
         expect(withoutComments).toContain(
            "mcpApp.use(MCP_ENDPOINT, cors({ origin: getMcpCorsOrigins() }));",
         );
         const mcpCorsRegistrations = withoutComments
            .split("\n")
            .map((line) => line.trim())
            .filter(
               (line) =>
                  line.startsWith("mcpApp.use(") && line.includes("cors("),
            );
         expect(mcpCorsRegistrations).toEqual([
            "mcpApp.use(MCP_ENDPOINT, cors({ origin: getMcpCorsOrigins() }));",
         ]);
      });
   });
});
