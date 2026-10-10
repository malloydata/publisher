// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import express from "express";
import type { Server } from "http";

/**
 * The skills route reads its name from an Express wildcard, which Express 4
 * has already percent-decoded. server.ts decoded it a second time, so a name
 * containing a bare `%` threw URIError and answered 500. This pins the framework
 * behaviour the route now relies on, with the route's own shape.
 */
describe("skills wildcard route", () => {
   let server: Server;
   let base: string;

   beforeAll(async () => {
      const app = express();
      app.get("/packages/:packageName/skills/*", (req, res) => {
         res.json({
            name: (req.params as unknown as Record<string, string>)[0] ?? "",
         });
      });
      await new Promise<void>((resolve) => {
         server = app.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      if (address === null || typeof address === "string") {
         throw new Error("no port");
      }
      base = `http://127.0.0.1:${address.port}`;
   });

   afterAll(() => {
      server.close();
   });

   const nameFor = async (urlPath: string) =>
      ((await (await fetch(`${base}${urlPath}`)).json()) as { name: string })
         .name;

   it("hands the handler a decoded name, slash included", async () => {
      expect(await nameFor("/packages/p/skills/revenue-rules/margin")).toBe(
         "revenue-rules/margin",
      );
      expect(await nameFor("/packages/p/skills/revenue-rules%2Fmargin")).toBe(
         "revenue-rules/margin",
      );
   });

   it("decodes exactly once, so an encoded percent stays a percent", async () => {
      expect(await nameFor("/packages/p/skills/a%2541")).toBe("a%41");
   });
});
