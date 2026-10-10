// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Makes the authorize-bypass safety argument behavioural instead of textual.
 *
 * The claim the whole header design rests on is: the bypass reaches the query
 * path from the request HEADER and from nowhere else — in particular not from a
 * `bypassAuthorize` field in the request body, because a deployment fronting this
 * API generally reuses the same generated schema for its own public inbound body,
 * so a body-settable flag would be a gate-disabling control any external caller
 * could set.
 *
 * Unit tests on `readBypassAuthorize` cover the reader, and a source assertion in
 * `authorize_bypass_header.spec.ts` pins that the route derives the value from it.
 * Neither sends a request. This does: a real Express app, `express.json()`, the
 * same middleware and the same `getQuery` call shape `server.ts` registers, with
 * only the controller method stubbed so the test can read what the route actually
 * passed. A regression that made the body work again would leave every other test
 * in the suite green.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import express from "express";
import { readFileSync } from "fs";
import { request as httpRequest } from "http";
import { resolve } from "path";
import sinon from "sinon";
import request from "supertest";

import {
   BYPASS_AUTHORIZE_SECRET_ENV,
   readBypassAuthorize,
} from "./authorize_bypass_header";
import type { QueryController } from "./controller/query.controller";
import { queryConcurrency } from "./query_concurrency";

/** Positional index of `bypassAuthorize` in `QueryController.getQuery`. */
const BYPASS_ARGUMENT = 11;

function buildApp(): { app: express.Express; getQuery: sinon.SinonStub } {
   const getQuery = sinon.stub().resolves({ result: "{}", resource: "r" });
   const queryController = { getQuery } as unknown as QueryController;

   const app = express();
   app.use(express.json());

   // Same wiring as server.ts's `execute-query-model` route: middleware, then the
   // controller call with `readBypassAuthorize(req)` in the last position.
   app.post(
      "/api/v0/environments/:environmentName/packages/:packageName/models/*?/query",
      queryConcurrency(),
      async (req, res) => {
         const modelPath = (req.params as Record<string, string>)["0"];
         const result = await queryController.getQuery(
            req.params.environmentName,
            req.params.packageName,
            modelPath,
            req.body.sourceName as string,
            req.body.queryName as string,
            req.body.query as string,
            req.body.compactJson === true,
            req.body.filterParams ?? req.body.sourceFilters,
            req.body.bypassFilters === true ? true : undefined,
            req.body.givens,
            {
               queryMetadata: req.body?.queryMetadata,
               queryClass: req.body?.queryClass,
               versionId: req.body?.versionId as string | undefined,
            },
            readBypassAuthorize(req),
         );
         res.status(200).json(result);
      },
   );

   return { app, getQuery };
}

const post = (headers: Record<string, string>, body: object) => {
   const { app, getQuery } = buildApp();
   let pending = request(app)
      .post("/api/v0/environments/env/packages/pkg/models/m.malloy/query")
      .send({ query: "run: orders -> { aggregate: c is count() }", ...body });
   for (const [name, value] of Object.entries(headers)) {
      pending = pending.set(name, value);
   }
   return pending.then((response) => ({
      response,
      bypass: getQuery.firstCall?.args[BYPASS_ARGUMENT],
   }));
};

const SECRET = "s3cret-bypass-value";

describe("authorize bypass wiring, over HTTP", () => {
   const saved = process.env[BYPASS_AUTHORIZE_SECRET_ENV];

   beforeAll(() => {
      process.env[BYPASS_AUTHORIZE_SECRET_ENV] = SECRET;
   });

   afterAll(() => {
      if (saved === undefined) {
         delete process.env[BYPASS_AUTHORIZE_SECRET_ENV];
      } else {
         process.env[BYPASS_AUTHORIZE_SECRET_ENV] = saved;
      }
   });

   it("passes the bypass through when the header presents the secret", async () => {
      const { response, bypass } = await post(
         { "x-publisher-bypass-authorize": SECRET },
         {},
      );
      expect(response.status).toBe(200);
      expect(bypass).toBe(true);
   });

   // THE regression. A body field must not reach the bypass argument, however
   // plausible it looks next to `bypassFilters` one slot over.
   it("ignores a bypassAuthorize field in the request body", async () => {
      const { response, bypass } = await post({}, { bypassAuthorize: true });
      expect(response.status).toBe(200);
      expect(bypass).toBeUndefined();
   });

   it("does not confuse bypassFilters for the authorize bypass", async () => {
      const { bypass } = await post({}, { bypassFilters: true });
      expect(bypass).toBeUndefined();
   });

   it("leaves gates enforced with no header at all", async () => {
      const { bypass } = await post({}, {});
      expect(bypass).toBeUndefined();
   });

   // `"true"` heads the list: it is the value that alone used to disable the
   // gates, and over HTTP it must now read as just another wrong secret.
   //
   // A whitespace-PADDED secret is deliberately absent: RFC 9110 section 5.5
   // requires a field parser to exclude leading and trailing whitespace before
   // evaluating a field value, so Node hands the route the bare secret and the
   // padded form is unrepresentable over HTTP. The reader's own spec covers the
   // untrimmed comparison, which a direct caller can still exercise.
   it.each([
      ["true"],
      ["false"],
      ["1"],
      [""],
      ["yes"],
      ["true,true"],
      [`${SECRET}x`],
   ])("leaves gates enforced for header value %p", async (value) => {
      const { bypass } = await post(
         { "x-publisher-bypass-authorize": value },
         {},
      );
      expect(bypass).toBeUndefined();
   });

   /**
    * A genuinely duplicated header, which supertest cannot express — calling
    * `.set()` twice replaces the value rather than appending a second line. Sent
    * through `node:http` with an array value, which emits two header lines, so
    * Node's own server-side join is what the route sees: the secret twice,
    * comma-joined. Not the secret, so it denies.
    */
   it("leaves gates enforced for a duplicated header", async () => {
      const { app, getQuery } = buildApp();
      const server = app.listen(0);
      try {
         const port = (server.address() as { port: number }).port;
         const body = JSON.stringify({ query: "run: orders -> { c is 1 }" });
         const status = await new Promise<number>((resolve, reject) => {
            const req = httpRequest(
               {
                  host: "127.0.0.1",
                  port,
                  method: "POST",
                  path: "/api/v0/environments/env/packages/pkg/models/m.malloy/query",
                  headers: {
                     "content-type": "application/json",
                     "content-length": Buffer.byteLength(body),
                     "x-publisher-bypass-authorize": [SECRET, SECRET],
                  },
               },
               (res) => {
                  res.resume();
                  res.on("end", () => resolve(res.statusCode ?? 0));
               },
            );
            req.on("error", reject);
            req.end(body);
         });
         expect(status).toBe(200);
         expect(getQuery.firstCall?.args[BYPASS_ARGUMENT]).toBeUndefined();
      } finally {
         server.close();
      }
   });

   // The header NAME is case-insensitive, as HTTP requires. The secret VALUE is
   // not: it is compared exactly, so only the name is varied here.
   it("accepts the header name case-insensitively, as HTTP requires", async () => {
      const { bypass } = await post(
         { "X-Publisher-Bypass-Authorize": SECRET },
         {},
      );
      expect(bypass).toBe(true);
   });

   it("leaves gates enforced for an upper-cased secret value", async () => {
      const { bypass } = await post(
         { "x-publisher-bypass-authorize": SECRET.toUpperCase() },
         {},
      );
      expect(bypass).toBeUndefined();
   });
});

/**
 * Fail-closed, proven over HTTP rather than only on the reader: with no secret
 * configured there is no value a caller can present, so the bypass is
 * unavailable regardless of what the header says. This is the state a naive
 * deployment runs in, and the one the old presence-only reader left wide open.
 */
describe("authorize bypass with no secret configured, over HTTP", () => {
   const saved = process.env[BYPASS_AUTHORIZE_SECRET_ENV];

   beforeAll(() => {
      delete process.env[BYPASS_AUTHORIZE_SECRET_ENV];
   });

   afterAll(() => {
      if (saved === undefined) {
         delete process.env[BYPASS_AUTHORIZE_SECRET_ENV];
      } else {
         process.env[BYPASS_AUTHORIZE_SECRET_ENV] = saved;
      }
   });

   it.each([["true"], [SECRET]])(
      "refuses the bypass for header value %p",
      async (value) => {
         const { response, bypass } = await post(
            { "x-publisher-bypass-authorize": value },
            {},
         );
         expect(response.status).toBe(200);
         expect(bypass).toBeUndefined();
      },
   );
});

/**
 * The legacy `/projects/…` alias is a live query route — `server.ts` imports
 * `registerLegacyRoutes` from `server-old.ts` — and it does NOT honour the
 * bypass. That asymmetry is deliberate (the alias exists for pre-rename SDK
 * compatibility and passes no `givens` either, so it cannot satisfy a gate at
 * all), but "deliberate" and "nobody noticed" look identical in a diff. Pinned
 * from the source so a future edit has to choose: wire it and update this test,
 * or leave it alone.
 */
describe("the legacy /projects alias accepts no bypass", () => {
   const legacySource = readFileSync(
      resolve(import.meta.dir, "server-old.ts"),
      "utf8",
   );

   it("passes no bypass argument to getQuery", () => {
      const withoutComments = legacySource
         .replace(/\/\*[\s\S]*?\*\//g, "")
         .replace(/^\s*\/\/.*$/gm, "");
      expect(withoutComments).not.toContain("readBypassAuthorize");
      expect(withoutComments).not.toContain("bypassAuthorize");
   });
});
