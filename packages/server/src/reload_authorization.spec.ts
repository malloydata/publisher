// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `?reload=true` GET recompiles the package and replaces the served model, so
 * it is gated while a plain metadata GET stays open. Both halves matter: gating
 * the reload is the fix, and leaving the plain read open is what keeps the fix
 * from being a breaking change to an ordinary GET.
 *
 * The route cases run over a real Express app wired the way both package routes
 * are, with only the controller stubbed, so the assertion is what a request
 * actually gets rather than what the helper returns in isolation.
 */

import { afterEach, describe, expect, it } from "bun:test";
import express from "express";
import { readFileSync } from "fs";
import { resolve } from "path";
import sinon from "sinon";
import request from "supertest";
import {
   authorizeReload,
   RELOAD_SECRET_ENV,
   RELOAD_SECRET_HEADER,
   reloadDeniedMessage,
} from "./reload_authorization";

const SECRET = "r3load-secret-value";

const withHeaders = (
   headers: Record<string, string | string[] | undefined>,
) => ({ headers });

const restoreEnv = (saved: string | undefined) => {
   if (saved === undefined) {
      delete process.env[RELOAD_SECRET_ENV];
   } else {
      process.env[RELOAD_SECRET_ENV] = saved;
   }
};

/**
 * The same wiring both package GET routes use: parse `reload`, gate it when
 * true, then call the controller.
 */
function buildApp(): { app: express.Express; getPackage: sinon.SinonStub } {
   const getPackage = sinon.stub().resolves({ name: "pkg" });

   const app = express();
   // codeql[js/missing-rate-limiting]: an in-process fixture, not a served
   // route. It exists to assert the gate's decision, and never binds a port or
   // reaches a package loader, so there is nothing here to rate-limit. Rate
   // limiting on the real routes is the deployment's concern.
   app.get(
      "/api/v0/environments/:environmentName/packages/:packageName",
      (req, res) => {
         const reload = req.query.reload === "true";
         if (reload) {
            const decision = authorizeReload(req);
            if (!decision.authorized) {
               res.status(403).json({
                  code: 403,
                  message: reloadDeniedMessage(decision.reason),
               });
               return;
            }
         }
         void getPackage(
            req.params.environmentName,
            req.params.packageName,
            reload,
         ).then((pkg: unknown) => res.status(200).json(pkg));
      },
   );

   return { app, getPackage };
}

const get = (query: string, headers: Record<string, string> = {}) => {
   const { app, getPackage } = buildApp();
   let pending = request(app).get(
      `/api/v0/environments/env/packages/pkg${query}`,
   );
   for (const [name, value] of Object.entries(headers)) {
      pending = pending.set(name, value);
   }
   return pending.then((response) => ({
      response,
      reloadArgument: getPackage.firstCall?.args[2],
      called: getPackage.called,
   }));
};

describe("reload authorization over HTTP", () => {
   const saved = process.env[RELOAD_SECRET_ENV];

   afterEach(() => restoreEnv(saved));

   // THE fix. An unauthenticated caller must not be able to trigger a full
   // package recompile.
   it("refuses ?reload=true with no secret configured", async () => {
      delete process.env[RELOAD_SECRET_ENV];
      const { response, called } = await get("?reload=true");
      expect(response.status).toBe(403);
      expect(called).toBe(false);
   });

   it("refuses ?reload=true when the header is absent", async () => {
      process.env[RELOAD_SECRET_ENV] = SECRET;
      const { response, called } = await get("?reload=true");
      expect(response.status).toBe(403);
      expect(called).toBe(false);
   });

   it("refuses ?reload=true with the wrong secret", async () => {
      process.env[RELOAD_SECRET_ENV] = SECRET;
      const { response, called } = await get("?reload=true", {
         [RELOAD_SECRET_HEADER]: "not-the-secret",
      });
      expect(response.status).toBe(403);
      expect(called).toBe(false);
   });

   it("allows ?reload=true with the configured secret", async () => {
      process.env[RELOAD_SECRET_ENV] = SECRET;
      const { response, reloadArgument } = await get("?reload=true", {
         [RELOAD_SECRET_HEADER]: SECRET,
      });
      expect(response.status).toBe(200);
      expect(reloadArgument).toBe(true);
   });

   // The header NAME is case-insensitive per HTTP, and `api-doc.yaml` spells it
   // `X-Publisher-Reload-Secret` while the reader indexes the lowercase form
   // Node normalizes to. A client sending it as the spec spells it must work.
   it("accepts the header name as api-doc.yaml spells it", async () => {
      process.env[RELOAD_SECRET_ENV] = SECRET;
      const { response, reloadArgument } = await get("?reload=true", {
         "X-Publisher-Reload-Secret": SECRET,
      });
      expect(response.status).toBe(200);
      expect(reloadArgument).toBe(true);
   });

   // The non-breaking half: an ordinary metadata read is not a reload and must
   // stay open, secret or no secret.
   it.each([[""], ["?reload=false"]])(
      "leaves a plain GET %p open with no secret configured",
      async (query) => {
         delete process.env[RELOAD_SECRET_ENV];
         const { response, reloadArgument } = await get(query);
         expect(response.status).toBe(200);
         expect(reloadArgument).toBe(false);
      },
   );

   it("leaves a plain GET open when a secret IS configured", async () => {
      process.env[RELOAD_SECRET_ENV] = SECRET;
      const { response, reloadArgument } = await get("");
      expect(response.status).toBe(200);
      expect(reloadArgument).toBe(false);
   });
});

describe("authorizeReload", () => {
   const saved = process.env[RELOAD_SECRET_ENV];

   afterEach(() => restoreEnv(saved));

   it.each([[undefined], [""], ["   "]])(
      "reports not-configured when the secret env is %p",
      (configured) => {
         if (configured === undefined) {
            delete process.env[RELOAD_SECRET_ENV];
         } else {
            process.env[RELOAD_SECRET_ENV] = configured;
         }
         expect(
            authorizeReload(withHeaders({ [RELOAD_SECRET_HEADER]: SECRET })),
         ).toEqual({ authorized: false, reason: "not-configured" });
      },
   );

   it("authorizes an exact match", () => {
      process.env[RELOAD_SECRET_ENV] = SECRET;
      expect(
         authorizeReload(withHeaders({ [RELOAD_SECRET_HEADER]: SECRET })),
      ).toEqual({ authorized: true });
   });

   // A secret is an exact value, and the compare hashes first so a differing
   // length denies rather than throwing out of timingSafeEqual.
   it.each([` ${SECRET} `, `${SECRET}x`, SECRET.slice(0, -1), ""])(
      "reports bad-secret for the presented value %p",
      (presented) => {
         process.env[RELOAD_SECRET_ENV] = SECRET;
         expect(
            authorizeReload(withHeaders({ [RELOAD_SECRET_HEADER]: presented })),
         ).toEqual({ authorized: false, reason: "bad-secret" });
      },
   );

   it("reports bad-secret for an array-valued header", () => {
      process.env[RELOAD_SECRET_ENV] = SECRET;
      expect(
         authorizeReload(
            withHeaders({ [RELOAD_SECRET_HEADER]: [SECRET, SECRET] }),
         ),
      ).toEqual({ authorized: false, reason: "bad-secret" });
   });

   // The two refusals must stay distinguishable: one means the feature is off,
   // the other means this caller's credential is wrong, and they call for
   // different operator action.
   it("gives the two refusal reasons different messages", () => {
      expect(reloadDeniedMessage("not-configured")).not.toBe(
         reloadDeniedMessage("bad-secret"),
      );
      expect(reloadDeniedMessage("not-configured")).toContain(
         RELOAD_SECRET_ENV,
      );
      expect(reloadDeniedMessage("bad-secret")).toContain(RELOAD_SECRET_HEADER);
   });
});

/**
 * Both package GET routes reach the same reload, so both must gate it. Pinned
 * from source because the legacy alias lives in `server-old.ts` and is easy to
 * miss: gating only the modern route would leave the recompile unauthenticated
 * at a second URL with every behavioural test above still green.
 *
 * Only line comments are stripped. A block-comment regex is unsound on these
 * files: `server.ts` carries route-path literals containing `/*` (the
 * `public/*` and `notebooks/*` wildcards), and a lazy `/\/\*[\s\S]*?\*\//`
 * treats one of those as a comment opener and deletes ~39KB of live code up to
 * the next `*` + `/` — the gate included. Matching the raw text cannot be
 * fooled that way, and a `//`-commented mention is what the line strip removes.
 */
describe("both package routes gate the reload", () => {
   const sourceOf = (file: string) =>
      readFileSync(resolve(import.meta.dir, file), "utf8").replace(
         /^\s*\/\/.*$/gm,
         "",
      );

   it.each([["server.ts"], ["server-old.ts"]])(
      "%s calls authorizeReload before reloading",
      (file) => {
         const source = sourceOf(file);
         expect(source).toContain("authorizeReload(req)");
         expect(source).toContain("reloadDeniedMessage(decision.reason)");
      },
   );
});
