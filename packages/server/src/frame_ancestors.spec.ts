// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The framing policy, at the two levels it can break.
 *
 * `parseFrameAncestors` decides the value. The middleware decides WHERE it is
 * applied, and that half is the one this change exists for: the policy used to
 * be set inside the `public/` file route, so every other document -- the whole
 * Console -- carried no framing header at all. A test that only checked the
 * value would have passed against that bug.
 */

import { describe, expect, it } from "bun:test";
import express from "express";
import path from "path";
import fs from "fs/promises";
import os from "os";
import request from "supertest";
import {
   FRAME_ANCESTORS_ENV,
   frameAncestorsMiddleware,
   parseFrameAncestors,
} from "./frame_ancestors";

/** A response double recording what a middleware set or removed. */
function responseDouble() {
   const headers = new Map<string, string>();
   return {
      headers,
      setHeader(name: string, value: string) {
         headers.set(name, value);
      },
      removeHeader(name: string) {
         headers.delete(name);
      },
   };
}

/** Runs the middleware over one request and returns the headers it left. */
function headersFor(raw: string | undefined): Map<string, string> {
   const res = responseDouble();
   let nexted = false;
   frameAncestorsMiddleware(raw)(
      {} as never,
      res as never,
      (() => {
         nexted = true;
      }) as never,
   );
   // A middleware that sets the header and then stalls the request would pass
   // every header assertion below while hanging the server.
   expect(nexted).toBe(true);
   return res.headers;
}

describe("parseFrameAncestors", () => {
   it("defaults to 'self' when unset", () => {
      expect(parseFrameAncestors(undefined)).toBe("'self'");
   });

   it("treats whitespace-only as unset rather than as an empty directive", () => {
      // The shape a deployment produces: an env var set by a template that had
      // no value to substitute. `frame-ancestors ` with nothing after it is not
      // valid CSP, and emitting it would drop the policy rather than widen it.
      expect(parseFrameAncestors("")).toBe("'self'");
      expect(parseFrameAncestors("   ")).toBe("'self'");
   });

   it("passes a configured value through, trimmed", () => {
      expect(parseFrameAncestors("https://app.example.com")).toBe(
         "https://app.example.com",
      );
      expect(parseFrameAncestors("  https://a.example  ")).toBe(
         "https://a.example",
      );
   });

   it("accepts the source-list forms a deployment actually writes", () => {
      // Several origins, a scheme source, and the wildcard an embedder keeps by
      // setting it explicitly. No validation here on purpose -- the browser is
      // the only real parser of this grammar.
      expect(parseFrameAncestors("https://a.example https://b.example")).toBe(
         "https://a.example https://b.example",
      );
      expect(parseFrameAncestors("https:")).toBe("https:");
      expect(parseFrameAncestors("*")).toBe("*");
   });
});

describe("frameAncestorsMiddleware", () => {
   it("sets the policy with no configuration, which is the whole point", () => {
      // The default has to be safe without an operator doing anything: this is
      // the case that was `*` before.
      expect(headersFor(undefined).get("Content-Security-Policy")).toBe(
         "frame-ancestors 'self'",
      );
   });

   it("honours a configured origin, so embedding stays opt-in rather than impossible", () => {
      expect(
         headersFor("https://app.example.com").get("Content-Security-Policy"),
      ).toBe("frame-ancestors https://app.example.com");
   });

   it("lets a deployment restore the old permissive behaviour explicitly", () => {
      // The migration path for an existing cross-origin embedder. Worth pinning:
      // if this stopped working, the change would be a wall rather than a
      // default, and the release note would be wrong.
      expect(headersFor("*").get("Content-Security-Policy")).toBe(
         "frame-ancestors *",
      );
   });

   it("removes X-Frame-Options so one policy is in force rather than two", () => {
      const res = responseDouble();
      res.setHeader("X-Frame-Options", "SAMEORIGIN");
      frameAncestorsMiddleware(undefined)(
         {} as never,
         res as never,
         (() => {}) as never,
      );
      expect(res.headers.has("X-Frame-Options")).toBe(false);
   });

   it("applies to every request, not only to a document route", () => {
      // The coverage half. The middleware reads nothing off the request -- no
      // path, no extension, no content type -- so there is no shape of request
      // that reaches a handler without the header. That is what closes the
      // Console gap, and it is why this asserts over varied requests rather
      // than one.
      for (const path of [
         "/",
         "/notebook/foo",
         "/dashboards/bar",
         "/api/v0/status",
         "/some/package/public/app.html",
      ]) {
         const res = responseDouble();
         frameAncestorsMiddleware(undefined)(
            { path } as never,
            res as never,
            (() => {}) as never,
         );
         expect(res.headers.get("Content-Security-Policy")).toBe(
            "frame-ancestors 'self'",
         );
      }
   });

   it("names the environment variable the docs name", () => {
      // A rename here silently un-configures every deployment that set the old
      // one, and nothing else in the build would catch it.
      expect(FRAME_ANCESTORS_ENV).toBe("PUBLISHER_FRAME_ANCESTORS");
   });
});

/**
 * The same policy over a real HTTP round trip.
 *
 * The assertions above run the middleware against a response double, which
 * proves what it sets but not that the header survives Express actually
 * answering a request. These do: a header applied by middleware can still be
 * lost to a handler that writes its own, to `sendFile`'s header handling, or to
 * an error path that answers before the middleware's response object is the one
 * used. Each route below is a response shape this server really produces.
 */
describe("frame-ancestors over a real request", () => {
   const appServing = (raw: string | undefined, staticRoot?: string) => {
      const app = express();
      app.use(frameAncestorsMiddleware(raw));
      app.get("/json", (_req, res) => res.json({ ok: true }));
      app.get("/html", (_req, res) => res.type("html").send("<p>hi</p>"));
      app.get("/boom", (_req, res) => res.status(500).send("no"));
      app.get("/redirect", (_req, res) => res.redirect("/json"));
      if (staticRoot) app.use("/static", express.static(staticRoot));
      return app;
   };

   it("is on every response shape, including errors and redirects", async () => {
      const app = appServing(undefined);
      for (const route of ["/json", "/html", "/boom", "/redirect"]) {
         const res = await request(app).get(route);
         expect(res.headers["content-security-policy"]).toBe(
            "frame-ancestors 'self'",
         );
      }
   });

   it("leaves the finalhandler 404 at least as strict as the policy", async () => {
      // Express's own 404 replaces the CSP with `default-src 'none'` rather
      // than appending to it, so this response does NOT carry
      // `frame-ancestors 'self'` -- and that is fine, because `default-src
      // 'none'` forbids framing outright and is the stricter answer. Asserted
      // rather than left unexamined: the failure worth catching is the reverse,
      // a 404 body that ends up framable, and that shows up here as either
      // header going permissive.
      const res = await request(appServing(undefined)).get("/nothing-here");
      expect(res.status).toBe(404);
      const csp = res.headers["content-security-policy"];
      expect(
         csp === "default-src 'none'" || csp === "frame-ancestors 'self'",
      ).toBe(true);
      expect(csp).not.toContain("frame-ancestors *");
   });

   it("is on a statically served file, which is how package HTML is returned", async () => {
      // The route the policy used to live inside. `express.static` sets its own
      // headers and can short-circuit, so this is the case most likely to lose
      // a header set upstream of it.
      const dir = await fs.mkdtemp(path.join(os.tmpdir(), "frame-ancestors-"));
      try {
         await fs.writeFile(path.join(dir, "app.html"), "<p>page</p>");
         const res = await request(appServing(undefined, dir)).get(
            "/static/app.html",
         );
         expect(res.status).toBe(200);
         expect(res.headers["content-security-policy"]).toBe(
            "frame-ancestors 'self'",
         );
      } finally {
         await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
      }
   });

   it("carries a configured origin end to end", async () => {
      const res = await request(appServing("https://app.example.com")).get(
         "/html",
      );
      expect(res.headers["content-security-policy"]).toBe(
         "frame-ancestors https://app.example.com",
      );
   });

   it("sends no X-Frame-Options alongside it", async () => {
      // Two framing policies that can disagree is the failure this avoids; the
      // wire is where it would actually show up.
      const res = await request(appServing(undefined)).get("/html");
      expect(res.headers["x-frame-options"]).toBeUndefined();
   });
});
