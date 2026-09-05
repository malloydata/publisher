// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Finding F-12 (Part A): verbose internal error bodies.
//
// `internalErrorToHttpError` mapped a generic error to httpError(500) and a
// ConnectionError to httpError(502) using `error.message` verbatim, so an
// internal detail (a filesystem path, an SQL fragment, an upstream host) was
// echoed to the client. The contract asserted here: a 500/502 mapped from an
// internal error returns a generic body, with the detail logged server-side
// rather than returned. The actionable 4xx client-error messages are unchanged.
//
// The permissive frame-ancestors default (Part B of the finding) is deferred: on
// this deployment the app embeds a served data-app cross-origin and the router
// forwards the worker CSP verbatim, so tightening the default to 'self' without a
// paired PUBLISHER_FRAME_ANCESTORS override (or a router-side injection) would
// break the embed. That change ships with the deployment coordination, not here.

import { describe, expect, it } from "bun:test";
import {
   BadRequestError,
   ConnectionError,
   internalErrorToHttpError,
} from "./errors";
import { getInternalError } from "./mcp/error_messages";

describe("internalErrorToHttpError does not leak internal detail (F-12 Part A)", () => {
   // A message carrying an internal marker a client must never receive: an
   // on-disk path and an SQL fragment.
   const INTERNAL_MARKER =
      "/var/lib/publisher/secrets/creds.json -- SELECT * FROM internal_accounts";

   it("returns a generic body for a 500 from an unrecognized internal error", () => {
      const { status, json } = internalErrorToHttpError(
         new Error(INTERNAL_MARKER),
      );
      expect(status).toBe(500);
      // The raw internal message must not be echoed back to the client.
      expect(json.message).not.toContain(INTERNAL_MARKER);
      expect(json.message).not.toContain("/var/lib/publisher/secrets");
      expect(json.message).not.toContain("internal_accounts");
   });

   it("returns a generic body for a 502 mapped from a ConnectionError", () => {
      const { status, json } = internalErrorToHttpError(
         new ConnectionError(INTERNAL_MARKER),
      );
      expect(status).toBe(502);
      expect(json.message).not.toContain(INTERNAL_MARKER);
      expect(json.message).not.toContain("/var/lib/publisher/secrets");
      expect(json.message).not.toContain("internal_accounts");
   });

   it("preserves a server-authored 502 message (caller-safe)", () => {
      // Not every 502 is a leak. "Table x.y not found" is written by this server,
      // names nothing internal, and tells the caller what to fix -- genericizing
      // the whole 502 class to suppress driver messages would destroy it.
      const { status, json } = internalErrorToHttpError(
         new ConnectionError("Table analytics.orders not found", {
            callerSafe: true,
         }),
      );
      expect(status).toBe(502);
      expect(json.message).toContain("Table analytics.orders not found");
   });

   it("generalizes a 502 that wraps a driver message (not caller-safe)", () => {
      // The default. A driver message can name an internal host/port, echo the
      // caller's SQL, or distinguish refused from timed-out from auth-failed.
      const { status, json } = internalErrorToHttpError(
         new ConnectionError("connect ECONNREFUSED 10.0.0.1:5432"),
      );
      expect(status).toBe(502);
      expect(json.message).not.toContain("ECONNREFUSED");
      expect(json.message).not.toContain("10.0.0.1");
   });

   it("preserves the actionable message on a client (4xx) error", () => {
      // Only internal 500/502 bodies are genericized; a client-actionable 4xx
      // message (e.g. a bad-request explanation) must still reach the caller.
      const { status, json } = internalErrorToHttpError(
         new BadRequestError("environmentName must match ^[a-z0-9-]+$"),
      );
      expect(status).toBe(400);
      expect(json.message).toContain("environmentName must match");
   });
});

// The same contract on the MCP transport. `classifyToolError` routes an
// unclassified error to `getInternalError`, and a ConnectionError matches none
// of its branches, so the driver text the HTTP mapper withholds would otherwise
// stay retrievable over /mcp -- the same finding on a second surface.
describe("getInternalError does not leak driver detail over MCP (F-12 Part A)", () => {
   it("withholds a driver message wrapped in a ConnectionError", () => {
      const { message } = getInternalError(
         "executeQuery",
         new ConnectionError("connect ECONNREFUSED 10.0.0.1:5432"),
      );
      expect(message).not.toContain("ECONNREFUSED");
      expect(message).not.toContain("10.0.0.1");
   });

   it("keeps a server-authored caller-safe message", () => {
      const { message } = getInternalError(
         "executeQuery",
         new ConnectionError("Table analytics.orders not found", {
            callerSafe: true,
         }),
      );
      expect(message).toContain("Table analytics.orders not found");
   });

   it("keeps the message of an operational error that is not a ConnectionError", () => {
      // Only the driver-wrapping class is withheld. Blanking everything here
      // returns callers to the unhelpful generic text classifyToolError exists
      // to avoid, so a store failure must still say what failed.
      const { message } = getInternalError(
         "getContext",
         new Error("the store exploded"),
      );
      expect(message).toContain("the store exploded");
   });
});
