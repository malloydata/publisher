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
