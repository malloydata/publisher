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

import { describe, expect, it, spyOn } from "bun:test";
import {
   BadRequestError,
   ConnectionError,
   internalErrorToHttpError,
} from "./errors";
import { getInternalError } from "./mcp/error_messages";
import { logger } from "./logger";

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
      // Not every 502 is a leak. A message this server composed names nothing
      // internal and tells the caller what to fix, so genericizing the whole
      // 502 class to suppress driver messages would destroy it. (A missing
      // table is a 404 via TableNotFoundError and never reaches this branch;
      // the opt-in is what keeps any remaining server-authored 502 readable.)
      const { status, json } = internalErrorToHttpError(
         new ConnectionError("Package name is undefined", {
            callerSafe: true,
         }),
      );
      expect(status).toBe(502);
      expect(json.message).toContain("Package name is undefined");
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
         new ConnectionError("Package name is undefined", {
            callerSafe: true,
         }),
      );
      expect(message).toContain("Package name is undefined");
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

// The two internal-failure classes are logged at different levels on purpose.
// An unrecognized error is our bug; an upstream connection failure is one a
// caller can drive in a loop, so it must not fill the error log or move an
// error-rate dashboard that tracks our own faults.
describe("internal-failure logging level (F-12 Part A)", () => {
   // Assert on the arguments of THIS call, found by a marker unique to the test,
   // rather than on a call count. Bun runs every spec file in one process under
   // --serial, so any other file that maps an internal error increments the same
   // spy and a count assertion fails for a reason that has nothing to do with
   // the contract.
   const callsWith = (spy: ReturnType<typeof spyOn>, marker: string) =>
      spy.mock.calls.filter((args: unknown[]) => {
         // The detail is nested under `error`: a top-level `message` would be
         // fused into winston's own info.message.
         const meta = args[1] as { error?: { message?: unknown } } | undefined;
         return String(meta?.error?.message ?? "").includes(marker);
      });

   it("logs an unrecognized internal error at error, not warn", () => {
      const marker = "unrecognized-marker-9f2a";
      const err = spyOn(logger, "error").mockImplementation(() => logger);
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         internalErrorToHttpError(new Error(marker));
         expect(callsWith(err, marker)).toHaveLength(1);
         expect(callsWith(warn, marker)).toHaveLength(0);
      } finally {
         err.mockRestore();
         warn.mockRestore();
      }
   });

   it("logs a driver-wrapped upstream failure at warn, not error", () => {
      const marker = "upstream-marker-4c7b";
      const err = spyOn(logger, "error").mockImplementation(() => logger);
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         internalErrorToHttpError(new ConnectionError(marker));
         expect(callsWith(warn, marker)).toHaveLength(1);
         expect(callsWith(err, marker)).toHaveLength(0);
      } finally {
         err.mockRestore();
         warn.mockRestore();
      }
   });
});

// The mapper is only a choke point for handlers that return its `json`. A
// handler that takes just the `status` and builds its own body from
// `error.message` re-opens the leak in a different response shape, which is how
// the watch-mode routes were echoing an internal filesystem path. This pins the
// shape those handlers must use: the body's text comes from the mapper.
describe("a handler with its own body shape still uses the mapper's text", () => {
   it("carries no internal detail when built from the mapper's json", () => {
      const internal =
         "ENOSPC: System limit for number of file watchers reached, watch '/var/lib/publisher/environments/acme'";
      const { status, json } = internalErrorToHttpError(new Error(internal));

      // What the watch-mode handlers now send: `{ error: json.message }`.
      const body = { error: json.message };

      expect(status).toBe(500);
      expect(body.error).not.toContain("/var/lib/publisher");
      expect(body.error).not.toContain("ENOSPC");
      // And it is still the mapper's actionable generic text, not empty.
      expect(body.error).toBe("Internal server error.");
   });

   it("keeps a 4xx message, so the shape does not blank client errors", () => {
      const { status, json } = internalErrorToHttpError(
         new BadRequestError("environmentName must match ^[a-z0-9-]+$"),
      );
      const body = { error: json.message };
      expect(status).toBe(400);
      expect(body.error).toContain("environmentName must match");
   });
});

// The two guards on the logged detail. Neither was pinned, which is how a
// truncation that dropped every stack frame shipped unnoticed.
describe("logged detail keeps a stable summary and real frames", () => {
   const capture = (run: () => void) => {
      const calls: Array<[string, Record<string, unknown>]> = [];
      const record = (m: string, meta: Record<string, unknown>) => {
         calls.push([m, meta]);
         return logger;
      };
      const err = spyOn(logger, "error").mockImplementation(
         record as unknown as typeof logger.error,
      );
      const warn = spyOn(logger, "warn").mockImplementation(
         record as unknown as typeof logger.warn,
      );
      try {
         run();
      } finally {
         err.mockRestore();
         warn.mockRestore();
      }
      return calls;
   };

   // A driver error echoing the caller's whole statement: the case
   // MAX_LOGGED_DETAIL_CHARS exists for, and the one that used to eat the stack.
   const hugeMessage = "SQL compilation error: " + "x".repeat(3200);

   it("keeps the summary free of the error text, so it can group and alert", () => {
      const calls = capture(() =>
         internalErrorToHttpError(new Error(hugeMessage)),
      );
      expect(calls).toHaveLength(1);
      const [summary, meta] = calls[0]!;
      // winston fuses a TOP-LEVEL `message` into info.message. Nesting under
      // `error` is what keeps this summary a constant.
      expect(summary).toBe("Unhandled internal error");
      expect(summary).not.toContain("SQL compilation");
      expect(meta).toHaveProperty("error");
      expect(meta).not.toHaveProperty("message");
   });

   it("retains stack frames even when the message alone exceeds the cap", () => {
      const calls = capture(() =>
         internalErrorToHttpError(new Error(hugeMessage)),
      );
      const detail = calls[0]![1].error as Record<string, string>;
      // The message is capped in its own field...
      expect(detail.message.length).toBe(2000);
      // ...and the stack's budget is spent on frames, not on repeating it.
      expect(detail.stack).not.toStartWith("SQL compilation");
      expect(detail.stack).toContain(" at ");
   });

   it("strips the separators JSON.stringify does not escape", () => {
      const nasty =
         "a" +
         String.fromCharCode(0x0a, 0x85, 0x2028, 0x2029) +
         "forged: level=error";
      const calls = capture(() => internalErrorToHttpError(new Error(nasty)));
      const detail = calls[0]![1].error as Record<string, string>;
      for (const code of [0x0a, 0x85, 0x2028, 0x2029]) {
         expect(detail.message).not.toContain(String.fromCharCode(code));
      }
   });
});
