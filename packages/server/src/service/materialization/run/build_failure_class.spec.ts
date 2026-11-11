// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   BadRequestError,
   ChainedShapeNotCarriedError,
   ChainedUpstreamMissingError,
   ConnectionAuthError,
   ConnectionFailedError,
   ConnectionPoolExhaustedError,
   MaterializationEligibilityError,
} from "../../../errors";
import {
   classifyBuildError,
   classifySourceFailure,
   failedUpstreamsOf,
   type FailedUpstream,
} from "./build_failure_class";

function strictMiss(sourceID: string): Error {
   return Object.assign(
      new Error(`Persisted source '${sourceID}' not found in manifest`),
      { code: "runtime-manifest-strict-miss" },
   );
}

describe("classifyBuildError", () => {
   it.each([
      [
         "the chained build's DuckDB memory bound",
         "Failed to materialize chained source 'pairs' into storage destination 'lake': Out of Memory Error: could not allocate block of size 256.0 KiB (732.4 MiB/732.4 MiB used)",
      ],
      [
         "a partitioned write that cannot pin a block",
         "Out of Memory Error: failed to pin block of size 256.0 KiB",
      ],
      ["a SQL syntax error", 'Parser Error: syntax error at or near "FORM"'],
      [
         "a DuckDB binder error",
         'Binder Error: Referenced column "x" not found in FROM clause!',
      ],
      [
         "a BigQuery name-resolution defect",
         "Unrecognized name: order_tot at [3:5]",
      ],
      ["a Malloy compile defect", "Model failed to compile: bad field"],
      ["a missing grant", "Permission denied while writing to dataset x"],
   ])("reads %s as deterministic", (_label, message) => {
      expect(classifyBuildError(new Error(message))).toBe(false);
   });

   it.each([
      ["a refused connection", "connect ECONNREFUSED 10.0.0.1:5432"],
      ["a reset socket", "socket hang up"],
      ["a timeout", "Query timed out after 600s"],
      ["a deadline", "Deadline exceeded while reading results"],
      [
         "a rate limit",
         "Exceeded rate limits: too many table update operations",
      ],
      ["a cancellation", "Build cancelled"],
   ])("reads %s as transient", (_label, message) => {
      expect(classifyBuildError(new Error(message))).toBe(true);
   });

   it("prefers a deterministic signature over a transient word the message echoes", () => {
      // A compile error quoting SQL that mentions a `timeout` column.
      expect(
         classifyBuildError(
            new Error('Binder Error: column "session_timeout" not found'),
         ),
      ).toBe(false);
   });

   it("reads the server's own error types before any message", () => {
      expect(
         classifyBuildError(
            new MaterializationEligibilityError({ message: "has a given" }),
         ),
      ).toBe(false);
      expect(classifyBuildError(new ChainedShapeNotCarriedError("x"))).toBe(
         false,
      );
      expect(classifyBuildError(new BadRequestError("no destination"))).toBe(
         false,
      );
      expect(classifyBuildError(new MalloyError("bad", []))).toBe(false);
      expect(classifyBuildError(new ConnectionAuthError("bad creds"))).toBe(
         false,
      );
      // The database was unreachable or every session busy: the query never ran.
      expect(classifyBuildError(new ConnectionFailedError("down"))).toBe(true);
      expect(classifyBuildError(new ConnectionPoolExhaustedError("busy"))).toBe(
         true,
      );
   });

   it("reads a system error code, on the error or its cause", () => {
      const direct = Object.assign(new Error("read failed"), {
         code: "ECONNRESET",
      });
      expect(classifyBuildError(direct)).toBe(true);
      const wrapped = new Error("query failed", {
         cause: Object.assign(new Error("x"), { code: "ETIMEDOUT" }),
      });
      expect(classifyBuildError(wrapped)).toBe(true);
      const aborted = Object.assign(new Error("stopped"), {
         name: "AbortError",
      });
      expect(classifyBuildError(aborted)).toBe(true);
   });

   it("leaves an unrecognized or ambiguous error unclassified", () => {
      expect(classifyBuildError(new Error("something odd"))).toBeUndefined();
      // A missing table may be a referenced upstream reclaimed mid-build.
      expect(
         classifyBuildError(
            new Error("Catalog Error: Table with name t__g3 does not exist!"),
         ),
      ).toBeUndefined();
      expect(classifyBuildError("not an error")).toBeUndefined();
   });

   it("reads an upstream merely absent from the instructions as transient", () => {
      expect(
         classifyBuildError(new ChainedUpstreamMissingError(["up"], "missing")),
      ).toBe(true);
      expect(classifyBuildError(strictMiss("up@file:///m.malloy"))).toBe(true);
   });
});

describe("classifySourceFailure", () => {
   const deterministic: FailedUpstream = {
      name: "orders",
      reason: "Unrecognized name: order_tot",
      retryable: false,
   };
   const transient: FailedUpstream = {
      name: "users",
      reason: "socket hang up",
      retryable: true,
   };
   const unknown: FailedUpstream = {
      name: "items",
      reason: "something odd",
      retryable: undefined,
   };
   const missing = new ChainedUpstreamMissingError(
      ["orders"],
      "persisted upstream 'orders' is not in this build's manifest",
   );

   it("inherits a deterministic upstream failure and names it", () => {
      const result = classifySourceFailure(missing, [deterministic]);
      expect(result.retryable).toBe(false);
      expect(result.upstreamDetail).toBe(
         "upstream 'orders' failed in this run: Unrecognized name: order_tot",
      );
   });

   it("stays transient when any failed upstream is transient", () => {
      expect(
         classifySourceFailure(missing, [deterministic, transient]).retryable,
      ).toBe(true);
   });

   it("is unclassified when no upstream is transient and one is unclassified", () => {
      expect(
         classifySourceFailure(missing, [deterministic, unknown]).retryable,
      ).toBeUndefined();
   });

   it("inherits through Malloy's strict miss on a colocated build", () => {
      expect(
         classifySourceFailure(strictMiss("orders"), [deterministic]).retryable,
      ).toBe(false);
   });

   it("classifies the source's own error when it is not a missing upstream", () => {
      // A non-strict build recomputed the failed upstream and then timed out.
      const own = classifySourceFailure(new Error("Query timed out"), [
         deterministic,
      ]);
      expect(own.retryable).toBe(true);
      expect(own.upstreamDetail).toBeUndefined();
   });

   it("reads a missing upstream that did not fail in this run as transient", () => {
      expect(classifySourceFailure(missing, []).retryable).toBe(true);
   });
});

describe("failedUpstreamsOf", () => {
   const failed = new Map<string, FailedUpstream>([
      ["a", { name: "a", reason: "x", retryable: false }],
      ["b", { name: "b", reason: "y", retryable: true }],
      ["c", { name: "c", reason: "z", retryable: false }],
   ]);

   it("finds the failed upstreams the plan says a source reads", () => {
      const ups = failedUpstreamsOf(
         "d",
         new Error("x"),
         new Map([["d", ["a", "zz"]]]),
         failed,
      );
      expect(ups.map((u) => u.name)).toEqual(["a"]);
   });

   it("adds the upstream a missing-upstream error names", () => {
      expect(
         failedUpstreamsOf(
            "d",
            new ChainedUpstreamMissingError(["b"], "missing"),
            new Map(),
            failed,
         ).map((u) => u.name),
      ).toEqual(["b"]);
      expect(
         failedUpstreamsOf("d", strictMiss("c"), new Map(), failed).map(
            (u) => u.name,
         ),
      ).toEqual(["c"]);
   });
});
