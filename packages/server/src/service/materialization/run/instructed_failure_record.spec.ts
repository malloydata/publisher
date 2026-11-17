// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A source's failure record, as `recordInstructedBuildFailure` writes it:
 *
 * - P6-21: with no connection config to redact against (the connection-free
 *   fallback), the reason still names no storage URL;
 * - P5-10: a source the run's cancel stopped records no `retryable`, since it
 *   did not fail (the run settles CANCELLED); a failure in a live run keeps
 *   its classification.
 */

import { describe, expect, it } from "bun:test";
import { recordInstructedBuildFailure } from "./materialization_instructed_run";

const ID = "srcaaaaaaaaaaaa";

function runOf(signal?: AbortSignal) {
   return {
      environment: {
         getApiConnection: () => {
            throw new Error("no connection config in this test");
         },
      },
      upstreamsBySourceID: new Map(),
      failedBySourceID: new Map(),
      failures: {} as Record<string, { reason: string; retryable?: boolean }>,
      failedReasons: [] as string[],
      entries: {},
      ...(signal ? { signal } : {}),
   };
}

function record(run: ReturnType<typeof runOf>, err: Error) {
   recordInstructedBuildFailure(
      run as never,
      {
         name: "src",
         sourceID: "src@file:///m.malloy",
         connectionName: "wh",
      } as never,
      { physicalTableName: "src_v1", materializedTableId: "mt" } as never,
      ID,
      err,
      { kind: "persist", tier: "database", method: "full" } as never,
   );
   return run.failures[ID];
}

/** A transient failure: the database reset the connection. */
const reset = () =>
   Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" });

describe("a source's failure record", () => {
   it("P6-21: names no storage URL when there is no config to redact against", () => {
      const failure = record(
         runOf(),
         new Error(
            "HTTP GET error on 'https://storage.googleapis.com/bkt/org_1/db_2/t.parquet' (HTTP 500)",
         ),
      );
      expect(failure.reason).toContain("<storage path>");
      expect(failure.reason).not.toMatch(/googleapis|bkt|org_1|db_2/);
   });

   it("P5-10: records no retryable for a source the run's cancel stopped", () => {
      const run = new AbortController();
      run.abort();
      expect(record(runOf(run.signal), reset()).retryable).toBeUndefined();
      // A live run's same failure keeps its classification.
      expect(record(runOf(), reset()).retryable).toBe(true);
   });
});
