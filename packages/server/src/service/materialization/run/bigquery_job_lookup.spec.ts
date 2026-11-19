// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import { issuePassthroughRead } from "./materialization_build_session";
import { BilledReadNotCapturedError } from "./build_attribution";

/**
 * A labelled BigQuery read runs as a script, and its rows sit in an anonymous
 * table that only the script's child job names. The lookup of that job is
 * retried a bounded number of times; when every attempt fails, the build fails
 * with `BilledReadNotCapturedError`, because the read was already billed.
 */

const METADATA = { team: "growth" };

/** A build session that answers the split read's statements by their shape. */
function session(childLookups: Array<() => unknown>): {
   session: DuckDBConnection;
   lookups: () => number;
} {
   let lookups = 0;
   const runSQL = async (sql: string) => {
      if (sql.includes("maxResults := 1")) return { rows: [{ job_id: "p" }] };
      if (sql.includes("bigquery_execute(")) {
         return { rows: [{ job_id: "parent-1" }] };
      }
      if (sql.includes("parentJobId :=")) {
         const answer =
            childLookups[Math.min(lookups, childLookups.length - 1)];
         lookups++;
         return answer();
      }
      throw new Error(`unexpected statement: ${sql}`);
   };
   return {
      session: { runSQL } as unknown as DuckDBConnection,
      lookups: () => lookups,
   };
}

const CHILD = {
   rows: [
      {
         job_id: "child-1",
         project: "proj",
         dataset: "_anon",
         table: "anon_tbl",
      },
   ],
};

describe("the BigQuery job-record lookup of a labelled read", () => {
   let warn: sinon.SinonStub;
   beforeEach(() => {
      warn = sinon.stub(logger, "warn");
   });
   afterEach(() => {
      warn.restore();
   });

   it("fails with BilledReadNotCapturedError naming the parent job after three failed attempts", async () => {
      const fake = session([
         () => {
            throw new Error("503 backend error");
         },
      ]);
      const err = await issuePassthroughRead(
         fake.session,
         "bigquery",
         "proj",
         "SELECT 1",
         METADATA,
      ).then(
         () => undefined,
         (e: unknown) => e,
      );

      expect(err).toBeInstanceOf(BilledReadNotCapturedError);
      expect((err as Error).message).toContain("parent job parent-1");
      expect((err as Error).message).toContain("after 3 attempts");
      expect((err as Error).message).toContain("503 backend error");
      expect(((err as Error).cause as Error).message).toBe("503 backend error");
      expect(fake.lookups()).toBe(3);
      // Each failed attempt but the last is logged as a retry.
      const retries = warn
         .getCalls()
         .filter((c) => String(c.args[0]).includes("retrying"));
      expect(
         retries.map((c) => (c.args[1] as { attempt: number }).attempt),
      ).toEqual([1, 2]);
   });

   it("retries a listing that answers before it names the child, and reads the child's table", async () => {
      const fake = session([
         () => ({ rows: [] }),
         () => ({
            rows: [{ job_id: "set-label", dataset: null, table: null }],
         }),
         () => CHILD,
      ]);
      const read = await issuePassthroughRead(
         fake.session,
         "bigquery",
         "proj",
         "SELECT 1",
         METADATA,
      );

      expect(fake.lookups()).toBe(3);
      expect(read.selectSQL).toBe(
         "SELECT * FROM bigquery_scan('proj._anon.anon_tbl')",
      );
      expect(read.jobId).toBe("child-1");
   });

   it("fails at once, without a lookup, when the script returns no parent job", async () => {
      const runSQL = async (sql: string) => {
         if (sql.includes("maxResults := 1"))
            return { rows: [{ job_id: "p" }] };
         if (sql.includes("bigquery_execute(")) return { rows: [] };
         throw new Error(`unexpected statement: ${sql}`);
      };
      await expect(
         issuePassthroughRead(
            { runSQL } as unknown as DuckDBConnection,
            "bigquery",
            "proj",
            "SELECT 1",
            METADATA,
         ),
      ).rejects.toBeInstanceOf(BilledReadNotCapturedError);
   });

   it("reads directly, with no job id, when the read carries no label", async () => {
      const fake = session([() => CHILD]);
      const read = await issuePassthroughRead(
         fake.session,
         "bigquery",
         "proj",
         "SELECT 1",
         undefined,
      );
      expect(read.jobId).toBeNull();
      expect(read.selectSQL).toContain("bigquery_query(");
      expect(fake.lookups()).toBe(0);
   });
});
