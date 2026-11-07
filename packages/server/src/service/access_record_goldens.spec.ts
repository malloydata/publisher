// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Golden access records, one per decision path: the whole record one query
 * emits through `withAccessRecord` around `Model.getQueryResults`, minus the
 * timing. Each golden pins what an auditor reads off the line (decision,
 * gates, outcome, status, the fields read with their classes), so a refactor
 * of the governance path that changes any of it fails here by name.
 *
 * The last golden pins a KNOWN-WRONG value on purpose: see its comment.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import { logger } from "../logger";
import { withAuthorizeModel } from "../test_helpers/authorize_fixture";
import type { Model } from "./model";
import { withAccessRecord, type AccessRecord } from "./access_record";

const MODEL = `##! experimental.givens
#(secure)
given: ORGS :: string[]
#(secure)
given: ROLES :: string[]
given: NOTE :: string is 'x'

source: open_src is duckdb.table('child') extend {
   #(classification) "none"
   dimension: label is name
   measure: n is count()
}

#(authorize) 'clinician' in $ROLES
source: chart is duckdb.table('child') extend {
   #(classification) "phi.direct_identifier"
   dimension: patient is name
   measure: n is count()
}

#(access_filter) org_id in $ORGS
source: accounts_src is duckdb.table('accounts') extend {
   #(classification) "phi.quasi_identifier"
   dimension: area is region
   measure: n is count()
}
`;

/** What the route knows before it runs; the same on every golden. */
const BASE = {
   environment: "env",
   package: "test-pkg",
   modelPath: "m.malloy",
   adHoc: true,
   caller: "alice@example.com",
} as const;

/** Every emitted record carries the event name and the route's base. */
const RECORD = { event: "access", ...BASE } as const;

/** An answered query's outcome; a live (not storage-served) answer. */
const OK = { outcome: "ok", status: 200, servedFrom: null } as const;

let info: ReturnType<typeof spyOn>;
beforeEach(() => {
   info = spyOn(logger, "info").mockImplementation(() => logger);
});
afterEach(() => {
   info.mockRestore();
});

/**
 * Run `query` through the one record-emitting wrapper and return the single
 * record it wrote, with `durationMs` and `executionTimeMs` checked and
 * dropped (timing is not part of a golden).
 */
async function recordOf(
   model: Model,
   query: string,
   givens: Record<string, GivenValue>,
   opts: { bypassAuthorize?: boolean } = {},
): Promise<Record<string, unknown>> {
   await withAccessRecord(BASE, undefined, (audit) =>
      model.getQueryResults(
         audit,
         undefined,
         undefined,
         query,
         {},
         true,
         givens,
         undefined,
         undefined,
         "compact",
         opts.bypassAuthorize ?? false,
      ),
   ).catch(() => undefined);
   const records = (info.mock.calls as unknown[][])
      .filter(([message]) => message === "Access record")
      .map(([, record]) => record as AccessRecord);
   expect(records).toHaveLength(1);
   const { durationMs, executionTimeMs, ...rest } = records[0];
   expect(typeof durationMs).toBe("number");
   if (rest.outcome === "ok") expect(typeof executionTimeMs).toBe("number");
   return rest;
}

const golden = (
   query: string,
   givens: Record<string, GivenValue>,
   opts: { bypassAuthorize?: boolean } = {},
) =>
   withAuthorizeModel(MODEL, async ({ model, error }) => {
      expect(error).toBeUndefined();
      return recordOf(model, query, givens, opts);
   });

describe("access record goldens, one per decision path", () => {
   it("open: an ungated source", async () => {
      expect(
         await golden("run: open_src -> { group_by: label; aggregate: n }", {}),
      ).toEqual({
         ...RECORD,
         source: "open_src",
         gates: { authorize: [], accessFilter: [] },
         decision: "open",
         givenNames: [],
         gateGivenNames: [],
         fieldsRead: [
            { path: "label", classifications: ["none"] },
            { path: "n", classifications: [] },
         ],
         fieldsReadComplete: true,
         rowCount: 2,
         ...OK,
      });
   });

   it("admitted: a lock admits the caller and no row filter applies", async () => {
      expect(
         await golden("run: chart -> { group_by: patient; aggregate: n }", {
            ROLES: ["clinician"],
         } as never),
      ).toEqual({
         ...RECORD,
         source: "chart",
         gates: { authorize: ["'clinician' in $ROLES"], accessFilter: [] },
         decision: "admitted",
         givenNames: ["ROLES"],
         secureGivens: { ROLES: ["clinician"] },
         gateGivenNames: [],
         fieldsRead: [
            { path: "n", classifications: [] },
            { path: "patient", classifications: ["phi.direct_identifier"] },
         ],
         fieldsReadComplete: true,
         rowCount: 2,
         ...OK,
      });
   });

   it("filtered: a row filter is grafted from a supplied given", async () => {
      expect(
         await golden("run: accounts_src -> { group_by: area; aggregate: n }", {
            ORGS: ["org1"],
            NOTE: "mine",
         } as never),
      ).toEqual({
         ...RECORD,
         source: "accounts_src",
         gates: { authorize: [], accessFilter: ["org_id in $ORGS"] },
         decision: "filtered",
         // A plain given's name is recorded, never its value.
         givenNames: ["NOTE", "ORGS"],
         secureGivens: { ORGS: ["org1"] },
         gateGivenNames: ["ORGS"],
         fieldsRead: [
            { path: "area", classifications: ["phi.quasi_identifier"] },
            { path: "n", classifications: [] },
         ],
         fieldsReadComplete: true,
         // org1: east and west.
         rowCount: 2,
         ...OK,
      });
   });

   it("bypassed: the data-management path skips the lock", async () => {
      expect(
         await golden(
            "run: chart -> { aggregate: n }",
            {},
            {
               bypassAuthorize: true,
            },
         ),
      ).toEqual({
         ...RECORD,
         source: "chart",
         // The gates in force are still reported; the decision says they were skipped.
         gates: { authorize: ["'clinician' in $ROLES"], accessFilter: [] },
         decision: "bypassed",
         givenNames: [],
         gateGivenNames: [],
         fieldsRead: [{ path: "n", classifications: [] }],
         fieldsReadComplete: true,
         rowCount: 1,
         ...OK,
      });
   });

   it("denied by lock: a caller the lock does not admit", async () => {
      const record = await golden("run: chart -> { aggregate: n }", {
         ROLES: ["viewer"],
      } as never);
      const { error, ...rest } = record;
      // A refused query compiled nothing for the caller: no source, gates,
      // decision or fieldsRead, only what the request named and the outcome.
      expect(rest).toEqual({ ...RECORD, outcome: "denied_lock", status: 403 });
      expect(typeof error).toBe("string");
   });

   it("denied by row level: the row filter's given is unbound", async () => {
      const record = await golden("run: accounts_src -> { aggregate: n }", {});
      const { error, ...rest } = record;
      expect(rest).toEqual({ ...RECORD, outcome: "denied_lock", status: 403 });
      expect(typeof error).toBe("string");
   });

   it("caller-declared source admitted through an inherited lock records today's 'open'", async () => {
      // KNOWN WRONG, pinned on purpose. The caller declares `mine is chart
      // extend {}` in the query text; enforcement walks `mine`'s ancestry,
      // finds `chart`'s lock and admits the clinician. But the record looks
      // its gates up by name in the load-time `apiSources`, where `mine` does
      // not exist, so it reports no gates and `decision: "open"` for a query
      // a lock decided. Phase 3 (V4) derives the record from what enforcement
      // evaluated and changes this golden deliberately to `admitted` with the
      // lock's expression.
      expect(
         await golden(
            "source: mine is chart extend { }\nrun: mine -> { group_by: patient; aggregate: n }",
            { ROLES: ["clinician"] } as never,
         ),
      ).toEqual({
         ...RECORD,
         source: "mine",
         gates: { authorize: [], accessFilter: [] },
         decision: "open",
         givenNames: ["ROLES"],
         secureGivens: { ROLES: ["clinician"] },
         gateGivenNames: [],
         fieldsRead: [
            { path: "n", classifications: [] },
            { path: "patient", classifications: ["phi.direct_identifier"] },
         ],
         fieldsReadComplete: true,
         rowCount: 2,
         ...OK,
      });
   });

   it("control: the same caller-declared source is refused when the inherited lock does not admit", async () => {
      // Proves the golden above went through the lock: enforcement is right,
      // only the record is wrong.
      const record = await golden(
         "source: mine is chart extend { }\nrun: mine -> { aggregate: n }",
         { ROLES: ["viewer"] } as never,
      );
      expect(record).toMatchObject({ outcome: "denied_lock", status: 403 });
   });
});
