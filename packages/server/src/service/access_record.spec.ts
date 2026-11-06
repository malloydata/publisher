// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The access record: the summary the query service attaches to an answered
 * query (source entered, gates, decision, given names, fields read with
 * classes, row count), the outcome a refusal maps to, the caller header, and
 * what one emitted record carries.
 */
import { describe, expect, it, spyOn } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import { DEFAULT_CALLER_HEADER } from "../constants";
import {
   AccessDeniedError,
   BadRequestError,
   ModelNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { withAuthorizeModel } from "../test_helpers/authorize_fixture";
import { withSettings } from "../test_helpers/settings_env";
import {
   callerHeaderName,
   emitAccessRecord,
   outcomeOf,
   summarizeQueryAccess,
   unrecordedAudit,
} from "./access_record";

const MODEL = `##! experimental.givens
#(secure)
given: ORGS :: string[]
given: NOTE :: string is 'x'

source: kids is duckdb.table('child') extend {
   #(classification) "none"
   dimension: label is name
}

#(access_filter) org_id in $ORGS
source: accounts_src is duckdb.table('accounts') extend {
   #(classification) "phi.quasi_identifier"
   dimension: area is region
   #(classification) "phi.direct_identifier"
   dimension: holder is owner
   #(classification) "phi.financial"
   measure: total is amount.sum()
   measure: n is count()
   join_one: kid is kids on kid.id = child_id
}

source: open_src is duckdb.table('child') extend {
   measure: n is count()
}
`;

describe("summarizeQueryAccess on an answered query", () => {
   it("records the source, the filter decision, the given names and the fields read with classes", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const audit = unrecordedAudit();
         await model.getQueryResults(
            audit,
            undefined,
            undefined,
            "run: accounts_src -> { group_by: area, kid.label; aggregate: total, n }",
            {},
            true,
            { ORGS: ["org1"], NOTE: "mine" } as unknown as Record<
               string,
               GivenValue
            >,
         );
         const access = audit.summary;
         expect(access).toBeDefined();
         expect(access?.source).toBe("accounts_src");
         expect(access?.decision).toBe("filtered");
         expect(access?.gates).toEqual({
            authorize: [],
            accessFilter: ["org_id in $ORGS"],
         });
         expect(access?.givenNames).toEqual(["NOTE", "ORGS"]);
         expect(access?.gateGivenNames).toEqual(["ORGS"]);
         // A secure given's value is the caller's resolved attribute; a plain
         // given's value is the caller's own and is not recorded.
         expect(access?.secureGivens).toEqual({ ORGS: ["org1"] });
         // org1 has three rows, all with child_id 1: two groups by area.
         expect(access?.rowCount).toBe(2);
         const read = Object.fromEntries(
            (access?.fieldsRead ?? []).map((f) => [f.path, f.classifications]),
         );
         expect(read["area"]).toEqual(["phi.quasi_identifier"]);
         expect(read["total"]).toEqual(["phi.financial"]);
         expect(read["kid.label"]).toEqual(["none"]);
         expect(read["n"]).toEqual([]);
         expect(access?.fieldsReadComplete).toBe(true);
         // Not read by this query, so not in the record.
         expect(read["holder"]).toBeUndefined();
         // The gate's own column is read by the graft, not by the caller.
         expect(read["org_id"]).toBeUndefined();
      });
   });

   it("records an open source as open, with no gates", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const audit = unrecordedAudit();
         await model.getQueryResults(
            audit,
            undefined,
            undefined,
            "run: open_src -> { aggregate: n }",
            {},
            true,
            {},
         );
         expect(audit.summary).toMatchObject({
            source: "open_src",
            decision: "open",
            gates: { authorize: [], accessFilter: [] },
            givenNames: [],
            gateGivenNames: [],
            rowCount: 1,
         });
      });
   });
});

const PATIENTS = `
source: patients is duckdb.table('child') extend {
   #(classification) "phi.direct_identifier"
   dimension: ssn is name
   dimension: plain is name
   view: by_ssn is { group_by: ssn }
}
`;

/** The `fieldsRead` of one ad-hoc query, by path, plus whether it is complete. */
async function readsOf(modelText: string, query: string) {
   return withAuthorizeModel(modelText, async ({ model }) => {
      const audit = unrecordedAudit();
      await model.getQueryResults(audit, undefined, undefined, query, {}, true);
      return {
         read: Object.fromEntries(
            (audit.summary?.fieldsRead ?? []).map((f) => [f.path, f]),
         ),
         complete: audit.summary?.fieldsReadComplete,
      };
   });
}

describe("fieldsRead resolves against the struct the query ran on", () => {
   it("lists the field behind a dimension an inline source defines", async () => {
      const { read, complete } = await readsOf(
         PATIENTS,
         "run: patients extend { dimension: s is ssn } -> { group_by: s }",
      );
      expect(read["ssn"]?.classifications).toEqual(["phi.direct_identifier"]);
      expect(read["s"]?.classifications).toEqual(["phi.direct_identifier"]);
      expect(complete).toBe(true);
   });

   it("lists the field behind a dimension a segment's own extend adds", async () => {
      const { read } = await readsOf(
         PATIENTS,
         "run: patients -> { extend: { dimension: s is ssn } group_by: s }",
      );
      expect(read["ssn"]?.classifications).toEqual(["phi.direct_identifier"]);
   });

   it("twin: a query that never reads the classified field lists none of it", async () => {
      const { read } = await readsOf(
         PATIENTS,
         "run: patients extend { dimension: p is plain } -> { group_by: p }",
      );
      expect(read["ssn"]).toBeUndefined();
      expect(read["p"]?.classifications).toEqual([]);
   });

   it("names the classes a relabel declared below its input", async () => {
      const { read } = await readsOf(
         `${PATIENTS.replace(
            "dimension: plain is name",
            `#(classification) "none"\n   dimension: plain is ssn`,
         )}`,
         "run: patients -> { group_by: plain }",
      );
      expect(read["plain"]).toMatchObject({
         classifications: ["none"],
         dropped: ["phi.direct_identifier"],
      });
   });

   it("reads a field five joins deep, past the old shallow walk, and says it is complete", async () => {
      const chain = `
source: e is duckdb.table('child') extend {
   #(classification) "none"
   dimension: ssn is name
}
source: d is duckdb.table('child') extend { join_one: e on e.id = id }
source: c is duckdb.table('child') extend { join_one: d on d.id = id }
source: b is duckdb.table('child') extend { join_one: c on c.id = id }
source: a is duckdb.table('child') extend { join_one: b on b.id = id }
`;
      const { read, complete } = await readsOf(
         chain,
         "run: a -> { group_by: b.c.d.e.ssn }",
      );
      expect(read["b.c.d.e.ssn"]?.classifications).toEqual(["none"]);
      expect(complete).toBe(true);
   });

   it("says the record is incomplete when a read cannot be placed", async () => {
      await withAuthorizeModel(PATIENTS, async ({ model }) => {
         const modelDef = (model as unknown as { modelDef: never }).modelDef;
         const summary = await summarizeQueryAccess({
            runnable: {
               getPreparedQuery: async () => ({
                  _query: {
                     structRef: "patients",
                     pipeline: [
                        {
                           refSummary: {
                              fieldUsage: [
                                 { path: ["ssn"] },
                                 { path: ["ghost"] },
                              ],
                           },
                        },
                     ],
                  },
               }),
            },
            modelDef,
            apiSources: [],
            modelGivens: [],
            source: "patients",
            givens: {},
            bypassAuthorize: false,
            grafts: [],
            rowCount: 0,
         });
         expect(summary.fieldsReadComplete).toBe(false);
         expect(summary.fieldsRead.map((f) => f.path)).toEqual(["ssn"]);
      });
   });

   it("says the record is incomplete when the query names no struct it can read", async () => {
      const summary = await summarizeQueryAccess({
         runnable: { getPreparedQuery: async () => ({ _query: {} }) },
         modelDef: undefined,
         apiSources: [],
         modelGivens: [],
         source: undefined,
         givens: {},
         bypassAuthorize: false,
         grafts: [],
         rowCount: 0,
      });
      expect(summary).toMatchObject({
         fieldsRead: [],
         fieldsReadComplete: false,
      });
   });
});

describe("outcomeOf", () => {
   it("maps a refusal to the gate's view of it", () => {
      expect(outcomeOf(new AccessDeniedError("x"), 403)).toBe("denied_lock");
      expect(outcomeOf(new ModelNotFoundError("x"), 404)).toBe(
         "refused_surface",
      );
      expect(outcomeOf(new Error("not queryable"), 404)).toBe(
         "refused_surface",
      );
      expect(outcomeOf(new BadRequestError("x"), 400)).toBe("bad_request");
      expect(outcomeOf(new Error("boom"), 500)).toBe("error");
   });
});

describe("callerHeaderName", () => {
   const headerFrom = (env: Record<string, string>) =>
      withSettings({ env }, () => callerHeaderName());

   it("defaults, and takes the deployment's header lowercased", () => {
      expect(headerFrom({})).toBe(DEFAULT_CALLER_HEADER);
      expect(headerFrom({ PUBLISHER_CALLER_HEADER: " X-Gateway-User " })).toBe(
         "x-gateway-user",
      );
      expect(headerFrom({ PUBLISHER_CALLER_HEADER: "" })).toBe(
         DEFAULT_CALLER_HEADER,
      );
   });
});

describe("emitAccessRecord", () => {
   it("writes one info line carrying the record", () => {
      const info = spyOn(logger, "info").mockImplementation(() => logger);
      try {
         emitAccessRecord({
            event: "access",
            environment: "env",
            package: "pkg",
            modelPath: "m.malloy",
            adHoc: true,
            caller: "alice@example.com",
            source: "accounts_src",
            decision: "filtered",
            fieldsRead: [
               { path: "area", classifications: ["phi.quasi_identifier"] },
            ],
            rowCount: 2,
            outcome: "ok",
            status: 200,
            durationMs: 12,
         });
         expect(info).toHaveBeenCalledTimes(1);
         const [message, record] = info.mock.calls[0] as unknown as [
            string,
            unknown,
         ];
         expect(message).toBe("Access record");
         expect(record).toMatchObject({
            event: "access",
            caller: "alice@example.com",
            source: "accounts_src",
            decision: "filtered",
            outcome: "ok",
            status: 200,
         });
      } finally {
         info.mockRestore();
      }
   });
});
