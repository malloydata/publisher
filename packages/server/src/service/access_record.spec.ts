// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The access record: the summary the query service attaches to an answered
 * query (source entered, gates, decision, given names, fields read with
 * classes, row count), the outcome a refusal maps to, the caller header, and
 * what one emitted record carries.
 */
import { afterEach, describe, expect, it, spyOn } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import {
   AccessDeniedError,
   BadRequestError,
   ModelNotFoundError,
} from "../errors";
import { logger } from "../logger";
import { withAuthorizeModel } from "../test_helpers/authorize_fixture";
import {
   callerHeaderName,
   DEFAULT_CALLER_HEADER,
   emitAccessRecord,
   outcomeOf,
} from "./access_record";

const MODEL = `##! experimental.givens
#(secure)
given: ORGS :: string[]
given: NOTE :: string is 'x'

source: kids is duckdb.table('child') extend {
   #(classification) none
   dimension: label is name
}

#(access_filter) org_id in $ORGS
source: accounts_src is duckdb.table('accounts') extend {
   #(classification) phi.quasi_identifier
   dimension: area is region
   #(classification) phi.direct_identifier
   dimension: holder is owner
   #(classification) phi.financial
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
         const answered = await model.getQueryResults(
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
         const access = answered.access;
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
         // Not read by this query, so not in the record.
         expect(read["holder"]).toBeUndefined();
         // The gate's own column is read by the graft, not by the caller.
         expect(read["org_id"]).toBeUndefined();
      });
   });

   it("records an open source as open, with no gates", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const answered = await model.getQueryResults(
            undefined,
            undefined,
            "run: open_src -> { aggregate: n }",
            {},
            true,
            {},
         );
         expect(answered.access).toMatchObject({
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
   const original = process.env.PUBLISHER_CALLER_HEADER;
   afterEach(() => {
      if (original === undefined) delete process.env.PUBLISHER_CALLER_HEADER;
      else process.env.PUBLISHER_CALLER_HEADER = original;
   });

   it("defaults, and takes the deployment's header lowercased", () => {
      delete process.env.PUBLISHER_CALLER_HEADER;
      expect(callerHeaderName()).toBe(DEFAULT_CALLER_HEADER);
      process.env.PUBLISHER_CALLER_HEADER = " X-Gateway-User ";
      expect(callerHeaderName()).toBe("x-gateway-user");
      process.env.PUBLISHER_CALLER_HEADER = "";
      expect(callerHeaderName()).toBe(DEFAULT_CALLER_HEADER);
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
