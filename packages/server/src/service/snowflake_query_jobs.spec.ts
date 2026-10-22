// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type snowflake from "snowflake-sdk";
import {
   decodeQueryJobId,
   encodeQueryJobId,
   QueryJobNotFoundError,
   SnowflakeQueryJobClient,
   valueConverter,
   type QueryJobClaims,
} from "./snowflake_query_jobs";

const claims: QueryJobClaims = {
   queryId: "01c7e835-0014-23f3-0003-8ea60150b0ee",
   environment: "env",
   package: "pkg",
   model: "orders.malloy",
   connection: "snowflake",
};
const target = { environment: "env", package: "pkg", model: "orders.malloy" };

describe("query job ids", () => {
   it("decodes the claims they were issued with", () => {
      expect(decodeQueryJobId(encodeQueryJobId(claims), target)).toEqual(
         claims,
      );
   });

   it("refuses an id whose claims were altered", () => {
      const [, sig] = encodeQueryJobId(claims).split(".");
      const altered = Buffer.from(
         JSON.stringify({ ...claims, queryId: "01c7e835-other" }),
      ).toString("base64url");
      expect(() => decodeQueryJobId(`${altered}.${sig}`, target)).toThrow(
         QueryJobNotFoundError,
      );
   });

   it.each([
      ["another environment", { ...target, environment: "other" }],
      ["another package", { ...target, package: "other" }],
      ["another model", { ...target, model: "other.malloy" }],
   ])("refuses a genuine id presented for %s", (_case, presented) => {
      expect(() =>
         decodeQueryJobId(encodeQueryJobId(claims), presented),
      ).toThrow(QueryJobNotFoundError);
   });

   it.each(["", "not-a-job", "a.b.c", "x.y"])("refuses %p", (jobId) => {
      expect(() => decodeQueryJobId(jobId, target)).toThrow(
         QueryJobNotFoundError,
      );
   });
});

function column(kind: "integer" | "decimal" | "timestamp" | "text") {
   return {
      isNumber: () => kind === "integer" || kind === "decimal",
      getScale: () => (kind === "decimal" ? 2 : 0),
      isDate: () => false,
      isTimestamp: () => kind === "timestamp",
   } as unknown as snowflake.Column;
}

/**
 * The shape of the wrapper snowflake-sdk returns for an integer under
 * `jsTreatIntegerAsBigInt` (a `big-integer` value): numeric through `valueOf`,
 * its digits through `toString`, and those digits again through `toJSON`.
 */
function bigInt(digits: string) {
   return {
      valueOf: () => Number(digits),
      toString: () => digits,
      toJSON: () => digits,
   };
}

describe("valueConverter", () => {
   it("sends the SDK's integer wrapper as a number when it is exact", () => {
      expect(valueConverter(column("integer"))(bigInt("9386147"))).toBe(
         9386147,
      );
   });

   it("sends an integer past 2^53 as its digits", () => {
      expect(
         valueConverter(column("integer"))(bigInt("9007199254740993")),
      ).toBe("9007199254740993");
      expect(valueConverter(column("integer"))(9007199254740993n)).toBe(
         "9007199254740993",
      );
   });

   it("keeps a decimal a number", () => {
      expect(valueConverter(column("decimal"))(12.5)).toBe(12.5);
   });

   it("sends a timestamp as ISO text", () => {
      expect(
         valueConverter(column("timestamp"))("2026-10-22T20:05:25.441+00:00"),
      ).toBe("2026-10-22T20:05:25.441Z");
   });

   it("passes text through and keeps null null", () => {
      expect(valueConverter(column("text"))(" Tiresias ")).toBe(" Tiresias ");
      expect(valueConverter(column("integer"))(null)).toBeNull();
   });
});

/** A client whose session is `conn`, without connecting. */
function clientWith(conn: Partial<snowflake.Connection>) {
   const client = new SnowflakeQueryJobClient("fingerprint", {});
   (client as unknown as { session: Promise<unknown> }).session =
      Promise.resolve(conn);
   return client;
}

/** A finished, failed statement, as getQueryStatusThrowIfError reports one. */
function failedWith(errorCode: string, message: string) {
   return clientWith({
      getQueryStatus: async () => "FAILED_WITH_ERROR",
      isStillRunning: () => false,
      getQueryStatusThrowIfError: async () => {
         throw Object.assign(new Error(message), {
            code: -1,
            data: { data: { queries: [{ errorCode }] } },
         });
      },
   } as unknown as Partial<snowflake.Connection>);
}

describe("SnowflakeQueryJobClient.status", () => {
   it("reads an explicit cancel as cancelled, by the statement's own code", async () => {
      expect(
         await failedWith("000604", "SQL execution canceled").status("q"),
      ).toEqual({
         state: "cancelled",
         snowflakeStatus: "FAILED_WITH_ERROR",
         error: { code: "000604", message: "SQL execution canceled" },
      });
   });

   it("reads a statement timeout as failed", async () => {
      const status = await failedWith(
         "000630",
         "Statement reached its statement or warehouse timeout of 600 second(s) and was canceled.",
      ).status("q");
      expect(status.state).toBe("failed");
      expect(status.error?.code).toBe("000630");
   });

   it("reports running and succeeded without asking for an error", async () => {
      const running = clientWith({
         getQueryStatus: async () => "RUNNING",
         isStillRunning: () => true,
      } as unknown as Partial<snowflake.Connection>);
      expect((await running.status("q")).state).toBe("running");
      const done = clientWith({
         getQueryStatus: async () => "SUCCESS",
         isStillRunning: () => false,
      } as unknown as Partial<snowflake.Connection>);
      expect((await done.status("q")).state).toBe("succeeded");
   });
});
