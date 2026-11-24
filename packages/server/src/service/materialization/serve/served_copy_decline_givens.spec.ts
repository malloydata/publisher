// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy remembers a query's decline by its text alone, without the
 * caller's givens. So no remembered decline may depend on them. The one
 * check that compiles with the givens, the result shape against the
 * author's compile, compares shapes (no given's value changes one); when the
 * author's compile itself fails, which a given can cause, it declines this
 * caller alone and remembers nothing.
 */

import { describe, expect, it } from "bun:test";
import { LegDecline } from "../domain/leg_decline";
import { ServedCopyServe } from "./served_copy_serve";

/** The result-shape check over one table that leaves a column out. */
async function check(args: {
   authorShape: (givens: unknown) => Promise<string>;
   givens: Record<string, unknown>;
   memo: Map<string, LegDecline>;
}): Promise<unknown> {
   const serve = new ServedCopyServe({
      authorResultShape: (_q: string, g: unknown) => args.authorShape(g),
   } as never);
   const compiled = {
      leftOutTables: new Set(["t"]),
      leftOutColumns: new Map([["s", ["region"]]]),
      declined: args.memo,
   };
   const attempt = {
      compiled,
      queryString: "run: s -> { select: * }",
      givens: args.givens,
      virtualMap: new Map(),
      runnable: {
         getPreparedResult: async () => ({ resultExplore: { allFields: [] } }),
      },
      compiling: <T>(work: () => Promise<T>) => work(),
      decline: (reason: string, code: never): never => {
         const e = new LegDecline(code, undefined, { message: reason });
         args.memo.set("run: s -> { select: * }", e);
         throw e;
      },
   };
   return (
      serve as unknown as {
         checkResultShape(a: unknown, t: unknown): Promise<void>;
      }
   )
      .checkResultShape(attempt, [{ tablePath: "t", sourceIDs: ["s"] }])
      .then(
         () => "served",
         (e) => (e instanceof LegDecline ? e.code : e),
      );
}

describe("the served copy's remembered declines", () => {
   it("forget a decline the author's compile failing with one caller's givens caused", async () => {
      const memo = new Map<string, LegDecline>();
      const shape = JSON.stringify([]);
      const authorShape = async (g: unknown) => {
         if ((g as { REGION?: unknown }).REGION === undefined) {
            throw new Error("given REGION has no value");
         }
         return shape;
      };
      expect(await check({ authorShape, givens: {}, memo })).toBe(
         "served_copy_not_served",
      );
      expect(memo.size).toBe(0);
      // Another caller, whose givens compile, is judged afresh.
      expect(
         await check({ authorShape, givens: { REGION: "east" }, memo }),
      ).toBe("served");
   });

   it("remember a shape that differs, which no given's value changes", async () => {
      const memo = new Map<string, LegDecline>();
      const authorShape = async () => JSON.stringify(["id", "region"]);
      expect(await check({ authorShape, givens: { REGION: "a" }, memo })).toBe(
         "served_copy_not_served",
      );
      expect(memo.size).toBe(1);
   });
});
