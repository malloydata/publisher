// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   CALLER_JOIN_MAX_DEPTH,
   CallerJoinWalkError,
   collectCallerJoins,
   isCallerAuthored,
   type CallerRegion,
   type IrStruct,
   type PreparedQueryIr,
} from "./caller_joins";

// Synthetic IR, shaped like what `getPreparedQuery()` returns, for the walk's
// refusals that no compilable query reaches.

const QUERY_URL = "internal://query";
const REGION: CallerRegion = { kind: "query", text: "" };
const CALLER = { url: QUERY_URL, range: { start: { line: 0 } } };
const AUTHOR = { url: "file:///m.malloy", range: { start: { line: 3 } } };
const AUTHOR_URLS: ReadonlySet<string> = new Set(["file:///m.malloy"]);

function join(alias: string, location = CALLER, fields: IrStruct[] = []) {
   return {
      type: "table",
      name: "t",
      as: alias,
      join: "one",
      location,
      fields,
   } as IrStruct;
}

function prepared(
   target: IrStruct,
   extra: Record<string, unknown> = {},
): PreparedQueryIr {
   return {
      _query: {
         location: CALLER,
         structRef: target,
         pipeline: [{ queryFields: [] }],
         ...extra,
      },
      _modelDef: { contents: {} } as never,
   };
}

describe("collectCallerJoins", () => {
   it("finds a caller join and skips an author one", () => {
      const found = collectCallerJoins(
         prepared({
            type: "table",
            location: AUTHOR,
            fields: [join("c"), join("a", AUTHOR)],
         }),
         REGION,
         AUTHOR_URLS,
      );
      expect(found.map((j) => j.alias)).toEqual(["c"]);
      expect(found[0].path).toEqual(["target", "fields", "c"]);
   });

   it("refuses a caller join in a place the walk does not read", () => {
      const ir = prepared(
         { type: "table", location: AUTHOR, fields: [] },
         { pipeline: [{ queryFields: [], somewhereNew: [join("x")] }] },
      );
      expect(() => collectCallerJoins(ir, REGION, AUTHOR_URLS)).toThrow(
         new CallerJoinWalkError(
            "a caller join sits where the walk does not look",
         ),
      );
   });

   it("refuses a join with no location, except in a composite's synthesized fields", () => {
      const unlocated = { type: "table", as: "u", join: "one" } as IrStruct;
      expect(() =>
         collectCallerJoins(
            prepared({ type: "table", location: AUTHOR, fields: [unlocated] }),
            REGION,
            AUTHOR_URLS,
         ),
      ).toThrow(new CallerJoinWalkError("a join carries no location"));
      expect(
         collectCallerJoins(
            prepared({
               type: "composite",
               location: AUTHOR,
               fields: [unlocated],
               sources: [],
            }),
            REGION,
            AUTHOR_URLS,
         ),
      ).toEqual([]);
   });

   it("refuses caller joins nested past the depth bound", () => {
      let inner = join("j0");
      for (let i = 1; i <= CALLER_JOIN_MAX_DEPTH; i++) {
         inner = join(`j${i}`, CALLER, [inner]);
      }
      expect(() =>
         collectCallerJoins(
            prepared({ type: "table", location: AUTHOR, fields: [inner] }),
            REGION,
            AUTHOR_URLS,
         ),
      ).toThrow(
         new CallerJoinWalkError("caller joins nest past the depth bound"),
      );
   });

   it("refuses a resolved composite join the declared run target lacks", () => {
      const ir = prepared(
         { type: "composite", location: AUTHOR, fields: [], sources: [] },
         {
            compositeResolvedSourceDef: {
               type: "table",
               location: AUTHOR,
               fields: [join("m", AUTHOR)],
            },
         },
      );
      expect(() => collectCallerJoins(ir, REGION, AUTHOR_URLS)).toThrow(
         new CallerJoinWalkError(
            "a resolved composite carries a join the declared source does not",
         ),
      );
   });
});

describe("isCallerAuthored", () => {
   const span: CallerRegion = {
      kind: "span",
      url: "file:///virtual.malloy",
      fromLine: 5,
      text: "",
   };

   it("reads a span by URL and first line", () => {
      const at = (line: number) => ({
         url: "file:///virtual.malloy",
         range: { start: { line } },
      });
      expect(isCallerAuthored(at(4), span, AUTHOR_URLS)).toBe(false);
      expect(isCallerAuthored(at(5), span, AUTHOR_URLS)).toBe(true);
      expect(
         isCallerAuthored(
            { url: "file:///m.malloy", range: { start: { line: 9 } } },
            span,
            AUTHOR_URLS,
         ),
      ).toBe(false);
   });

   it("reads the author's only under one of the model's own URLs", () => {
      expect(isCallerAuthored(AUTHOR, REGION, AUTHOR_URLS)).toBe(false);
      expect(isCallerAuthored(CALLER, REGION, AUTHOR_URLS)).toBe(true);
      expect(
         isCallerAuthored({ url: "file:///other.malloy" }, REGION, AUTHOR_URLS),
      ).toBe(true);
   });

   it("reads an unlocated node as caller-written", () => {
      expect(isCallerAuthored(undefined, REGION, AUTHOR_URLS)).toBe(true);
      expect(isCallerAuthored(undefined, span, AUTHOR_URLS)).toBe(true);
   });
});

describe("a record or array is not a join", () => {
   it("is not collected, and does not trip the completeness scan", () => {
      const record = {
         type: "record",
         name: "r",
         join: "one",
         location: CALLER,
         fields: [],
      } as IrStruct;
      expect(
         collectCallerJoins(
            prepared({ type: "table", location: AUTHOR, fields: [record] }),
            REGION,
            AUTHOR_URLS,
         ),
      ).toEqual([]);
   });
});
