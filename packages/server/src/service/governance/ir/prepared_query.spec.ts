// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The decoders in `prepared_query.ts` replaced ad hoc ones whose semantics
 * differ on purpose; each difference is pinned here so a "simplification"
 * that merges two of them fails by name.
 */
import { describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import {
   preparedQueryView,
   probeFilterList,
   resolveStructRef,
   resolveStructRefSource,
   sqlTargetRef,
   structRefName,
} from "./prepared_query";

const source = { type: "table", name: "s", fields: [] };
const notASource = { type: "query", name: "q" };
const modelDef = {
   contents: { s: source, q: notASource },
} as unknown as ModelDef;

describe("preparedQueryView", () => {
   it("reads _query and _modelDef without falling back", () => {
      const view = preparedQueryView({
         _query: {
            structRef: "s",
            compositeResolvedSourceDef: source,
            location: { url: "internal://x" },
            pipeline: [],
         },
      });
      expect(view.structRef).toBe("s");
      expect(view.compositeResolvedSourceDef).toBe(source);
      expect(view.location).toEqual({ url: "internal://x" });
      expect(view.pipeline).toEqual([]);
      expect(view.modelDef).toBeUndefined();
   });

   it("tolerates a prepared query with no _query", () => {
      const view = preparedQueryView({});
      expect(view.query).toBeUndefined();
      expect(view.structRef).toBeUndefined();
      expect(preparedQueryView(undefined).query).toBeUndefined();
   });
});

describe("structRef decoders", () => {
   it("resolveStructRef takes any contents entry for a name", () => {
      expect(resolveStructRef("s", modelDef)).toBe(source as never);
      expect(resolveStructRef("q", modelDef)).toBe(notASource as never);
      expect(resolveStructRef("missing", modelDef)).toBeUndefined();
      expect(resolveStructRef(source, undefined)).toBe(source as never);
      expect(resolveStructRef(7, modelDef)).toBeUndefined();
   });

   it("resolveStructRefSource requires a source for a name, not for an object", () => {
      expect(resolveStructRefSource("s", modelDef)).toBe(source as never);
      expect(resolveStructRefSource("q", modelDef)).toBeUndefined();
      expect(resolveStructRefSource(notASource, modelDef)).toBe(
         notASource as never,
      );
   });

   it("sqlTargetRef prefers the resolved composite member, never a union", () => {
      const member = { type: "table", name: "m" };
      expect(
         sqlTargetRef(
            preparedQueryView({
               _query: { structRef: "s", compositeResolvedSourceDef: member },
            }),
         ),
      ).toBe(member);
      expect(
         sqlTargetRef(preparedQueryView({ _query: { structRef: "s" } })),
      ).toBe("s");
   });

   it("structRefName is `as || name`: an empty alias falls through", () => {
      expect(structRefName("s")).toBe("s");
      expect(structRefName({ as: "a", name: "n" })).toBe("a");
      expect(structRefName({ as: "", name: "n" })).toBe("n");
      expect(structRefName(undefined)).toBeUndefined();
   });

   it("probeFilterList reads an inline ref only", () => {
      const filterList = [{ code: "x" }];
      expect(
         probeFilterList(
            preparedQueryView({ _query: { structRef: { filterList } } }),
         ),
      ).toBe(filterList);
      expect(
         probeFilterList(preparedQueryView({ _query: { structRef: "s" } })),
      ).toBeUndefined();
   });
});
