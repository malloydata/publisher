// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { Annotations } from "@malloydata/malloy";
import {
   payloadOfNote,
   payloadsOnRoute,
   PUBLISHER_ROUTES,
   readRouteList,
   readRouteTag,
   RECOGNIZED_PERSIST_KEYS,
   routeOfNote,
} from "./annotation_reader";

function annotationsOf(lines: string[]): Annotations {
   return new Annotations({
      notes: lines.map((text, line) => ({
         text,
         at: {
            url: "file:///m.malloy",
            range: {
               start: { line, character: 0 },
               end: { line, character: text.length },
            },
         },
      })),
   } as ConstructorParameters<typeof Annotations>[0]);
}

describe("routeOfNote and payloadOfNote", () => {
   it("routes bracketed, sigil and MOTLY notes as Malloy does", () => {
      expect(routeOfNote("#(doc) Orders")).toBe("doc");
      expect(routeOfNote("  #(authorize) org_id = $ORG")).toBe("authorize");
      expect(routeOfNote("#@ persist")).toBe("@");
      expect(routeOfNote("##! experimental.persistence")).toBe("!");
      expect(routeOfNote("# label=Orders")).toBe("");
   });

   it("says a malformed prefix declares nothing", () => {
      expect(routeOfNote('#(classification "x")')).toBeUndefined();
   });

   it("reads a block note's payload like a line note's", () => {
      expect(payloadOfNote("#(doc) One line").trim()).toBe("One line");
      // A compiled block note, as annotations carry it: dedented, no closer.
      const block = "#|(doc)\nFirst\nSecond";
      expect(routeOfNote(block)).toBe("doc");
      expect(payloadOfNote(block)).toBe("First\nSecond");
   });

   it("collects the payloads on one route, in order", () => {
      expect(
         payloadsOnRoute(
            ["#(doc) a", "# label=x", "#(doc) b", "#(index)"],
            "doc",
         ).map((p) => p.trim()),
      ).toEqual(["a", "b"]);
   });
});

describe("the #@ route's keys", () => {
   it("recognizes exactly the persist keys Publisher read before the registry", () => {
      expect([...RECOGNIZED_PERSIST_KEYS].sort()).toEqual(
         [
            "persist",
            "name",
            "storage",
            "realization",
            "refresh",
            "watermark",
            "merge_key",
            "freshness",
            "queryMetadata",
            "partition",
            "sharing",
            "schedule",
         ].sort(),
      );
   });

   it("accepts the pre-aggregation directive's keys on the same route", () => {
      for (const key of ["preaggregate", "grain", "namespace"]) {
         expect(PUBLISHER_ROUTES["@"]?.keys?.has(key)).toBe(true);
      }
   });

   it("finds an unknown key (a warning) and a retired one (refused), and nothing else", () => {
      const { tag, findings } = readRouteTag(
         annotationsOf([
            `#@ persist name="orders" merge_keys="id" schedule="0 * * * *"`,
         ]),
         "@",
      );
      expect(tag.text("name")).toBe("orders");
      expect(
         findings.map((f) => [f.code, f.tier, f.message.split("`")[1]]),
      ).toEqual([
         ["persist-unknown-key", "unservable", "merge_keys="],
         ["persist-retired-key", "malformed", "schedule="],
      ]);
      expect(findings[1]?.rewrite).toContain("materialization.schedule");
   });
});

describe("readRouteList", () => {
   it("reads a quoted string or a quoted list, as a sorted set", () => {
      expect(
         readRouteList("classification", ' ["b", "a", "a"]', {
            list: "optional",
         }),
      ).toEqual({ values: ["a", "b"] });
      expect(
         readRouteList("classification", '"x"', { list: "optional" }),
      ).toEqual({ values: ["x"] });
   });

   it("refuses an unquoted value, a non-list where one is required, and an environment read", () => {
      expect(
         readRouteList("classification", "x", { list: "optional" }).malformed,
      ).toContain("is not quoted");
      expect(
         readRouteList("carries", '"x"', { list: "required" }).malformed,
      ).toContain("is not a list");
      expect(
         readRouteList("carries", '["@env.X"]', { list: "required" }).malformed,
      ).toContain("reads the environment");
   });
});
