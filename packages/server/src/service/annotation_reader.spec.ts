// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { Annotations } from "@malloydata/malloy";
import {
   findingSeverity,
   isOnRoute,
   persistAnnotationFindings,
   payloadOfNote,
   payloadsOnRoute,
   persistUnparseableFinding,
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

   // Malloy calls a prefix glued to its payload malformed. Publisher's readers
   // have always read one on their routes, and a `#(carries)` that stopped
   // counting would fail its leak check open, so isOnRoute and payloadsOnRoute
   // keep reading them.
   it("reads a glued prefix on a Publisher route as that route, as the replaced regexes did", () => {
      expect(routeOfNote('#(carries)["phi.clinical"]')).toBeUndefined();
      expect(isOnRoute('#(carries)["phi.clinical"]', "carries")).toBe(true);
      expect(
         payloadsOnRoute(['#(carries)["phi.clinical"]'], "carries"),
      ).toEqual(['["phi.clinical"]']);
      expect(isOnRoute("#(secure)x", "secure")).toBe(true);
      expect(isOnRoute("#(index)values=all", "index")).toBe(true);
      expect(payloadsOnRoute(['#"Orders, one row'], '"')).toEqual([
         "Orders, one row",
      ]);
   });

   it("does not read a different route, or a route Publisher does not own", () => {
      // `#(index-v2)` is its own route; the old `\b` regex matched it by accident.
      expect(isOnRoute("#(index-v2) x", "index")).toBe(false);
      expect(isOnRoute("#(docs) x", "doc")).toBe(false);
      expect(isOnRoute("#(someone_else)x", "someone_else")).toBe(false);
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

describe("persist annotation findings", () => {
   it("suggests the key an unknown one is a near miss of", () => {
      const [finding] = persistAnnotationFindings(
         annotationsOf(['#@ persist name="t" mergekey="id"']),
         "daily",
      );
      expect(finding?.message).toContain("did you mean `merge_key=`?");
      // Nothing near: no suggestion.
      const [far] = persistAnnotationFindings(
         annotationsOf(['#@ persist name="t" colour="red"']),
         "daily",
      );
      expect(far?.message).not.toContain("did you mean");
   });

   it("says what an unknown key costs on an incremental source", () => {
      const [finding] = persistAnnotationFindings(
         annotationsOf(['#@ persist name="t" mergekey="id"']),
         "daily",
         "m.malloy",
         { incremental: true },
      );
      expect(finding?.message).toContain("range replace");
      expect(finding?.message).toContain("row identity");
   });

   it("names the recovered source when a broken #@ line sits beside a good one", () => {
      const planned = persistUnparseableFinding(
         { message: "unterminated string" },
         "m.malloy",
         { subject: "daily", line: '#@ refresh="incremental" watermark="x' },
      );
      expect(planned.message).toContain("built from its other #@ lines");
      expect(planned.message).not.toContain("served live");
      expect(planned.subject).toBe("daily");
      const dropped = persistUnparseableFinding(
         { message: "unterminated string" },
         "m.malloy",
      );
      expect(dropped.message).toContain("served live");
   });

   it("checks a persist source against the persist keys, not the preaggregate ones", () => {
      const findings = persistAnnotationFindings(
         annotationsOf(['#@ persist name="t" grain="x"']),
         "daily",
         "m.malloy",
      );
      expect(findings).toEqual([
         expect.objectContaining({
            code: "persist-unknown-key",
            tier: "unservable",
            subject: "daily",
            model: "m.malloy",
         }),
      ]);
      expect(findingSeverity(findings[0])).toBe("warn");
   });

   it("builds the unparseable finding Malloy's build log reports, naming the line", () => {
      const finding = persistUnparseableFinding(
         {
            message: "Unterminated string",
            at: {
               url: "file:///m.malloy",
               range: {
                  start: { line: 4, character: 0 },
                  end: { line: 4, character: 9 },
               },
            },
         },
         "m.malloy",
      );
      expect(finding).toMatchObject({
         code: "persist-unparseable",
         tier: "malformed",
         model: "m.malloy",
      });
      expect(finding.message).toContain("line 5");
      expect(findingSeverity(finding)).toBe("error");
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
