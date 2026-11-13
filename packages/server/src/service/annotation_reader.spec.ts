// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { Annotations } from "@malloydata/malloy";
import {
   findingSeverity,
   indexNotePayload,
   isIndexNote,
   isOnRoute,
   marksAgentHidden,
   MATERIALIZE_V2_ROUTES,
   publisherRouteOf,
   persistAnnotationFindings,
   payloadOfNote,
   payloadsOnRoute,
   persistUnparseableFinding,
   PUBLISHER_ROUTES,
   readRouteList,
   readRouteTag,
   RECOGNIZED_PERSIST_KEYS,
   routeOfNote,
   tagPayloadKeys,
} from "./annotation_reader";
import { GENERATOR_KEYS } from "./materialization/plan/build_file_generator";

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

describe("the materialization v2 routes", () => {
   it("registers #(materialize) with the keys its generator reads", () => {
      expect([...(PUBLISHER_ROUTES["materialize"]?.keys ?? [])].sort()).toEqual(
         [...GENERATOR_KEYS].sort(),
      );
   });

   it("registers #(preaggregate) with A1's keys: grain, storage, namespace", () => {
      expect(PUBLISHER_ROUTES["preaggregate"]).toMatchObject({
         payload: "tag",
      });
      expect(
         [...(PUBLISHER_ROUTES["preaggregate"]?.keys ?? [])].sort(),
      ).toEqual(["grain", "namespace", "storage"]);
      expect([...MATERIALIZE_V2_ROUTES].sort()).toEqual([
         "materialize",
         "preaggregate",
      ]);
   });

   it("refuses an unknown key on a v2 route, where v1's #@ only warns", () => {
      for (const [route, line, key, near] of [
         [
            "materialize",
            '#(materialize) nmae="t" storage="lake"',
            "nmae=",
            "name",
         ],
         ["preaggregate", '#(preaggregate) grian=["a"]', "grian=", "grain"],
      ] as const) {
         const { findings } = readRouteTag(annotationsOf([line]), route);
         expect(findings.map((f) => [f.code, f.tier])).toEqual([
            [`${route}-unknown-key`, "malformed"],
         ]);
         expect(findings[0]?.message).toContain(`\`${key}\``);
         expect(findings[0]?.message).toContain(`did you mean \`${near}=\`?`);
         // Named as it is written, `#(route)`.
         expect(findings[0]?.message).toContain(`\`#(${route})\` reads`);
      }
      expect(
         readRouteTag(
            annotationsOf(['#(materialize) name="t" inline=["c"]']),
            "materialize",
         ).findings,
      ).toEqual([]);
   });

   it("does not read a v2 route glued to its payload, which Malloy routes nowhere", () => {
      expect(publisherRouteOf('#(materialize)name="t"')).toBeUndefined();
      expect(publisherRouteOf('#(preaggregate)grain=["a"]')).toBeUndefined();
      expect(publisherRouteOf('#(materialize) name="t"')).toBe("materialize");
      // The routes that were always read glued still are.
      expect(publisherRouteOf('#(carries)["none"]')).toBe("carries");
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

describe("marksAgentHidden", () => {
   // The matchers the catalog, the plan and the source extraction each wrote
   // by hand before the one reader; the reader must hide everything they hid.
   const OLD_SOURCE = /^#\|?\s*[([{<]?\s*agent-hidden\b/;
   const OLD_FILE = /^##\|?\s*[([{<]?\s*agent-hidden\b/;
   const notes = [
      "#(agent-hidden)",
      "#[agent-hidden]",
      "#<agent-hidden>",
      "#{agent-hidden}",
      "#|(agent-hidden)\n",
      "#|(agent-hidden) because\n",
      "#(agent-hidden)x",
      '#(agent-hidden)["x"]',
      "#( agent-hidden )",
      "#agent-hidden",
      "# agent-hidden",
      "# (agent-hidden)",
      "#(agent-hidden-v2)",
      "#(Agent-Hidden)",
      "#(agent_hidden)",
      "#(agenthidden)",
      "#(doc) agent-hidden",
      "# hidden",
      "  #(agent-hidden)",
      "",
   ];
   const all = [...notes, ...notes.map((n) => (n ? `#${n}` : n))];

   it("hides exactly what the hand-written matchers hid, at each level", () => {
      for (const text of all) {
         expect([text, marksAgentHidden(text, "source")]).toEqual([
            text,
            OLD_SOURCE.test(text),
         ]);
         expect([text, marksAgentHidden(text, "file")]).toEqual([
            text,
            OLD_FILE.test(text),
         ]);
      }
   });

   it("reads a block note, a glued prefix and a malformed prefix", () => {
      expect(marksAgentHidden("#|(agent-hidden)\n", "source")).toBe(true);
      expect(marksAgentHidden("##|(agent-hidden)\n", "file")).toBe(true);
      expect(marksAgentHidden("#(agent-hidden)x", "source")).toBe(true);
      expect(marksAgentHidden("#( agent-hidden )", "source")).toBe(true);
      expect(marksAgentHidden("##agent-hidden", "file")).toBe(true);
   });

   it("keeps the levels apart", () => {
      expect(marksAgentHidden("##(agent-hidden)", "source")).toBe(false);
      expect(marksAgentHidden("#(agent-hidden)", "file")).toBe(false);
   });
});

describe("index notes", () => {
   it("reads the index shapes, parenthesis-only, at the object level", () => {
      for (const text of [
         "#(index)",
         "#(index_values) n=200",
         "#( index_values n=200 )",
         "# (index) n=1",
         "#(index)n=1",
      ]) {
         expect([text, isIndexNote(text)]).toEqual([text, true]);
      }
      for (const text of [
         "#|(index)\n",
         "#[index]",
         "##(index)",
         "#(indexer)",
         "#(doc) index",
      ]) {
         expect([text, isIndexNote(text)]).toEqual([text, false]);
      }
   });

   it("takes the payload Malloy routes, or the one inside, glued to or after the parens", () => {
      expect(indexNotePayload("#(index) n=200")).toBe("n=200");
      expect(indexNotePayload("#(index_values) n=200 refresh=full")).toBe(
         "n=200 refresh=full",
      );
      expect(indexNotePayload("#( index_values n=200 )")).toBe("n=200");
      expect(indexNotePayload("#(index n=5) schedule=x")).toBe(
         "n=5 schedule=x",
      );
      expect(indexNotePayload("#(index)n=1")).toBe("n=1");
      expect(indexNotePayload("# (index) n=1")).toBe("n=1");
      expect(indexNotePayload("#(index")).toBeUndefined();
      expect(indexNotePayload("#|(index) n=1")).toBeUndefined();
      expect(indexNotePayload("#(doc) hello")).toBeUndefined();
   });
});

describe("tagPayloadKeys", () => {
   it("reads top-level keys as a tag, folded to lower case", () => {
      expect(tagPayloadKeys('n=200 Sharing=x freshness.window="1h"')).toEqual([
         "n",
         "sharing",
         "freshness",
      ]);
      expect(tagPayloadKeys("a { sharing=x }")).toEqual(["a"]);
      expect(tagPayloadKeys("sharing=x -sharing")).toEqual([]);
      expect(tagPayloadKeys("")).toEqual([]);
   });

   it("never hydrates an @env. value into the result", () => {
      expect(tagPayloadKeys("sharing=@env.HOME")).toEqual(["sharing"]);
   });

   it("reads an unparseable payload word by word", () => {
      expect(tagPayloadKeys("sharing=private n=")).toEqual(["sharing", "n"]);
      expect(tagPayloadKeys('"sharing=x"')).toEqual([]);
   });

   it("names the key of a word that does not parse on its own", () => {
      expect(tagPayloadKeys("sharing=")).toEqual(["sharing"]);
      expect(tagPayloadKeys('n=1 Sharing="a b" m=')).toEqual([
         "n",
         "sharing",
         "m",
      ]);
      expect(tagPayloadKeys("n=(1 sharing=x)")).toEqual(["n", "sharing"]);
      expect(tagPayloadKeys("'sharing'=x")).toEqual(["sharing"]);
      expect(tagPayloadKeys("@env.X= n=")).toEqual(["n"]);
   });
});
