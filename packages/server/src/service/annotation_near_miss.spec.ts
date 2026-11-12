// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import {
   ANNOTATION_MALFORMED_ROUTE_CODE,
   ANNOTATION_NEAR_MISS_CODE,
   annotationNearMisses,
   authoredNotes,
} from "./annotation_near_miss";
import { collectAuthorizeNearMisses, misspelledGateRoute } from "./authorize";
import { nearMissLogMessages } from "./compile_source";

function scan(text: string) {
   return annotationNearMisses([{ text }]);
}

describe("annotationNearMisses", () => {
   const nearMisses: [string, string][] = [
      ['#(clasification) "phi.x"', "#(classification)"],
      ['#(classifications) "phi.x"', "#(classification)"],
      ["#(Doc) Revenue, net of refunds", "#(doc)"],
      ["#(docs) Revenue, net of refunds", "#(doc)"],
      ["#(markdowns) **bold**", "#(markdown)"],
      ['#(caries) ["none"]', "#(carries)"],
      ["# (doc) Revenue", "#(doc)"],
      ["#( doc ) Revenue", "#(doc)"],
      ["#doc Revenue", "#(doc)"],
      ['#@persist name="t"', "#@ persist"],
      ['#persist name="t"', "#@ persist"],
      ['#(classification "phi.x")', "#(classification)"],
      ['#@preaggregate grain="a"', "#@ preaggregate"],
      ['# persist name="t"', "#@ persist"],
      ['# preaggregate grain="a"', "#@ preaggregate"],
      ['#(persist) name="t"', "#@ persist"],
      ['#@ persits name="t"', "#@ persist"],
      ['#@ presist name="t"', "#@ persist"],
      ['#@ preagregate grain="a"', "#@ preaggregate"],
   ];
   for (const [text, meant] of nearMisses) {
      it(`reads \`${text}\` as an attempt at \`${meant}\``, () => {
         const [finding] = scan(text);
         expect(finding).toMatchObject({
            code: ANNOTATION_NEAR_MISS_CODE,
            tier: "unservable",
         });
         expect(finding?.message).toContain(`did you mean \`${meant}\`?`);
         expect(finding?.rewrite).toBe(`write \`${meant}\``);
      });
   }

   it("leaves a misspelled gate to the load's refusal, which sees it", () => {
      for (const [text, route] of [
         ["#(authorise) org_id = $ORG", "authorize"],
         ["#(autorize) org_id = $ORG", "authorize"],
         ["# (authorise) org_id = $ORG", "authorize"],
         ["#(acess_filter) org_id = $ORG", "access_filter"],
         ["#(access_filtr) org_id = $ORG", "access_filter"],
      ] as const) {
         expect(scan(text)).toEqual([]);
         expect(misspelledGateRoute(text)).toBe(route);
         expect(collectAuthorizeNearMisses([text], route)).toHaveLength(1);
      }
   });

   it("reports a capitalized directive, which Malloy never plans", () => {
      for (const text of [
         '#@ Persist name="t"',
         '#@ PERSIST name="t"',
         '# Persist name="t"',
      ]) {
         const [finding] = scan(text);
         expect(finding).toMatchObject({ code: ANNOTATION_NEAR_MISS_CODE });
         expect(finding?.message).toContain("`#@ persist`");
      }
      expect(scan('#@ persist name="t"')).toEqual([]);
   });

   it("does not read a word that contains a gate's name as a misspelled gate", () => {
      for (const text of [
         "#(authorized) not a gate",
         "#(reauthorize) x",
         "#(unauthorize) x",
         "#(xauthorize) x",
         "#(access_filters) x",
         "#(noaccessfilter) x",
         "#(authorize) org_id = $ORG",
         "#(access_filter) org_id = $ORG",
         "#(author) x",
         "#(acme/authorise) x",
      ]) {
         expect(misspelledGateRoute(text)).toBeUndefined();
      }
   });

   it("leaves the gate spellings the load refuses to that refusal", () => {
      for (const text of [
         "#(AUTHORIZE) org_id = $ORG",
         "# (authorize) org_id = $ORG",
         "#(access-filter) org_id = $ORG",
         "#( authorize ) org_id = $ORG",
      ]) {
         expect(scan(text)).toEqual([]);
      }
      // The refusal still sees each of them, through the shared matcher.
      expect(
         collectAuthorizeNearMisses(
            ["#(AUTHORIZE) org_id = $ORG"],
            "authorize",
         ),
      ).toEqual(["#(AUTHORIZE) org_id = $ORG"]);
   });

   it("reports a prefix Malloy cannot route", () => {
      const [finding] = scan("#percent");
      expect(finding).toMatchObject({
         code: ANNOTATION_MALFORMED_ROUTE_CODE,
         tier: "unservable",
      });
      expect(finding?.rewrite).toContain("`# …`");
   });

   it("reports nothing for the routes Publisher reads, glued prefixes included", () => {
      for (const text of [
         '#(classification) "phi.x"',
         "#(doc) Revenue",
         "#|(doc) Revenue\n|#",
         "#(markdown) **bold**",
         '#(carries) ["none"]',
         '#(carries)["none"]',
         '#"Orders',
         "#(secure)",
         "#(index) weight=2",
         "#(authorize) org_id = $ORG",
         '#@ persist name="t"',
         '#@ name="t" storage="lake"',
         '#@ preaggregate grain="a"',
         "##! experimental { persistence }",
      ]) {
         expect(scan(text)).toEqual([]);
      }
   });

   it("reports nothing for render tags or another app's routes", () => {
      for (const text of [
         "# bar_chart",
         "# percent",
         "# size=lg",
         '# label="Total"',
         "#(my_app) anything",
         "#(https://example.com/v1) anything",
         "#(v1.2.3) anything",
         "#(dashboard) anything",
         "#(authorized_by) a person",
      ]) {
         expect(scan(text)).toEqual([]);
      }
   });

   it("carries the note's subject and location", () => {
      const at = {
         url: "file:///m.malloy",
         range: {
            start: { line: 3, character: 0 },
            end: { line: 3, character: 10 },
         },
      };
      const [finding] = annotationNearMisses([
         { text: "#(Doc) x", subject: "orders.total", at },
      ]);
      expect(finding).toMatchObject({ subject: "orders.total", at });
   });
});

describe("authoredNotes", () => {
   const duckdb = new DuckDBConnection("duckdb");
   afterAll(async () => {
      await duckdb.close();
   });

   async function compile(files: Record<string, string>, entry: string) {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map(
               Object.entries(files).map(([name, text]) => [
                  `file:///${name}`,
                  text,
               ]),
            ),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      return (await runtime.loadModel(new URL(`file:///${entry}`)).getModel())
         ._modelDef;
   }

   it("reads every note the file wrote, once each, and none it imported", async () => {
      const modelDef = await compile(
         {
            "base.malloy": `#(Doc) the base
source: base is duckdb.sql("SELECT 1 AS a")
`,
            "m.malloy": `##(docs) the model
import "base.malloy"

#(clasification) "phi.x"
source: orders is base extend {
  #(Doc) a dimension
  dimension: b is a + 1
  #(Doc) a view
  view: by_b is { group_by: b }
}

source: copy is orders

#(Doc) a query
query: q is orders -> by_b
`,
         },
         "m.malloy",
      );
      const notes = authoredNotes(modelDef);
      expect(notes.map((n) => [n.subject, n.text.trim()])).toEqual([
         [undefined, "##(docs) the model"],
         ["q", "#(Doc) a query"],
         ["orders", '#(clasification) "phi.x"'],
         ["orders.b", "#(Doc) a dimension"],
         ["orders.by_b", "#(Doc) a view"],
      ]);
      const findings = annotationNearMisses(notes);
      expect(findings.map((f) => f.code)).toEqual(
         Array(5).fill(ANNOTATION_NEAR_MISS_CODE),
      );
      // The editor's warnings carry each finding's location.
      const messages = nearMissLogMessages(modelDef);
      expect(messages).toHaveLength(5);
      expect(messages.every((m) => m.severity === "warn" && m.at)).toBe(true);
   });
});
