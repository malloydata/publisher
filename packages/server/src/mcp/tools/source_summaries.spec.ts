// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Index-time source summaries: what the model is shown (the field list), what
 * a reply must look like, and when a stored summary is reused or rewritten.
 * The chat model is the real provider layer over a scripted reply, so the
 * single JSON repair and the retry behaviour are the production ones.
 */

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS,
   sourceSummaryPromptHash,
} from "../../prompts/source_summary";
import {
   scriptedChat,
   summaryReply,
   type ScriptedChat,
} from "../../test_helpers/get_context_llm_harness";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createSourceSummariesTable } from "../../storage/duckdb/schema";
import { KEY_SEPARATOR, type EmbeddableEntity } from "./embedding_index";
import {
   SOURCE_SUMMARY_JOIN_DEPTH,
   SOURCE_SUMMARY_MAX_CHARS,
   SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS,
   SOURCE_SUMMARY_MAX_FIELDS,
   SOURCE_SUMMARY_MAX_JOINED_SOURCES,
   SOURCE_SUMMARY_MAX_PROMPT_CHARS,
   SourceSummaryStageError,
   buildSourceSummaryInputs,
   countSummarizableSources,
   currentSummaryHashes,
   loadSourceSummaries,
   resolveSourceSummaries,
   sourceSummaryInputsDigest,
   summaryKey,
   validateSourceSummary,
   type SourceSummarySettings,
} from "./source_summaries";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "source-summaries-spec-"));
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
   await db.initialize();
   await createSourceSummariesTable(db);
});

afterAll(async () => {
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(async () => {
   await db.run("DELETE FROM source_summaries");
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const MODEL = "m.malloy";

const source = (name: string, doc = ""): EmbeddableEntity => ({
   kind: "source",
   name,
   source: name,
   modelPath: MODEL,
   embedDoc: doc,
});

const field = (
   kind: string,
   src: string,
   name: string,
   doc = "",
   extra: Partial<EmbeddableEntity> = {},
): EmbeddableEntity => ({
   kind,
   name,
   source: src,
   modelPath: MODEL,
   embedDoc: doc,
   ...extra,
});

/** orders joins customers; `bare` has no doc; `ghost` has no fields at all. */
function shop(): EmbeddableEntity[] {
   return [
      source("orders", "One row per order."),
      field("dimension", "orders", "state", "State the order ships to.", {
         dataType: "string",
      }),
      field("dimension", "orders", "id", "", { dataType: "number" }),
      field("measure", "orders", "total_revenue", "Sum of order revenue.", {
         dataType: "number",
      }),
      field("view", "orders", "by_month", "Orders per month."),
      field("join", "orders", "customer", "", {
         relationship: "many_to_one",
         joinTarget: "customers",
      }),
      source("customers", "One row per customer."),
      field("dimension", "customers", "region", "Sales region.", {
         dataType: "string",
      }),
      field("measure", "customers", "customer_count", "Distinct customers.", {
         dataType: "number",
      }),
      source("bare"),
      field("dimension", "bare", "code", "", { dataType: "string" }),
      source("ghost", "Has nothing in it."),
   ];
}

function settingsFor(
   chat: ScriptedChat,
   over: Partial<SourceSummarySettings> = {},
): SourceSummarySettings {
   return {
      chat: chat.model,
      modelId: "openai-compatible/scripted",
      instructions: DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS,
      promptHash: sourceSummaryPromptHash(DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS),
      concurrency: 1,
      maxCallsPerSync: 300,
      ...over,
   };
}

const resolve = (
   entities: EmbeddableEntity[],
   settings: SourceSummarySettings,
   extra: { callBudget?: number } = {},
) =>
   resolveSourceSummaries({
      db,
      environmentName: "env",
      packageName: "pkg",
      entities,
      settings,
      ...extra,
   });

/** The stored summaries by source name; for specs with one model file. */
const stored = async () =>
   new Map(
      [...(await loadSourceSummaries(db, "env", "pkg"))].map(([key, value]) => [
         key.split(KEY_SEPARATOR)[1],
         value,
      ]),
   );

/** Names of the sources a chat received prompts for, in order. */
const sourcesAsked = (chat: ScriptedChat) =>
   chat.prompts.map((p) => /^Source name: (.*)$/m.exec(p)?.[1]);

const promptFor = (name: string) =>
   buildSourceSummaryInputs(shop()).find((i) => i.source === name)?.prompt ??
   "";

// ---------------------------------------------------------------------------
// What the model is shown
// ---------------------------------------------------------------------------

describe("the field list", () => {
   it("groups fields as Dimensions, Measures, Views, Joins, each as `name (type): doc`", () => {
      const prompt = promptFor("orders");
      expect(prompt).toContain(
         [
            "Dimensions:",
            "- state (string): State the order ships to.",
            "- id (number)",
            "Measures:",
            "- total_revenue (number): Sum of order revenue.",
            "Views:",
            "- by_month: Orders per month.",
            "Joins:",
            "- customer (many_to_one, source customers)",
         ].join("\n"),
      );
   });

   it("nests each joined source under it with its own fields", () => {
      const prompt = promptFor("orders");
      expect(prompt).toContain(
         [
            "Joined source customers as customer (many_to_one):",
            "  Dimensions:",
            "  - region (string): Sales region.",
            "  Measures:",
            "  - customer_count (number): Distinct customers.",
         ].join("\n"),
      );
   });

   it("gives the source name, its doc, and the fields inside a data fence", () => {
      const prompt = promptFor("orders");
      expect(prompt).toContain("Source name: orders");
      expect(prompt).toContain("Source documentation:\nOne row per order.");
      expect(prompt).toContain("<fields>\nDimensions:");
      expect(prompt).toContain("</fields>");
      expect(prompt).toContain('"one_line_summary"');
   });

   it('says "No source docs." when the source has no #(doc) text', () => {
      expect(promptFor("bare")).toContain(
         "Source documentation:\nNo source docs.",
      );
   });

   it("skips a source with no fields of its own", () => {
      const names = buildSourceSummaryInputs(shop()).map((i) => i.source);
      expect(names).toEqual(["orders", "customers", "bare"]);
      expect(countSummarizableSources(shop())).toBe(3);
   });

   it("does not loop when two sources join each other", () => {
      const entities = [
         ...shop(),
         field("join", "customers", "orders", "", {
            relationship: "one_to_many",
            joinTarget: "orders",
         }),
      ];
      const prompt = buildSourceSummaryInputs(entities).find(
         (i) => i.source === "orders",
      )?.prompt as string;
      // customers is expanded under orders; orders is not expanded again inside it.
      expect(prompt.match(/Joined source/g)).toHaveLength(1);
      expect(prompt).toContain("Joined source customers as customer");
   });

   it(`follows joins ${SOURCE_SUMMARY_JOIN_DEPTH} deep and no further`, () => {
      const chain = ["s0", "s1", "s2", "s3", "s4", "s5"];
      const entities = chain.flatMap((name, i) => [
         source(name, `Source ${name}.`),
         field("dimension", name, `d_${name}`, "", { dataType: "string" }),
         ...(i < chain.length - 1
            ? [
                 field("join", name, `to_${chain[i + 1]}`, "", {
                    relationship: "many_to_one",
                    joinTarget: chain[i + 1],
                 }),
              ]
            : []),
      ]);
      const prompt = buildSourceSummaryInputs(entities)[0].prompt;
      expect(prompt).toContain("Joined source s1 as to_s1");
      expect(prompt).toContain("Joined source s2 as to_s1.to_s2");
      expect(prompt).toContain("Joined source s3 as to_s1.to_s2.to_s3");
      expect(prompt).not.toContain("Joined source s4");
   });

   it(`shows at most ${SOURCE_SUMMARY_MAX_JOINED_SOURCES} joined sources and says how many are left out`, () => {
      const total = SOURCE_SUMMARY_MAX_JOINED_SOURCES + 3;
      const entities = [
         source("hub", "The hub."),
         field("dimension", "hub", "id", "", { dataType: "number" }),
         ...Array.from({ length: total }, (_, i) => [
            source(`spoke${i}`, ""),
            field("dimension", `spoke${i}`, "x", "", { dataType: "string" }),
            field("join", "hub", `j${i}`, "", {
               relationship: "one_to_one",
               joinTarget: `spoke${i}`,
            }),
         ]).flat(),
      ];
      const prompt = buildSourceSummaryInputs(entities)[0].prompt;
      expect(prompt.match(/Joined source/g)).toHaveLength(
         SOURCE_SUMMARY_MAX_JOINED_SOURCES,
      );
      expect(prompt).toContain("(3 more joined sources are not shown.)");
   });

   it(`caps a source at ${SOURCE_SUMMARY_MAX_FIELDS} fields, spread over the groups, and says so`, () => {
      const entities = [
         source("wide", "A wide source."),
         ...Array.from({ length: 250 }, (_, i) =>
            field("dimension", "wide", `d${i}`, "", { dataType: "string" }),
         ),
         ...Array.from({ length: 10 }, (_, i) =>
            field("measure", "wide", `m${i}`, "", { dataType: "number" }),
         ),
         field("view", "wide", "overview", "The overview."),
      ];
      const prompt = buildSourceSummaryInputs(entities)[0].prompt;
      const lines = prompt.split("\n").filter((l) => l.startsWith("- "));
      expect(lines).toHaveLength(SOURCE_SUMMARY_MAX_FIELDS);
      // The small groups are kept whole; the big one gives up the room.
      expect(lines.filter((l) => l.startsWith("- m"))).toHaveLength(10);
      expect(prompt).toContain("- overview: The overview.");
      expect(lines.filter((l) => l.startsWith("- d"))).toHaveLength(189);
      expect(prompt).toContain(
         `(Showing ${SOURCE_SUMMARY_MAX_FIELDS} of 261 fields. The rest are not shown.)`,
      );
   });

   it("says nothing about truncation when every field fits", () => {
      expect(promptFor("orders")).not.toContain("Showing");
   });

   it("never sends an access predicate or code", () => {
      const entities = [
         source("locked", "Locked down.\n#(access_filter) tenant = 'SECRET'"),
         field("dimension", "locked", "tenant", "Tenant. #(authorize) SECRET", {
            dataType: "string",
            code: "SECRET-CODE",
         }),
      ];
      const prompt = buildSourceSummaryInputs(entities)[0].prompt;
      expect(prompt).toContain("Locked down.");
      expect(prompt).not.toContain("SECRET");
      expect(prompt).not.toContain("access_filter");
   });

   it("is the same text on every run, so its hash is stable", () => {
      expect(promptFor("orders")).toBe(promptFor("orders"));
   });
});

// ---------------------------------------------------------------------------
// What a reply must look like
// ---------------------------------------------------------------------------

describe("the reply validator", () => {
   const ok = {
      summary: "A summary that names `state`.",
      one_line_summary: "One row per order.",
   };
   const check = (value: unknown, hasDoc = true, name = "orders") =>
      validateSourceSummary(name, hasDoc)(value);

   it("accepts a summary and a one-liner and trims them", () => {
      expect(
         check({ summary: "  Text.  ", one_line_summary: " One line. " }),
      ).toEqual({ summary: "Text.", oneLineSummary: "One line." });
   });

   it("refuses anything that is not an object", () => {
      for (const bad of [null, "text", 3, [ok]]) {
         expect(() => check(bad)).toThrow("expected a JSON object");
      }
   });

   it("names every missing or empty key", () => {
      expect(() => check({})).toThrow(
         '"summary" is missing or is not a non-empty string; "one_line_summary" is missing or is not a non-empty string',
      );
      expect(() => check({ ...ok, summary: "   " })).toThrow('"summary"');
      expect(() => check({ ...ok, one_line_summary: 5 })).toThrow(
         '"one_line_summary"',
      );
   });

   it("refuses a one-liner over 120 characters and says how long it was", () => {
      expect(() => check({ ...ok, one_line_summary: "x".repeat(121) })).toThrow(
         '"one_line_summary" is 121 characters; the limit is 120',
      );
      expect(() =>
         check({ ...ok, one_line_summary: "x".repeat(120) }),
      ).not.toThrow();
   });

   it("refuses a one-liner that spans lines", () => {
      expect(() => check({ ...ok, one_line_summary: "a\nb" })).toThrow(
         '"one_line_summary" must be one line',
      );
   });

   it("refuses a summary over the limit", () => {
      expect(() =>
         check({ ...ok, summary: "x".repeat(SOURCE_SUMMARY_MAX_CHARS + 1) }),
      ).toThrow(`the limit is ${SOURCE_SUMMARY_MAX_CHARS}`);
   });

   it("writes exactly The `<name>` source. for a source with no docs, whatever the model said", () => {
      for (const said of [
         "The `bare` source.",
         "Codes for things.",
         "The bare source.",
         "The `other` source.",
         "two\nlines",
         "x".repeat(300),
         "",
         undefined,
      ]) {
         const reply =
            said === undefined
               ? { summary: ok.summary }
               : { ...ok, one_line_summary: said };
         expect(check(reply, false, "bare").oneLineSummary).toBe(
            "The `bare` source.",
         );
      }
   });

   it("still refuses a missing summary for a source with no docs", () => {
      expect(() => check({ one_line_summary: "x" }, false, "bare")).toThrow(
         '"summary" is missing',
      );
   });

   it("lets a documented source use any one-liner, including that one", () => {
      expect(() =>
         check({ ...ok, one_line_summary: "The `orders` source." }),
      ).not.toThrow();
   });

   it("reports every problem at once, for the model's single re-ask", () => {
      expect(() =>
         check({ summary: "", one_line_summary: "x".repeat(130) }),
      ).toThrow(/"summary".*"one_line_summary" is 130/);
   });
});

// ---------------------------------------------------------------------------
// Generation and storage
// ---------------------------------------------------------------------------

describe("generating summaries", () => {
   afterEach(() => undefined);

   it("writes one summary per source, one call each, and skips a source with no fields", async () => {
      const chat = scriptedChat(summaryReply);
      const out = await resolve(shop(), settingsFor(chat));
      expect(sourcesAsked(chat)).toEqual(["orders", "customers", "bare"]);
      expect(out.calls).toBe(3);
      expect(out.progress).toEqual({ done: 3, total: 3, capped: false });
      const rows = await stored();
      expect([...rows.keys()].sort()).toEqual(["bare", "customers", "orders"]);
      expect(rows.get("bare")?.oneLineSummary).toBe("The `bare` source.");
      expect(rows.get("orders")?.oneLineSummary).toBe("One row per order.");
      expect(rows.get("orders")?.summary).toContain("`total_revenue`");
   });

   it("an unchanged package makes zero calls on the next sync", async () => {
      const first = scriptedChat(summaryReply);
      await resolve(shop(), settingsFor(first));
      const second = scriptedChat(summaryReply);
      const out = await resolve(shop(), settingsFor(second));
      expect(second.prompts).toHaveLength(0);
      expect(out.calls).toBe(0);
      expect(out.progress).toEqual({ done: 3, total: 3, capped: false });
   });

   it("changing one source's doc rewrites exactly that source", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const changed = shop().map((e) =>
         e.kind === "source" && e.name === "bare"
            ? { ...e, embedDoc: "Now it has a doc." }
            : e,
      );
      const chat = scriptedChat(summaryReply);
      await resolve(changed, settingsFor(chat));
      expect(sourcesAsked(chat)).toEqual(["bare"]);
      expect((await stored()).get("bare")?.oneLineSummary).toBe(
         "Now it has a doc.",
      );
   });

   it("changing a field's doc rewrites the source that owns it and the ones that nest it", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const changed = shop().map((e) =>
         e.name === "region" ? { ...e, embedDoc: "Territory." } : e,
      );
      const chat = scriptedChat(summaryReply);
      await resolve(changed, settingsFor(chat));
      // customers owns `region`; orders shows customers' fields under its join.
      expect(sourcesAsked(chat)).toEqual(["orders", "customers"]);
   });

   it("changing a field on a source nothing joins rewrites only that source", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const changed = [
         ...shop(),
         field("dimension", "bare", "label", "A label.", {
            dataType: "string",
         }),
      ];
      const chat = scriptedChat(summaryReply);
      await resolve(changed, settingsFor(chat));
      expect(sourcesAsked(chat)).toEqual(["bare"]);
   });

   it("changing only a field's type rewrites its source", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const changed = shop().map((e) =>
         e.name === "code" ? { ...e, dataType: "number" } : e,
      );
      const chat = scriptedChat(summaryReply);
      await resolve(changed, settingsFor(chat));
      expect(sourcesAsked(chat)).toEqual(["bare"]);
   });

   it("changing the prompt rewrites every summary", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const chat = scriptedChat(summaryReply);
      const instructions = `${DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS}\nWrite in French.`;
      await resolve(
         shop(),
         settingsFor(chat, {
            instructions,
            promptHash: sourceSummaryPromptHash(instructions),
         }),
      );
      expect(sourcesAsked(chat)).toEqual(["orders", "customers", "bare"]);
      expect(chat.prompts).toHaveLength(3);
   });

   it("sends the package's own instructions as the system text", async () => {
      const chat = scriptedChat(summaryReply);
      const spy: string[] = [];
      const wrapped = {
         ...chat.model,
         completeJson: ((req: { system?: string }) => {
            spy.push(req.system ?? "");
            return (chat.model.completeJson as (r: unknown) => unknown)(req);
         }) as typeof chat.model.completeJson,
      };
      await resolve(
         shop(),
         settingsFor(chat, {
            chat: wrapped,
            instructions: "Package instructions.",
            promptHash: sourceSummaryPromptHash("Package instructions."),
         }),
      );
      expect(spy).toHaveLength(3);
      expect(spy.every((s) => s === "Package instructions.")).toBe(true);
   });

   it("changing the model rewrites every summary", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const chat = scriptedChat(summaryReply);
      await resolve(
         shop(),
         settingsFor(chat, { modelId: "openai-compatible/other" }),
      );
      expect(chat.prompts).toHaveLength(3);
   });

   it("deletes the row of a source that left the package, or that lost all its fields", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const without = shop().filter(
         (e) => e.name !== "bare" && e.source !== "customers",
      );
      const chat = scriptedChat(summaryReply);
      await resolve(without, settingsFor(chat));
      // `orders` no longer has customers' fields under its join, so it is rewritten.
      expect(sourcesAsked(chat)).toEqual(["orders"]);
      expect([...(await stored()).keys()]).toEqual(["orders"]);
   });

   it("at the call limit, keeps what fit, leaves the rest without a summary, and serves nothing stale", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const changed = shop().map((e) =>
         e.kind === "source" && e.name !== "ghost"
            ? { ...e, embedDoc: `${e.embedDoc} Edited.` }
            : e,
      );
      const chat = scriptedChat(summaryReply);
      const out = await resolve(changed, settingsFor(chat), { callBudget: 1 });
      expect(chat.prompts).toHaveLength(1);
      expect(out.progress).toEqual({ done: 1, total: 3, capped: true });
      const rows = await stored();
      // The one rewritten source is current; the two it could not reach were
      // deleted instead of being left to answer with an out-of-date summary.
      expect([...rows.keys()]).toEqual(["orders"]);
      expect(rows.get("orders")?.oneLineSummary).toBe("One row per order.");
   });

   it("a call budget of 0 makes no call", async () => {
      const chat = scriptedChat(summaryReply);
      const out = await resolve(shop(), settingsFor(chat), { callBudget: 0 });
      expect(chat.prompts).toHaveLength(0);
      expect(out.progress.capped).toBe(true);
   });

   it("reports progress as sources are saved", async () => {
      const seen: number[] = [];
      await resolveSourceSummaries({
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: shop(),
         settings: settingsFor(scriptedChat(summaryReply)),
         onProgress: (p) => seen.push(p.done),
      });
      expect(seen).toEqual([0, 1, 2, 3]);
   });

   it("never has more calls in flight than the concurrency setting", async () => {
      const chat = scriptedChat(summaryReply, { delayMs: 15 });
      await resolve(shop(), settingsFor(chat, { concurrency: 2 }));
      expect(chat.maxInFlight()).toBe(2);
      const serial = scriptedChat(summaryReply, { delayMs: 5 });
      await db.run("DELETE FROM source_summaries");
      await resolve(shop(), settingsFor(serial, { concurrency: 1 }));
      expect(serial.maxInFlight()).toBe(1);
   });

   it("keeps each package's rows apart", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const chat = scriptedChat(summaryReply);
      await resolveSourceSummaries({
         db,
         environmentName: "env",
         packageName: "other",
         entities: shop(),
         settings: settingsFor(chat),
      });
      expect(chat.prompts).toHaveLength(3);
      expect((await stored()).size).toBe(3);
   });
});

describe("a model that gets it wrong", () => {
   it("is not re-asked about the one-liner of a source with no docs: the sync writes it", async () => {
      const chat = scriptedChat((prompt) => {
         if (/^Source name: bare$/m.test(prompt)) {
            return JSON.stringify({
               summary: "Holds codes.",
               one_line_summary: "Holds codes for things.",
            });
         }
         return summaryReply(prompt);
      });
      await resolve(shop(), settingsFor(chat));
      // One call per source, no repair.
      expect(chat.prompts).toHaveLength(3);
      expect((await stored()).get("bare")?.oneLineSummary).toBe(
         "The `bare` source.",
      );
   });

   it("fails the stage, naming it, when the reply is still wrong after the re-ask", async () => {
      const chat = scriptedChat((prompt) =>
         /^Source name: bare$/m.test(prompt)
            ? JSON.stringify({ summary: "", one_line_summary: "Wrong." })
            : summaryReply(prompt),
      );
      const error = (await resolve(shop(), settingsFor(chat)).catch(
         (e: unknown) => e,
      )) as SourceSummaryStageError;
      expect(error).toBeInstanceOf(SourceSummaryStageError);
      expect(error.stage).toBe("source_summary");
      expect(error.message).toContain(
         "Source summary generation failed after 2 of 3 sources",
      );
      expect(error.message).toContain('"summary" is missing');
      // What was saved before the failure stays.
      expect([...(await stored()).keys()].sort()).toEqual([
         "customers",
         "orders",
      ]);
   });

   it("fails the stage when the model call itself keeps failing, and a retry resumes", async () => {
      let down = true;
      const chat = scriptedChat((prompt) => {
         if (down && /^Source name: customers$/m.test(prompt)) {
            throw new Error("the LLM is down");
         }
         return summaryReply(prompt);
      });
      const error = (await resolve(shop(), settingsFor(chat)).catch(
         (e: unknown) => e,
      )) as SourceSummaryStageError;
      expect(error).toBeInstanceOf(SourceSummaryStageError);
      expect(error.message).toContain("the LLM is down");
      expect(error.message).toContain("after 1 of 3 sources");
      expect([...(await stored()).keys()]).toEqual(["orders"]);

      down = false;
      const retry = scriptedChat(summaryReply);
      const out = await resolve(shop(), settingsFor(retry));
      // Only the two that were not saved are asked for.
      expect(sourcesAsked(retry)).toEqual(["customers", "bare"]);
      expect(out.progress).toEqual({ done: 3, total: 3, capped: false });
   });
});

// ---------------------------------------------------------------------------
// The readiness fingerprint's share
// ---------------------------------------------------------------------------

describe("sourceSummaryInputsDigest", () => {
   const settings = { promptHash: "p1", modelId: "m1" };

   it("is empty when summaries are off", () => {
      expect(sourceSummaryInputsDigest(shop(), undefined)).toBe("");
   });

   it("is stable for the same inputs", () => {
      expect(sourceSummaryInputsDigest(shop(), settings)).toBe(
         sourceSummaryInputsDigest(shop(), settings),
      );
   });

   it("moves when a field type, a join target, the prompt or the model changes", () => {
      const base = sourceSummaryInputsDigest(shop(), settings);
      const retyped = shop().map((e) =>
         e.name === "id" ? { ...e, dataType: "string" } : e,
      );
      const retargeted = shop().map((e) =>
         e.name === "customer" ? { ...e, joinTarget: "bare" } : e,
      );
      expect(sourceSummaryInputsDigest(retyped, settings)).not.toBe(base);
      expect(sourceSummaryInputsDigest(retargeted, settings)).not.toBe(base);
      expect(
         sourceSummaryInputsDigest(shop(), { ...settings, promptHash: "p2" }),
      ).not.toBe(base);
      expect(
         sourceSummaryInputsDigest(shop(), { ...settings, modelId: "m2" }),
      ).not.toBe(base);
   });
});

// ---------------------------------------------------------------------------
// A source is identified by its file as well as its name
// ---------------------------------------------------------------------------

/**
 * Two model files that each define `orders`, and each a `cust` that `orders`
 * joins, with different documentation and different fields.
 */
function twoModels(): EmbeddableEntity[] {
   const inModel = (
      modelPath: string,
      e: Omit<EmbeddableEntity, "modelPath">,
   ): EmbeddableEntity => ({ ...e, modelPath });
   const retail = (e: Omit<EmbeddableEntity, "modelPath">) =>
      inModel("retail.malloy", e);
   const wholesale = (e: Omit<EmbeddableEntity, "modelPath">) =>
      inModel("wholesale.malloy", e);
   return [
      retail({
         kind: "source",
         name: "orders",
         source: "orders",
         embedDoc: "Retail orders.",
      }),
      retail({
         kind: "dimension",
         name: "state",
         source: "orders",
         embedDoc: "State shipped to.",
         dataType: "string",
      }),
      retail({
         kind: "join",
         name: "buyer",
         source: "orders",
         embedDoc: "",
         relationship: "one",
         joinTarget: "cust",
         joinTargetModelPath: "retail.malloy",
      }),
      retail({
         kind: "source",
         name: "cust",
         source: "cust",
         embedDoc: "Retail customers.",
      }),
      retail({
         kind: "dimension",
         name: "loyalty_tier",
         source: "cust",
         embedDoc: "Loyalty programme tier.",
         dataType: "string",
      }),
      wholesale({
         kind: "source",
         name: "orders",
         source: "orders",
         embedDoc: "Wholesale orders.",
      }),
      wholesale({
         kind: "dimension",
         name: "po_number",
         source: "orders",
         embedDoc: "Purchase order number.",
         dataType: "string",
      }),
      wholesale({
         kind: "join",
         name: "buyer",
         source: "orders",
         embedDoc: "",
         relationship: "one",
         joinTarget: "cust",
         joinTargetModelPath: "wholesale.malloy",
      }),
      wholesale({
         kind: "source",
         name: "cust",
         source: "cust",
         embedDoc: "Trade accounts.",
      }),
      wholesale({
         kind: "dimension",
         name: "account_manager",
         source: "cust",
         embedDoc: "Account manager.",
         dataType: "string",
      }),
   ];
}

describe("a source name defined in two model files", () => {
   const inputs = () => buildSourceSummaryInputs(twoModels());
   const promptOf = (modelPath: string, name: string) =>
      inputs().find((i) => i.modelPath === modelPath && i.source === name)
         ?.prompt ?? "";

   it("is summarized once per file, each from its own fields and docs", () => {
      expect(
         inputs()
            .map((i) => `${i.modelPath}:${i.source}`)
            .sort(),
      ).toEqual([
         "retail.malloy:cust",
         "retail.malloy:orders",
         "wholesale.malloy:cust",
         "wholesale.malloy:orders",
      ]);
      expect(promptOf("retail.malloy", "orders")).toContain("Retail orders.");
      expect(promptOf("retail.malloy", "orders")).toContain("state");
      expect(promptOf("retail.malloy", "orders")).not.toContain("po_number");
      expect(promptOf("wholesale.malloy", "orders")).toContain(
         "Wholesale orders.",
      );
      expect(promptOf("wholesale.malloy", "orders")).toContain("po_number");
   });

   it("nests the joined source from the file the join names, not the first file with that name", () => {
      const retail = promptOf("retail.malloy", "orders");
      const wholesale = promptOf("wholesale.malloy", "orders");
      expect(retail).toContain("loyalty_tier");
      expect(retail).not.toContain("account_manager");
      expect(wholesale).toContain("account_manager");
      expect(wholesale).not.toContain("loyalty_tier");
   });

   it("stores and reads a summary per file", async () => {
      const chat = scriptedChat(summaryReply);
      await resolve(twoModels(), settingsFor(chat));
      expect(chat.prompts).toHaveLength(4);
      const rows = await loadSourceSummaries(db, "env", "pkg");
      expect(rows.size).toBe(4);
      const retail = rows.get(summaryKey("retail.malloy", "orders"));
      const wholesale = rows.get(summaryKey("wholesale.malloy", "orders"));
      expect(retail?.oneLineSummary).toBe("Retail orders.");
      expect(wholesale?.oneLineSummary).toBe("Wholesale orders.");
      expect(retail?.summary).toContain("loyalty_tier");
      expect(wholesale?.summary).toContain("account_manager");
   });

   it("an unchanged package makes no call, and a change to one file rewrites only that file's source", async () => {
      await resolve(twoModels(), settingsFor(scriptedChat(summaryReply)));
      const quiet = scriptedChat(summaryReply);
      await resolve(twoModels(), settingsFor(quiet));
      expect(quiet.prompts).toHaveLength(0);

      const changed = twoModels().map((e) =>
         e.modelPath === "wholesale.malloy" && e.name === "po_number"
            ? { ...e, embedDoc: "The buyer's purchase order." }
            : e,
      );
      const chat = scriptedChat(summaryReply);
      await resolve(changed, settingsFor(chat));
      expect(chat.prompts).toHaveLength(1);
      expect(chat.prompts[0]).toContain("Wholesale orders.");
   });

   it("removing one file removes only its rows", async () => {
      await resolve(twoModels(), settingsFor(scriptedChat(summaryReply)));
      const onlyRetail = twoModels().filter(
         (e) => e.modelPath === "retail.malloy",
      );
      const chat = scriptedChat(summaryReply);
      await resolve(onlyRetail, settingsFor(chat));
      expect(chat.prompts).toHaveLength(0);
      const rows = await loadSourceSummaries(db, "env", "pkg");
      expect([...rows.keys()].sort()).toEqual([
         summaryKey("retail.malloy", "cust"),
         summaryKey("retail.malloy", "orders"),
      ]);
   });

   it("resolves a join with no file recorded only when the name is unambiguous", () => {
      const without = twoModels().map((e) => {
         const { joinTargetModelPath: _drop, ...rest } = e;
         return rest as EmbeddableEntity;
      });
      // `cust` exists in both files, so a bare name cannot say which: it is not
      // expanded, rather than guessed.
      const prompt =
         buildSourceSummaryInputs(without).find(
            (i) => i.modelPath === "retail.malloy" && i.source === "orders",
         )?.prompt ?? "";
      expect(prompt).not.toContain("loyalty_tier");
      expect(prompt).not.toContain("account_manager");
   });
});

// ---------------------------------------------------------------------------
// A stored summary is served only while it matches the inputs now
// ---------------------------------------------------------------------------

describe("reading stored summaries against the current inputs", () => {
   const settings = {
      promptHash: sourceSummaryPromptHash(DEFAULT_SOURCE_SUMMARY_INSTRUCTIONS),
      modelId: "openai-compatible/scripted",
   };

   it("serves a row whose input hash is the current one", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const rows = await loadSourceSummaries(
         db,
         "env",
         "pkg",
         currentSummaryHashes(shop(), settings),
      );
      expect(rows.size).toBe(3);
   });

   it("drops a row whose source changed since the sync, until the sync rewrites it", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      // A reload changed a field of `orders`; the sync has not run yet.
      const reloaded = shop().map((e) =>
         e.name === "state" ? { ...e, embedDoc: "Where it ships." } : e,
      );
      const rows = await loadSourceSummaries(
         db,
         "env",
         "pkg",
         currentSummaryHashes(reloaded, settings),
      );
      expect(rows.has(summaryKey(MODEL, "orders"))).toBe(false);
      // The sources the change did not reach are still served.
      expect(rows.has(summaryKey(MODEL, "bare"))).toBe(true);
   });

   it("drops every row when the prompt or the model changed", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      for (const changed of [
         { ...settings, promptHash: "another-prompt" },
         { ...settings, modelId: "another/model" },
      ]) {
         const rows = await loadSourceSummaries(
            db,
            "env",
            "pkg",
            currentSummaryHashes(shop(), changed),
         );
         expect(rows.size).toBe(0);
      }
   });

   it("drops the row of a source that is no longer summarized", async () => {
      await resolve(shop(), settingsFor(scriptedChat(summaryReply)));
      const without = shop().filter((e) => e.source !== "bare");
      const rows = await loadSourceSummaries(
         db,
         "env",
         "pkg",
         currentSummaryHashes(without, settings),
      );
      expect(rows.has(summaryKey(MODEL, "bare"))).toBe(false);
   });

   it("computes the current hashes once for one entity list and one setting", () => {
      const list = Object.freeze(shop());
      expect(currentSummaryHashes(list, settings)).toBe(
         currentSummaryHashes(list, settings),
      );
   });
});

// ---------------------------------------------------------------------------
// The prompt has a size
// ---------------------------------------------------------------------------

describe("the size of the prompt", () => {
   const big = (n: number) => "w".repeat(n);

   it("cuts one field's doc to a fixed length", () => {
      const entities = [
         source("wide", "Short."),
         field("dimension", "wide", "essay", big(5_000), {
            dataType: "string",
         }),
      ];
      const prompt = buildSourceSummaryInputs(entities)[0].prompt;
      const line = prompt.split("\n").find((l) => l.startsWith("- essay"));
      expect(line).toBeDefined();
      expect((line as string).length).toBeLessThan(
         SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS + 60,
      );
      expect(line).toContain("...");
   });

   it("cuts the source's own doc too", () => {
      const entities = [
         source("wide", big(9_000)),
         field("dimension", "wide", "one", "", { dataType: "string" }),
      ];
      const prompt = buildSourceSummaryInputs(entities)[0].prompt;
      expect(prompt.length).toBeLessThan(SOURCE_SUMMARY_MAX_PROMPT_CHARS);
      expect(prompt).not.toContain(
         big(SOURCE_SUMMARY_MAX_FIELD_DOC_CHARS + 50),
      );
   });

   it("keeps a source with 200 long fields and 20 joined sources under the total cap, and says so", () => {
      const entities: EmbeddableEntity[] = [source("hub", "The hub.")];
      for (let i = 0; i < SOURCE_SUMMARY_MAX_FIELDS; i++) {
         entities.push(
            field("dimension", "hub", `f${i}`, big(900), {
               dataType: "string",
            }),
         );
      }
      for (let j = 0; j < SOURCE_SUMMARY_MAX_JOINED_SOURCES; j++) {
         entities.push(
            field("join", "hub", `j${j}`, "", {
               relationship: "one",
               joinTarget: `t${j}`,
               joinTargetModelPath: MODEL,
            }),
            source(`t${j}`, "A joined source."),
         );
         for (let i = 0; i < SOURCE_SUMMARY_MAX_FIELDS; i++) {
            entities.push(
               field("dimension", `t${j}`, `g${i}`, big(900), {
                  dataType: "string",
               }),
            );
         }
      }
      const prompt = buildSourceSummaryInputs(entities).find(
         (i) => i.source === "hub",
      )?.prompt as string;
      expect(prompt.length).toBeLessThanOrEqual(
         SOURCE_SUMMARY_MAX_PROMPT_CHARS,
      );
      // What was left out is said, so the model does not take it for the whole.
      expect(prompt).toMatch(/not shown|Showing/);
      // The source's own fields come first and are what survives.
      expect(prompt).toContain("- f0 ");
   });
});
