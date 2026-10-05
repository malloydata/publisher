// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stored source summaries in a get_context answer: `source_info.one_line_summary`
 * on every ranked card that has one, `source_info.summary` only when the request
 * is narrowed to that source, and the full summary in the source match prompt.
 * The whole path runs: the sync writes the summaries (scripted chat), the
 * request reads them.
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
import { _clearChatModelForTests } from "../../providers/active";
import { shopPackage } from "../../test_helpers/get_context_llm_fixture";
import { summaryViewFor } from "./get_context_tool";
import { summaryKey } from "./source_summaries";
import {
   callPayload,
   keywordReply,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat as installChat,
   type ScriptedChat,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
import type { PackageRetrievalSettings } from "../../service/package_retrieval";

let h: SemanticHarness;
beforeAll(async () => {
   h = await semanticHarness();
});
afterAll(async () => {
   await h.close();
});
beforeEach(async () => {
   h.reset();
   await h.db.run("DELETE FROM source_summaries");
});
afterEach(() => _clearChatModelForTests());

interface Card {
   source_info: {
      resource_id: { source: string };
      one_line_summary?: string;
      summary?: string;
      docs?: string;
   };
   entities?: unknown[];
}

const target = (target_type: string, search_text?: string) => ({
   target_type,
   ...(search_text === undefined ? {} : { search_text }),
});

/**
 * Summaries on, and refine and rerank set so a field search keeps every source
 * it found (rerank would otherwise cut the cards down to one).
 */
const ON: Partial<PackageRetrievalSettings> = {
   sourceSummary: { enabled: true },
   refine: { enabled: "auto", minLevel: "LOW" },
   rerank: { enabled: false, topSources: 8 },
};

/**
 * Wait for the sync that writes the summaries. A request with only source
 * targets does not wait for the index, so every request here is preceded by a
 * field search, which does.
 */
async function warm(
   handler: Parameters<typeof untilSemantic>[0],
   scopes: unknown,
) {
   await untilSemantic(handler, {
      search_targets: [target("dimension", "state")],
      scopes,
   });
}

interface Asked {
   chat: ScriptedChat;
   cards: Card[];
   byName: Record<string, Card["source_info"]>;
   payload: Record<string, unknown>;
}

async function ask(
   slug: string,
   params: Record<string, unknown>,
   options: {
      retrieval?: Partial<PackageRetrievalSettings>;
      reply?: (prompt: string) => string;
   } = {},
): Promise<Asked> {
   const chat = scriptedChat(options.reply ?? keywordReply);
   installChat(chat.model, { concurrency: 1 });
   const handler = h.handlerFor(shopPackage(options.retrieval ?? ON));
   await warm(handler, [{ environment: "llm", package: slug }]);
   chat.prompts.length = 0;
   const { payload } = await callPayload(handler, {
      ...params,
      scopes: [
         {
            environment: "llm",
            package: slug,
            ...(params.scopes as Record<string, unknown>[] | undefined)?.[0],
         },
      ],
   });
   const cards = (payload.sources ?? []) as Card[];
   return {
      chat,
      cards,
      payload,
      byName: Object.fromEntries(
         cards.map((c) => [c.source_info.resource_id.source, c.source_info]),
      ),
   };
}

const isSourceMatchPrompt = (p: string) => p.includes("Source search phrase:");

describe("source_info summary fields", () => {
   it("an entity search carries the one-liner on every card and no full summary", async () => {
      const { cards, byName } = await ask("entity-search", {
         search_targets: [target("dimension", "state the order ships to")],
      });
      expect(cards.length).toBeGreaterThan(1);
      expect(byName.orders.one_line_summary).toBe(
         "One row per customer order.",
      );
      expect(byName.customers.one_line_summary).toBe("One row per customer.");
      for (const card of cards)
         expect(card.source_info.summary).toBeUndefined();
   });

   it("the stored one-liner replaces the doc's first line, and key order is stable", async () => {
      const { cards } = await ask("order", {
         search_targets: [target("dimension", "state the order ships to")],
      });
      const keys = Object.keys(cards[0].source_info);
      expect(keys.indexOf("one_line_summary")).toBeGreaterThan(
         keys.indexOf("resource_id"),
      );
   });

   it("a scope that pins the source carries the full summary", async () => {
      const { cards } = await ask("pinned", {
         search_targets: [target("dimension", "state the order ships to")],
         scopes: [{ source: "customers" }],
      });
      expect(cards).toHaveLength(1);
      expect(cards[0].source_info.summary).toContain("`region`");
      expect(cards[0].source_info.one_line_summary).toBe(
         "One row per customer.",
      );
   });

   it("a source search that matched exactly one source carries its full summary", async () => {
      const { cards, byName } = await ask("one-match", {
         search_targets: [target("source", "one row per shipment")],
      });
      expect(Object.keys(byName)).toEqual(["shipments"]);
      expect(cards[0].source_info.summary).toContain("`ship_state`");
   });

   it("a source search that matched several sources carries the one-liners only", async () => {
      const { cards } = await ask("many-match", {
         search_targets: [target("source", "one row per order customer")],
      });
      expect(cards.length).toBeGreaterThan(1);
      for (const card of cards) {
         expect(card.source_info.one_line_summary).toBeDefined();
         expect(card.source_info.summary).toBeUndefined();
      }
   });

   it("in a mixed request only the one matched source gets the summary", async () => {
      const { byName } = await ask("mixed", {
         search_targets: [
            target("source", "one row per shipment"),
            target("dimension", "state the order ships to"),
         ],
      });
      expect(byName.shipments.summary).toContain("`carrier`");
      expect(byName.orders?.summary).toBeUndefined();
      expect(byName.orders?.one_line_summary).toBeDefined();
   });

   it("a source with no stored summary keeps its card as it was", async () => {
      // The call limit leaves all but one source without a summary. The model's
      // one-liners are marked so they cannot be mistaken for the doc's.
      const marked = (prompt: string) => {
         if (!prompt.includes("Source name: ")) return keywordReply(prompt);
         const reply = JSON.parse(keywordReply(prompt));
         return JSON.stringify({
            ...reply,
            one_line_summary: `LLM: ${reply.one_line_summary}`,
         });
      };
      const chat = scriptedChat(marked);
      installChat(chat.model, { concurrency: 1, maxCallsPerSync: 1 });
      const handler = h.handlerFor(shopPackage(ON));
      const { payload } = await untilSemantic(handler, {
         search_targets: [target("dimension", "state")],
         scopes: [{ environment: "llm", package: "partial" }],
      });
      const cards = payload.sources as Card[];
      const rows = await h.db.all<{ source_name: string }>(
         "SELECT source_name FROM source_summaries",
      );
      expect(rows).toHaveLength(1);
      expect(cards.length).toBeGreaterThan(1);
      for (const card of cards) {
         const { one_line_summary, docs, resource_id } = card.source_info;
         if (resource_id.source === rows[0].source_name) {
            expect(one_line_summary).toStartWith("LLM: ");
         } else {
            expect(one_line_summary).toBe(docs?.split("\n")[0]);
         }
      }
   });

   it("does not show a stored summary that no longer matches its source", async () => {
      // A reload changed the sources and the sync has not rewritten their
      // summaries yet. A request with only source targets does not wait for the
      // index, so it reads whatever is stored; each row is stamped with the hash
      // of the source as it was, which is not the source now.
      const marked = (prompt: string) => {
         if (!prompt.includes("Source name: ")) return keywordReply(prompt);
         const reply = JSON.parse(keywordReply(prompt));
         return JSON.stringify({
            summary: `STORED-BEFORE-THE-EDIT ${reply.summary}`,
            one_line_summary: `LLM: ${reply.one_line_summary}`,
         });
      };
      const chat = scriptedChat(marked);
      installChat(chat.model, { concurrency: 1 });
      const handler = h.handlerFor(shopPackage(ON));
      const scopes = [{ environment: "llm", package: "stale-read" }];
      await warm(handler, scopes);
      const written = await h.db.all("SELECT 1 FROM source_summaries");
      expect(written.length).toBeGreaterThan(0);
      await h.db.run(
         "UPDATE source_summaries SET input_hash = 'the-source-before-the-edit'",
      );

      chat.prompts.length = 0;
      const { payload } = await callPayload(handler, {
         search_targets: [target("source", "one row per shipment")],
         scopes,
      });
      const cards = payload.sources as Card[];
      expect(cards.length).toBeGreaterThan(0);
      for (const card of cards) {
         expect(card.source_info.summary).toBeUndefined();
         expect(card.source_info.one_line_summary).not.toStartWith("LLM: ");
      }
      // Nor does the model that picks sources see it.
      for (const prompt of chat.prompts.filter(isSourceMatchPrompt)) {
         expect(prompt).not.toContain("STORED-BEFORE-THE-EDIT");
      }
   });

   it("shows the stored summary again once it matches", async () => {
      const marked = (prompt: string) => {
         if (!prompt.includes("Source name: ")) return keywordReply(prompt);
         const reply = JSON.parse(keywordReply(prompt));
         return JSON.stringify({
            ...reply,
            one_line_summary: `LLM: ${reply.one_line_summary}`,
         });
      };
      const chat = scriptedChat(marked);
      installChat(chat.model, { concurrency: 1 });
      const handler = h.handlerFor(shopPackage(ON));
      const scopes = [{ environment: "llm", package: "fresh-read" }];
      await warm(handler, scopes);
      const { payload } = await callPayload(handler, {
         search_targets: [target("source", "one row per shipment")],
         scopes,
      });
      const cards = payload.sources as Card[];
      expect(cards.length).toBeGreaterThan(0);
      for (const card of cards) {
         expect(card.source_info.one_line_summary).toStartWith("LLM: ");
      }
   });
});

describe("a source defined in two files", () => {
   const stored = new Map([
      [summaryKey("a.malloy", "orders"), { summary: "A", oneLineSummary: "a" }],
      [summaryKey("b.malloy", "orders"), { summary: "B", oneLineSummary: "b" }],
   ]);
   const card = (modelPath: string, withSourceRow: boolean) => ({
      key: summaryKey(modelPath, "orders"),
      source: "orders",
      rows: withSourceRow
         ? [{ kind: "source" } as unknown as { kind: string }]
         : [],
   });

   it("a pinned source name opens the full summary on both files' cards", () => {
      const view = summaryViewFor(stored, { sourceName: "orders" }, [
         card("a.malloy", false),
         card("b.malloy", false),
      ] as never);
      expect([...(view?.full ?? [])].sort()).toEqual([
         summaryKey("a.malloy", "orders"),
         summaryKey("b.malloy", "orders"),
      ]);
   });

   it("a card is looked up by file and name, so each file shows its own", () => {
      const view = summaryViewFor(stored, {}, [
         card("a.malloy", true),
         card("b.malloy", true),
      ] as never);
      expect(view?.stored.get(summaryKey("a.malloy", "orders"))?.summary).toBe(
         "A",
      );
      expect(view?.stored.get(summaryKey("b.malloy", "orders"))?.summary).toBe(
         "B",
      );
   });
});

describe("with summaries off", () => {
   it("the card is the one it always was: the doc's first line, and no summary key", async () => {
      const { cards, chat } = await ask(
         "off",
         { search_targets: [target("dimension", "state the order ships to")] },
         { retrieval: { sourceSummary: { enabled: false } } },
      );
      expect(chat.prompts.some((p) => p.includes("Source name: "))).toBe(false);
      for (const card of cards) {
         expect(card.source_info).not.toHaveProperty("summary");
         expect(card.source_info.one_line_summary).toBe(
            card.source_info.docs?.split("\n")[0],
         );
      }
   });
});

describe("turning summaries off after they were written", () => {
   it("stops showing the stored ones: the table is not read for a package that has them off", async () => {
      const params = {
         search_targets: [target("dimension", "state the order ships to")],
         scopes: [{ source: "customers" }],
      };
      const on = await ask("toggle", params);
      expect(on.cards[0].source_info.summary).toBeDefined();
      const rows = await h.db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM source_summaries",
      );
      expect(rows[0].n).toBeGreaterThan(0);

      const off = await ask("toggle", params, {
         retrieval: { ...ON, sourceSummary: { enabled: false } },
      });
      const info = off.cards[0].source_info;
      expect(info).not.toHaveProperty("summary");
      expect(info.one_line_summary).toBe(info.docs?.split("\n")[0]);
   });
});

describe("the source match prompt", () => {
   const longSummary = (name: string) =>
      `Long summary of ${name}. ${"It covers the grain and the main fields in detail. ".repeat(30)}END-OF-${name}`;
   const reply = (prompt: string) => {
      if (prompt.includes("Source name: ")) {
         const name = /^Source name: (.*)$/m.exec(prompt)![1];
         return JSON.stringify({
            summary: longSummary(name),
            one_line_summary: `The \`${name}\` source.`.replace(
               /`(orders|customers|shipments)`/,
               "`$1`",
            ),
         });
      }
      return keywordReply(prompt);
   };

   it("shows each candidate's whole summary after its documentation", async () => {
      const { chat } = await ask(
         "match-prompt",
         { search_targets: [target("source", "one row per shipment")] },
         { reply },
      );
      const prompts = chat.prompts.filter(isSourceMatchPrompt);
      expect(prompts.length).toBeGreaterThan(0);
      const prompt = prompts[0];
      for (const name of ["orders", "customers", "shipments"]) {
         // Not cut: the end marker, 1,500+ characters in, is there.
         expect(prompt).toContain(`END-OF-${name}`);
      }
      const block = prompt
         .split("\n\n")
         .find((b) => b.includes("/shipping.malloy/shipments"));
      expect(block).toMatch(
         /Documentation: One row per shipment leaving a warehouse\.\nSummary: Long summary of shipments\./,
      );
   });

   it("keeps the documentation cut to 500 characters while the summary is whole", async () => {
      const longDoc = `${"Doc sentence that goes on. ".repeat(40)}DOC-TAIL`;
      const models = {
         "wide.malloy": {
            getSourceInfos: () => [
               {
                  name: "wide",
                  annotations: [`#(doc) ${longDoc}`],
                  schema: {
                     fields: [
                        { kind: "dimension", name: "state", annotations: [] },
                     ],
                  },
               },
            ],
            getQueries: () => [],
         },
      } as Record<string, unknown>;
      const pkg = {
         listModels: async () => [{ path: "wide.malloy" }],
         getModel: (p: string) => models[p],
         getRetrievalSettings: () => ({
            representation: "single",
            keyphrases: "never",
            ...ON,
            prompts: {},
         }),
      };
      const chat = scriptedChat(reply);
      installChat(chat.model, { concurrency: 1 });
      const handler = h.handlerFor(pkg);
      const scopes = [{ environment: "llm", package: "doc-cut" }];
      await warm(handler, scopes);
      chat.prompts.length = 0;
      await callPayload(handler, {
         search_targets: [target("source", "wide")],
         scopes,
      });
      const prompt = chat.prompts.find(isSourceMatchPrompt) as string;
      expect(prompt).not.toContain("DOC-TAIL");
      expect(prompt).toMatch(/Documentation: [^\n]{500}\.\.\.\nSummary: /);
      expect(prompt).toContain("END-OF-wide");
   });

   it("is unchanged for a source with no stored summary", async () => {
      const { chat } = await ask(
         "match-prompt-off",
         { search_targets: [target("source", "one row per shipment")] },
         { reply, retrieval: { sourceSummary: { enabled: false } } },
      );
      const prompt = chat.prompts.find(isSourceMatchPrompt) as string;
      expect(prompt).toContain("Documentation: ");
      expect(prompt).not.toContain("Summary: ");
   });
});
