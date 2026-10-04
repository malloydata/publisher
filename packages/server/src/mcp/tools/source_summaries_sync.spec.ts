// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Source summaries inside the embedding sync: they run after keyphrases and
 * before any vector is written, share the sync's call limit, are rewritten only
 * when what the model is shown changes, and fail the sync loudly with the stage
 * named.
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
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import {
   _clearChatModelForTests,
   _setChatModelForTests,
} from "../../providers/active";
import { setRetrievalConfig } from "../../retrieval_config";
import { EmbeddingProvider } from "../../service/embedding_provider";
import type { Package } from "../../service/package";
import type { PackageRetrievalSettings } from "../../service/package_retrieval";
import {
   scriptedChat,
   summaryReply,
   type ScriptedChat,
} from "../../test_helpers/get_context_llm_harness";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   createEntityEmbeddingsTable,
   createEntityKeyphrasesTable,
   createSourceSummariesTable,
} from "../../storage/duckdb/schema";
import {
   EmbeddableEntity,
   _clearProviderCooldownForTests,
   _resetEmbeddingIndexStateForTests,
   deletePackageEmbeddings,
   getEmbeddingIndexStatus,
   trySemanticSearch,
} from "./embedding_index";
import { embeddingSyncQueue } from "./embedding_sync_queue";
import { KEY_SEPARATOR } from "./embedding_index";
import { loadSourceSummaries } from "./source_summaries";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "summary-sync-spec-"));
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
   await db.initialize();
   await createEntityEmbeddingsTable(db);
   await createEntityKeyphrasesTable(db);
   await createSourceSummariesTable(db);
});

afterAll(async () => {
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(async () => {
   _resetEmbeddingIndexStateForTests();
   await db.run("DELETE FROM entity_embeddings");
   await db.run("DELETE FROM entity_keyphrases");
   await db.run("DELETE FROM source_summaries");
});

afterEach(async () => {
   await embeddingSyncQueue.idle();
   _clearChatModelForTests();
   setRetrievalConfig(undefined);
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const words = (count: number) =>
   Array.from({ length: count }, (_, i) => `word${i}`).join(" ");

const entity = (
   kind: string,
   name: string,
   src: string,
   embedDoc = "",
   extra: Partial<EmbeddableEntity> = {},
): EmbeddableEntity => ({
   kind,
   name,
   source: src,
   modelPath: "m.malloy",
   embedDoc,
   ...extra,
});

/** Two documented sources; one field has a long doc, so keyphrases need a call. */
function entities(
   over: (e: EmbeddableEntity) => EmbeddableEntity = (e) => e,
): readonly EmbeddableEntity[] {
   return Object.freeze(
      [
         entity("source", "orders", "orders", "One row per order."),
         entity("dimension", "state", "orders", "State.", {
            dataType: "string",
         }),
         entity("measure", "revenue", "orders", words(30), {
            dataType: "number",
         }),
         entity("source", "customers", "customers", "One row per customer."),
         entity("dimension", "region", "customers", "Region.", {
            dataType: "string",
         }),
         entity("source", "ghost", "ghost", "Nothing in it."),
      ].map(over),
   );
}

/** Answers keyphrase prompts with `kp <name>` and summary prompts with the stand-in summary. */
function chatFor(
   fail: (prompt: string) => boolean = () => false,
): ScriptedChat {
   return scriptedChat((prompt) => {
      if (fail(prompt)) throw new Error("the LLM is down");
      if (prompt.includes("<entities>")) {
         const block = /<entities>\n([\s\S]*?)\n<\/entities>/.exec(prompt);
         const sent = JSON.parse(block?.[1] ?? "[]") as {
            id: string;
            name: string;
         }[];
         return JSON.stringify(
            Object.fromEntries(
               sent.map((e) => [e.id, { keyphrase: `kp ${e.name}` }]),
            ),
         );
      }
      return summaryReply(prompt);
   });
}

const isSummary = (prompt: string) => prompt.includes("Source name: ");
const summaryPrompts = (chat: ScriptedChat) => chat.prompts.filter(isSummary);
const keyphrasePrompts = (chat: ScriptedChat) =>
   chat.prompts.filter((p) => p.includes("<entities>"));
const sourcesAsked = (chat: ScriptedChat) =>
   summaryPrompts(chat).map((p) => /^Source name: (.*)$/m.exec(p)?.[1]);

function recordingEmbedder() {
   const sent: string[] = [];
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      sent.push(...body.input);
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => ({
               index,
               embedding: [1, t.length % 5, 1],
            })),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
   return {
      sent,
      provider: new EmbeddingProvider(
         {
            apiKey: "k",
            model: "m",
            baseUrl: "https://stub.example.com/v1",
            minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
         },
         fetchStub,
      ),
   };
}

const pkgWith = (retrieval: Partial<PackageRetrievalSettings> = {}): Package =>
   ({
      getRetrievalSettings: () => ({
         representation: "single",
         keyphrases: "auto",
         prompts: {},
         ...retrieval,
      }),
   }) as unknown as Package;

async function search(
   provider: EmbeddingProvider,
   pkg: Package,
   ents: readonly EmbeddableEntity[] = entities(),
) {
   for (let i = 0; i < 400; i++) {
      const result = await trySemanticSearch({
         db,
         provider,
         pkg,
         environmentName: "env",
         packageName: "sync",
         entities: ents,
         queries: [{ targetIndex: 0, text: "find it", kinds: ["dimension"] }],
         perSourceWindow: 10,
      });
      if (!("unavailable" in result) || result.unavailable !== "indexing") {
         return result;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
   }
   throw new Error("sync never completed");
}

const status = (
   provider: EmbeddingProvider,
   pkg: Package,
   ents: readonly EmbeddableEntity[] = entities(),
) => getEmbeddingIndexStatus(db, provider, "env", "sync", ents, pkg);

/** The stored summaries by source name; every source here is in one file. */
const rows = async () =>
   new Map(
      [...(await loadSourceSummaries(db, "env", "sync"))].map(
         ([key, value]) => [key.split(KEY_SEPARATOR)[1], value],
      ),
   );

// ---------------------------------------------------------------------------

describe("source summaries in the sync", () => {
   it("runs after the keyphrases, before any vector, and reports its progress", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const { provider } = recordingEmbedder();
      const pkg = pkgWith();
      await search(provider, pkg);

      const order = chat.prompts.map((p) => (isSummary(p) ? "summary" : "kp"));
      expect(order).toEqual(["kp", "summary", "summary"]);
      expect(sourcesAsked(chat)).toEqual(["orders", "customers"]);
      expect([...(await rows()).keys()]).toEqual(["orders", "customers"]);

      const s = await status(provider, pkg);
      expect(s.status).toBe("ready");
      expect(s.sourceSummaryProgress).toEqual({
         done: 2,
         total: 2,
         capped: false,
      });
      // Keyphrases are unaffected.
      expect(s.keyphraseProgress).toEqual({ done: 1, total: 1, capped: false });
   });

   it("shows the total before the first sync has run", async () => {
      _setChatModelForTests(chatFor().model);
      const { provider } = recordingEmbedder();
      const s = await status(provider, pkgWith());
      expect(s.status).toBe("indexing");
      expect(s.sourceSummaryProgress).toEqual({ done: 0, total: 2 });
   });

   it("a restart makes zero summary calls, zero keyphrase calls and zero embedding calls", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await search(recordingEmbedder().provider, pkgWith());
      expect(chat.prompts).toHaveLength(3);

      // A restart forgets the in-memory sync record; the tables remain.
      _resetEmbeddingIndexStateForTests();
      const second = recordingEmbedder();
      await search(second.provider, pkgWith());
      expect(chat.prompts).toHaveLength(3);
      expect(second.sent).toEqual(["find it"]);
   });

   it("a republish with the same content (a new package instance) makes no call", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await search(recordingEmbedder().provider, pkgWith());
      const calls = chat.prompts.length;
      await search(recordingEmbedder().provider, pkgWith());
      expect(chat.prompts).toHaveLength(calls);
   });

   it("editing one source's doc rewrites that source only", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await search(recordingEmbedder().provider, pkgWith());
      const before = chat.prompts.length;

      const edited = entities((e) =>
         e.kind === "source" && e.name === "customers"
            ? { ...e, embedDoc: "One row per customer account." }
            : e,
      );
      await search(recordingEmbedder().provider, pkgWith(), edited);
      expect(chat.prompts.slice(before).filter(isSummary)).toHaveLength(1);
      expect(sourcesAsked(chat).slice(2)).toEqual(["customers"]);
      expect((await rows()).get("customers")?.oneLineSummary).toBe(
         "One row per customer account.",
      );
   });

   it("a change only the model is shown (a field's type) moves the package back to indexing and rewrites its source", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const pkg = pkgWith({ keyphrases: "never" });
      const { provider } = recordingEmbedder();
      await search(provider, pkg);
      expect((await status(provider, pkg)).status).toBe("ready");
      const before = chat.prompts.length;

      // The type is in neither row text, so only the summary's inputs notice it.
      const retyped = entities((e) =>
         e.name === "region" ? { ...e, dataType: "number" } : e,
      );
      expect((await status(provider, pkg, retyped)).status).toBe("indexing");
      await search(provider, pkg, retyped);
      expect(chat.prompts.slice(before)).toHaveLength(1);
      expect(sourcesAsked(chat).slice(2)).toEqual(["customers"]);
      expect((await status(provider, pkg, retyped)).status).toBe("ready");
   });

   it("editing the prompt file rewrites every summary and nothing else", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await search(recordingEmbedder().provider, pkgWith());
      const before = chat.prompts.length;

      const second = recordingEmbedder();
      await search(
         second.provider,
         pkgWith({
            prompts: {
               sourceSummary: {
                  path: "p.md",
                  text: "New instructions.",
                  hash: "h2",
               },
            },
         }),
      );
      const after = chat.prompts.slice(before);
      expect(after.filter(isSummary)).toHaveLength(2);
      expect(after.filter((p) => !isSummary(p))).toHaveLength(0);
      expect(second.sent).toEqual(["find it"]);
   });

   it("a different model rewrites every summary", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await search(recordingEmbedder().provider, pkgWith());
      const before = chat.prompts.length;

      _resetEmbeddingIndexStateForTests();
      const other = chatFor();
      _setChatModelForTests(other.model, { concurrency: 1, model: "other" });
      await search(recordingEmbedder().provider, pkgWith());
      expect(chat.prompts).toHaveLength(before);
      expect(summaryPrompts(other)).toHaveLength(2);
   });

   it("an LLM failure is an error with stage source_summary, embeds nothing, and a retry resumes", async () => {
      let down = true;
      const chat = chatFor((p) => down && /^Source name: customers$/m.test(p));
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const { provider, sent } = recordingEmbedder();
      const pkg = pkgWith();
      let outcome = await trySemanticSearch({
         db,
         provider,
         pkg,
         environmentName: "env",
         packageName: "sync",
         entities: entities(),
         queries: [{ targetIndex: 0, text: "find it", kinds: ["dimension"] }],
         perSourceWindow: 10,
      });
      expect("unavailable" in outcome && outcome.unavailable).toBe("indexing");
      for (let i = 0; i < 200; i++) {
         outcome = await trySemanticSearch({
            db,
            provider,
            pkg,
            environmentName: "env",
            packageName: "sync",
            entities: entities(),
            queries: [
               { targetIndex: 0, text: "find it", kinds: ["dimension"] },
            ],
            perSourceWindow: 10,
         });
         if ("unavailable" in outcome && outcome.unavailable === "cooldown")
            break;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      const s = await status(provider, pkg);
      expect(s.status).toBe("error");
      expect(s.stage).toBe("source_summary");
      expect(s.reason).toBe("cooldown");
      expect(s.lastError?.message).toContain(
         "Source summary generation failed after 1 of 2 sources",
      );
      expect(s.lastError?.message).toContain("the LLM is down");
      // No vector was written, and the one summary that was saved stays.
      expect(sent).toEqual([]);
      const embedded = await db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings",
      );
      expect(embedded[0].n).toBe(0);
      expect([...(await rows()).keys()]).toEqual(["orders"]);

      down = false;
      _clearProviderCooldownForTests();
      const before = summaryPrompts(chat).length;
      await search(provider, pkg);
      expect((await status(provider, pkg)).status).toBe("ready");
      // Only the source that was not saved is asked for again.
      expect(summaryPrompts(chat).slice(before)).toHaveLength(1);
      expect(sourcesAsked(chat).slice(-1)).toEqual(["customers"]);
   });

   it("shares maxCallsPerSync with the keyphrases and says when it stopped", async () => {
      const chat = chatFor();
      // One keyphrase call, then one summary call; the second summary is left.
      _setChatModelForTests(chat.model, {
         concurrency: 1,
         maxCallsPerSync: 2,
      });
      const { provider } = recordingEmbedder();
      const pkg = pkgWith();
      await search(provider, pkg);
      expect(keyphrasePrompts(chat)).toHaveLength(1);
      expect(summaryPrompts(chat)).toHaveLength(1);
      const s = await status(provider, pkg);
      expect(s.status).toBe("ready");
      expect(s.sourceSummaryProgress).toEqual({
         done: 1,
         total: 2,
         capped: true,
      });
      expect([...(await rows()).keys()]).toEqual(["orders"]);
   });

   it("enabled: false makes no summary call and reports no progress", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const { provider } = recordingEmbedder();
      const pkg = pkgWith({ sourceSummary: { enabled: false } });
      await search(provider, pkg);
      expect(summaryPrompts(chat)).toHaveLength(0);
      expect(keyphrasePrompts(chat)).toHaveLength(1);
      const s = await status(provider, pkg);
      expect(s.sourceSummaryProgress).toBeUndefined();
      expect([...(await rows()).keys()]).toEqual([]);
   });

   it("with no LLM configured, auto acts as off", async () => {
      _setChatModelForTests(null);
      const { provider } = recordingEmbedder();
      const pkg = pkgWith();
      await search(provider, pkg);
      const s = await status(provider, pkg);
      expect(s.status).toBe("ready");
      expect(s.sourceSummaryProgress).toBeUndefined();
      expect([...(await rows()).keys()]).toEqual([]);
   });

   it("enabled: true with an LLM runs the step like auto", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await search(
         recordingEmbedder().provider,
         pkgWith({ sourceSummary: { enabled: true } }),
      );
      expect(summaryPrompts(chat)).toHaveLength(2);
   });

   it("works with keyphrases never: summaries do not depend on them", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await search(
         recordingEmbedder().provider,
         pkgWith({ keyphrases: "never" }),
      );
      expect(keyphrasePrompts(chat)).toHaveLength(0);
      expect(summaryPrompts(chat)).toHaveLength(2);
   });

   it("deleting the package deletes its summaries", async () => {
      _setChatModelForTests(chatFor().model, { concurrency: 1 });
      await search(recordingEmbedder().provider, pkgWith());
      expect((await rows()).size).toBe(2);
      await deletePackageEmbeddings(db, "env", "sync");
      expect((await rows()).size).toBe(0);
   });
});
