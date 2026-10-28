// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The source summary step as the search index build runs it
 * (resolveSourceSummarySettings, then resolveSourceSummaries): it shares the
 * build's call limit, rewrites a summary only when what the model is shown
 * changes, fails loudly with the stage named, and leaves what it saved.
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
   _clearChatModelForTests,
   _setChatModelForTests,
} from "../../providers/active";
import { setRetrievalConfig } from "../../retrieval_config";
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
   deletePackageEmbeddings,
   KEY_SEPARATOR,
   type EmbeddableEntity,
} from "./embedding_index";
import { SyncRequestBudget } from "./get_context_llm";
import {
   SOURCE_SUMMARY_SMALL_CONTEXT_PROMPT_CHARS,
   SourceSummaryStageError,
   loadSourceSummaries,
   resolveSourceSummaries,
} from "./source_summaries";
import { resolveSourceSummarySettings } from "./source_summary_settings";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "summary-build-spec-"));
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
   await db.initialize();
   await createSourceSummariesTable(db);
   // deletePackageEmbeddings also clears the tables an earlier release wrote.
   await createEntityEmbeddingsTable(db);
   await createEntityKeyphrasesTable(db);
});

afterAll(async () => {
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(async () => {
   await db.run("DELETE FROM source_summaries");
});

afterEach(() => {
   _clearChatModelForTests();
   setRetrievalConfig(undefined);
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

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

/** Two documented sources with fields, and a third with none, which gets no summary. */
function entities(
   over: (e: EmbeddableEntity) => EmbeddableEntity = (e) => e,
): readonly EmbeddableEntity[] {
   return Object.freeze(
      [
         entity("source", "orders", "orders", "One row per order."),
         entity("dimension", "state", "orders", "State.", {
            dataType: "string",
         }),
         entity("source", "customers", "customers", "One row per customer."),
         entity("dimension", "region", "customers", "Region.", {
            dataType: "string",
         }),
         entity("source", "ghost", "ghost", "Nothing in it."),
      ].map(over),
   );
}

function chatFor(
   fail: (prompt: string) => boolean = () => false,
): ScriptedChat {
   return scriptedChat((prompt) => {
      if (fail(prompt)) throw new Error("the LLM is down");
      return summaryReply(prompt);
   });
}

const summaryPrompts = (chat: ScriptedChat) =>
   chat.prompts.filter((p) => p.includes("Source name: "));
const sourcesAsked = (chat: ScriptedChat) =>
   summaryPrompts(chat).map((p) => /^Source name: (.*)$/m.exec(p)?.[1]);

const pkgWith = (retrieval: Partial<PackageRetrievalSettings> = {}): Package =>
   ({
      getRetrievalSettings: () => ({ prompts: {}, ...retrieval }),
   }) as unknown as Package;

/** The step as the build runs it; `undefined` when the package has summaries off. */
async function step(
   pkg: Package,
   ents: readonly EmbeddableEntity[] = entities(),
) {
   const settings = resolveSourceSummarySettings(pkg);
   if (!settings) return undefined;
   return resolveSourceSummaries({
      db,
      environmentName: "env",
      packageName: "build",
      entities: ents,
      settings,
      budget: new SyncRequestBudget(settings.maxCallsPerSync),
   });
}

/** The stored summaries by source name; every source here is in one file. */
const rows = async () =>
   new Map(
      [...(await loadSourceSummaries(db, "env", "build"))].map(
         ([key, value]) => [key.split(KEY_SEPARATOR)[1], value],
      ),
   );

// ---------------------------------------------------------------------------

describe("source summaries for a local model", () => {
   it("use the smaller prompt cap only when the provider is ollama", () => {
      const cap = (provider: "ollama" | "openai") => {
         _setChatModelForTests(chatFor().model, { provider });
         return resolveSourceSummarySettings(pkgWith())?.maxPromptChars;
      };
      expect(cap("ollama")).toBe(SOURCE_SUMMARY_SMALL_CONTEXT_PROMPT_CHARS);
      expect(cap("openai")).toBeUndefined();
   });
});

describe("source summaries in the index build", () => {
   it("writes one summary per documented source and reports its progress", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const outcome = await step(pkgWith());
      expect(sourcesAsked(chat)).toEqual(["orders", "customers"]);
      expect([...(await rows()).keys()]).toEqual(["orders", "customers"]);
      expect(outcome?.progress).toEqual({ done: 2, total: 2, capped: false });
   });

   it("a rebuild of the same content makes no call", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await step(pkgWith());
      const calls = chat.prompts.length;
      await step(pkgWith());
      expect(chat.prompts).toHaveLength(calls);
   });

   it("editing one source's doc rewrites that source only", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await step(pkgWith());
      const before = chat.prompts.length;

      const edited = entities((e) =>
         e.kind === "source" && e.name === "customers"
            ? { ...e, embedDoc: "One row per customer account." }
            : e,
      );
      await step(pkgWith(), edited);
      expect(chat.prompts.slice(before)).toHaveLength(1);
      expect(sourcesAsked(chat).slice(2)).toEqual(["customers"]);
      expect((await rows()).get("customers")?.oneLineSummary).toBe(
         "One row per customer account.",
      );
   });

   it("a change only the model is shown (a field's type) rewrites its source", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await step(pkgWith());
      const before = chat.prompts.length;
      const retyped = entities((e) =>
         e.name === "region" ? { ...e, dataType: "number" } : e,
      );
      await step(pkgWith(), retyped);
      expect(chat.prompts.slice(before)).toHaveLength(1);
      expect(sourcesAsked(chat).slice(2)).toEqual(["customers"]);
   });

   it("editing the prompt file rewrites every summary", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await step(pkgWith());
      const before = chat.prompts.length;
      await step(
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
      expect(chat.prompts.slice(before)).toHaveLength(2);
   });

   it("a different model rewrites every summary", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await step(pkgWith());
      const before = chat.prompts.length;
      const other = chatFor();
      _setChatModelForTests(other.model, { concurrency: 1, model: "other" });
      await step(pkgWith());
      expect(chat.prompts).toHaveLength(before);
      expect(summaryPrompts(other)).toHaveLength(2);
   });

   it("an LLM failure is an error with the stage named, and a retry resumes", async () => {
      let down = true;
      const chat = chatFor((p) => down && /^Source name: customers$/m.test(p));
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const pkg = pkgWith();
      const error = (await step(pkg).then(
         () => undefined,
         (e: unknown) => e,
      )) as SourceSummaryStageError;
      expect(error).toBeInstanceOf(SourceSummaryStageError);
      expect(error.message).toContain(
         "Source summary generation failed after 1 of 2 sources",
      );
      expect(error.message).toContain("the LLM is down");
      // The one summary that was saved stays.
      expect([...(await rows()).keys()]).toEqual(["orders"]);

      down = false;
      const before = summaryPrompts(chat).length;
      await step(pkg);
      // Only the source that was not saved is asked for again.
      expect(summaryPrompts(chat).slice(before)).toHaveLength(1);
      expect(sourcesAsked(chat).slice(-1)).toEqual(["customers"]);
   });

   it("stops at maxCallsPerSync and says so", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, {
         concurrency: 1,
         maxCallsPerSync: 1,
      });
      const outcome = await step(pkgWith());
      expect(summaryPrompts(chat)).toHaveLength(1);
      expect(outcome?.progress).toEqual({ done: 1, total: 2, capped: true });
      expect([...(await rows()).keys()]).toEqual(["orders"]);
   });

   it("enabled: false makes no call", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      expect(
         await step(pkgWith({ sourceSummary: { enabled: false } })),
      ).toBeUndefined();
      expect(summaryPrompts(chat)).toHaveLength(0);
      expect([...(await rows()).keys()]).toEqual([]);
   });

   it("with no LLM configured, auto acts as off", async () => {
      _setChatModelForTests(null);
      expect(await step(pkgWith())).toBeUndefined();
      expect([...(await rows()).keys()]).toEqual([]);
   });

   it("enabled: true with an LLM runs the step like auto", async () => {
      const chat = chatFor();
      _setChatModelForTests(chat.model, { concurrency: 1 });
      await step(pkgWith({ sourceSummary: { enabled: true } }));
      expect(summaryPrompts(chat)).toHaveLength(2);
   });

   it("deleting the package deletes its summaries", async () => {
      _setChatModelForTests(chatFor().model, { concurrency: 1 });
      await step(pkgWith());
      expect((await rows()).size).toBe(2);
      await deletePackageEmbeddings(db, "env", "build");
      expect((await rows()).size).toBe(0);
   });
});
