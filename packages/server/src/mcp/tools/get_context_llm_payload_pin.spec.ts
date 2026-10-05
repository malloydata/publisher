// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Pins the COMPLETE get_context response when the LLM stages run, the way
 * get_context_payload_pin.spec.ts does for the paths without them. The chat
 * model is a deterministic keyword stand-in (see keywordReply), so the scores,
 * the order and the cuts are reproducible.
 *
 * Golden file: testdata/get_context_llm_payloads.golden.json. Regenerate it
 * after an INTENDED change with
 *
 *    UPDATE_GOLDEN=1 bun test src/mcp/tools/get_context_llm_payload_pin.spec.ts
 *
 * and read the diff in review. A missing golden fails the run. The golden for
 * the paths with no LLM is a different file and is not touched here.
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
import * as path from "path";
import { _clearChatModelForTests } from "../../providers/active";
import {
   callPayload,
   keywordReply,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
import { shopPackage } from "../../test_helpers/get_context_llm_fixture";
import type { PackageRetrievalSettings } from "../../service/package_retrieval";

const GOLDEN_PATH = path.join(
   __dirname,
   "testdata",
   "get_context_llm_payloads.golden.json",
);
const UPDATE = process.env.UPDATE_GOLDEN === "1";

const observed: Record<string, unknown> = {};
let golden: Record<string, unknown> | undefined;
let h: SemanticHarness;

beforeAll(async () => {
   h = await semanticHarness();
   if (UPDATE) return;
   if (!fs.existsSync(GOLDEN_PATH)) {
      throw new Error(
         `Golden file missing: ${GOLDEN_PATH}. Generate it with UPDATE_GOLDEN=1 and commit it; ` +
            "this spec never creates it on its own.",
      );
   }
   golden = JSON.parse(fs.readFileSync(GOLDEN_PATH, "utf8"));
});

afterAll(async () => {
   await h.close();
   if (UPDATE) {
      const sorted = Object.fromEntries(
         Object.entries(observed).sort(([a], [b]) => (a < b ? -1 : 1)),
      );
      fs.mkdirSync(path.dirname(GOLDEN_PATH), { recursive: true });
      fs.writeFileSync(GOLDEN_PATH, JSON.stringify(sorted, null, 2) + "\n");
   }
});

beforeEach(() => h.reset());
afterEach(() => _clearChatModelForTests());

function pin(name: string, payload: unknown) {
   observed[name] = payload;
   if (UPDATE) return;
   expect(
      name in (golden as object),
      `no golden entry for "${name}"; run with UPDATE_GOLDEN=1`,
   ).toBe(true);
   const expected = (golden as Record<string, unknown>)[name];
   expect(payload).toEqual(expected);
   // toEqual ignores key order; the wire does not.
   expect(JSON.stringify(payload)).toBe(JSON.stringify(expected));
}

const target = (target_type: string, search_text?: string) => ({
   target_type,
   ...(search_text === undefined ? {} : { search_text }),
});

/** One scenario: a fresh package and index, the keyword chat, one pinned payload. */
function scenario(
   name: string,
   params: Record<string, unknown>,
   options: {
      retrieval?: Partial<PackageRetrievalSettings>;
      chat?: (prompt: string, n: number) => string;
      /** A listing never turns "semantic", so it is asked once. */
      listing?: boolean;
   } = {},
) {
   it(name, async () => {
      useChat(scriptedChat(options.chat ?? keywordReply).model);
      const slug = name.replace(/[^a-z0-9]+/gi, "-");
      const handler = h.handlerFor(shopPackage(options.retrieval));
      const full = {
         ...params,
         scopes: [
            {
               environment: "llm",
               package: slug,
               ...(params.scopes as Record<string, unknown>[] | undefined)?.[0],
            },
         ],
      };
      const { payload } = options.listing
         ? await callPayload(handler, full)
         : await untilSemantic(handler, full);
      pin(name, payload);
   });
}

describe("get_context LLM payload pin", () => {
   scenario("refine and rerank: one target", {
      search_targets: [target("dimension", "state the order ships to")],
   });
   scenario("refine and rerank: multiple targets", {
      search_targets: [
         target("measure", "sum of order revenue"),
         target("dimension", "state the order ships to"),
         target("source", "one row per customer"),
      ],
   });
   scenario(
      "refine only",
      { search_targets: [target("dimension", "state the order ships to")] },
      { retrieval: { rerank: { enabled: false, topSources: 8 } } },
   );
   scenario(
      "rerank only",
      { search_targets: [target("dimension", "state the order ships to")] },
      { retrieval: { refine: { enabled: false, minLevel: "MEDIUM" } } },
   );
   scenario(
      "minLevel HIGH keeps only exact matches",
      { search_targets: [target("dimension", "state the order ships to")] },
      {
         retrieval: {
            refine: { enabled: "auto", minLevel: "HIGH" },
            rerank: { enabled: false, topSources: 8 },
         },
      },
   );
   scenario(
      "minLevel LOW keeps every rated candidate",
      { search_targets: [target("dimension", "state the order ships to")] },
      {
         retrieval: {
            refine: { enabled: "auto", minLevel: "LOW" },
            rerank: { enabled: false, topSources: 8 },
         },
      },
   );
   scenario(
      "topSources 2 counts the discarded source in total_available",
      { search_targets: [target("dimension", "state the order ships to")] },
      {
         retrieval: {
            refine: { enabled: "auto", minLevel: "LOW" },
            rerank: { enabled: "auto", topSources: 2 },
         },
      },
   );
   scenario("scoped to one source: one card, so rerank is skipped", {
      search_targets: [target("dimension", "state the order ships to")],
      scopes: [{ source: "customers" }],
   });
   scenario(
      "both stages off",
      { search_targets: [target("dimension", "state the order ships to")] },
      {
         retrieval: {
            refine: { enabled: false, minLevel: "MEDIUM" },
            rerank: { enabled: false, topSources: 8 },
         },
      },
   );
   scenario(
      "listing runs no LLM stage",
      { search_targets: [target("source")] },
      { listing: true },
   );
   scenario(
      "a model that returns an unusable reply fails the call",
      { search_targets: [target("dimension", "state the order ships to")] },
      { chat: () => "I cannot help with that." },
   );
});

// Source summaries on: the sync writes them (the keyword chat's stand-in),
// the response reads them. Refine keeps every rated field and rerank is off so
// more than one card comes back.
const SUMMARIES = {
   sourceSummary: { enabled: true },
   refine: { enabled: "auto", minLevel: "LOW" },
   rerank: { enabled: false, topSources: 8 },
} as const;

describe("get_context LLM payload pin: source summaries", () => {
   scenario(
      "source summaries: every card carries the one-liner, none the full summary",
      { search_targets: [target("dimension", "state the order ships to")] },
      { retrieval: SUMMARIES },
   );
   scenario(
      "source summaries: a pinned source carries the full summary",
      {
         search_targets: [target("dimension", "state the order ships to")],
         scopes: [{ source: "customers" }],
      },
      { retrieval: SUMMARIES },
   );
   scenario(
      "source summaries: a source search that matched one source carries its summary",
      {
         search_targets: [
            target("source", "one row per shipment"),
            target("dimension", "state the order ships to"),
         ],
      },
      { retrieval: SUMMARIES },
   );
});
