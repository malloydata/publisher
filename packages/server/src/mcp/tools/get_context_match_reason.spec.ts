// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `matched_targets[].match_reason` on the wire: present only when refine ran
 * and the model gave a reason, and shaped exactly like the rest of
 * matched_targets ({search_text, relevance, match_reason}).
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
import {
   keywordReply,
   numberedLines,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat as installChat,
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
beforeEach(() => h.reset());
afterEach(() => _clearChatModelForTests());

interface Card {
   entities?: Array<{
      name: string;
      matched_targets?: Array<{
         search_text: string;
         relevance: number;
         match_reason?: string;
      }>;
   }>;
}

/** Answer refine prompts with `refineReply`; every other stage gets the keyword stand-in. */
const refineWith =
   (refineReply: (prompt: string) => string) => (prompt: string) =>
      prompt.includes("<candidates>")
         ? refineReply(prompt)
         : keywordReply(prompt);

async function ask(
   slug: string,
   reply: (prompt: string, n: number) => string,
   retrieval: Partial<PackageRetrievalSettings> = {},
) {
   installChat(scriptedChat(reply).model);
   const handler = h.handlerFor(shopPackage(retrieval));
   const { payload } = await untilSemantic(handler, {
      search_targets: [
         { target_type: "dimension", search_text: "state the order ships to" },
      ],
      scopes: [{ environment: "llm", package: slug }],
   });
   const cards = payload.sources as Card[];
   return cards.flatMap((c) => c.entities ?? []);
}

describe("matched_targets[].match_reason", () => {
   it("carries the refine reason next to the search text and relevance", async () => {
      const entities = await ask("with-reason", keywordReply);
      const targets = entities.flatMap((e) => e.matched_targets ?? []);
      expect(targets.length).toBeGreaterThan(0);
      for (const t of targets) {
         expect(Object.keys(t)).toEqual([
            "search_text",
            "relevance",
            "match_reason",
         ]);
         expect(t.search_text).toBe("state the order ships to");
         expect(t.match_reason).toMatch(/^Shares \d words? with the phrase\.$/);
      }
   });

   it("omits match_reason when the model gave none, and still answers", async () => {
      const entities = await ask(
         "no-reason",
         refineWith((prompt) =>
            JSON.stringify(
               numberedLines(prompt).map(([index]) => ({
                  index,
                  score: "HIGH",
               })),
            ),
         ),
      );
      const targets = entities.flatMap((e) => e.matched_targets ?? []);
      expect(targets.length).toBeGreaterThan(0);
      for (const t of targets) {
         expect(Object.keys(t)).toEqual(["search_text", "relevance"]);
      }
   });

   it("omits match_reason when refine did not run", async () => {
      const entities = await ask("refine-off", keywordReply, {
         refine: { enabled: false, minLevel: "MEDIUM" },
      });
      const targets = entities.flatMap((e) => e.matched_targets ?? []);
      expect(targets.length).toBeGreaterThan(0);
      for (const t of targets) {
         expect(Object.keys(t)).toEqual(["search_text", "relevance"]);
      }
   });

   it("truncates an overlong reason to 200 characters on the wire", async () => {
      const entities = await ask(
         "long-reason",
         refineWith((prompt) =>
            JSON.stringify(
               numberedLines(prompt).map(([index]) => ({
                  index,
                  score: "HIGH",
                  reason: "word ".repeat(100),
               })),
            ),
         ),
      );
      const reasons = entities.flatMap((e) =>
         (e.matched_targets ?? []).map((t) => t.match_reason as string),
      );
      expect(reasons.length).toBeGreaterThan(0);
      for (const r of reasons) expect(r.length).toBeLessThanOrEqual(200);
   });
});
