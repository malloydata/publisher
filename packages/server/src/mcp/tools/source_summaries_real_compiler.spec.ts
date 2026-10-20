// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Summaries are per (model file, source), and the index build must see the same
 * entity list a request does. This runs the summary step over the entities the
 * real compiler produces for two files that each define `cust`, where the
 * second file's `orders` joins its own `cust`. Handed a list deduplicated by
 * (kind, source, name), the second file's `cust` would never be summarized, its
 * `orders` would be summarized from a prompt where the join did not resolve,
 * and a request would then reject the stored row because its hash was computed
 * over the full list.
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
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createSourceSummariesTable } from "../../storage/duckdb/schema";
import {
   compileModelFiles,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import {
   scriptedChat,
   summaryReply,
} from "../../test_helpers/get_context_llm_harness";
import { getPackageIndex } from "./package_entities";
import {
   currentSummaryHashes,
   loadSourceSummaries,
   resolveSourceSummaries,
   summaryKey,
} from "./source_summaries";
import { resolveSourceSummarySettings } from "./source_summary_settings";

const FILES = {
   "a.malloy": `
source: cust is duckdb.sql("select 1 as id, 'x' as region") extend {
  #(doc) Customers as the first file sees them.
  dimension: a_region is region
}
`,
   "b.malloy": `
source: cust is duckdb.sql("select 1 as id, 'gold' as tier") extend {
  #(doc) Customers as the second file sees them.
  dimension: b_tier is tier
}

source: orders is duckdb.sql("select 1 as id, 1 as cust_id") extend {
  #(doc) One row per order.
  dimension: order_id is id
  join_one: cust on cust.id = cust_id
}
`,
};

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "summary-real-spec-"));
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

afterEach(() => {
   _clearChatModelForTests();
   setRetrievalConfig(undefined);
});

/** A package for the settings. */
const settingsPkg = (): Package =>
   ({ getRetrievalSettings: () => ({ prompts: {} }) }) as unknown as Package;

describe("source summaries in the index build, over the real compiler's entities", () => {
   it("summarizes every source in every file, and the request accepts what the build stored", async () => {
      const { pkg: compiled } = await compileModelFiles(FILES);
      const index = await getPackageIndex(
         storeServing(compiled),
         "env",
         "build",
      );
      const chat = scriptedChat(summaryReply);
      _setChatModelForTests(chat.model, { concurrency: 1 });
      const settings = resolveSourceSummarySettings(settingsPkg())!;

      // The build hands the step the package's own list.
      const outcome = await resolveSourceSummaries({
         db,
         environmentName: "env",
         packageName: "build",
         entities: index.directEntities,
         settings,
      });

      const asked = chat.prompts.map(
         (p) => /^Source name: (.*)$/m.exec(p)?.[1],
      );
      expect(asked.sort()).toEqual(["cust", "cust", "orders"]);

      // Each file's rows are there under its own key.
      const current = currentSummaryHashes(index.directEntities, settings);
      expect([...current.keys()].sort()).toEqual(
         [
            summaryKey("a.malloy", "cust"),
            summaryKey("b.malloy", "cust"),
            summaryKey("b.malloy", "orders"),
         ].sort(),
      );
      // The request side reads them against the hashes of ITS list. Every row
      // the build stored must pass that check, or it is dropped on every request.
      const served = await loadSourceSummaries(db, "env", "build", current);
      expect([...served.keys()].sort()).toEqual([...current.keys()].sort());

      // The second file's `orders` was written from a prompt in which its own
      // `cust` resolved, not from one where the join was left unexpanded.
      const ordersPrompt = chat.prompts.find((p) =>
         p.includes("Source name: orders"),
      );
      expect(ordersPrompt).toContain("b_tier");
      expect(ordersPrompt).not.toContain("a_region");

      // And the step counted every source as done.
      expect(outcome.progress).toEqual({ done: 3, total: 3, capped: false });
   });
});
