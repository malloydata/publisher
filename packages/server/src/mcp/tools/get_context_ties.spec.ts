// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Equal scores must order the same way every time. The same field name in
// several sources embeds to the same vector, so they tie exactly; before the
// scan broke ties on source and kind, DuckDB's parallel scan decided which one
// fell inside the per-target window and in what order the cards came back, so
// two identical requests could return different answers.

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import {
   _clearRetrievalConfigForTests,
   _setRetrievalConfigForTests,
} from "../../retrieval/retrieval_config";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { captureHandler, parse, sourceNames, storeFor } from "./retrieval_test_kit";

const SOURCES = ["delta", "alpha", "charlie", "bravo"];
const source = (name: string) => ({
   name,
   annotations: [`#(doc) Rows of ${name}.`],
   schema: {
      fields: [
         { kind: "measure", name: "total", annotations: ["#(doc) The total amount."] },
      ],
   },
});
const model = { getSourceInfos: () => SOURCES.map(source), getQueries: () => [] };
const pkg = { listModels: async () => [{ path: "m.malloy" }], getModel: () => model };

// Every embedded text maps to the same vector, so every `total` ties.
function provider(): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => ({
               index,
               embedding: /total/i.test(t) ? [1, 0] : [0, 1],
            })),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
   return new EmbeddingProvider(
      {
         apiKey: "t",
         model: "stub",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
}

const params = {
   search_targets: [{ target_type: "measure", search_text: "the total" }],
   scopes: [{ environment: "ties", package: "p" }],
};

describe("get_context ties", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-ties-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
   });
   beforeEach(() => {
      _setEmbeddingProviderForTests(provider());
      _resetEmbeddingIndexStateForTests();
   });
   afterEach(() => _clearRetrievalConfigForTests());

   async function ask(perTargetLimit: number | null) {
      _setRetrievalConfigForTests({ candidates: { perTargetLimit } });
      const handler = captureHandler(storeFor(pkg, db));
      for (let i = 0; i < 400; i++) {
         const payload = parse(await handler(params as never));
         if (payload.retrieval === "semantic") return payload;
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error("never became semantic");
   }

   it("orders tied cards by source name, the same way every time", async () => {
      const first = sourceNames(await ask(null));
      expect(first).toEqual([...first].sort());
      for (let i = 0; i < 5; i++) {
         expect(sourceNames(await ask(null))).toEqual(first);
      }
   });

   it("cuts a window of one at the alphabetically first source", async () => {
      for (let i = 0; i < 5; i++) {
         expect(sourceNames(await ask(1))).toEqual(["alpha"]);
      }
   });
});
