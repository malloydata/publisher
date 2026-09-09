/**
 * Which model path `get_context` reports a source under, and which sources it
 * reports at all.
 *
 * A source is queryable at EVERY model path that resolves it — its own file
 * and every file that imports it — so each pairing is its own card. This used
 * to be keyed on the bare source name, which collapsed the pairings and kept
 * whichever file the package's file walk reached first. That walk is
 * `fs.stat`-completion ordered, so two servers on identical bytes could report
 * different paths for the same source, and agents queried a file the source
 * did not resolve in.
 *
 * The namespace itself (which sources a file can resolve, and which the
 * `#(agent-hidden)` tags cover) is pinned against the real compiler in
 * `service/source_namespace.spec.ts`. These cover the index wiring on top.
 */
import { beforeEach, describe, expect, it } from "bun:test";
import { registerGetContextTool } from "./get_context_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import {
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";

beforeEach(() => {
   _setEmbeddingProviderForTests(null);
   _resetEmbeddingIndexStateForTests();
});

type Content = Array<{
   type?: string;
   text?: string;
   resource?: { text: string };
}>;
type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Content;
}>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (_n: string, _d: string, _s: unknown, h: Handler) => {
         handler = h;
      },
   };
   registerGetContextTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("handler was not registered");
   return handler;
}
const parse = (r: { content: Content }) =>
   JSON.parse(r.content[0].resource!.text);
const envWith = (getPackage: () => Promise<unknown>) =>
   ({ getPackage, getStaleCompileErrors: () => new Map() }) as never;

const source = (name: string) => ({
   name,
   annotations: [],
   schema: { fields: [{ kind: "dimension", name: "state", annotations: [] }] },
});

/**
 * `defs.malloy` declares `shared` and `only_here`; `uses.malloy` imports
 * `shared` and declares `mine`. `shared` therefore resolves — and is
 * queryable — under both paths.
 */
function twoFilePackage(agentHidden: Record<string, string[]> = {}) {
   const models: Record<string, string[]> = {
      "defs.malloy": ["shared", "only_here"],
      "uses.malloy": ["shared", "mine"],
   };
   return {
      listModels: async () => Object.keys(models).map((path) => ({ path })),
      getModel: (path: string) =>
         models[path]
            ? {
                 getSourceInfos: () => models[path].map(source),
                 getQueries: () => [],
                 getAgentHiddenSourceNames: () =>
                    new Set(agentHidden[path] ?? []),
              }
            : undefined,
   };
}

/** The whole payload, for the tests that read the paging envelope. */
const payloadFor = async (
   pkg: unknown,
   params: Record<string, unknown> = {},
) => {
   const handler = captureHandler({
      getEnvironment: async () => envWith(async () => pkg),
   });
   return parse(
      await handler({
         environmentName: "e",
         packageName: "p",
         search_targets: [{ target_type: "source" }],
         scopes: [{ environment: "e", package: "p" }],
         ...params,
      }),
   );
};

const cardsFor = async (pkg: unknown) => {
   const handler = captureHandler({
      getEnvironment: async () => envWith(async () => pkg),
   });
   const payload = parse(
      await handler({
         environmentName: "e",
         packageName: "p",
         search_targets: [{ target_type: "source" }],
         scopes: [{ environment: "e", package: "p" }],
      }),
   );
   return (payload.sources ?? []).map(
      (s: { source_info: { resource_id: Record<string, string> } }) => ({
         source: s.source_info.resource_id.source,
         model_path: s.source_info.resource_id.model_path,
      }),
   );
};

const sorted = (rows: Array<{ source: string; model_path: string }>) =>
   [...rows].sort((a, b) =>
      `${a.source}|${a.model_path}`.localeCompare(
         `${b.source}|${b.model_path}`,
      ),
   );

describe("get_context source attribution", () => {
   it("reports a shared source under every file that resolves it", async () => {
      expect(sorted(await cardsFor(twoFilePackage()))).toEqual([
         { source: "mine", model_path: "uses.malloy" },
         { source: "only_here", model_path: "defs.malloy" },
         // Both, not one picked by file-walk order.
         { source: "shared", model_path: "defs.malloy" },
         { source: "shared", model_path: "uses.malloy" },
      ]);
   });

   it("reports the same pairings whatever order the models are listed in", async () => {
      const forward = twoFilePackage();
      const reversed = {
         ...twoFilePackage(),
         listModels: async () => [
            { path: "uses.malloy" },
            { path: "defs.malloy" },
         ],
      };
      expect(sorted(await cardsFor(reversed))).toEqual(
         sorted(await cardsFor(forward)),
      );
   });

   it("drops an agent-hidden source from the file that hides it, keeping the other path", async () => {
      // Source-level `#(agent-hidden)` in defs.malloy travels with the struct,
      // so both files hide it. A file-level tag would not — see
      // service/source_namespace.spec.ts.
      const rows = await cardsFor(
         twoFilePackage({ "defs.malloy": ["only_here"] }),
      );
      expect(sorted(rows)).toEqual([
         { source: "mine", model_path: "uses.malloy" },
         { source: "shared", model_path: "defs.malloy" },
         { source: "shared", model_path: "uses.malloy" },
      ]);
   });

   /**
    * `limit` and the paging envelope count CARDS, which is what the response
    * returns. A source resolvable from two files is two cards, so windowing
    * that buckets on the bare source name spends one slot on both and then
    * lets `toSourceResults` fan them out past the limit -- and reports
    * `returned` (cards) against a `total_available` counted in names.
    */
   it("counts a repeated source once per card in limit and the envelope", async () => {
      const all = await payloadFor(twoFilePackage(), {
         search_targets: [{ target_type: "source", search_text: "shared" }],
      });
      // Two cards match: shared under defs.malloy and under uses.malloy.
      // Keyed on the bare name this was 1, while `sources` still held 2.
      expect(all.total_available).toBe(2);
      expect(all.sources).toHaveLength(2);
      expect(all.returned).toBe(all.sources.length);

      const capped = await payloadFor(twoFilePackage(), {
         search_targets: [{ target_type: "source", search_text: "shared" }],
         limit: 1,
      });
      // The cut is real, not silently exceeded by the fan-out downstream.
      expect(capped.sources).toHaveLength(1);
      expect(capped.returned).toBe(1);
      expect(capped.total_available).toBe(2);
   });

   it("hides nothing when a model exposes no agent-hidden accessor", async () => {
      // Package.getModel is duck-typed at several call sites; an older double
      // must not blank the listing.
      const legacy = {
         listModels: async () => [{ path: "defs.malloy" }],
         getModel: () => ({
            getSourceInfos: () => [source("shared")],
            getQueries: () => [],
         }),
      };
      expect(await cardsFor(legacy)).toEqual([
         { source: "shared", model_path: "defs.malloy" },
      ]);
   });
});
