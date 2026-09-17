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
 * The namespace itself (which sources a file can resolve) is pinned against
 * the real compiler in
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

const source = (name: string, fields: string[] = ["state"]) => ({
   name,
   annotations: [],
   schema: {
      fields: fields.map((field) => ({
         kind: "dimension",
         name: field,
         annotations: [],
      })),
   },
});

/**
 * `defs.malloy` declares `shared` and `only_here`; `uses.malloy` imports
 * `shared` and declares `mine`. `shared` therefore resolves — and is
 * queryable — under both paths.
 */
function twoFilePackage() {
   const models: Record<string, string[]> = {
      "defs.malloy": ["shared", "only_here"],
      "uses.malloy": ["shared", "mine"],
   };
   return {
      listModels: async () => Object.keys(models).map((path) => ({ path })),
      getModel: (path: string) =>
         models[path]
            ? {
                 // Not `.map(source)`: map passes the index as the
                 // second argument, which `source` now reads as `fields`.
                 getSourceInfos: () => models[path].map((name) => source(name)),
                 getQueries: () => [],
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

   it("reports the same pairings IN ORDER whatever order the models are listed in", async () => {
      const forward = twoFilePackage();
      const reversed = {
         ...twoFilePackage(),
         listModels: async () => [
            { path: "uses.malloy" },
            { path: "defs.malloy" },
         ],
      };
      // Compared UNSORTED, deliberately. Sorting both sides asserts set
      // equality, which the walk delivers whether or not it sorts its model
      // listing -- so it stayed green with the sort at collectEntities
      // deleted, while the claim being made is that two servers on identical
      // bytes emit identical output.
      expect(await cardsFor(reversed)).toEqual(await cardsFor(forward));
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

   /**
    * The listing path does not go through `windowBySource`, and counted a
    * drill-down as 1-of-1 outright. A source pinned by name still resolves in
    * every file that imports it, so the drill-down is N cards and the envelope
    * has to say N.
    */
   it("counts every resolving path in a drill-down envelope", async () => {
      const drill = await payloadFor(twoFilePackage(), {
         search_targets: [{ target_type: "dimension" }],
         scopes: [{ environment: "e", package: "p", source: "shared" }],
      });
      expect(drill.sources).toHaveLength(2);
      expect(drill.returned).toBe(2);
      // Was hard-coded to 1, so the envelope contradicted its own payload.
      expect(drill.total_available).toBe(2);
   });

   /**
    * `scopes[].entity_name` had no test anywhere in the server, on either
    * retrieval mode, while a pinned name also turns `include_code` on -- so
    * "pinning narrows to this entity" was an unpinned claim about the very
    * request that returns the most. The semantic half is pinned in
    * tests/integration/mcp/mcp_get_context_semantic.integration.spec.ts.
    */
   it("narrows a lexical drill-down to the pinned entity", async () => {
      const oneFile = {
         listModels: async () => [{ path: "defs.malloy" }],
         getModel: () => ({
            getSourceInfos: () => [source("shared", ["state", "city"])],
            getQueries: () => [],
         }),
      };
      const payload = await payloadFor(oneFile, {
         search_targets: [{ target_type: "dimension" }],
         scopes: [
            {
               environment: "e",
               package: "p",
               source: "shared",
               entity_name: "city",
            },
         ],
      });
      const names = (payload.sources ?? []).flatMap(
         (card: { entities?: Array<{ name: string }> }) =>
            (card.entities ?? []).map((entity) => entity.name),
      );
      expect(names).toEqual(["city"]);
   });

   it("reports an unknown drill-down source as empty, not as one card", async () => {
      const missing = await payloadFor(twoFilePackage(), {
         search_targets: [{ target_type: "dimension" }],
         scopes: [{ environment: "e", package: "p", source: "nope" }],
      });
      expect(missing.sources).toEqual([]);
      expect(missing.total_available).toBe(0);
      expect(missing.warnings).toBeUndefined();
   });
});
