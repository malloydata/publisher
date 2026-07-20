import { describe, expect, it } from "bun:test";
import { docText, sanitize, registerGetContextTool } from "./get_context_tool";
import type { EnvironmentStore } from "../../service/environment_store";

describe("get_context docText", () => {
   it("extracts #(doc) text from annotation lines", () => {
      expect(docText(["#(doc) Total revenue in USD."])).toBe(
         "Total revenue in USD.",
      );
   });

   it("accepts Annotation objects ({ value }) as well as raw strings", () => {
      expect(docText([{ value: "#(doc) Customer state." }])).toBe(
         "Customer state.",
      );
   });

   it("joins multiple #(doc) lines and collapses whitespace", () => {
      expect(docText(["#(doc) line one", "#(doc)   line   two"])).toBe(
         "line one line two",
      );
   });

   it("falls back to the raw lines when no #(doc) annotation is present", () => {
      expect(docText(["# bar_chart"])).toBe("# bar_chart");
   });

   it("returns an empty string for missing or empty annotations", () => {
      expect(docText()).toBe("");
      expect(docText([])).toBe("");
   });
});

describe("get_context sanitize", () => {
   it("strips lunr operator characters so a plain-English query never throws", () => {
      const cleaned = sanitize('revenue: +top ~10 "exact" -minus ^boost *wild');
      expect(cleaned).not.toMatch(/[~^:*+\-"]/);
   });

   it("keeps an ordinary query intact", () => {
      expect(sanitize("revenue by product category")).toBe(
         "revenue by product category",
      );
   });
});

// Capture the tool handler that registerGetContextTool passes to McpServer.tool,
// so each discovery tier can be exercised against a mocked EnvironmentStore.
type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource: { text: string } }>;
}>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (_name: string, _desc: string, _shape: unknown, h: Handler) => {
         handler = h;
      },
   };
   registerGetContextTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Array<{ resource: { text: string } }> }) {
   return JSON.parse(result.content[0].resource.text);
}

// A model with one source (order_items) carrying one dimension (state).
const mockModel = {
   getSourceInfos: () => [
      {
         name: "order_items",
         annotations: [],
         schema: {
            fields: [{ kind: "dimension", name: "state", annotations: [] }],
         },
      },
   ],
   getQueries: () => [],
};
const mockPackage = {
   listModels: async () => [{ path: "ecommerce.malloy" }],
   getModel: () => mockModel,
};

// A package with more than 10 sources, to prove the enumeration tier is not
// silently capped the way ranked retrieval is.
const manySourceNames = Array.from({ length: 12 }, (_, i) => `s${i}`);
const mockManySourceModel = {
   getSourceInfos: () =>
      manySourceNames.map((name) => ({
         name,
         annotations: [],
         schema: { fields: [] },
      })),
   getQueries: () => [],
};
const mockManySourcePackage = {
   listModels: async () => [{ path: "many.malloy" }],
   getModel: () => mockManySourceModel,
};

describe("get_context discovery tiers", () => {
   it("tier 1: no environment lists environments with their package names", async () => {
      const handler = captureHandler({
         listEnvironments: async () =>
            [
               {
                  name: "malloy-samples",
                  packages: [{ name: "ecommerce" }, { name: "imdb" }],
               },
            ] as never,
      });
      const { results } = parse(await handler({}));
      expect(results).toEqual([
         {
            kind: "environment",
            name: "malloy-samples",
            packages: ["ecommerce", "imdb"],
         },
      ]);
   });

   it("tier 2: environment without a package lists the packages", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               listPackages: async () => [
                  { name: "ecommerce", description: "Ecommerce demo" },
               ],
            }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "malloy-samples" }),
      );
      expect(results).toEqual([
         {
            kind: "package",
            name: "ecommerce",
            description: "Ecommerce demo",
            environmentName: "malloy-samples",
         },
      ]);
   });

   it("tier 2: surfaces an unresolved environment as a tool error", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new Error("Environment 'nope' could not be resolved");
         },
      });
      const result = await handler({ environmentName: "nope" });
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.results).toEqual([]);
      expect(parsed.error).toContain("could not be resolved");
   });

   it("tier 3: package without a query lists only its sources", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockPackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
         }),
      );
      expect(results).toEqual([
         {
            kind: "source",
            name: "order_items",
            source: "order_items",
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            modelPath: "ecommerce.malloy",
            doc: "",
         },
      ]);
   });

   it("tier 4: a query retrieves the matching entity", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockPackage }) as never,
      });
      const { results } = parse(
         await handler({
            environmentName: "malloy-samples",
            packageName: "ecommerce",
            query: "state",
         }),
      );
      expect(
         results.some(
            (r: { name: string; kind: string }) =>
               r.name === "state" && r.kind === "dimension",
         ),
      ).toBe(true);
   });

   it("tier 1: surfaces a listEnvironments failure as a tool error", async () => {
      const handler = captureHandler({
         listEnvironments: async () => {
            throw new Error("environment store not initialized");
         },
      });
      const result = await handler({});
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.results).toEqual([]);
      expect(parsed.error).toContain("not initialized");
   });

   it("tier 3: lists every source, not just the first 10", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockManySourcePackage }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "e", packageName: "p" }),
      );
      expect(results).toHaveLength(12);
   });

   it("tier 3: honors an explicit limit when given", async () => {
      const handler = captureHandler({
         getEnvironment: async () =>
            ({ getPackage: async () => mockManySourcePackage }) as never,
      });
      const { results } = parse(
         await handler({ environmentName: "e", packageName: "p", limit: 5 }),
      );
      expect(results).toHaveLength(5);
   });
});
