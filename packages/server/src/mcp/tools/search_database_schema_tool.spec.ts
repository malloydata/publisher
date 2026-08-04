import { describe, expect, it } from "bun:test";
import type { EnvironmentStore } from "../../service/environment_store";
import {
   bareTableName,
   malloySourceSnippet,
   registerSearchDatabaseSchemaTool,
} from "./search_database_schema_tool";

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
   registerSearchDatabaseSchemaTool(
      fakeServer as never,
      store as EnvironmentStore,
   );
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Array<{ resource: { text: string } }> }) {
   return JSON.parse(result.content[0].resource.text);
}

/**
 * A connection as the config actually holds it: secrets included. The tool must
 * project this down to name and type, so the spec passes the dangerous shape
 * rather than a convenient one.
 */
const SECRET = "-----BEGIN PRIVATE KEY-----AAAABBBBCCCC";
const CONNECTIONS = [
   {
      name: "warehouse",
      type: "bigquery",
      bigqueryConnection: {
         defaultProjectId: "proj",
         serviceAccountKeyJson: `{"private_key":"${SECRET}"}`,
      },
   },
   {
      name: "analytics",
      type: "postgres",
      postgresConnection: {
         host: "db.internal",
         password: "hunter2-should-never-appear",
      },
   },
];

function storeWithConnections(
   packages: { name: string }[] = [],
): Partial<EnvironmentStore> {
   return {
      listEnvironments: async () => [{ name: "examples" }] as never,
      getEnvironment: async () =>
         ({
            listApiConnections: () => CONNECTIONS,
            listPackages: async () => packages,
         }) as never,
   };
}

describe("malloySourceSnippet", () => {
   it("builds a source line from the connection and the qualified path", () => {
      expect(malloySourceSnippet("duckdb", "main.orders", "orders")).toBe(
         "source: orders is duckdb.table('main.orders') extend { }",
      );
   });

   it("keeps a three-part path verbatim rather than re-deriving it", () => {
      expect(
         malloySourceSnippet("snow", "DB.PUBLIC.ORDERS", "ORDERS"),
      ).toContain("table('DB.PUBLIC.ORDERS')");
   });

   it("backticks a table name that is not a bare identifier", () => {
      expect(
         malloySourceSnippet("c", "s.order items", "order items"),
      ).toContain("source: `order items` is");
   });

   it("backticks a connection name that is not a bare identifier", () => {
      expect(malloySourceSnippet("my-conn", "s.t", "t")).toContain(
         "is `my-conn`.table(",
      );
   });
});

describe("bareTableName", () => {
   it("takes the last segment of a dotted path", () => {
      expect(bareTableName("sales.orders")).toBe("orders");
      expect(bareTableName("memory.main.orders")).toBe("orders");
   });

   it("names a file-path table after the file, not its extension", () => {
      expect(bareTableName("s3://bucket/data/customers.parquet")).toBe(
         "customers",
      );
      expect(bareTableName("data/regions.csv")).toBe("regions");
   });

   it("strips a data-file extension with no directory part", () => {
      expect(bareTableName("customers.parquet")).toBe("customers");
   });

   it("leaves an unqualified name alone", () => {
      expect(bareTableName("orders")).toBe("orders");
   });
});

describe("malloy_searchDatabaseSchema tiers", () => {
   it("lists environments with their connections when given no arguments", async () => {
      const handler = captureHandler(storeWithConnections());
      const payload = parse(await handler({}));
      expect(payload.environments).toHaveLength(1);
      expect(payload.environments[0].name).toBe("examples");
      expect(payload.environments[0].connections).toEqual([
         { name: "warehouse", type: "bigquery" },
         { name: "analytics", type: "postgres" },
      ]);
   });

   it("lists an environment's connections when given only the environment", async () => {
      const handler = captureHandler(storeWithConnections());
      const payload = parse(await handler({ environmentName: "examples" }));
      expect(payload.environmentName).toBe("examples");
      expect(payload.connections).toEqual([
         { name: "warehouse", type: "bigquery" },
         { name: "analytics", type: "postgres" },
      ]);
   });

   // The credential-leak rule in CLAUDE.md, pinned as a test rather than a
   // convention: an ApiConnection carries service-account JSON and passwords,
   // and this tool is the newest thing that touches one.
   it("never emits connection credentials in any tier", async () => {
      const handler = captureHandler(storeWithConnections());
      for (const params of [{}, { environmentName: "examples" }]) {
         const raw = JSON.stringify(await handler(params));
         expect(raw).not.toContain(SECRET);
         expect(raw).not.toContain("hunter2-should-never-appear");
         expect(raw).not.toContain("serviceAccountKeyJson");
         expect(raw).not.toContain("password");
         expect(raw).not.toContain("db.internal");
      }
   });

   // The per-package "duckdb" sandbox is synthesized on demand by
   // ConnectionController and appears in no config listing. Without this the
   // drill-down reports "no connections" on the setup the bundled examples
   // ship with, which is the first thing a new user runs.
   it("advertises the per-package duckdb sandbox when the environment has packages", async () => {
      const handler = captureHandler(
         storeWithConnections([
            { name: "storefront" },
            { name: "html-data-app" },
         ]),
      );
      const payload = parse(await handler({ environmentName: "examples" }));
      const sandbox = payload.connections.find(
         (c: { name: string }) => c.name === "duckdb",
      );
      expect(sandbox).toEqual({
         name: "duckdb",
         type: "duckdb",
         scope: "package",
         packages: ["storefront", "html-data-app"],
      });
   });

   it("omits the sandbox connection when the environment has no packages", async () => {
      const handler = captureHandler(storeWithConnections([]));
      const payload = parse(await handler({ environmentName: "examples" }));
      expect(
         payload.connections.some((c: { name: string }) => c.name === "duckdb"),
      ).toBe(false);
   });

   it("still lists an environment whose connections cannot be read", async () => {
      const handler = captureHandler({
         listEnvironments: async () => [{ name: "broken" }] as never,
         getEnvironment: async () => {
            throw new Error("environment failed to load");
         },
      });
      const payload = parse(await handler({}));
      expect(payload.environments).toEqual([
         { name: "broken", connections: [] },
      ]);
   });

   it("returns a tool error when the environment does not exist", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new Error("Environment not found");
         },
      });
      const result = await handler({
         environmentName: "nope",
         connectionName: "c",
      });
      expect(result.isError).toBe(true);
      expect(parse(result).tables).toEqual([]);
   });
});
