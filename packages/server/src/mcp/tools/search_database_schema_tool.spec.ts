import { describe, expect, it, mock } from "bun:test";

// Tiers 3, 4 and 5 go through ConnectionController, so they were never driven
// through the handler at all: the tier-5 defect (returning a DIFFERENT table as
// a success) and the missing load-shedding both got through review green
// because nothing exercised these paths.
const controllerCalls: string[] = [];
let schemasResult: unknown[] = [];
let tablesResult: unknown[] = [];
mock.module("../../controller/connection.controller", () => ({
   ConnectionController: class {
      async listSchemas() {
         controllerCalls.push("listSchemas");
         return schemasResult;
      }
      async listTables() {
         controllerCalls.push("listTables");
         return tablesResult;
      }
   },
}));
import type { EnvironmentStore } from "../../service/environment_store";
import {
   bareTableName,
   canPasteSource,
   isPastableTablePath,
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
      // Identifiers are ALWAYS quoted: a table named `table` or `source` is a
      // Malloy reserved word, and a bare alias there does not compile.
      expect(malloySourceSnippet("duckdb", "main.orders", "orders")).toBe(
         "source: `orders` is `duckdb`.table('main.orders') extend { }",
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

   // CodeQL caught this class: escaping the delimiter but not the escape
   // character itself. A name ending in a backslash would consume the closing
   // backtick and swallow the rest of the line.
   it("escapes backslashes in a quoted identifier, not just backticks", () => {
      const snippet = malloySourceSnippet("c", "s.t", "odd\\name");
      expect(snippet).toContain("source: `odd\\\\name` is");
      // The identifier must terminate: exactly one unescaped closing backtick.
      expect(snippet).toContain("` is `c`.table(");
   });

   it("escapes a backtick inside an identifier", () => {
      expect(malloySourceSnippet("c", "s.t", "we`ird")).toContain(
         "source: `we\\`ird` is",
      );
   });

   // Malloy escapes string literals with a backslash, NOT by doubling the
   // quote as SQL does, so a warehouse path with an apostrophe needs \'.
   it("escapes quotes and backslashes in the table path", () => {
      expect(
         malloySourceSnippet(
            "c",
            "s3://bucket/o'brien/orders.parquet",
            "orders",
         ),
      ).toContain("table('s3://bucket/o\\'brien/orders.parquet')");
      expect(malloySourceSnippet("c", "a\\b", "t")).toContain(
         "table('a\\\\b')",
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

describe("reserved words in the emitted source line", () => {
   // Verified against the compiler: `source: table is ...` fails with
   // "'table' is a reserved word", while the backticked form compiles. The
   // field is described as ready to use, so it has to be.
   it("quotes a table named after a Malloy reserved word", () => {
      expect(malloySourceSnippet("duckdb", "main.table", "table")).toBe(
         "source: `table` is `duckdb`.table('main.table') extend { }",
      );
      expect(malloySourceSnippet("duckdb", "main.source", "source")).toContain(
         "source: `source` is",
      );
   });
});

describe("tiers 3, 4 and 5 through the handler", () => {
   const admitted: string[] = [];
   function store(): Partial<EnvironmentStore> {
      return {
         getEnvironment: async (name: string) =>
            ({
               assertCanAdmitQuery: () => admitted.push(name),
               listApiConnections: () => CONNECTIONS,
               listPackages: async () => [{ name: "p" }],
            }) as never,
      };
   }
   const base = { environmentName: "examples", connectionName: "warehouse" };

   it("tier 3 lists schemas and sheds load first", async () => {
      admitted.length = 0;
      schemasResult = [{ name: "sales", isDefault: false, isHidden: false }];
      const payload = parse(await captureHandler(store())(base));
      expect(payload.schemas).toEqual([
         { name: "sales", isDefault: false, isHidden: false },
      ]);
      expect(admitted).toEqual(["examples"]);
   });

   it("tier 4 pages, and sheds load first", async () => {
      admitted.length = 0;
      tablesResult = [
         { resource: "sales.a", columns: [{ name: "x", type: "int" }] },
         { resource: "sales.b", columns: [{ name: "y", type: "int" }] },
      ];
      const payload = parse(
         await captureHandler(store())({
            ...base,
            schemaName: "sales",
            limit: 1,
         }),
      );
      expect(payload.returned).toBe(1);
      expect(payload.totalAvailable).toBe(2);
      expect(payload.nextOffset).toBe(1);
      expect(admitted).toEqual(["examples"]);
   });

   // The defect: when the dialect ignores the table filter and the schema holds
   // exactly ONE table, the tool used to return that unrelated table as a
   // success, with the column cap waived, as though it were the one asked for.
   it("tier 5 errors rather than returning a different table", async () => {
      tablesResult = [
         { resource: "sales.orders", columns: [{ name: "x", type: "int" }] },
      ];
      const result = await captureHandler(store())({
         ...base,
         schemaName: "sales",
         tableName: "customers",
      });
      expect(result.isError).toBe(true);
      expect(parse(result).tables).toEqual([]);
   });

   it("tier 5 returns the requested table when it is present", async () => {
      admitted.length = 0;
      tablesResult = [
         { resource: "sales.customers", columns: [{ name: "x", type: "int" }] },
      ];
      const payload = parse(
         await captureHandler(store())({
            ...base,
            schemaName: "sales",
            tableName: "customers",
         }),
      );
      expect(payload.tables[0].tableName).toBe("customers");
      expect(admitted).toEqual(["examples"]);
   });
});

describe("only pastable table paths get a malloySource", () => {
   // Verified against the compiler: `table('main.awkward name')` fails with
   // "Invalid DuckDB table path". Quoting the segment inside the string fixes
   // it, but that quoting is per dialect, so the field is omitted instead of
   // emitting a line that will not compile.
   it("accepts plain dotted paths and clean file paths", () => {
      expect(isPastableTablePath("main.orders")).toBe(true);
      expect(isPastableTablePath("memory.main.orders")).toBe(true);
      expect(isPastableTablePath("data/customers.parquet")).toBe(true);
      expect(isPastableTablePath("s3://bucket/data/x.parquet")).toBe(true);
   });

   it("rejects a dotted path whose segment needs identifier quoting", () => {
      for (const bad of [
         "main.awkward name",
         "main.o'brien",
         "main.back`tick",
         "main.back\\slash",
         "",
      ]) {
         expect(isPastableTablePath(bad)).toBe(false);
      }
   });

   // These were asserted the WRONG way round before. Checked against DuckDB's
   // file-path grammar in @malloydata/malloy: a space, an apostrophe and a
   // parenthesis are all outside it, so these emit lines that do not compile.
   it("rejects a file path containing a character DuckDB's grammar excludes", () => {
      for (const bad of [
         "s3://bucket/o'brien/x.parquet",
         "gs://b/sales data/o.csv",
         "reports/Q1 report.parquet",
         "data/file(1).csv",
      ]) {
         expect(isPastableTablePath(bad)).toBe(false);
      }
   });

   // Conservative on purpose: these compile on SOME dialects (a hyphen on
   // BigQuery and DuckDB, a leading digit on MySQL and Databricks) and not on
   // others. Malloy does not export getDialect, so rather than approximate ten
   // grammars, only what every dialect accepts gets a pasteable line.
   it("rejects segments that only some dialects would accept", () => {
      expect(isPastableTablePath("public.my-table")).toBe(false);
      expect(isPastableTablePath("mydb.2024_orders")).toBe(false);
   });

   // A bare data-file name has no slash, so it took the dotted branch and
   // passed. DuckDB resolves `orders.parquet` as schema `orders`, table
   // `parquet`, because its validator tries identifiers before file paths.
   it("rejects a bare data-file name, which DuckDB reads as schema.table", () => {
      expect(isPastableTablePath("orders.parquet")).toBe(false);
      expect(isPastableTablePath("customers.csv")).toBe(false);
      // With a directory it IS the file grammar, and is fine.
      expect(isPastableTablePath("data/orders.parquet")).toBe(true);
   });

   // The field and the warning explaining its absence must share a predicate,
   // or one reason for omitting it goes unexplained.
   it("treats a nameless table as unpastable, not just an odd path", () => {
      expect(canPasteSource({ tableName: "", resource: "a/b/" })).toBe(false);
      expect(canPasteSource({ tableName: "t", resource: "main.t" })).toBe(true);
   });
});

describe("the unpastable warning actually reaches both tiers", () => {
   // Nothing pinned this firing: the tests covered the predicate in isolation,
   // so the tier-5 gap would not have been caught by a regression.
   function storeFor(): Partial<EnvironmentStore> {
      return {
         getEnvironment: async () =>
            ({
               assertCanAdmitQuery: () => {},
               listApiConnections: () => CONNECTIONS,
               listPackages: async () => [{ name: "p" }],
            }) as never,
      };
   }
   const base = { environmentName: "e", connectionName: "warehouse" };

   it("warns on the listing tier", async () => {
      tablesResult = [
         { resource: "sales.odd name", columns: [{ name: "x", type: "int" }] },
      ];
      const payload = parse(
         await captureHandler(storeFor())({ ...base, schemaName: "sales" }),
      );
      expect(payload.tables[0].malloySource).toBeUndefined();
      expect(payload.warnings?.join(" ")).toContain("malloySource is omitted");
   });

   it("warns on the single-table tier too", async () => {
      tablesResult = [
         { resource: "sales.odd name", columns: [{ name: "x", type: "int" }] },
      ];
      const payload = parse(
         await captureHandler(storeFor())({
            ...base,
            schemaName: "sales",
            tableName: "odd name",
         }),
      );
      expect(payload.tables[0].malloySource).toBeUndefined();
      expect(payload.warnings?.join(" ")).toContain("malloySource is omitted");
   });
});

describe("the warning covers BOTH reasons malloySource is withheld", () => {
   // The field is withheld for an unusable path OR an empty name. The warning
   // counted only the first, so a directory URL matching zero blobs lost the
   // field with no explanation. This fails if the filter reverts to the
   // narrower predicate.
   function storeFor(): Partial<EnvironmentStore> {
      return {
         getEnvironment: async () =>
            ({
               assertCanAdmitQuery: () => {},
               listApiConnections: () => CONNECTIONS,
               listPackages: async () => [{ name: "p" }],
            }) as never,
      };
   }

   it("warns when the path is fine but the table has no usable name", async () => {
      // A directory-shaped URL: pastable charset, but bareTableName is "".
      tablesResult = [
         { resource: "https://acct.blob.core.windows.net/c/dir/", columns: [] },
      ];
      const payload = parse(
         await captureHandler(storeFor())({
            environmentName: "e",
            connectionName: "warehouse",
            schemaName: "s",
         }),
      );
      expect(payload.tables[0].malloySource).toBeUndefined();
      expect(payload.warnings?.join(" ")).toContain("malloySource is omitted");
   });
});
