// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Retrieval eval for search_database_schema: the lexical baseline vs the
 * optional embedding-backed semantic mode.
 *
 * This exists because the last retrieval feature shipped without one. Semantic
 * get_context (#923) merged having only ever been measured against a stub
 * model, so "semantic retrieval works here" was never actually evidence. Nothing
 * in this feature should be promoted on the same basis, and the semantic path
 * ships off by default partly for that reason.
 *
 * It builds its own fixture: a warehouse-shaped schema created inside the
 * bundled storefront package's DuckDB sandbox, because the bundled example data
 * is addressed as files and registers no tables to search. So this runs against
 * the default config with no warehouse credentials.
 *
 *   bun run packages/server/src/mcp/tools/schema_search_eval.ts [K]
 *
 * A/B: run once against a server started WITHOUT the embedding variables (the
 * lexical baseline), then once against a server started with BOTH
 * `EMBEDDING_API_KEY` and `EMBEDDING_INDEX_CONNECTION_SCHEMA=true`, and compare
 * the two summaries. Cases marked `gap` share no tokens with their target
 * table's name or columns, so lexical is expected to miss them; they are the
 * cases that justify the semantic path at all.
 *
 * Targets PUBLISHER_URL (default http://localhost:4000) and MCP_URL (default
 * http://localhost:4040/mcp). Start the server first.
 */

// Marks this file as a module. Without it TypeScript treats a script with no
// imports as global scope, so its `EvalCase` and `main` collide with
// get_context_eval.ts's and the whole package fails to typecheck.
export {};

const K = Number(process.argv[2] ?? 3);
const PUBLISHER_URL = process.env.PUBLISHER_URL ?? "http://localhost:4000";
const MCP_URL = process.env.MCP_URL ?? "http://localhost:4040/mcp";

const ENVIRONMENT = "examples";
const PACKAGE = "storefront";
const CONNECTION = "duckdb";
const SCHEMA = "memory.main";

/** The fixture schema. Names are ordinary warehouse names, not Malloy-ish ones. */
const FIXTURE_TABLES = [
   "customer_accounts (account_id INTEGER, email_address VARCHAR, full_name VARCHAR, signup_date DATE, lifetime_value DECIMAL(10,2))",
   "order_headers (order_id INTEGER, account_id INTEGER, order_date DATE, total_amount DECIMAL(10,2), order_status VARCHAR)",
   "order_line_items (line_id INTEGER, order_id INTEGER, product_sku VARCHAR, quantity INTEGER, unit_price DECIMAL(10,2))",
   "product_catalog (product_sku VARCHAR, product_name VARCHAR, category VARCHAR, list_price DECIMAL(10,2))",
   "shipment_tracking (shipment_id INTEGER, order_id INTEGER, carrier_name VARCHAR, tracking_number VARCHAR, delivered_at TIMESTAMP)",
   "marketing_email_campaigns (campaign_id INTEGER, campaign_name VARCHAR, sent_at TIMESTAMP, subject_line VARCHAR)",
   "web_session_events (session_id VARCHAR, account_id INTEGER, event_type VARCHAR, occurred_at TIMESTAMP)",
   "supplier_invoices (invoice_id INTEGER, supplier_name VARCHAR, invoice_date DATE, amount_due DECIMAL(10,2), paid_flag BOOLEAN)",
];

interface EvalCase {
   query: string;
   /** Table name expected in the top-K results. */
   expect: string;
   /** No token overlap with the target: lexical is expected to miss. */
   gap?: boolean;
}

const CASES: EvalCase[] = [
   { query: "customer email addresses", expect: "customer_accounts" },
   { query: "what products do we sell", expect: "product_catalog" },
   { query: "unpaid supplier bills", expect: "supplier_invoices" },
   { query: "shipping and delivery tracking", expect: "shipment_tracking" },
   { query: "order totals by date", expect: "order_headers" },
   { query: "individual items within an order", expect: "order_line_items" },
   // Gap cases: the words a person uses share no stem with the identifiers.
   { query: "website visits", expect: "web_session_events", gap: true },
   {
      query: "newsletter blasts",
      expect: "marketing_email_campaigns",
      gap: true,
   },
   {
      query: "how much is each buyer worth",
      expect: "customer_accounts",
      gap: true,
   },
   { query: "parcel courier handoffs", expect: "shipment_tracking", gap: true },
];

async function runSql(sqlStatement: string): Promise<void> {
   const url = `${PUBLISHER_URL}/api/v0/environments/${ENVIRONMENT}/packages/${PACKAGE}/connections/${CONNECTION}/sqlQuery`;
   const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sqlStatement }),
   });
   if (!response.ok) {
      throw new Error(
         `fixture SQL failed (${response.status}): ${(await response.text()).slice(0, 200)}`,
      );
   }
}

interface SearchPayload {
   tables?: { tableName: string }[];
   ranking?: string;
   totalAvailable?: number;
   error?: string;
}

async function search(query: string): Promise<SearchPayload> {
   const response = await fetch(MCP_URL, {
      method: "POST",
      headers: {
         "Content-Type": "application/json",
         Accept: "application/json, text/event-stream",
      },
      body: JSON.stringify({
         jsonrpc: "2.0",
         id: 1,
         method: "tools/call",
         params: {
            name: "search_database_schema",
            arguments: {
               environmentName: ENVIRONMENT,
               connectionName: CONNECTION,
               packageName: PACKAGE,
               schemaName: SCHEMA,
               searchQuery: query,
               limit: K,
            },
         },
      }),
   });
   const body = await response.text();
   // The endpoint answers as SSE; the JSON-RPC envelope is the `data:` line.
   const line = body
      .split("\n")
      .find((l) => l.startsWith("data:"))
      ?.slice(5);
   if (!line)
      throw new Error(`no data frame in response: ${body.slice(0, 200)}`);
   const rpc = JSON.parse(line);
   const text = rpc?.result?.content?.[0]?.resource?.text;
   if (!text) throw new Error(`no payload: ${line.slice(0, 200)}`);
   return JSON.parse(text) as SearchPayload;
}

async function main(): Promise<void> {
   console.log(`Building fixture schema in ${CONNECTION}:${SCHEMA} ...`);
   for (const table of FIXTURE_TABLES) {
      await runSql(`CREATE TABLE IF NOT EXISTS ${table}`);
   }

   const probe = await search("customer");
   if (probe.error) {
      console.error(`search failed: ${probe.error}`);
      process.exit(1);
   }
   console.log(
      `Fixture ready: ${probe.totalAvailable} tables in scope.\n` +
         `schema search retrieval eval, recall@${K} (endpoint ${MCP_URL})\n`,
   );

   const modesSeen = new Set<string>();
   let hits = 0;
   let gapHits = 0;
   let gapTotal = 0;

   for (const testCase of CASES) {
      const payload = await search(testCase.query);
      if (payload.ranking) modesSeen.add(payload.ranking);
      const names = (payload.tables ?? []).map((t) =>
         t.tableName.toLowerCase(),
      );
      const hit = names.includes(testCase.expect.toLowerCase());
      if (hit) hits++;
      if (testCase.gap) {
         gapTotal++;
         if (hit) gapHits++;
      }
      const mark = hit ? "HIT " : "MISS";
      const tag = testCase.gap ? " [gap]" : "";
      console.log(
         `${mark}${tag} "${testCase.query}" -> expected ${testCase.expect}; got [${names.join(", ")}]`,
      );
   }

   const pct = ((hits / CASES.length) * 100).toFixed(0);
   console.log(
      `\nrecall@${K}: ${hits}/${CASES.length} (${pct}%)  mode(s): ${[...modesSeen].join(", ") || "none reported"}`,
   );
   console.log(
      `gap cases:  ${gapHits}/${gapTotal} (the ones the semantic mode exists to win)`,
   );
}

main().catch((error) => {
   console.error(error instanceof Error ? error.message : String(error));
   process.exit(1);
});
