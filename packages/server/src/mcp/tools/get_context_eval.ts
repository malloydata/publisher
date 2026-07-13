/**
 * Lexical-baseline eval for malloy_getContext.
 *
 * Runs a labeled set of plain-English queries against the live MCP endpoint
 * and reports recall@K: whether the expected entity appears in the top-K results.
 * This establishes the lexical (lunr/BM25) baseline so retrieval quality is
 * measurable; an embeddings comparison is deferred to if/when an embedding
 * provider is added (see the EPC eval risk).
 *
 *   bun run packages/server/src/mcp/tools/get_context_eval.ts [K]
 *
 * Targets MCP_URL (default http://localhost:4040/mcp) over the demo
 * environment (EVAL_ENV, default "samples"); start the server first.
 */

interface EvalCase {
   pkg: string;
   query: string;
   expect: string; // substring expected in a top-K result name (case-insensitive)
}

interface ResultEntity {
   kind: string;
   name: string;
   source?: string | null;
}

// Ground truth verified against the demo packages (ecommerce, faa).
const CASES: EvalCase[] = [
   {
      pkg: "ecommerce",
      query: "revenue by product category",
      expect: "category",
   },
   { pkg: "ecommerce", query: "total sales revenue", expect: "total_sales" },
   { pkg: "ecommerce", query: "customer state location", expect: "state" },
   { pkg: "ecommerce", query: "order count", expect: "order_count" },
   { pkg: "ecommerce", query: "product brand", expect: "brand" },
   { pkg: "faa", query: "flights by carrier", expect: "carrier" },
   { pkg: "faa", query: "airport", expect: "airport" },
   { pkg: "faa", query: "aircraft model", expect: "aircraft" },
   // Known lexical gap: the field is "dep_delay", which does not share tokens with
   // "departure delay", so a token-based index misses it. Kept to surface the gap.
   { pkg: "faa", query: "departure delay", expect: "delay" },
];

const ENDPOINT = process.env.MCP_URL || "http://localhost:4040/mcp";
const EVAL_ENV = process.env.EVAL_ENV || "samples";
const K = Number(process.argv[2] || 5);

async function getContext(
   pkg: string,
   query: string,
   limit: number,
): Promise<ResultEntity[]> {
   const res = await fetch(ENDPOINT, {
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
            name: "malloy_getContext",
            arguments: {
               environmentName: EVAL_ENV,
               packageName: pkg,
               query,
               limit,
            },
         },
      }),
   });
   const text = await res.text();
   const dataLine = text.split("\n").find((l) => l.startsWith("data: "));
   if (!dataLine) return [];
   const msg = JSON.parse(dataLine.slice(6)) as {
      result?: { content?: { resource?: { text?: string } }[] };
   };
   const payloadText = msg.result?.content?.[0]?.resource?.text;
   if (!payloadText) return [];
   const payload = JSON.parse(payloadText) as { results?: ResultEntity[] };
   return payload.results ?? [];
}

async function main(): Promise<void> {
   console.log(
      `get_context lexical baseline, recall@${K} (endpoint ${ENDPOINT})\n`,
   );
   let hits = 0;
   for (const c of CASES) {
      const results = await getContext(c.pkg, c.query, K);
      const rank = results.findIndex((r) =>
         r.name.toLowerCase().includes(c.expect.toLowerCase()),
      );
      const hit = rank >= 0;
      if (hit) hits++;
      const top =
         results
            .slice(0, 3)
            .map((r) => `${r.kind}:${r.name}`)
            .join(", ") || "(none)";
      const tag = hit ? `HIT@${rank + 1}` : "MISS  ";
      console.log(`  [${tag}] ${c.pkg} / "${c.query}" (want ~${c.expect})`);
      console.log(`            top: ${top}`);
   }
   const pct = ((hits / CASES.length) * 100).toFixed(0);
   console.log(`\nrecall@${K}: ${hits}/${CASES.length} (${pct}%)`);
   console.log(
      "Misses occur where the field name does not share tokens with the query",
   );
   console.log(
      "(e.g. 'departure delay' vs a 'dep_delay' field). Closing that gap is the",
   );
   console.log(
      "motivation for an optional embeddings provider; lexical ships as the v1 baseline.",
   );
}

main().catch((err) => {
   console.error(err);
   process.exit(1);
});
