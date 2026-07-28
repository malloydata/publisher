/**
 * Retrieval eval for malloy_getContext: lexical baseline vs the optional
 * embedding-backed semantic mode.
 *
 * Runs a labeled set of plain-English queries against the live MCP endpoint
 * and reports recall@K: whether the expected entity appears in the top-K
 * results. Each case is labeled with the retrieval mode the server reported
 * (`semantic` / `lexical`; servers without an embedding provider report no
 * mode and run the lexical baseline).
 *
 * A/B: run once against a server started WITHOUT `EMBEDDING_API_KEY`
 * (lexical baseline), then once against a server started with it, and
 * compare the two summaries. The cases marked `gap` share no tokens with
 * their target entity's name or docs, so the lexical run is expected to
 * miss them and the semantic run to hit them.
 *
 *   bun run packages/server/src/mcp/tools/get_context_eval.ts [K]
 *
 * Targets MCP_URL (default http://localhost:4040/mcp); start the server
 * first. Cases cover the bundled `examples` environment (storefront), so
 * the default config works out of the box, plus the classic ecommerce/faa
 * cases for a malloy-samples config (`samples` environment); cases whose
 * environment is not served are skipped, not counted as misses.
 */

interface EvalCase {
   env: string;
   pkg: string;
   query: string;
   expect: string; // substring expected in a top-K result name (case-insensitive)
   gap?: boolean; // true = no token overlap; lexical is expected to miss
}

interface ResultEntity {
   kind: string;
   name: string;
   source?: string | null;
}

interface GetContextPayload {
   retrieval?: string;
   results?: ResultEntity[];
   error?: string;
}

// Ground truth verified against the bundled examples (storefront) and the
// malloy-samples demo packages (ecommerce, faa).
const CASES: EvalCase[] = [
   // examples/storefront: served by the default config, works out of the box.
   {
      env: "examples",
      pkg: "storefront",
      query: "revenue by product category",
      expect: "category",
   },
   {
      env: "examples",
      pkg: "storefront",
      query: "top selling products",
      expect: "top_products",
   },
   {
      env: "examples",
      pkg: "storefront",
      query: "monthly sales trend",
      expect: "sales_by_month",
   },
   // Token-gap cases: no word here appears in the entity's name or #(doc)
   // text, so a token-based index misses them; embeddings should not.
   {
      env: "examples",
      pkg: "storefront",
      query: "how much money did we make",
      expect: "total_sales",
      gap: true,
   },
   {
      env: "examples",
      pkg: "storefront",
      query: "refund percentage",
      expect: "return_rate",
      gap: true,
   },
   // malloy-samples (needs a config serving them as environment "samples").
   {
      env: "samples",
      pkg: "ecommerce",
      query: "revenue by product category",
      expect: "category",
   },
   {
      env: "samples",
      pkg: "ecommerce",
      query: "total sales revenue",
      expect: "total_sales",
   },
   {
      env: "samples",
      pkg: "ecommerce",
      query: "customer state location",
      expect: "state",
   },
   {
      env: "samples",
      pkg: "ecommerce",
      query: "order count",
      expect: "order_count",
   },
   {
      env: "samples",
      pkg: "ecommerce",
      query: "product brand",
      expect: "brand",
   },
   {
      env: "samples",
      pkg: "faa",
      query: "flights by carrier",
      expect: "carrier",
   },
   { env: "samples", pkg: "faa", query: "airport", expect: "airport" },
   { env: "samples", pkg: "faa", query: "aircraft model", expect: "aircraft" },
   // The original lexical-gap case: the field is "dep_delay", which shares
   // no tokens with "departure delay".
   {
      env: "samples",
      pkg: "faa",
      query: "departure delay",
      expect: "delay",
      gap: true,
   },
];

const ENDPOINT = process.env.MCP_URL || "http://localhost:4040/mcp";
const K = Number(process.argv[2] || 5);
const WARMUP_ATTEMPTS = 15;
const WARMUP_DELAY_MS = 2_000;

async function callGetContext(
   args: Record<string, unknown>,
): Promise<GetContextPayload> {
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
         params: { name: "malloy_getContext", arguments: args },
      }),
   });
   const text = await res.text();
   const dataLine = text.split("\n").find((l) => l.startsWith("data: "));
   if (!dataLine) return { results: [] };
   const msg = JSON.parse(dataLine.slice(6)) as {
      result?: { content?: { resource?: { text?: string } }[] };
   };
   const payloadText = msg.result?.content?.[0]?.resource?.text;
   if (!payloadText) return { results: [] };
   return JSON.parse(payloadText) as GetContextPayload;
}

/** True when the environment exists on the server (tier-2 listing works). */
async function environmentServed(env: string): Promise<boolean> {
   const payload = await callGetContext({ environmentName: env });
   return payload.error === undefined;
}

/**
 * On a semantic-capable server the first query against a package kicks off
 * the embedding sync and answers lexically; poll until the mode flips (or
 * give up and report whatever mode the server settles on).
 */
async function warmUp(env: string, pkg: string): Promise<string | undefined> {
   let mode: string | undefined;
   for (let attempt = 0; attempt < WARMUP_ATTEMPTS; attempt++) {
      const payload = await callGetContext({
         environmentName: env,
         packageName: pkg,
         query: "warm up",
         limit: 1,
      });
      mode = payload.retrieval;
      // No marker: the server has no embedding provider configured, so
      // there is nothing to warm up.
      if (mode === undefined || mode === "semantic") return mode;
      await new Promise((resolve) => setTimeout(resolve, WARMUP_DELAY_MS));
   }
   return mode;
}

async function main(): Promise<void> {
   console.log(
      `get_context retrieval eval, recall@${K} (endpoint ${ENDPOINT})\n`,
   );

   const servedEnvs = new Map<string, boolean>();
   for (const env of new Set(CASES.map((c) => c.env))) {
      servedEnvs.set(env, await environmentServed(env));
   }
   for (const [env, served] of servedEnvs) {
      if (!served) {
         console.log(
            `  [SKIP] environment "${env}" is not served; skipping its cases`,
         );
      }
   }

   const warmed = new Set<string>();
   let hits = 0;
   let scored = 0;
   const modesSeen = new Set<string>();

   for (const c of CASES) {
      if (!servedEnvs.get(c.env)) continue;

      const pkgKey = `${c.env}/${c.pkg}`;
      if (!warmed.has(pkgKey)) {
         warmed.add(pkgKey);
         await warmUp(c.env, c.pkg);
      }

      const payload = await callGetContext({
         environmentName: c.env,
         packageName: c.pkg,
         query: c.query,
         limit: K,
      });
      const results = payload.results ?? [];
      const mode = payload.retrieval ?? "lexical (no provider)";
      modesSeen.add(mode);

      scored++;
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
      const gap = c.gap ? " [gap]" : "";
      console.log(
         `  [${tag}] [${mode}] ${c.env}/${c.pkg} / "${c.query}" (want ~${c.expect})${gap}`,
      );
      console.log(`            top: ${top}`);
   }

   if (scored === 0) {
      console.log("\nNo cases ran: no listed environment is served.");
      process.exit(1);
   }
   const pct = ((hits / scored) * 100).toFixed(0);
   console.log(
      `\nrecall@${K}: ${hits}/${scored} (${pct}%)  mode(s): ${[...modesSeen].join(", ")}`,
   );
   console.log(
      "The [gap] cases share no tokens with their target entity, so a lexical",
   );
   console.log(
      "run misses them by construction; a semantic run (EMBEDDING_API_KEY set)",
   );
   console.log("is expected to close them. Compare the two summaries.");
}

main().catch((err) => {
   console.error(err);
   process.exit(1);
});
