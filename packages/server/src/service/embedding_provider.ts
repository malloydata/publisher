import { EmbeddingConfig, getEmbeddingConfig } from "../config";

/** Timeout for bulk (index-build) embedding calls. */
export const EMBEDDING_BATCH_TIMEOUT_MS = 30_000;
/**
 * Timeout for the single per-request query embedding. Much shorter than
 * the bulk timeout: this call sits on the latency path of every semantic
 * `malloy_getContext` call, and a slow endpoint must degrade to lexical
 * quickly rather than stall the tool.
 */
export const EMBEDDING_QUERY_TIMEOUT_MS = 5_000;
/** Per-input character cap (matches the hosted indexing pipeline). */
export const MAX_EMBED_INPUT_CHARS = 1_024;
/** Inputs per HTTP request; large packages embed in several requests. */
export const MAX_EMBED_BATCH_SIZE = 512;

type FetchFn = typeof fetch;

interface EmbeddingResponseItem {
   index?: number;
   embedding?: number[];
}

/**
 * Collapse whitespace (newlines confuse some embedding models) and cap
 * the input length, mirroring the hosted indexing pipeline's input
 * preparation. Never returns an empty string: providers reject empty
 * input items with a 400, which would fail a whole package's batch, so
 * text that collapses to nothing (a whitespace-only backtick identifier)
 * becomes a placeholder token instead. This is the single choke point
 * for that invariant; the index hashes this same function's output, so
 * hash and sent text cannot drift. Exported for tests and for
 * content-hashing: the hash must cover the text actually sent, so a
 * truncation-boundary edit re-embeds.
 */
export function prepareEmbeddingInput(text: string): string {
   const cleaned = text.replace(/\s+/g, " ").trim();
   const capped =
      cleaned.length > MAX_EMBED_INPUT_CHARS
         ? cleaned.slice(0, MAX_EMBED_INPUT_CHARS)
         : cleaned;
   return capped || "-";
}

/**
 * Minimal client for an OpenAI-compatible `/embeddings` endpoint, called
 * with global fetch (no provider SDK; see fetchFromPublisherDataplane for
 * the outbound-HTTP precedent this follows). The API key travels only in
 * the Authorization header and must never be logged; error messages carry
 * at most a 200-char body excerpt and never echo request headers.
 */
export class EmbeddingProvider {
   constructor(
      private config: EmbeddingConfig,
      private fetchFn: FetchFn = fetch,
   ) {}

   get model(): string {
      return this.config.model;
   }

   get dimensions(): number | undefined {
      return this.config.dimensions;
   }

   /**
    * Embed `texts` in order. Inputs are prepared (whitespace-collapsed,
    * capped) and sent in batches of {@link MAX_EMBED_BATCH_SIZE}. Throws
    * on any HTTP, timeout, or malformed-response failure; callers own the
    * fallback-to-lexical decision.
    */
   async embedBatch(texts: string[], timeoutMs: number): Promise<number[][]> {
      const vectors: number[][] = [];
      for (let i = 0; i < texts.length; i += MAX_EMBED_BATCH_SIZE) {
         const chunk = texts
            .slice(i, i + MAX_EMBED_BATCH_SIZE)
            .map(prepareEmbeddingInput);
         vectors.push(...(await this.embedChunk(chunk, timeoutMs)));
      }
      return vectors;
   }

   private async embedChunk(
      inputs: string[],
      timeoutMs: number,
   ): Promise<number[][]> {
      const url = `${this.config.baseUrl}/embeddings`;
      const body: Record<string, unknown> = {
         model: this.config.model,
         input: inputs,
      };
      if (this.config.dimensions !== undefined) {
         body.dimensions = this.config.dimensions;
      }

      let response: Response;
      try {
         response = await this.fetchFn(url, {
            method: "POST",
            headers: {
               "Content-Type": "application/json",
               Authorization: `Bearer ${this.config.apiKey}`,
            },
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(timeoutMs),
         });
      } catch (error) {
         const reason =
            (error as Error)?.name === "TimeoutError"
               ? `timed out after ${timeoutMs}ms`
               : (error as Error).message;
         throw new Error(`Embedding request to ${url} failed: ${reason}`);
      }

      if (!response.ok) {
         // Auth-failure bodies commonly reflect the presented credential
         // (OpenAI's 401 echoes partial key characters; a self-hosted
         // proxy may echo the whole token), and this message is logged
         // by callers. Drop those bodies entirely, and scrub any literal
         // occurrence of the key from the rest.
         let detail: string;
         if (response.status === 401 || response.status === 403) {
            detail = "authentication failed; check EMBEDDING_API_KEY";
         } else {
            const bodyText = await response.text().catch(() => "");
            detail = bodyText
               .split(this.config.apiKey)
               .join("[REDACTED]")
               .slice(0, 200);
         }
         throw new Error(
            `Embedding request to ${url} failed (${response.status}): ${detail}`,
         );
      }

      const json = (await response.json()) as {
         data?: EmbeddingResponseItem[];
      };
      const data = json?.data;
      if (!Array.isArray(data) || data.length !== inputs.length) {
         throw new Error(
            `Embedding response from ${url} malformed: expected ${inputs.length} embeddings, got ${Array.isArray(data) ? data.length : "none"}`,
         );
      }

      // The response documents per-item `index` fields; place by index
      // defensively rather than assuming order.
      const vectors = new Array<number[] | undefined>(inputs.length);
      for (let i = 0; i < data.length; i++) {
         const item = data[i];
         const idx = typeof item?.index === "number" ? item.index : i;
         if (
            !Array.isArray(item?.embedding) ||
            item.embedding.length === 0 ||
            idx < 0 ||
            idx >= inputs.length ||
            vectors[idx] !== undefined
         ) {
            throw new Error(
               `Embedding response from ${url} malformed at item ${i}`,
            );
         }
         vectors[idx] = item.embedding;
      }
      return vectors as number[][];
   }
}

// Cached on a config fingerprint, never on null: a call after the env
// changes (tests, operator restarts with new vars are moot, but the
// integration suite runs many specs in one process) always sees the
// current configuration instead of a stale provider or a sticky "off".
let cached: { fingerprint: string; provider: EmbeddingProvider } | null = null;
let testOverride: { provider: EmbeddingProvider | null } | null = null;

/**
 * Whether the operator has turned the embedding feature on at all.
 * Malformed companion vars still count as configured: the operator
 * clearly intends the feature, so the retrieval marker and a warning
 * must surface rather than silently reporting plain lexical.
 */
export function embeddingConfigured(): boolean {
   if (testOverride) {
      return testOverride.provider !== null;
   }
   try {
      return getEmbeddingConfig() !== null;
   } catch {
      return true;
   }
}

/**
 * The process-wide provider for the current embedding configuration, or
 * null when `EMBEDDING_API_KEY` is unset. Throws on malformed companion
 * env vars (see getEmbeddingConfig); callers on the tool path catch and
 * degrade.
 */
export function getEmbeddingProvider(): EmbeddingProvider | null {
   if (testOverride) {
      return testOverride.provider;
   }
   const config = getEmbeddingConfig();
   if (!config) {
      cached = null;
      return null;
   }
   const fingerprint = [
      config.baseUrl,
      config.model,
      config.dimensions ?? "",
      config.apiKey,
   ].join("\u0000");
   if (!cached || cached.fingerprint !== fingerprint) {
      cached = { fingerprint, provider: new EmbeddingProvider(config) };
   }
   return cached.provider;
}

/** Test seam: force the provider (or null). Undo with _clear...(). */
export function _setEmbeddingProviderForTests(
   provider: EmbeddingProvider | null,
): void {
   testOverride = { provider };
}

export function _clearEmbeddingProviderForTests(): void {
   testOverride = null;
   cached = null;
}
