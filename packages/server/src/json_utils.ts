/**
 * JSON.stringify replacer that renders BigInt as a number.
 *
 * Load-bearing wherever raw driver output is serialized. `compactResult` is
 * `queryResults.data.value` straight from the connector, and DuckDB returns
 * `count()` as a BigInt, so JSON.stringify throws
 * `TypeError: Do not know how to serialize a BigInt` on the single most common
 * query anyone writes. The wrapped `Malloy.Result` path does not need this,
 * because API.util.wrapResult has already normalized values into typed cells.
 *
 * Lossy above Number.MAX_SAFE_INTEGER. Pre-existing behavior, kept as-is so the
 * MCP and REST paths agree; a row count large enough to matter is not reachable
 * under the response caps.
 */
export function bigIntReplacer(_key: string, value: unknown): unknown {
   if (typeof value === "bigint") {
      return Number(value);
   }
   return value;
}
