/**
 * JSON.stringify replacer for raw driver output, which can carry BigInt.
 *
 * Load-bearing wherever `compactResult` is serialized: it is
 * `queryResults.data.value` straight from the connector, and a DuckDB BIGINT
 * column arrives as a JS BigInt, so a plain JSON.stringify throws
 * `TypeError: Do not know how to serialize a BigInt`. The wrapped
 * `Malloy.Result` path does not need this, because API.util.wrapResult has
 * already normalized values into typed cells.
 *
 * Values inside the safe integer range become numbers, which is what callers
 * have always received and what keeps the MCP and REST payloads byte-identical.
 * Outside it `Number()` rounds silently: 9007199254740993 becomes ...992, and an
 * agent that filters or joins on that id gets a wrong or empty answer with no
 * error. Those are emitted as strings instead, which is what Malloy itself does
 * for the same case (API.util.wrapResult attaches an exact `string_value`
 * beside the lossy `number_value` whenever the subtype is bigint).
 *
 * So the JSON type varies with magnitude. That is deliberate: a column only
 * yields a string for a value a JSON number cannot represent, and every such
 * value was previously being corrupted, so nothing a caller correctly receives
 * today changes.
 */
const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);

export function bigIntReplacer(_key: string, value: unknown): unknown {
   if (typeof value === "bigint") {
      return value > MAX_SAFE_BIGINT || value < -MAX_SAFE_BIGINT
         ? value.toString()
         : Number(value);
   }
   return value;
}
