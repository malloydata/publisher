// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * JSON.stringify replacer for raw driver output, which can carry BigInt.
 *
 * Required wherever `compactResult` is serialized: it is connector output, and
 * a DuckDB BIGINT arrives as a BigInt, which plain JSON.stringify rejects.
 *
 * Safe-range values become numbers. Larger ones become strings, as Malloy's
 * own `wrapResult` does, because `Number()` would round them silently and an
 * agent joining on that id would get a wrong answer. The JSON type therefore
 * varies with magnitude, deliberately.
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
