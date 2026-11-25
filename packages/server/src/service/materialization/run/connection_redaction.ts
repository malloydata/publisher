// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An error message with the credential values of the connections it may
 * echo removed, for a record callers read and a log an operator reads.
 * Pure.
 */

import { escapeSQL } from "../../duckdb_session_setup";
import { redactPgSecrets, redactStorageUrls } from "../../../pg_helpers";

/** Connection-config keys whose string values are credentials to redact. */
const SENSITIVE_KEY =
   /pass(word)?|secret|private_?key|service_?account|access_?key|token|connection_?string|account/i;

/** Collect credential string values (from sensitively-named keys) in a config. */
function collectSensitiveValues(
   value: unknown,
   out: Set<string>,
   seen: WeakSet<object> = new WeakSet(),
): void {
   if (value === null || typeof value !== "object") return;
   // Callers pass a LIVE connection whose driver/pool graph can be cyclic;
   // without this guard the stack overflow would replace the error being
   // redacted. Mirrors the guard in redactSensitive.
   if (seen.has(value)) return;
   seen.add(value);
   if (Array.isArray(value)) {
      for (const v of value) collectSensitiveValues(v, out, seen);
      return;
   }
   for (const [key, v] of Object.entries(value)) {
      if (typeof v === "string" && v.length >= 4 && SENSITIVE_KEY.test(key)) {
         out.add(v);
      } else {
         collectSensitiveValues(v, out, seen);
      }
   }
}

/**
 * Redact the concrete credential values found in the given connections out of
 * an error message, keeping the rest legible ("schema not found" still tells
 * the operator what to fix) while never leaking passwords, secrets, service
 * account JSON or connection strings a federation or attach error can echo.
 */
export function redactConnectionSecrets(
   message: string,
   ...connections: unknown[]
): string {
   const secrets = new Set<string>();
   // One `seen` across arguments: a source and destination may share a subgraph.
   const seen = new WeakSet<object>();
   for (const c of connections) collectSensitiveValues(c, secrets, seen);
   // A storage URL (bucket, organization and database paths) too: the result
   // is recorded on the run, which callers read.
   let redacted = redactStorageUrls(redactPgSecrets(message));
   for (const s of secrets) {
      redacted = redacted.split(s).join("***");
      // A DuckDB error may echo the SQL, where a secret with a single quote
      // appears escaped (`''`), so redact the escaped form too.
      const escaped = escapeSQL(s);
      if (escaped !== s) redacted = redacted.split(escaped).join("***");
   }
   return redacted;
}
