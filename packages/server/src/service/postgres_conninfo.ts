// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A Postgres connection's settings as the drivers read them: the TLS modes and
 * statement timeout it may set, the node-pg connection string the query path
 * opens, the node-pg client a load's advisory lock opens, and the libpq conninfo a DuckDB `TYPE postgres` attach opens (direct,
 * or through the SSH tunnel of a proxied connection), with the credentials
 * moved into a DuckDB secret so no attach path carries one.
 *
 * A leaf: it imports no connection module, so the connection-type registry
 * (service/dialects/) and connection.ts can both use it.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { createHash } from "crypto";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "fs";
import os from "os";
import path from "path";
import { Client, type ClientConfig } from "pg";
import tls from "tls";
import { components } from "../api";
import { logger } from "../logger";
import { externalEnv, type ExternalVariable } from "../settings";
import { escapeSQL } from "./duckdb_session_setup";
import type { ProxyEndpoint } from "./proxy";

// TLS modes accepted on a postgres connection, proxied or direct, so
// config-load validation and the connect-time builders share one list.
// Mirrors `sslmode` in api-doc.yaml.
export const PROXIED_SSLMODES = [
   "disable",
   "no-verify",
   "verify-ca",
   "verify-full",
] as const;

/**
 * Throws unless `sslmode` is one of PROXIED_SSLMODES and, for `verify-ca`, a
 * readable CA bundle is available to pin it against. `subject` leads each
 * error message.
 */
export function validatePostgresSslmode(
   subject: string,
   sslmode: string,
): void {
   if (!(PROXIED_SSLMODES as readonly string[]).includes(sslmode)) {
      throw new Error(
         `${subject} has unsupported sslmode '${sslmode}' ` +
            `(expected ${PROXIED_SSLMODES.join(" | ")}).`,
      );
   }
   if (sslmode === "verify-ca") {
      const caBundle = externalEnv("NODE_EXTRA_CA_CERTS");
      if (!caBundle || !existsSync(caBundle)) {
         throw new Error(
            `${subject} uses sslmode 'verify-ca' but no readable ` +
               `CA bundle is available (NODE_EXTRA_CA_CERTS is unset or points at a missing file). ` +
               `Add the CA bundle to the image or use sslmode 'no-verify'.`,
         );
      }
   }
}

// Postgres's own ceiling for statement_timeout, which is an int in ms. A larger
// value makes the session SET fail on every connection that applies it.
const POSTGRES_MAX_STATEMENT_TIMEOUT_MS = 2_147_483_647;

/**
 * The connection's statement timeout, validated. Returns undefined when unset.
 *
 * Every consumer interpolates this value into SQL (`SET statement_timeout`) or
 * a libpq conninfo string, so anything but an integer from 1 to
 * POSTGRES_MAX_STATEMENT_TIMEOUT_MS is rejected here rather than trusted: a JSON
 * body is not held to the schema before it reaches this code.
 */
export function postgresStatementTimeoutMs(
   name: string,
   pg: components["schemas"]["PostgresConnection"],
): number | undefined {
   const value: unknown = pg.statementTimeoutMilliseconds;
   if (value === undefined || value === null) return undefined;
   if (
      typeof value !== "number" ||
      !Number.isInteger(value) ||
      value < 1 ||
      value > POSTGRES_MAX_STATEMENT_TIMEOUT_MS
   ) {
      throw new Error(
         `Connection '${name}' has an invalid statementTimeoutMilliseconds ` +
            `${JSON.stringify(value)} (expected an integer from 1 to ${POSTGRES_MAX_STATEMENT_TIMEOUT_MS}).`,
      );
   }
   return value;
}

/**
 * The pg connectionString query params for a direct connection's own sslmode.
 * Same meanings as the proxied query path (buildProxiedSslQuery): `no-verify`
 * encrypts without verifying, `verify-ca` pins the chain to NODE_EXTRA_CA_CERTS
 * without the hostname (libpq-compatible parsing is what makes pg honor that),
 * and `verify-full` checks the chain and the real hostname.
 */
function directSslParams(
   sslmode: (typeof PROXIED_SSLMODES)[number],
): Record<string, string> {
   switch (sslmode) {
      case "disable":
      case "no-verify":
      case "verify-full":
         return { sslmode };
      case "verify-ca":
         return {
            uselibpqcompat: "true",
            sslmode: "verify-ca",
            sslrootcert: externalEnv("NODE_EXTRA_CA_CERTS") ?? "",
         };
   }
}

/**
 * The connectionString a postgres connection is opened with, or undefined when
 * the individual host/port/user/password/database fields should be used as-is.
 *
 * A raw `connectionString` always wins. Otherwise the sslmode is the
 * connection's own `sslmode` when `applySslmode` is set and the field is
 * present, else the deployment PGSSLMODE, else none. `applySslmode` is false
 * for a proxied connection, which applies its sslmode on the tunnel instead
 * and must not have it baked into a string that targets the real host.
 */
export function buildPostgresConnectionString(
   config: components["schemas"]["PostgresConnection"],
   { applySslmode }: { applySslmode: boolean },
): string | undefined {
   if (config.connectionString) {
      return config.connectionString;
   }

   const params = new URLSearchParams();
   const pgSslmode = externalEnv("PGSSLMODE");
   if (applySslmode && config.sslmode != null) {
      for (const [key, value] of Object.entries(
         directSslParams(config.sslmode),
      )) {
         params.set(key, value);
      }
   } else if (pgSslmode) {
      params.set("sslmode", pgSslmode);
   } else {
      return undefined;
   }
   const auth =
      config.userName && config.password
         ? `${encodeURIComponent(config.userName)}:${encodeURIComponent(
              config.password,
           )}@`
         : config.userName
           ? `${encodeURIComponent(config.userName)}@`
           : "";
   const host = config.host ?? "localhost";
   const port = config.port ? `:${config.port}` : "";
   const database = config.databaseName
      ? `/${encodeURIComponent(config.databaseName)}`
      : "";
   return `postgresql://${auth}${host}${port}${database}?${params.toString()}`;
}

// A server switch that sets statement_timeout: `-cstatement_timeout=N` or
// `--statement_timeout=N` (Postgres reads `-` in a switch name as `_`). The
// two-token `-c statement_timeout=N` form is handled where tokens are walked.
const STATEMENT_TIMEOUT_SWITCH = /^(?:-c|--)statement[_-]timeout=/i;

/**
 * libpq `options` with every statement_timeout switch removed and the
 * connection's own appended, so the string carries exactly one. Other switches
 * are kept in order. Splits on unescaped whitespace, as the server does.
 */
function mergeStatementTimeoutOption(
   existing: string,
   ms: number,
): { value: string; replaced: boolean } {
   const tokens = existing.split(/(?<!\\)\s+/).filter((t) => t !== "");
   const kept: string[] = [];
   let replaced = false;
   for (let i = 0; i < tokens.length; i++) {
      if (
         tokens[i] === "-c" &&
         /^statement[_-]timeout=/i.test(tokens[i + 1] ?? "")
      ) {
         i++;
         replaced = true;
      } else if (STATEMENT_TIMEOUT_SWITCH.test(tokens[i])) {
         replaced = true;
      } else {
         kept.push(tokens[i]);
      }
   }
   kept.push("-c", `statement_timeout=${ms}`);
   return { value: kept.join(" "), replaced };
}

/**
 * The key/value pairs of a libpq keyword/value conninfo string, with each pair's
 * span so it can be replaced in place. Values are unquoted and unescaped.
 * Stops at the first token that is not a `key=value` pair.
 */
function conninfoPairs(
   conninfo: string,
): { key: string; value: string; start: number; end: number }[] {
   const pairs: { key: string; value: string; start: number; end: number }[] =
      [];
   // ASCII only, as libpq: a Unicode space inside a password is password data.
   const isSpace = (c: string | undefined) =>
      c !== undefined && /[ \t\n\v\f\r]/.test(c);
   let i = 0;
   while (i < conninfo.length) {
      while (isSpace(conninfo[i])) i++;
      if (i >= conninfo.length) break;
      const start = i;
      let key = "";
      while (
         i < conninfo.length &&
         conninfo[i] !== "=" &&
         !isSpace(conninfo[i])
      ) {
         key += conninfo[i++];
      }
      while (isSpace(conninfo[i])) i++;
      if (conninfo[i] !== "=") break;
      i++;
      while (isSpace(conninfo[i])) i++;
      let value = "";
      if (conninfo[i] === "'") {
         i++;
         while (i < conninfo.length && conninfo[i] !== "'") {
            if (conninfo[i] === "\\" && i + 1 < conninfo.length) i++;
            value += conninfo[i++];
         }
         i++;
      } else {
         while (i < conninfo.length && !isSpace(conninfo[i])) {
            if (conninfo[i] === "\\" && i + 1 < conninfo.length) i++;
            value += conninfo[i++];
         }
      }
      pairs.push({ key, value, start, end: Math.min(i, conninfo.length) });
   }
   return pairs;
}

function decodeUriComponentOrRaw(value: string): string {
   try {
      return decodeURIComponent(value);
   } catch {
      return value;
   }
}

/** libpq has no `no-verify`; "encrypt, don't verify" is `require`. The one spelling every site maps to. */
const LIBPQ_NO_VERIFY_SSLMODE = "require";

const isUriConnectionString = (connectionString: string) =>
   /^postgres(ql)?:\/\//i.test(connectionString);

/** Applies `fn` to a URI's query parameters; the `?` is dropped when none remain. */
function mapUriQuery(uri: string, fn: (params: string[]) => string[]): string {
   const queryStart = uri.indexOf("?");
   const base = queryStart === -1 ? uri : uri.slice(0, queryStart);
   const params = fn(
      queryStart === -1 ? [] : uri.slice(queryStart + 1).split("&"),
   );
   return params.length ? `${base}?${params.join("&")}` : base;
}

/**
 * Makes a raw connectionString's sslmode one libpq accepts: `no-verify` becomes
 * `require`, and a string with no sslmode gets `require` when the deployment's
 * PGSSLMODE is `no-verify`, because libpq reads that variable itself and rejects it.
 * A string naming a libpq `service`, or a deployment `PGSERVICE`, is left alone: its
 * file's sslmode outranks the variable, and an appended `require` would override a
 * stricter one (a service file with no sslmode then fails closed under `no-verify`).
 * In a URI, node-pg-only params libpq rejects are normalized too: `uselibpqcompat` is
 * dropped, and `ssl` yields to an `sslmode` (node-pg's precedence) or is respelled.
 */
function libpqSslmodeInRawString(connectionString: string): string {
   const envNoVerify =
      externalEnv("PGSSLMODE")?.toLowerCase() === "no-verify" &&
      !externalEnv("PGSERVICE");
   const rewritten = `sslmode=${LIBPQ_NO_VERIFY_SSLMODE}`;
   if (isUriConnectionString(connectionString)) {
      return mapUriQuery(connectionString, (params) => {
         const keys = params.map((param) =>
            decodeUriComponentOrRaw(param.split("=")[0]),
         );
         const hasSslmode = keys.includes("sslmode");
         // node-pg turns ssl=false into verified TLS when a cert param is present.
         const hasCert = keys.some((key) =>
            ["sslrootcert", "sslcert", "sslkey"].includes(key),
         );
         let seen = false;
         const mapped = params.flatMap((param, i) => {
            const [, ...rest] = param.split("=");
            const value = decodeUriComponentOrRaw(rest.join("="));
            if (keys[i] === "service") seen = true;
            if (keys[i] === "uselibpqcompat") return [];
            if (keys[i] === "ssl") {
               if (hasSslmode) return [];
               if (value === "no-verify") {
                  seen = true;
                  return [rewritten];
               }
               if (value === "1") return ["ssl=true"];
               if (value === "0") {
                  if (hasCert) return [];
                  seen = true;
                  return ["sslmode=disable"];
               }
               return [param];
            }
            if (keys[i] !== "sslmode") return [param];
            seen = true;
            return [value === "no-verify" ? rewritten : param];
         });
         return !seen && envNoVerify ? [...mapped, rewritten] : mapped;
      });
   }
   const pairs = conninfoPairs(connectionString);
   let result = connectionString;
   for (const pair of [...pairs].reverse()) {
      if (pair.key === "sslmode" && pair.value === "no-verify") {
         result =
            result.slice(0, pair.start) + rewritten + result.slice(pair.end);
      }
   }
   return !pairs.some(
      (pair) => pair.key === "sslmode" || pair.key === "service",
   ) && envNoVerify
      ? `${result} ${rewritten}`
      : result;
}

/**
 * A raw connectionString with `sslmode=no-verify` rewritten to `require` (node-pg
 * spells "encrypt, don't verify" that way; libpq rejects it) and the connection's
 * statement timeout added, in whichever of libpq's two forms the string is
 * written: a URI takes the timeout as an `options` query parameter, a
 * keyword/value string as an `options` pair.
 *
 * libpq honors only the last `options`, so the timeout is merged into it
 * (adding another would drop the string's other server settings). A
 * statement_timeout the string already sets is replaced with a warning: the
 * explicit field wins, as on the query driver.
 */
function libpqConnectionString(
   name: string,
   rawConnectionString: string,
   pg: components["schemas"]["PostgresConnection"],
): string {
   const connectionString = libpqSslmodeInRawString(rawConnectionString);
   const ms = postgresStatementTimeoutMs(name, pg);
   if (ms === undefined) return connectionString;
   const merged = (existing: string | undefined): string => {
      if (existing === undefined) return `-c statement_timeout=${ms}`;
      const { value, replaced } = mergeStatementTimeoutOption(existing, ms);
      if (replaced) {
         logger.warn(
            `Connection '${name}' sets statementTimeoutMilliseconds and its connectionString's ` +
               `options also set statement_timeout; statementTimeoutMilliseconds applies.`,
         );
      }
      return value;
   };

   if (isUriConnectionString(connectionString)) {
      const isOptions = (param: string) =>
         decodeUriComponentOrRaw(param.split("=", 1)[0]) === "options";
      return mapUriQuery(connectionString, (params) => {
         const last = params.map(isOptions).lastIndexOf(true);
         if (last === -1) {
            return [
               ...params,
               `options=${encodeURIComponent(merged(undefined))}`,
            ];
         }
         const raw = params[last].slice(params[last].indexOf("=") + 1);
         const out = [...params];
         out[last] =
            `options=${encodeURIComponent(merged(decodeUriComponentOrRaw(raw)))}`;
         return out;
      });
   }

   const existing = conninfoPairs(connectionString)
      .filter((pair) => pair.key === "options")
      .pop();
   if (!existing) {
      return `${connectionString} ${pgConninfoPair("options", merged(undefined))}`;
   }
   return (
      connectionString.slice(0, existing.start) +
      pgConninfoPair("options", merged(existing.value)) +
      connectionString.slice(existing.end)
   );
}

/**
 * The libpq conninfo a DuckDB `TYPE postgres` attach opens a postgres connection
 * with.
 *
 * A connection given as fields uses its own `sslmode` when it sets one, and the
 * deployment PGSSLMODE otherwise. `applyConnectionOptions` decides whether
 * `statementTimeoutMilliseconds` reaches the string: false for a DuckLake
 * catalog, true for an attached database and a federated source. A raw
 * connectionString keeps its own settings (and, for a catalog, ignores the
 * `sslmode` field), but `sslmode=no-verify` (or an inherited
 * PGSSLMODE=no-verify) becomes `require`, which libpq accepts.
 *
 * Exported for tests.
 */
export function buildPgConnectionString(
   pg: components["schemas"]["PostgresConnection"],
   {
      name,
      applyConnectionOptions,
   }: { name: string; applyConnectionOptions: boolean },
): string {
   if (pg.connectionString) {
      if (!applyConnectionOptions) {
         return libpqSslmodeInRawString(pg.connectionString);
      }
      if (pg.sslmode != null) {
         logger.warn(
            `Connection '${name}' sets both postgresConnection.sslmode and ` +
               `connectionString; sslmode is ignored and the connectionString's own sslmode applies.`,
         );
      }
      return libpqConnectionString(name, pg.connectionString, pg);
   }

   const parts: string[] = [];
   if (pg.host) parts.push(pgConninfoPair("host", pg.host));
   if (pg.port) parts.push(pgConninfoPair("port", String(pg.port)));
   if (pg.databaseName) parts.push(pgConninfoPair("dbname", pg.databaseName));
   if (pg.userName) parts.push(pgConninfoPair("user", pg.userName));
   if (pg.password) parts.push(pgConninfoPair("password", pg.password));

   if (applyConnectionOptions) {
      parts.push(...libpqStatementTimeoutParts(name, pg));
   }
   // A DuckLake catalog honors its own sslmode too: libpq keeps the last
   // sslmode it reads, so falling through to PGSSLMODE (allow on a worker)
   // would make every catalog attach try plaintext first.
   if (pg.sslmode != null) {
      parts.push(
         ...libpqSslmodeParts(
            `Connection '${name}'`,
            pg.sslmode,
            defaultProxiedAttachTrust(),
         ),
      );
      return parts.join(" ");
   }

   const pgSSLMode = externalEnv("PGSSLMODE");

   if (pgSSLMode) {
      const mapping: Record<string, string> = {
         "no-verify": LIBPQ_NO_VERIFY_SSLMODE,
         disable: "disable",
         allow: "allow",
         prefer: "prefer",
         require: "require",
         "verify-ca": "verify-ca",
         "verify-full": "verify-full",
      };

      const sslmode = mapping[pgSSLMode.toLowerCase()];
      if (sslmode) parts.push(`sslmode=${sslmode}`);
   }

   return parts.join(" ");
}

/**
 * One libpq conninfo keyword/value pair. libpq takes a bare value only when it
 * has no whitespace, quote or backslash; any other value is single-quoted with
 * `\` and `'` backslash-escaped, so a password containing a space is not read
 * as a second keyword. Exported for tests.
 */
export function pgConninfoPair(key: string, value: string): string {
   if (value !== "" && !/[\s'\\]/.test(value)) return `${key}=${value}`;
   return `${key}='${value.replace(/([\\'])/g, "\\$1")}'`;
}

/** The keywords a Postgres attach carries in its secret rather than its path. */
const PG_SECRET_KEYS = new Set(["user", "password", "sslpassword"]);

/**
 * The keyword/value pairs of a libpq connection URI, as libpq reads one
 * (`postgres[ql]://[user[:password]@][host[:port][,…]][/dbname][?k=v&…]`),
 * percent-decoded. A `ssl=true`, `ssl=1` or `ssl=no-verify` parameter becomes
 * `sslmode=require` unless the URI has an `sslmode`, as the attach reads them.
 * Undefined when `uri` is not a URI.
 */
function pgUriPairs(uri: string): { key: string; value: string }[] | undefined {
   const scheme = /^postgres(?:ql)?:\/\//i.exec(uri);
   if (!scheme) return undefined;
   const s = uri.slice(scheme[0].length);
   const pairs: { key: string; value: string }[] = [];
   let i = 0;
   // libpq: the userinfo ends at the first `@` before any `/`.
   let at = 0;
   while (at < s.length && s[at] !== "@" && s[at] !== "/") at++;
   if (s[at] === "@") {
      const userinfo = s.slice(0, at);
      const colon = userinfo.indexOf(":");
      const user = colon === -1 ? userinfo : userinfo.slice(0, colon);
      if (user !== "") {
         pairs.push({ key: "user", value: decodeUriComponentOrRaw(user) });
      }
      if (colon !== -1) {
         pairs.push({
            key: "password",
            value: decodeUriComponentOrRaw(userinfo.slice(colon + 1)),
         });
      }
      i = at + 1;
   }
   const hosts: string[] = [];
   const ports: string[] = [];
   for (;;) {
      let host = "";
      if (s[i] === "[") {
         const close = s.indexOf("]", i);
         const end = close === -1 ? s.length : close;
         host = s.slice(i + 1, end);
         i = close === -1 ? s.length : close + 1;
      } else {
         const start = i;
         while (i < s.length && !":/?,".includes(s[i])) i++;
         host = s.slice(start, i);
      }
      let port = "";
      if (s[i] === ":") {
         const start = ++i;
         while (i < s.length && !"/?,".includes(s[i])) i++;
         port = s.slice(start, i);
      }
      hosts.push(decodeUriComponentOrRaw(host));
      ports.push(decodeUriComponentOrRaw(port));
      if (s[i] !== ",") break;
      i++;
   }
   if (hosts.some((host) => host !== "")) {
      pairs.push({ key: "host", value: hosts.join(",") });
   }
   if (ports.some((port) => port !== "")) {
      pairs.push({ key: "port", value: ports.join(",") });
   }
   if (s[i] === "/") {
      const start = ++i;
      while (i < s.length && s[i] !== "?") i++;
      const dbname = s.slice(start, i);
      if (dbname !== "") {
         pairs.push({ key: "dbname", value: decodeUriComponentOrRaw(dbname) });
      }
   }
   if (s[i] === "?") {
      const params = s.slice(i + 1).split("&");
      const hasSslmode = params.some(
         (param) => decodeUriComponentOrRaw(param.split("=")[0]) === "sslmode",
      );
      for (const param of params) {
         if (param === "") continue;
         const eq = param.indexOf("=");
         const key = decodeUriComponentOrRaw(
            eq === -1 ? param : param.slice(0, eq),
         );
         const value = decodeUriComponentOrRaw(
            eq === -1 ? "" : param.slice(eq + 1),
         );
         // node-pg's `ssl` spellings, which yield to an `sslmode` as in libpqSslmodeInRawString.
         if (key === "ssl" && ["true", "1", "no-verify"].includes(value)) {
            if (!hasSslmode) {
               pairs.push({ key: "sslmode", value: LIBPQ_NO_VERIFY_SSLMODE });
            }
         } else {
            pairs.push({ key, value });
         }
      }
   }
   return pairs;
}

/**
 * The keyword/value pairs of a libpq connection string in either form, a
 * `dbname` that is itself a connection string expanded in place as libpq
 * expands it. Throws, naming `subject` but not the string, when the string
 * does not read as one.
 */
function pgConninfoKeyValues(
   subject: string,
   conninfo: string,
   depth = 0,
): { key: string; value: string }[] {
   const fromUri = pgUriPairs(conninfo.trim());
   const pairs =
      fromUri ??
      (() => {
         const read = conninfoPairs(conninfo);
         const consumed = read.length === 0 ? 0 : read[read.length - 1].end;
         if (conninfo.slice(consumed).trim() !== "") {
            throw new Error(
               `${subject}: the Postgres connection string could not be read as libpq keyword/value pairs or a postgres:// URI.`,
            );
         }
         return read.map(({ key, value }) => ({ key, value }));
      })();
   return pairs.flatMap((pair) =>
      pair.key === "dbname" &&
      depth < 2 &&
      (pair.value.includes("=") || /^postgres(?:ql)?:\/\//i.test(pair.value))
         ? pgConninfoKeyValues(subject, pair.value, depth + 1)
         : [pair],
   );
}

/** `host:port/dbname` with sorted hosts and no PGPORT or PGHOST default; undefined when unsure, so two databases it cannot tell apart never compare equal. */
export function pgDatabaseTarget(conninfo: string): string | undefined {
   const params = new Map<string, string>();
   try {
      for (const { key, value } of pgConninfoKeyValues("", conninfo)) {
         params.set(key, value);
      }
   } catch {
      return undefined;
   }
   const dbname = params.get("dbname");
   const hosts = params.get("host")?.split(",");
   const ports = (params.get("port") || "5432").split(",");
   if (
      !dbname ||
      !hosts ||
      hosts.some((host) => host === "") ||
      params.has("service") ||
      params.has("hostaddr") ||
      ports.some((port) => !/^\d+$/.test(port)) ||
      (ports.length !== 1 && ports.length !== hosts.length)
   ) {
      return undefined;
   }
   return `${hosts
      .map(
         (host, i) =>
            `${host.toLowerCase()}:${Number(ports[ports.length === 1 ? 0 : i])}`,
      )
      .sort()
      .join(",")}/${dbname}`;
}

/**
 * A Postgres connection string split for a DuckDB attach: `path` carries
 * every keyword but the user, password and client-key password
 * (`sslpassword`), which go to a DuckDB secret
 * instead ({@link createPgAttachSecret}), so an attach path carries no
 * credential. libpq reads the last of a repeated keyword, and so does this.
 * Exported for tests.
 */
export function splitPgAttachCredentials(
   subject: string,
   conninfo: string,
): { path: string; user?: string; password?: string; sslpassword?: string } {
   let user: string | undefined;
   let password: string | undefined;
   let sslpassword: string | undefined;
   const kept: string[] = [];
   for (const { key, value } of pgConninfoKeyValues(subject, conninfo)) {
      if (key === "user") user = value;
      else if (key === "password") password = value;
      else if (key === "sslpassword") sslpassword = value;
      else if (!PG_SECRET_KEYS.has(key)) kept.push(pgConninfoPair(key, value));
   }
   return { path: kept.join(" "), user, password, sslpassword };
}

/**
 * libpq settings node-pg cannot follow, each with the variable libpq falls back
 * to and the values that demand more than node-pg gives; `disable`, `prefer`
 * (libpq's default for `channel_binding` and `gssencmode`) and a TLS minimum
 * Node already meets (its default is TLSv1.2) ask for nothing it lacks.
 */
const UNHONORED_SETTINGS: ReadonlyArray<{
   key: string;
   env?: ExternalVariable;
   demands: (value: string) => boolean;
}> = [
   // node-pg reads a password file from PGPASSFILE only, so the key would be dropped.
   { key: "passfile", demands: (v) => v !== "" },
   {
      key: "channel_binding",
      env: "PGCHANNELBINDING",
      demands: (v) => v.toLowerCase() === "require",
   },
   { key: "require_auth", env: "PGREQUIREAUTH", demands: (v) => v !== "" },
   {
      key: "gssencmode",
      env: "PGGSSENCMODE",
      demands: (v) => v.toLowerCase() === "require",
   },
   { key: "sslcrl", env: "PGSSLCRL", demands: (v) => v !== "" },
   { key: "sslcrldir", env: "PGSSLCRLDIR", demands: (v) => v !== "" },
   {
      key: "ssl_min_protocol_version",
      env: "PGSSLMINPROTOCOLVERSION",
      demands: (v) => v.toLowerCase() === "tlsv1.3",
   },
];

/**
 * A node-pg client config for a libpq connection string, for work the DuckDB
 * attach cannot do (a session advisory lock). It reads the string as libpq
 * does, the deployment's PGSSLMODE and certificate files included (the string
 * wins), and refuses what it cannot follow: a `service`, a `hostaddr` (in the
 * string or as PGSERVICE / PGHOSTADDR), or a list of hosts, none of which a
 * single node-pg connection can take, and a security setting node-pg would
 * ignore ({@link UNHONORED_SETTINGS}), from the string or the environment. The
 * TLS mapping is {@link pgClientSsl}. A dead connection is noticed by keepalive, and a query that stops
 * answering by its timeout. Exported for tests.
 */
export function pgClientConfig(
   subject: string,
   conninfo: string,
): ClientConfig & { plaintextFallback: boolean } {
   const params = new Map<string, string>();
   for (const { key, value } of pgConninfoKeyValues(subject, conninfo)) {
      params.set(key, value);
   }
   const unsupported = ["service", "hostaddr"].find((key) => params.has(key));
   const ambient = (["PGSERVICE", "PGHOSTADDR"] as const).find((name) =>
      externalEnv(name),
   );
   const listed = ["host", "port"].find((key) =>
      params.get(key)?.includes(","),
   );
   if (ambient) {
      throw new Error(
         `${subject}: ${ambient} is set in the environment, which loads cannot use; unset it and spell host, port and dbname.`,
      );
   }
   if (unsupported || listed) {
      throw new Error(
         `${subject}: the connection string sets ${unsupported ? `'${unsupported}'` : `more than one '${listed}'`}, which loads cannot use; spell host, port and dbname.`,
      );
   }
   for (const { key, env, demands } of UNHONORED_SETTINGS) {
      const inString = params.has(key);
      const value = inString
         ? params.get(key)
         : env === undefined
           ? undefined
           : externalEnv(env);
      if (value && demands(value)) {
         throw new Error(
            inString
               ? `${subject}: the connection string sets '${key}', which the load lock's client cannot honor; remove it.`
               : `${subject}: ${env} is set in the environment, which the load lock's client cannot honor; unset it.`,
         );
      }
   }
   const port = params.get("port") || undefined;
   for (const [key, name] of [
      ["sslrootcert", "PGSSLROOTCERT"],
      ["sslcert", "PGSSLCERT"],
      ["sslkey", "PGSSLKEY"],
   ] as const) {
      const file = externalEnv(name);
      if (!params.has(key) && file) params.set(key, file);
   }
   const requested = params.get("sslmode") ?? externalEnv("PGSSLMODE");
   const systemRoots = params.get("sslrootcert") === "system";
   // libpq 16: public roots vouch for any hostname, so `sslrootcert=system` allows only verify-full.
   const sslmode =
      requested === "no-verify"
         ? LIBPQ_NO_VERIFY_SSLMODE
         : (requested ?? (systemRoots ? "verify-full" : undefined));
   if (systemRoots && sslmode !== "verify-full") {
      throw new Error(
         `${subject}: weak sslmode '${sslmode}' may not be used with sslrootcert=system (use 'verify-full').`,
      );
   }
   return {
      host: params.get("host") || undefined,
      port: port === undefined ? undefined : Number(port),
      database: params.get("dbname"),
      user: params.get("user"),
      password: params.get("password"),
      application_name: "publisher-load-lock",
      ssl: pgClientSsl(subject, sslmode, params),
      plaintextFallback: TLS_OPTIONAL_SSLMODES.has(sslmode),
      connectionTimeoutMillis: 10_000,
      keepAlive: true,
      keepAliveInitialDelayMillis: 10_000,
      query_timeout: 30_000,
   };
}

/**
 * A connected node-pg client for a libpq connection string. `onError` hears a
 * dropped idle connection, which unhandled would end the process.
 * `queryTimeoutMillis` replaces the default bound on a query's answer.
 */
export async function connectPgClient(
   subject: string,
   conninfo: string,
   onError: (error: Error) => void = () => undefined,
   queryTimeoutMillis?: number,
): Promise<Client> {
   const { plaintextFallback, ...config } = pgClientConfig(subject, conninfo);
   const connect = async (ssl: ClientConfig["ssl"]) => {
      const client = new Client({
         ...config,
         ssl,
         ...(queryTimeoutMillis ? { query_timeout: queryTimeoutMillis } : {}),
      });
      client.on("error", onError);
      await client.connect();
      return client;
   };
   try {
      return await connect(config.ssl);
   } catch (error) {
      // libpq's `prefer`: plaintext only when the server refuses TLS.
      if (plaintextFallback && /does not support SSL/i.test(String(error))) {
         return connect(false);
      }
      throw error;
   }
}

/** The sslmodes under which libpq tries TLS but accepts a server without it. */
const TLS_OPTIONAL_SSLMODES = new Set<string | undefined>([
   undefined,
   "allow",
   "prefer",
]);

/**
 * libpq's `sslmode` as a node-pg `ssl` option, with the connection string's
 * client certificate (`sslcert`, `sslkey`, and `sslpassword` as the key's
 * passphrase). node-pg cannot fall back from TLS to plaintext itself, so the
 * TLS-optional modes ask for TLS and the caller retries in plaintext only on
 * the server's refusal (`plaintextFallback`). `require` with an `sslrootcert`
 * verifies the chain, as libpq does. `verify-*` trust `sslrootcert`; without
 * one, `verify-full` trusts the runtime's ambient anchors and `verify-ca` only
 * NODE_EXTRA_CA_CERTS, as the DuckDB attach does ({@link libpqSslmodeParts}).
 */
export function pgClientSsl(
   subject: string,
   sslmode: string | undefined,
   params: ReadonlyMap<string, string>,
): ClientConfig["ssl"] {
   const sslrootcert = params.get("sslrootcert");
   const pem = (key: string, file: string) => {
      try {
         return readFileSync(file, "utf8");
      } catch (error) {
         throw new Error(
            `${subject}: cannot read the ${key} file '${file}' (${(error as NodeJS.ErrnoException).code ?? "unreadable"}).`,
         );
      }
   };
   // `system` reads Node's roots plus NODE_EXTRA_CA_CERTS, not OpenSSL's store as libpq does.
   const ca = () =>
      sslrootcert && sslrootcert !== "system"
         ? pem("sslrootcert", sslrootcert)
         : sslmode === "verify-ca"
           ? pem(
                "NODE_EXTRA_CA_CERTS",
                verifyCaBundle(subject, defaultProxiedAttachTrust()),
             )
           : readFileSync(ambientTrustBundlePath(), "utf8");
   const [sslcert, sslkey, passphrase] = [
      params.get("sslcert"),
      params.get("sslkey"),
      params.get("sslpassword"),
   ];
   const clientAuth = () => ({
      ...(sslcert ? { cert: pem("sslcert", sslcert) } : {}),
      ...(sslkey ? { key: pem("sslkey", sslkey) } : {}),
      ...(passphrase ? { passphrase } : {}),
   });
   const verifyChain = () => ({
      rejectUnauthorized: true,
      ca: ca(),
      checkServerIdentity: () => undefined,
      ...clientAuth(),
   });
   switch (sslmode) {
      case "disable":
         return false;
      case undefined:
      case "allow":
      case "prefer":
         return { rejectUnauthorized: false, ...clientAuth() };
      case "require":
         return sslrootcert
            ? verifyChain()
            : { rejectUnauthorized: false, ...clientAuth() };
      case "verify-ca":
         return verifyChain();
      case "verify-full":
         return { rejectUnauthorized: true, ca: ca(), ...clientAuth() };
      default:
         throw new Error(`${subject} has unsupported sslmode '${sslmode}'.`);
   }
}

/**
 * The name of the DuckDB secret a Postgres attach under `handle` reads its
 * credentials from: one per handle, unique however two handles sanitize.
 */
export function pgAttachSecretName(handle: string): string {
   const digest = createHash("sha256")
      .update(handle)
      .digest("hex")
      .slice(0, 12);
   return `pg_attach_${handle.replace(/[^a-zA-Z0-9_]/g, "_")}_${digest}`.toLowerCase();
}

/**
 * Prepare a DuckDB `TYPE postgres` attach of `conninfo` under `handle`: its
 * user, password and `sslpassword` go into a temporary DuckDB secret, never persisted and
 * redacted when listed, and the returned `path` is what the ATTACH names,
 * with no credential in it. `secret` is the secret's name, for the ATTACH's
 * `SECRET` option (or DuckLake's `METADATA_PARAMETERS`); undefined when the
 * string carries none of them. Runs before an instance's
 * lock, as every attach does.
 */
export async function createPgAttachSecret(
   connection: DuckDBConnection,
   handle: string,
   conninfo: string,
): Promise<{ path: string; secret?: string }> {
   const { path, user, password, sslpassword } = splitPgAttachCredentials(
      `Connection '${handle}'`,
      conninfo,
   );
   if (
      user === undefined &&
      password === undefined &&
      sslpassword === undefined
   ) {
      return { path };
   }
   const secret = pgAttachSecretName(handle);
   const options = [
      "TYPE postgres",
      ...(user !== undefined ? [`USER '${escapeSQL(user)}'`] : []),
      ...(password !== undefined ? [`PASSWORD '${escapeSQL(password)}'`] : []),
      ...(sslpassword !== undefined
         ? [`SSLPASSWORD '${escapeSQL(sslpassword)}'`]
         : []),
   ];
   await connection.runSQL(
      `CREATE OR REPLACE TEMPORARY SECRET ${secret} (${options.join(", ")});`,
   );
   return { path, secret };
}

/**
 * The CA material a proxied storage build hands libpq. Injected so a test needs
 * neither the process environment nor a file on disk.
 */
export interface ProxiedAttachTrust {
   /** The deployment's trusted CA bundle (NODE_EXTRA_CA_CERTS), when set. */
   caBundle?: string;
   /**
    * Path of a bundle holding the runtime's ambient trust anchors — its bundled
    * roots plus `caBundle` — which is the set the query path verifies
    * `verify-full` against.
    */
   ambientBundle: () => string;
}

function defaultProxiedAttachTrust(): ProxiedAttachTrust {
   return {
      caBundle: externalEnv("NODE_EXTRA_CA_CERTS") || undefined,
      ambientBundle: ambientTrustBundlePath,
   };
}

const ambientTrustBundles = new Map<string | undefined, string>();

let ambientTrustDir: string | undefined;

/**
 * Writes the runtime's ambient trust anchors (`tls.rootCertificates` plus
 * NODE_EXTRA_CA_CERTS) to one PEM file for libpq's `sslrootcert`, which takes
 * exactly one file.
 *
 * Written by this process into a directory it created (`mkdtemp`, 0700), never
 * reused from a path that merely exists: a file at a predictable name would let
 * whoever wrote it choose the CAs a `verify-full` build trusts. An unreadable
 * NODE_EXTRA_CA_CERTS is skipped with a warning, as both runtimes do at
 * startup, so the build trusts what the query path does. Exported for tests.
 */
export function ambientTrustBundlePath(): string {
   const extra = externalEnv("NODE_EXTRA_CA_CERTS") || undefined;
   const cached = ambientTrustBundles.get(extra);
   if (cached) return cached;
   let extraPem: string | undefined;
   if (extra) {
      try {
         extraPem = readFileSync(extra, "utf8").trim();
      } catch (err) {
         logger.warn(
            `Ignoring NODE_EXTRA_CA_CERTS for the storage build's trust bundle: ${extra} could not be read (${err instanceof Error ? err.message : String(err)}).`,
         );
      }
   }
   const pem =
      [...tls.rootCertificates, ...(extraPem ? [extraPem] : [])].join("\n") +
      "\n";
   const digest = createHash("sha256").update(pem).digest("hex").slice(0, 16);
   ambientTrustDir ??= mkdtempSync(
      path.join(os.tmpdir(), "publisher-ambient-ca-"),
   );
   const file = path.join(ambientTrustDir, `${digest}.pem`);
   writeFileSync(file, pem, { mode: 0o600 });
   ambientTrustBundles.set(extra, file);
   return file;
}

type ProxiedSslmode = (typeof PROXIED_SSLMODES)[number];

/**
 * libpq keyword/value string for a Postgres source reached through the SSH
 * tunnel. libpq dials `hostaddr` (the tunnel's local endpoint) but sends `host`
 * (the database's own name) as SNI and checks the certificate against it, so
 * every `sslmode`, `verify-full` included, keeps its meaning through the
 * tunnel. TLS maps as on the query path (resolveProxiedTls):
 *
 *  - unset / `no-verify` → `require`: encrypt without verifying, so a force-SSL
 *    target is not rejected for plaintext;
 *  - `disable` → `disable`;
 *  - `verify-ca` → against the NODE_EXTRA_CA_CERTS bundle;
 *  - `verify-full` → against the ambient trust anchors plus NODE_EXTRA_CA_CERTS,
 *    so a publicly-trusted target builds as it queries.
 *
 * Exported for tests.
 */
export function buildProxiedPgAttachString(
   name: string,
   pg: components["schemas"]["PostgresConnection"],
   endpoint: ProxyEndpoint,
   trust: ProxiedAttachTrust = defaultProxiedAttachTrust(),
): string {
   if (!pg.host) {
      // validateConnectionShape requires host; guard so the certificate is
      // never checked against an empty name.
      throw new Error(
         `Connection proxy on '${name}' requires an explicit host on the postgres connection.`,
      );
   }
   const parts = [
      pgConninfoPair("host", pg.host),
      pgConninfoPair("hostaddr", endpoint.host),
      pgConninfoPair("port", String(endpoint.port)),
   ];
   if (pg.databaseName) parts.push(pgConninfoPair("dbname", pg.databaseName));
   if (pg.userName) parts.push(pgConninfoPair("user", pg.userName));
   if (pg.password) parts.push(pgConninfoPair("password", pg.password));
   parts.push(
      ...libpqSslmodeParts(
         `Connection proxy on '${name}'`,
         pg.sslmode ?? "no-verify",
         trust,
      ),
   );
   parts.push(...libpqStatementTimeoutParts(name, pg));
   return parts.join(" ");
}

/**
 * The root `verify-ca` trusts when no `sslrootcert` names one: the deployment's
 * NODE_EXTRA_CA_CERTS alone, never the public roots, which would vouch for any
 * server since verify-ca checks no hostname.
 */
function verifyCaBundle(subject: string, trust: ProxiedAttachTrust): string {
   if (!trust.caBundle) {
      throw new Error(
         `${subject} uses sslmode 'verify-ca' but no trusted CA bundle is available (NODE_EXTRA_CA_CERTS is unset).`,
      );
   }
   return trust.caBundle;
}

/**
 * The libpq conninfo pairs for an explicit sslmode, shared by the proxied and
 * the direct DuckDB attach so a mode means the same thing on both. `subject` is
 * the phrase an error leads with.
 */
function libpqSslmodeParts(
   subject: string,
   mode: ProxiedSslmode,
   trust: ProxiedAttachTrust,
): string[] {
   // Typed against PROXIED_SSLMODES, so a mode added there without a case here
   // is a compile error.
   switch (mode) {
      case "disable":
         return ["sslmode=disable"];
      case "no-verify":
         return [`sslmode=${LIBPQ_NO_VERIFY_SSLMODE}`];
      case "verify-ca":
         return [
            "sslmode=verify-ca",
            pgConninfoPair("sslrootcert", verifyCaBundle(subject, trust)),
         ];
      case "verify-full":
         return [
            "sslmode=verify-full",
            pgConninfoPair("sslrootcert", trust.ambientBundle()),
         ];
      default: {
         const unhandled: never = mode;
         throw new Error(
            `${subject} has unsupported sslmode '${String(unhandled)}' (expected ${PROXIED_SSLMODES.join(" | ")}).`,
         );
      }
   }
}

/**
 * The libpq conninfo pair that applies a connection's statement timeout, as a
 * server option on every session the attach opens. Empty when it sets none.
 */
function libpqStatementTimeoutParts(
   name: string,
   pg: components["schemas"]["PostgresConnection"],
): string[] {
   const ms = postgresStatementTimeoutMs(name, pg);
   return ms === undefined
      ? []
      : [pgConninfoPair("options", `-c statement_timeout=${ms}`)];
}
