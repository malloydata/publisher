// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { externalEnv, settings } from "./settings";
import {
   isSpanContextValid,
   trace,
   type SpanContext,
} from "@opentelemetry/api";
import { AxiosError } from "axios";
import { RequestHandler } from "express";
import winston from "winston";
import { redactPgSecrets, redactSignedUrlParams } from "./pg_helpers";

const isTelemetryEnabled = Boolean(externalEnv("OTEL_EXPORTER_OTLP_ENDPOINT"));

/** Past this depth a value is logged as `[Depth]`, so no line can overflow the stack. */
const MAX_MASK_DEPTH = 1000;

/** What a value that cannot be read safely is logged as. */
const UNREADABLE = "[Unreadable]";

/**
 * Whether a string can hold anything {@link maskedString} masks: every signed
 * URL parameter has an `=` (or an encoded one: `%…3D`, or JSON-escaped
 * `\u003d`), S3's echo a `<`, a Postgres password `password=` or a URI's
 * `://`. One scan, so the strings of
 * an ordinary line cost nothing more.
 */
const MAY_HOLD_SECRET = /[=%<]|:\/\/|\\u00/;

/**
 * `s` with a signed URL's signature, credential or token and a Postgres
 * password masked.
 */
function maskedString(s: string): string {
   if (!MAY_HOLD_SECRET.test(s)) return s;
   const signed = redactSignedUrlParams(s);
   return /:\/\/|password/i.test(signed) ? redactPgSecrets(signed) : signed;
}

/** `value` as a masked string, for one that cannot be walked. Never throws. */
function maskedFallback(value: unknown): string {
   try {
      return maskedString(String(value));
   } catch {
      return UNREADABLE;
   }
}

/** One walk: the values being walked now, and the copies a cycle reached. */
interface Walk {
   walking: Set<object>;
   /** A value's copy, made when a cycle reaches back to it while it is walked. */
   cycleCopies?: Map<object, unknown[] | Record<string, unknown>>;
}

/**
 * `value` with every string in it masked (a signed URL's secrets and a
 * Postgres password; {@link maskedString}), for a log line:
 *
 * - The original is returned when nothing in it changes; otherwise a copy, so
 *   a caller's object is never altered. A cycle reads the copy.
 * - An Error is logged as a plain object of its name, message, stack and own
 *   fields (`cause`, `code`, …), each masked and enumerable, so a nested
 *   Error prints its message. Its class's own methods (`toJSON`, getters)
 *   are not called.
 * - Another object with a `toJSON` (a URL, say), except a Date, is logged as
 *   what its `toJSON` returns, masked.
 * - Binary data (a Buffer, a typed array, an ArrayBuffer) is passed through
 *   unwalked.
 * - Past {@link MAX_MASK_DEPTH} levels a value is logged as `[Depth]`.
 * - Never throws: a value that cannot be read (a revoked proxy, a throwing
 *   getter) is logged as its masked `String()`.
 *
 * Exported for its spec.
 */
export function maskedForLog(value: unknown): unknown {
   return masked(value, { walking: new Set() }, 0);
}

function masked(value: unknown, walk: Walk, depth: number): unknown {
   if (typeof value === "string") return maskedString(value);
   if (value === null || typeof value !== "object") return value;
   if (walk.walking.has(value)) {
      // A cycle back to a value still being walked reads its copy.
      walk.cycleCopies ??= new Map();
      let copy = walk.cycleCopies.get(value);
      if (!copy) {
         copy = Array.isArray(value) ? [] : {};
         walk.cycleCopies.set(value, copy);
      }
      return copy;
   }
   if (depth >= MAX_MASK_DEPTH) return "[Depth]";
   try {
      if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
         return value;
      }
      if (value instanceof Error) return maskedError(value, walk, depth);
      if (value instanceof Date) return value;
      const toJSON = (value as { toJSON?: unknown }).toJSON;
      if (typeof toJSON === "function") {
         const json: unknown = toJSON.call(value);
         // A toJSON that returns its own object is read field by field.
         if (json !== value) return masked(json, walk, depth + 1);
      }
      return maskedFields(value, walk, depth);
   } catch {
      return maskedFallback(value);
   }
}

/**
 * An array's items or an object's own enumerable fields, masked: the original
 * when none changes and no cycle reached it, else a copy (a plain object for
 * a class instance).
 */
function maskedFields(value: object, walk: Walk, depth: number): unknown {
   walk.walking.add(value);
   let out: unknown[] | undefined;
   let keys: string[] | undefined;
   let maskedKeys: string[] | undefined;
   try {
      if (Array.isArray(value)) {
         for (let i = 0; i < value.length; i++) {
            const v: unknown = value[i];
            const m = masked(v, walk, depth + 1);
            if (m !== v && !out) out = value.slice(0, i);
            out?.push(m);
         }
      } else {
         keys = Object.keys(value);
         const record = value as Record<string, unknown>;
         for (let i = 0; i < keys.length; i++) {
            // A key can quote a signed URL too.
            const k = maskedString(keys[i]);
            if (k !== keys[i] && !maskedKeys) maskedKeys = keys.slice(0, i);
            maskedKeys?.push(k);
            const v = record[keys[i]];
            const m = masked(v, walk, depth + 1);
            if (m !== v && !out) {
               out = keys.slice(0, i).map((key) => record[key]);
            }
            out?.push(m);
         }
      }
   } finally {
      walk.walking.delete(value);
   }
   const forced = walk.cycleCopies?.get(value);
   if (!out && !maskedKeys && !forced) return value;
   if (Array.isArray(value)) {
      const copy = (forced as unknown[] | undefined) ?? [];
      copy.push(...(out ?? value));
      return copy;
   }
   const record = value as Record<string, unknown>;
   const copy =
      (forced as Record<string, unknown> | undefined) ??
      (Object.getPrototypeOf(value) === null ? Object.create(null) : {});
   keys!.forEach(
      (key, i) =>
         (copy[maskedKeys ? maskedKeys[i] : key] = out ? out[i] : record[key]),
   );
   return copy;
}

/** Fields Bun adds to every Error, which repeat its stack's first frame. */
const RUNTIME_ERROR_FIELDS = new Set([
   "line",
   "column",
   "sourceURL",
   "originalLine",
   "originalColumn",
]);

/**
 * An Error as a plain object: its name, message and stack, then every other
 * own field (`cause`, `code`, …), each masked and enumerable. A field that
 * cannot be read is logged as {@link UNREADABLE}.
 */
function maskedError(
   error: Error,
   walk: Walk,
   depth: number,
): Record<string, unknown> {
   walk.walking.add(error);
   const copy: Record<string, unknown> = {};
   const read = (key: string): unknown => {
      try {
         return masked(
            (error as unknown as Record<string, unknown>)[key],
            walk,
            depth + 1,
         );
      } catch {
         return UNREADABLE;
      }
   };
   try {
      copy.name = read("name");
      copy.message = read("message");
      copy.stack = read("stack");
      for (const key of Object.getOwnPropertyNames(error)) {
         if (key === "name" || key === "message" || key === "stack") continue;
         if (RUNTIME_ERROR_FIELDS.has(key)) continue;
         copy[key] = read(key);
      }
   } finally {
      walk.walking.delete(error);
   }
   // A cycle through the Error (a cause pointing back) read its copy.
   const forced = walk.cycleCopies?.get(error) as
      | Record<string, unknown>
      | undefined;
   return forced ? Object.assign(forced, copy) : copy;
}

/**
 * The format step that masks every line ({@link maskedForLog}). It builds a
 * fresh entry, so a caller's object (an Error logged as the whole entry,
 * `logger.error(err)`) is never altered: each enumerable field masked, and
 * `message` and `stack`, which such an Error holds non-enumerable, masked and
 * carried. Never throws.
 */
const maskSignedUrlSecrets = winston.format((info) => {
   const out: Record<string | symbol, unknown> = {};
   for (const symbol of Object.getOwnPropertySymbols(info)) {
      out[symbol] = (info as unknown as Record<symbol, unknown>)[symbol];
   }
   const walk: Walk = { walking: new Set() };
   let keys: string[];
   try {
      keys = Object.keys(info);
   } catch {
      keys = [];
   }
   for (const key of keys) {
      // A metadata key can quote a signed URL too.
      const outKey = maskedString(key);
      try {
         out[outKey] = masked((info as Record<string, unknown>)[key], walk, 0);
      } catch {
         out[outKey] = UNREADABLE;
      }
   }
   for (const key of ["message", "stack"]) {
      if (Object.prototype.hasOwnProperty.call(out, key)) continue;
      try {
         const v = (info as Record<string, unknown>)[key];
         if (v !== undefined) out[key] = masked(v, walk, 0);
      } catch {
         out[key] = UNREADABLE;
      }
   }
   return out as typeof info;
});

/**
 * The log's format: JSON when telemetry is on, the readable line otherwise,
 * each starting with {@link maskSignedUrlSecrets}. Exported for its spec,
 * which runs both.
 */
export function publisherLogFormat(json: boolean): winston.Logform.Format {
   return json
      ? winston.format.combine(
           maskSignedUrlSecrets(),
           winston.format.uncolorize(),
           winston.format.timestamp(),
           winston.format.errors({ stack: true }),
           winston.format.json(),
        )
      : winston.format.combine(
           maskSignedUrlSecrets(),
           winston.format.colorize(),
           winston.format.simple(),
        );
}

export const logger = guardedEntries(
   winston.createLogger({
      // An unknown level is the default, debug, with a startup warning.
      level: settings.get("logging.level"),
      format: publisherLogFormat(isTelemetryEnabled),
      // Check mode's stdout carries its findings alone, so its log goes to
      // stderr, every level.
      transports: [
         new winston.transports.Console(
            settings.get("check")
               ? { stderrLevels: Object.keys(winston.config.npm.levels) }
               : {},
         ),
      ],
   }),
);

/**
 * `log` with each level method (`logger.error(err)`, `logger.warn(obj)`),
 * and `log.log(obj)` or `log.log(level, obj)`, handed a fresh entry when
 * called with one object: winston writes the level
 * onto the object it is given, which would alter a caller's Error and throws
 * on a frozen one. The fresh entry carries the object's own fields and its
 * `message` and `stack`; the format step masks them. Exported for its spec.
 */
export function guardedEntries(log: winston.Logger): winston.Logger {
   for (const level of Object.keys(log.levels)) {
      const method = (log as unknown as Record<string, unknown>)[level];
      if (typeof method !== "function") continue;
      (log as unknown as Record<string, unknown>)[level] = function (
         this: winston.Logger,
         ...args: unknown[]
      ) {
         if (
            args.length === 1 &&
            args[0] !== null &&
            typeof args[0] === "object"
         ) {
            args = [freshEntry(args[0])];
         }
         return method.apply(this, args);
      };
   }
   // `log.log(entry)` and `log.log(level, entry)` hand winston the object too.
   const logMethod = log.log;
   (log as unknown as { log: unknown }).log = function (
      this: winston.Logger,
      ...args: unknown[]
   ) {
      const isObject = (v: unknown): v is object =>
         v !== null && typeof v === "object";
      if (args.length === 1 && isObject(args[0])) {
         args = [freshEntry(args[0])];
      } else if (
         args.length === 2 &&
         typeof args[0] === "string" &&
         isObject(args[1])
      ) {
         args = [args[0], freshEntry(args[1])];
      }
      return (logMethod as (...a: unknown[]) => unknown).apply(this, args);
   };
   return log;
}

/** A plain copy of `entry` for winston to write the level onto. Never throws. */
function freshEntry(entry: object): Record<string, unknown> {
   try {
      const out: Record<string, unknown> = { ...entry };
      for (const key of ["message", "stack"]) {
         if (!(key in out)) {
            const v = (entry as Record<string, unknown>)[key];
            if (v !== undefined) out[key] = v;
         }
      }
      // An Error with no message prints its name, never `[object Object]`.
      if (entry instanceof Error && !entry.message) {
         out.message = entry.name || "";
      }
      return out;
   } catch {
      return { message: maskedFallback(entry) };
   }
}

/**
 * The 32-hex trace ID from a W3C `traceparent`
 * (`00-<trace-id>-<parent-id>-<flags>`) or a bare trace ID; undefined if
 * invalid.
 */
function extractTraceIdFromTraceparent(
   traceparent: string | undefined,
): string | undefined {
   if (!traceparent) {
      return undefined;
   }

   const parts = traceparent.split("-");
   const traceId =
      parts.length >= 2 ? parts[1] : parts.length == 1 ? parts[0] : undefined;
   if (traceId && traceId.length === 32 && /^[0-9a-fA-F]{32}$/.test(traceId)) {
      return traceId;
   }

   return undefined;
}

const DISABLE_RESPONSE_LOGGING = !settings.get("logging.responseBodiesEnabled");

/** `1234.5` -> `"1.23s"`, `12.3` -> `"12.30ms"`. */
export function formatDuration(durationMs: number): string {
   if (durationMs >= 1000) {
      const seconds = durationMs / 1000;
      return `${seconds.toFixed(2)}s`;
   }
   return `${durationMs.toFixed(2)}ms`;
}

// Keys whose values are masked before reaching a log transport: connection
// credentials in request/response bodies, and credential headers in axios
// error logs. Matched case-insensitively, so header casing cannot slip by.
export const SENSITIVE_KEY_NAMES = [
   // connection config credentials
   "password",
   "connectionString",
   "serviceAccountKeyJson",
   "privateKey",
   "privateKeyPass",
   "secret",
   "secretAccessKey",
   "sessionToken",
   "sasUrl",
   "clientSecret",
   "oauthClientSecret",
   "peakaKey",
   "token",
   "accessToken",
   "apiKey",
   "api_key",
   // credential-bearing HTTP headers
   "authorization",
   "proxy-authorization",
   "cookie",
   "set-cookie",
   "x-api-key",
];
const SENSITIVE_KEYS: ReadonlySet<string> = new Set(
   SENSITIVE_KEY_NAMES.map((name) => name.toLowerCase()),
);

const REDACTED = "[REDACTED]";

/** Longest body, as serialized JSON, the access log carries whole. */
export const MAX_LOGGED_BODY_CHARS = 2048;

/**
 * A body for the access log: whole when it serializes within
 * {@link MAX_LOGGED_BODY_CHARS}, otherwise its length and that many leading
 * characters, so one large compile overlay or result set cannot become a
 * multi-megabyte log line.
 */
export function boundedForLog(value: unknown): unknown {
   let serialized: string | undefined;
   try {
      serialized = JSON.stringify(value);
   } catch {
      return "[Unserializable]";
   }
   if (serialized === undefined || serialized.length <= MAX_LOGGED_BODY_CHARS) {
      return value;
   }
   return {
      truncated: true,
      length: serialized.length,
      head: serialized.slice(0, MAX_LOGGED_BODY_CHARS),
   };
}

/**
 * Deep-copy `value` with every property whose key names a credential masked,
 * recursing through objects and arrays (connections arrive nested under
 * `connections[]`). Never mutates the input.
 */
export function redactSensitive(
   value: unknown,
   seen: WeakSet<object> = new WeakSet(),
): unknown {
   if (value === null || typeof value !== "object" || value instanceof Date) {
      return value;
   }
   if (seen.has(value)) {
      return "[Circular]";
   }
   seen.add(value);
   if (Array.isArray(value)) {
      return value.map((item) => redactSensitive(item, seen));
   }
   const result: Record<string, unknown> = {};
   for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
      result[key] = SENSITIVE_KEYS.has(key.toLowerCase())
         ? REDACTED
         : redactSensitive(val, seen);
   }
   return result;
}

/** The W3C trace id of a request, from its `traceparent` header, when it carries one. */
export function traceIdOf(req: {
   headers: Record<string, string | string[] | undefined>;
}): string | undefined {
   const traceparent = req.headers["traceparent"];
   return extractTraceIdFromTraceparent(
      typeof traceparent === "string" ? traceparent : undefined,
   );
}

const PROBE_PATHS: ReadonlySet<string> = new Set([
   "/health",
   "/health/liveness",
   "/health/readiness",
   "/metrics",
]);

/** A health probe or metrics scrape: left out of the access log and request metrics, which they would drown. */
export function isProbePath(path: string): boolean {
   return PROBE_PATHS.has(path);
}

/**
 * The trace a request runs in: its span's (the HTTP instrumentation's, which
 * continues an incoming `traceparent`), else the header's own.
 */
function requestTraceId(
   req: Parameters<typeof traceIdOf>[0],
   spanContext: SpanContext | undefined,
): string | undefined {
   return spanContext && isSpanContextValid(spanContext)
      ? spanContext.traceId
      : traceIdOf(req);
}

export const loggerMiddleware: RequestHandler = (req, res, next) => {
   const startTime = performance.now();
   // Read now: the span is active while the request is dispatched, not
   // necessarily when `finish` fires.
   const spanContext = trace.getActiveSpan()?.spanContext();
   const resJson = res.json;
   res.json = (body: unknown) => {
      res.locals.body = body;
      return resJson.call(res, body);
   };
   res.on("finish", () => {
      if (isProbePath(req.path)) return;
      const durationMs = performance.now() - startTime;
      const logMetadata: Record<string, unknown> = {
         statusCode: res.statusCode,
         durationMs: Math.round(durationMs * 100) / 100,
         // The same, formatted; kept for readers of the older field.
         duration: formatDuration(durationMs),
         params: req.params,
         query: req.query,
      };
      // Redaction is by key name only, so a request's query text and given
      // values, and a response's result rows, pass through: kept to debug.
      if (logger.isDebugEnabled()) {
         logMetadata.payload = boundedForLog(redactSensitive(req.body));
         if (!DISABLE_RESPONSE_LOGGING) {
            logMetadata.response = boundedForLog(
               redactSensitive(res.locals.body),
            );
         }
      }
      const traceId = requestTraceId(req, spanContext);
      if (traceId) {
         logMetadata.traceId = traceId;
      }
      logger.info(`${req.method} ${req.url}`, logMetadata);
   });
   next();
};

/**
 * The log line for an AxiosError, with secrets removed: response fields are
 * redacted, and the raw request (which holds the outgoing Authorization
 * header) is reduced to a few safe descriptors.
 */
export function buildAxiosErrorLog(error: AxiosError): {
   message: string;
   meta: Record<string, unknown>;
} {
   if (error.response) {
      return {
         message: "Axios server-side error",
         meta: {
            url: error.response.config.url,
            status: error.response.status,
            headers: redactSensitive(error.response.headers),
            data: redactSensitive(error.response.data),
         },
      };
   }
   if (error.request) {
      // No response. `error.request` carries the outgoing headers, so only
      // safe descriptors are logged.
      return {
         message: "Axios client-side error",
         meta: {
            method: error.config?.method,
            url: error.config?.url,
            code: error.code,
         },
      };
   }
   // Failed while setting up; `config.headers` is still on the error, so only
   // the message is logged.
   return {
      message: "Axios unknown error",
      meta: { message: error.message },
   };
}

export const logAxiosError = (error: AxiosError) => {
   const { message, meta } = buildAxiosErrorLog(error);
   logger.error(message, meta);
};
