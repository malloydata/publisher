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
import { redactSignedUrlParams } from "./pg_helpers";

const isTelemetryEnabled = Boolean(externalEnv("OTEL_EXPORTER_OTLP_ENDPOINT"));

/**
 * `value` with every string in it, at any depth, passed through
 * `redactSignedUrlParams`: a signed URL's signature, credential or token
 * never reaches the log, from any line (a driver's error a package load, a
 * publish or a request quotes).
 *
 * - A plain object, an array and an Error are copied, never altered, so a
 *   caller's object is left as it was. Each is copied once, before its
 *   contents are walked, so a cycle reaches the copy, never the original's
 *   unmasked strings.
 * - An Error stays an Error of its own class, with its message, stack and
 *   every other own field (`cause`, `code`, …) masked.
 * - Binary data (a Buffer, a typed array, an ArrayBuffer) holds no text a
 *   format prints and is passed through unwalked.
 * - Any other object (a Date, a class instance) is passed through unless an
 *   own field changes, then copied as a plain object.
 *
 * Exported for its spec.
 */
export function maskedForLog(value: unknown): unknown {
   return masked(value, new WeakMap());
}

function masked(value: unknown, seen: WeakMap<object, unknown>): unknown {
   if (typeof value === "string") return redactSignedUrlParams(value);
   if (value === null || typeof value !== "object") return value;
   if (seen.has(value)) return seen.get(value);
   if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) return value;
   if (value instanceof Error) {
      const copy = Object.create(Object.getPrototypeOf(value)) as Error;
      seen.set(value, copy);
      for (const key of Object.getOwnPropertyNames(value)) {
         const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
         Object.defineProperty(copy, key, {
            ...descriptor,
            ...("value" in descriptor
               ? { value: masked(descriptor.value, seen) }
               : {}),
         });
      }
      return copy;
   }
   if (Array.isArray(value)) {
      const copy: unknown[] = [];
      seen.set(value, copy);
      for (const item of value) copy.push(masked(item, seen));
      return copy;
   }
   const proto = Object.getPrototypeOf(value);
   if (proto === Object.prototype || proto === null) {
      const copy: Record<string, unknown> = {};
      seen.set(value, copy);
      for (const [key, v] of Object.entries(value)) copy[key] = masked(v, seen);
      return copy;
   }
   // A Date, a class instance: itself unless an own field changes.
   seen.set(value, value);
   let copy: Record<string, unknown> | undefined;
   for (const [key, v] of Object.entries(value)) {
      const m = masked(v, seen);
      if (m !== v) {
         copy ??= { ...(value as Record<string, unknown>) };
         copy[key] = m;
      }
   }
   if (copy) seen.set(value, copy);
   return copy ?? value;
}

/**
 * The format step that masks every line's strings ({@link maskedForLog}):
 * each enumerable field, and `message` and `stack` themselves, which a whole
 * Error logged as the entry (`logger.error(err)`) holds non-enumerable.
 */
const maskSignedUrlSecrets = winston.format((info) => {
   const seen = new WeakMap<object, unknown>();
   for (const key of Object.keys(info)) {
      info[key] = masked(info[key], seen);
   }
   for (const key of ["message", "stack"] as const) {
      const v = (info as Record<string, unknown>)[key];
      if (typeof v === "string") {
         Object.defineProperty(info, key, {
            value: redactSignedUrlParams(v),
            writable: true,
            configurable: true,
            enumerable: Object.prototype.propertyIsEnumerable.call(info, key),
         });
      }
   }
   return info;
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

export const logger = winston.createLogger({
   // An unknown level is the default, debug, with a startup warning.
   level: settings.get("logging.level"),
   format: publisherLogFormat(isTelemetryEnabled),
   transports: [new winston.transports.Console()],
});

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
