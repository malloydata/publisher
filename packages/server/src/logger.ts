// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { AxiosError } from "axios";
import { RequestHandler } from "express";
import winston from "winston";

const isTelemetryEnabled = Boolean(process.env.OTEL_EXPORTER_OTLP_ENDPOINT);

// LOG_LEVEL, default "debug".
const VALID_LOG_LEVELS = ["error", "warn", "info", "verbose", "debug", "silly"];
const getLogLevel = (): string => {
   if (process.env.LOG_LEVEL) {
      const logLevel = process.env.LOG_LEVEL.toLowerCase();
      if (VALID_LOG_LEVELS.includes(logLevel)) {
         return logLevel;
      } else {
         console.error(
            `Invalid log level: ${process.env.LOG_LEVEL}. Valid log levels are: ${VALID_LOG_LEVELS.join(", ")}. Defaulting to "debug".`,
         );
      }
   }
   return "debug";
};

export const logger = winston.createLogger({
   level: getLogLevel(),
   format: isTelemetryEnabled
      ? winston.format.combine(
           winston.format.uncolorize(),
           winston.format.timestamp(),
           winston.format.errors({ stack: true }),
           winston.format.json(),
        )
      : winston.format.combine(
           winston.format.colorize(),
           winston.format.simple(),
        ),
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

const DISABLE_RESPONSE_LOGGING =
   process.env.DISABLE_RESPONSE_LOGGING === "true" ||
   process.env.DISABLE_RESPONSE_LOGGING === "1";

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

export const loggerMiddleware: RequestHandler = (req, res, next) => {
   const startTime = performance.now();
   const resJson = res.json;
   res.json = (body: unknown) => {
      res.locals.body = body;
      return resJson.call(res, body);
   };
   res.on("finish", () => {
      const endTime = performance.now();
      const durationMs = endTime - startTime;

      const traceparent = req.headers["traceparent"] as string | undefined;
      const traceId = extractTraceIdFromTraceparent(traceparent);

      const logMetadata: Record<string, unknown> = {
         statusCode: res.statusCode,
         duration: formatDuration(durationMs),
         payload: redactSensitive(req.body),
         params: req.params,
         query: req.query,
      };

      if (!DISABLE_RESPONSE_LOGGING) {
         logMetadata.response = redactSensitive(res.locals.body);
      }

      if (traceId) {
         logMetadata.traceId = traceId;
      }

      // Probes and scrapes would drown the log.
      if (
         req.url !== "/metrics" &&
         req.url !== "/health" &&
         req.url !== "/health/liveness" &&
         req.url !== "/health/readiness"
      ) {
         logger.info(`${req.method} ${req.url}`, logMetadata);
      }
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
