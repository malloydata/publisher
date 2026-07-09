import { AxiosError } from "axios";
import { RequestHandler } from "express";
import winston from "winston";

const isTelemetryEnabled = Boolean(process.env.OTEL_EXPORTER_OTLP_ENDPOINT);

// Determine log level from environment variable
// Valid levels: error, warn, info, verbose, debug, silly
// Default: 'debug'
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

//-
/**
 * Extracts the trace ID from a W3C traceparent header.
 * Format: version-trace-id-parent-id-trace-flags
 * Example: 00-81f2264f363f1b5596c84ab29e6be171-83ef39df12ab6bab-01
 *
 * @param traceparent The traceparent header value
 * @returns The trace ID (32 hex characters) or undefined if invalid
 */
function extractTraceIdFromTraceparent(
   traceparent: string | undefined,
): string | undefined {
   if (!traceparent) {
      return undefined;
   }

   // format of traceparent can either be: version-traceId-parentId-traceFlags or traceId
   const parts = traceparent.split("-");
   const traceId =
      parts.length >= 2 ? parts[1] : parts.length == 1 ? parts[0] : undefined;
   // Validate that the traceId is 32 hex characters
   if (traceId && traceId.length === 32 && /^[0-9a-fA-F]{32}$/.test(traceId)) {
      return traceId;
   }

   return undefined;
}

const DISABLE_RESPONSE_LOGGING =
   process.env.DISABLE_RESPONSE_LOGGING === "true" ||
   process.env.DISABLE_RESPONSE_LOGGING === "1";

/**
 * Format duration in milliseconds to a human-readable string with unit
 * @param durationMs Duration in milliseconds
 * @returns Formatted string with 2 decimal places and unit (s or ms)
 */
export function formatDuration(durationMs: number): string {
   // If duration is >= 1000ms, show in seconds, otherwise show in milliseconds
   if (durationMs >= 1000) {
      const seconds = durationMs / 1000;
      return `${seconds.toFixed(2)}s`;
   }
   return `${durationMs.toFixed(2)}ms`;
}

// Connection endpoints carry provider credentials in their request/response
// bodies (BigQuery serviceAccountKeyJson, Snowflake privateKey/password, Postgres
// password/connectionString, S3 secretAccessKey/sessionToken, Azure clientSecret/
// sasUrl, Databricks token, ...), and axios error logs can carry credential HTTP
// headers (Authorization, Cookie). Those values are masked by key name before
// they reach a log transport. Matching is case-insensitive so header casing
// (Authorization vs authorization) does not slip through.
const SENSITIVE_KEY_NAMES = [
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
 * Deep-copy `value`, replacing any property whose key names a credential
 * (SENSITIVE_KEYS) with a placeholder. The match is by key name (case-
 * insensitive) and recurses through nested objects and arrays, since
 * connections arrive nested under `connections[]`. Primitives and Dates pass
 * through unchanged and the input is never mutated; used to keep secrets out of
 * the request/response and axios-error logs.
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

      // Extract trace ID from traceparent header if present
      const traceparent = req.headers["traceparent"] as string | undefined;
      const traceId = extractTraceIdFromTraceparent(traceparent);

      const logMetadata: Record<string, unknown> = {
         statusCode: res.statusCode,
         duration: formatDuration(durationMs),
         payload: redactSensitive(req.body),
         params: req.params,
         query: req.query,
      };

      // Only include response body if response logging is enabled
      if (!DISABLE_RESPONSE_LOGGING) {
         logMetadata.response = redactSensitive(res.locals.body);
      }

      // Add traceId to log metadata if present
      if (traceId) {
         logMetadata.traceId = traceId;
      }

      // Skip logging for metrics and health endpoints to reduce log noise
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
 * Build the message + metadata logged for an AxiosError, with secrets removed.
 * A failing upstream call can echo a connection config in `response.data` or
 * return credential headers, and the raw `error.request` / `error` objects hold
 * the outgoing Authorization header. Response fields are redacted and the raw
 * request object is reduced to a few safe descriptors, so credentials never
 * reach the log. Split out from `logAxiosError` so it can be unit tested.
 */
export function buildAxiosErrorLog(error: AxiosError): {
   message: string;
   meta: Record<string, unknown>;
} {
   if (error.response) {
      // The request was made and the server responded with a status code
      // that falls out of the range of 2xx.
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
      // The request was made but no response was received. `error.request` is
      // the raw ClientRequest/XHR and carries the outgoing headers (including
      // Authorization), so log only safe descriptors from the config.
      return {
         message: "Axios client-side error",
         meta: {
            method: error.config?.method,
            url: error.config?.url,
            code: error.code,
         },
      };
   }
   // Something happened setting up the request. The AxiosError still holds
   // `config.headers`, so log only the message.
   return {
      message: "Axios unknown error",
      meta: { message: error.message },
   };
}

export const logAxiosError = (error: AxiosError) => {
   const { message, meta } = buildAxiosErrorLog(error);
   logger.error(message, meta);
};
