import { AxiosError } from "axios";
import { RequestHandler } from "express";
import winston from "winston";

const isTelemetryEnabled = Boolean(process.env.OTEL_EXPORTER_OTLP_ENDPOINT);

export const logger = winston.createLogger({
   format: isTelemetryEnabled
      ? winston.format.combine(
           winston.format.uncolorize(),
           winston.format.timestamp(),
           winston.format.metadata({
              fillExcept: ["message", "level", "timestamp"],
           }),
           winston.format.json(),
        )
      : winston.format.combine(
           winston.format.colorize(),
           winston.format.simple(),
        ),
   transports: [new winston.transports.Console()],
});

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

export const loggerMiddleware: RequestHandler = (req, res, next) => {
   const startTime = performance.now();
   const resJson = res.json;
   res.json = (body: unknown) => {
      res.locals.body = body;
      return resJson.call(res, body);
   };
   res.on("finish", () => {
      const endTime = performance.now();

      // Extract trace ID from traceparent header if present
      const traceparent = req.headers["traceparent"] as string | undefined;
      const traceId = extractTraceIdFromTraceparent(traceparent);

      const logMetadata: Record<string, unknown> = {
         statusCode: res.statusCode,
         duration: endTime - startTime,
         payload: req.body,
         response: res.locals.body,
         params: req.params,
         query: req.query,
      };

      // Add traceId to log metadata if present
      if (traceId) {
         logMetadata.traceId = traceId;
      }

      logger.info(`${req.method} ${req.url}`, logMetadata);
   });
   next();
};

export const logAxiosError = (error: AxiosError) => {
   if (error.response) {
      // The request was made and the server responded with a status code
      // that falls out of the range of 2xx
      logger.error("Axios server-side error", {
         url: error.response.config.url,
         status: error.response.status,
         headers: error.response.headers,
         data: error.response.data,
      });
   } else if (error.request) {
      // The request was made but no response was received
      // `error.request` is an instance of XMLHttpRequest in the browser and an instance of
      // http.ClientRequest in node.js
      logger.error("Axios client-side error", { error: error.request });
   } else {
      // Something happened in setting up the request that triggered an Error
      logger.error("Axios unknown error", { error });
   }
};
