import { metrics } from "@opentelemetry/api";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-proto";
import { PrometheusExporter } from "@opentelemetry/exporter-prometheus";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import {
   ExpressInstrumentation,
   ExpressLayerType,
} from "@opentelemetry/instrumentation-express";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import {
   ATTR_SERVICE_NAME,
   ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import type { NextFunction, Request, Response } from "express";
import { logger } from "./logger";

let prometheusExporter: PrometheusExporter | null = null;
let sdk: NodeSDK | null = null;

export function getPrometheusMetricsHandler() {
   if (!prometheusExporter) {
      throw new Error("Prometheus exporter not initialized");
   }
   return (req: Request, res: Response) => {
      prometheusExporter!.getMetricsRequestHandler(req, res);
   };
}

/**
 * Shuts down the OpenTelemetry SDK gracefully.
 * Should be called during application shutdown.
 */
export async function shutdownSDK(): Promise<void> {
   if (sdk) {
      await sdk.shutdown();
      sdk = null;
   }
}

function instrument() {
   const otelCollectorUrl = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;

   prometheusExporter = new PrometheusExporter({
      preventServerStart: true,
   });

   const instrumentations = [
      getNodeAutoInstrumentations(),
      new ExpressInstrumentation({
         ignoreLayersType: [ExpressLayerType.MIDDLEWARE],
         ignoreLayers: [/\/health/, /\/metrics/],
      }),
   ];

   sdk = new NodeSDK({
      resource: resourceFromAttributes({
         [ATTR_SERVICE_NAME]: "publisher",
         [ATTR_SERVICE_VERSION]: "1.0.0",
      }),
      autoDetectResources: true,
      metricReader: prometheusExporter,
      instrumentations,
      ...(otelCollectorUrl && {
         spanProcessors: [
            new BatchSpanProcessor(
               new OTLPTraceExporter({
                  url: `${otelCollectorUrl}/v1/traces`,
               }),
            ),
         ],
         logRecordProcessors: [
            new BatchLogRecordProcessor(
               new OTLPLogExporter({
                  url: `${otelCollectorUrl}/v1/logs`,
               }),
            ),
         ],
      }),
   });

   sdk.start();

   if (otelCollectorUrl) {
      logger.info(
         `OpenTelemetry SDK initialized with OTLP collector at ${otelCollectorUrl}`,
      );
   } else {
      logger.info("OpenTelemetry SDK initialized with Prometheus metrics only");
   }
}

instrument();

// --- HTTP metrics middleware ---

const meter = metrics.getMeter("publisher");

const httpRequestDuration = meter.createHistogram(
   "http_server_request_duration_ms",
   {
      description: "Duration of HTTP requests in milliseconds",
      unit: "ms",
      advice: {
         explicitBucketBoundaries: [
            5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 60000,
         ],
      },
   },
);

const httpRequestCount = meter.createCounter("http_server_requests_total", {
   description: "Total number of HTTP requests",
});

const IGNORED_PATHS = new Set([
   "/health",
   "/health/liveness",
   "/health/readiness",
   "/metrics",
]);

export function httpMetricsMiddleware(
   req: Request,
   res: Response,
   next: NextFunction,
) {
   const start = performance.now();

   res.on("finish", () => {
      if (IGNORED_PATHS.has(req.path)) return;

      const duration = performance.now() - start;
      const attrs = {
         "http.method": req.method,
         "http.route": req.route?.path ?? req.path,
         "http.status_code": res.statusCode,
      };

      httpRequestDuration.record(duration, attrs);
      httpRequestCount.add(1, attrs);
   });

   next();
}
