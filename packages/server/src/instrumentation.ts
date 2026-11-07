// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { externalEnv } from "./settings";
import { monitorEventLoopDelay } from "node:perf_hooks";
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
import type { Request, Response } from "express";
import { logger } from "./logger";
import { publisherMeter } from "./telemetry";
import { SERVER_VERSION } from "./version";

export { httpMetricsMiddleware } from "./http_metrics";

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

export async function shutdownSDK(): Promise<void> {
   if (sdk) {
      await sdk.shutdown();
      sdk = null;
   }
}

function instrument() {
   const otelCollectorUrl = externalEnv("OTEL_EXPORTER_OTLP_ENDPOINT");

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
         [ATTR_SERVICE_VERSION]: SERVER_VERSION,
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
            new BatchLogRecordProcessor({
               exporter: new OTLPLogExporter({
                  url: `${otelCollectorUrl}/v1/logs`,
               }),
            }),
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

const meter = publisherMeter();

// Event-loop delay is the only way the synchronous liveness probe can fail, so
// p50/p99/max let an operator correlate liveness restarts with loop pressure.
// Not registered where the runtime lacks `monitorEventLoopDelay` (some Bun
// builds).
const eventLoopHistogram = (() => {
   try {
      const histogram = monitorEventLoopDelay({ resolution: 20 });
      histogram.enable();
      return histogram;
   } catch {
      return null;
   }
})();

const eventLoopLagP50 = meter.createObservableGauge(
   "publisher_event_loop_lag_p50_ms",
   {
      description:
         "Event loop delay p50 since the last scrape, in milliseconds",
      unit: "ms",
   },
);
const eventLoopLagP99 = meter.createObservableGauge(
   "publisher_event_loop_lag_p99_ms",
   {
      description:
         "Event loop delay p99 since the last scrape, in milliseconds",
      unit: "ms",
   },
);
const eventLoopLagMax = meter.createObservableGauge(
   "publisher_event_loop_lag_max_ms",
   {
      description:
         "Event loop delay max since the last scrape, in milliseconds",
      unit: "ms",
   },
);

// One batch, so the histogram reset cannot race the reads.
if (eventLoopHistogram) {
   const histogram = eventLoopHistogram;
   meter.addBatchObservableCallback(
      (observableResult) => {
         observableResult.observe(
            eventLoopLagP50,
            histogram.percentile(50) / 1e6,
         );
         observableResult.observe(
            eventLoopLagP99,
            histogram.percentile(99) / 1e6,
         );
         observableResult.observe(eventLoopLagMax, histogram.max / 1e6);
         histogram.reset();
      },
      [eventLoopLagP50, eventLoopLagP99, eventLoopLagMax],
   );
}
