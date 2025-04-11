import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { OTLPMetricExporter } from "@opentelemetry/exporter-metrics-otlp-proto";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-proto";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
   ATTR_SERVICE_NAME,
   ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import {
   PeriodicExportingMetricReader,
   ConsoleMetricExporter,
   PushMetricExporter,
} from "@opentelemetry/sdk-metrics";
import {
   BatchSpanProcessor,
   ConsoleSpanExporter,
   SpanExporter,
} from "@opentelemetry/sdk-trace-base";
import {
   BatchLogRecordProcessor,
   ConsoleLogRecordExporter,
   LogRecordExporter,
} from "@opentelemetry/sdk-logs";

let traceExporter: SpanExporter;
let metricExporter: PushMetricExporter;
let logExporter: LogRecordExporter;

const otelCollectorUrl = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
if (otelCollectorUrl) {
   console.log(
      "Initializing OpenTelemetry SDK with OTLP collector at",
      otelCollectorUrl,
   );
   traceExporter = new OTLPTraceExporter({
      url: `${otelCollectorUrl}/v1/traces`,
      headers: {},
   });
   metricExporter = new OTLPMetricExporter({
      url: `${otelCollectorUrl}/v1/metrics`,
      headers: {},
   });
   logExporter = new OTLPLogExporter({
      url: `${otelCollectorUrl}/v1/logs`,
      headers: {},
   });
} else {
   console.log(
      "No OTLP collector URL found, initializing console-based telemetry",
   );
   traceExporter = new ConsoleSpanExporter();
   metricExporter = new ConsoleMetricExporter();
   logExporter = new ConsoleLogRecordExporter();
}

const metricReader = new PeriodicExportingMetricReader({
   exporter: metricExporter,
});
const sdk = new NodeSDK({
   serviceName: "publisher",
   resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: "publisher",
      [ATTR_SERVICE_VERSION]: "1.0.0",
   }),
   autoDetectResources: true,
   traceExporter: traceExporter,
   metricReader: metricReader,
   instrumentations: [getNodeAutoInstrumentations()],
   spanProcessors: [new BatchSpanProcessor(traceExporter)],
   logRecordProcessors: [new BatchLogRecordProcessor(logExporter)],
});

sdk.start();
console.log("Initialized OpenTelemetry SDK");
