// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, describe, expect, it } from "bun:test";
import { SpanStatusCode } from "@opentelemetry/api";
import {
   BasicTracerProvider,
   InMemorySpanExporter,
   SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-node";
import { withSpan } from "./telemetry";

describe("withSpan", () => {
   const exporter = new InMemorySpanExporter();
   const provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
   });

   // The provider's own tracer, not the global one: another spec may have
   // registered a global provider first, and a second registration is ignored.
   const tracer = provider.getTracer("test");
   afterAll(async () => {
      await provider.shutdown();
   });

   it("ends a span with the attributes and returns the value", async () => {
      exporter.reset();
      const value = await withSpan(
         "publisher.test",
         { "malloy.package": "pkg" },
         async (span) => {
            span.setAttribute("extra", 1);
            return 42;
         },
         tracer,
      );
      expect(value).toBe(42);
      const [span] = exporter.getFinishedSpans();
      expect(span.name).toBe("publisher.test");
      expect(span.attributes).toEqual({ "malloy.package": "pkg", extra: 1 });
      expect(span.status.code).toBe(SpanStatusCode.UNSET);
   });

   it("marks a throw as an error, records it, and rethrows", async () => {
      exporter.reset();
      await expect(
         withSpan(
            "publisher.test",
            {},
            async () => {
               throw new Error("boom");
            },
            tracer,
         ),
      ).rejects.toThrow("boom");
      const [span] = exporter.getFinishedSpans();
      expect(span.status).toEqual({
         code: SpanStatusCode.ERROR,
         message: "boom",
      });
      expect(span.events.map((event) => event.name)).toEqual(["exception"]);
   });
});
