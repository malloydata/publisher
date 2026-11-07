// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import {
   context,
   ROOT_CONTEXT,
   trace,
   TraceFlags,
   type Context,
   type ContextManager,
} from "@opentelemetry/api";
import type { AxiosError } from "axios";
import { EventEmitter } from "events";
import type { Request, Response } from "express";
import {
   SENSITIVE_KEY_NAMES,
   buildAxiosErrorLog,
   logger,
   loggerMiddleware,
   MAX_LOGGED_BODY_CHARS,
   redactSensitive,
} from "./logger";
import { linesMentioning } from "./test_helpers/log_lines";

describe("redactSensitive", () => {
   it("redacts every name in SENSITIVE_KEY_NAMES (reflective lock)", () => {
      // Iterates the list itself so a newly-added credential field cannot
      // be introduced without redaction, and pins apiKey/api_key.
      const input: Record<string, string> = {};
      for (const name of SENSITIVE_KEY_NAMES) input[name] = "leak-me";
      const out = redactSensitive(input) as Record<string, unknown>;
      for (const name of SENSITIVE_KEY_NAMES) {
         expect(out[name]).toBe("[REDACTED]");
      }
      expect(SENSITIVE_KEY_NAMES).toContain("apiKey");
      expect(SENSITIVE_KEY_NAMES).toContain("api_key");
   });

   it("masks every known credential field at the top level", () => {
      const input = {
         password: "hunter2",
         connectionString: "postgresql://u:p@h/db",
         serviceAccountKeyJson: '{"private_key":"-----BEGIN..."}',
         privateKey: "-----BEGIN PRIVATE KEY-----",
         privateKeyPass: "passphrase",
         secret: "gcs-hmac-secret",
         secretAccessKey: "aws-secret",
         sessionToken: "aws-session",
         sasUrl: "https://a.blob.core.windows.net/c?sig=abc",
         clientSecret: "azure-client-secret",
         oauthClientSecret: "databricks-oauth-secret",
         peakaKey: "peaka-api-key",
         token: "databricks-pat",
         accessToken: "motherduck-token",
      };
      const out = redactSensitive(input) as Record<string, unknown>;
      for (const key of Object.keys(input)) {
         expect(out[key]).toBe("[REDACTED]");
      }
   });

   it("keeps non-secret fields intact", () => {
      const input = {
         name: "bigquery",
         type: "bigquery",
         defaultProjectId: "my-project",
         host: "db.example.com",
         port: 5432,
      };
      expect(redactSensitive(input)).toEqual(input);
   });

   it("redacts secrets nested inside the environment PATCH payload shape", () => {
      const input = {
         name: "demo___malloy-samples",
         connections: [
            {
               name: "bigquery",
               type: "bigquery",
               bigqueryConnection: {
                  defaultProjectId: "vscode-demo",
                  serviceAccountKeyJson: '{"private_key":"-----BEGIN..."}',
               },
            },
            {
               name: "warehouse",
               type: "snowflake",
               snowflakeConnection: {
                  account: "acct",
                  username: "svc",
                  password: "s3cret",
                  privateKey: "-----BEGIN PRIVATE KEY-----",
               },
            },
         ],
      };
      expect(redactSensitive(input)).toEqual({
         name: "demo___malloy-samples",
         connections: [
            {
               name: "bigquery",
               type: "bigquery",
               bigqueryConnection: {
                  defaultProjectId: "vscode-demo",
                  serviceAccountKeyJson: "[REDACTED]",
               },
            },
            {
               name: "warehouse",
               type: "snowflake",
               snowflakeConnection: {
                  account: "acct",
                  username: "svc",
                  password: "[REDACTED]",
                  privateKey: "[REDACTED]",
               },
            },
         ],
      });
   });

   it("recurses through arrays of secrets", () => {
      const out = redactSensitive([{ password: "a" }, { password: "b" }]);
      expect(out).toEqual([
         { password: "[REDACTED]" },
         { password: "[REDACTED]" },
      ]);
   });

   it("passes primitives through unchanged", () => {
      expect(redactSensitive("plain")).toBe("plain");
      expect(redactSensitive(42)).toBe(42);
      expect(redactSensitive(null)).toBeNull();
      expect(redactSensitive(undefined)).toBeUndefined();
   });

   it("preserves Date values instead of destructuring them", () => {
      const d = new Date("2026-07-08T00:00:00.000Z");
      const out = redactSensitive({ createdAt: d }) as { createdAt: Date };
      expect(out.createdAt).toBeInstanceOf(Date);
      expect(out.createdAt.getTime()).toBe(d.getTime());
   });

   it("does not mutate the input object", () => {
      const input = { password: "hunter2", nested: { token: "abc" } };
      redactSensitive(input);
      expect(input.password).toBe("hunter2");
      expect(input.nested.token).toBe("abc");
   });

   it("tolerates circular references", () => {
      const input: Record<string, unknown> = { password: "hunter2" };
      input.self = input;
      const out = redactSensitive(input) as Record<string, unknown>;
      expect(out.password).toBe("[REDACTED]");
      expect(out.self).toBe("[Circular]");
   });

   it("matches credential keys case-insensitively", () => {
      const out = redactSensitive({
         Password: "hunter2",
         Authorization: "Bearer abc",
      }) as Record<string, unknown>;
      expect(out.Password).toBe("[REDACTED]");
      expect(out.Authorization).toBe("[REDACTED]");
   });

   it("masks credential HTTP headers", () => {
      const out = redactSensitive({
         "content-type": "application/json",
         authorization: "Bearer secret-token",
         cookie: "session=abc",
      }) as Record<string, unknown>;
      expect(out["content-type"]).toBe("application/json");
      expect(out.authorization).toBe("[REDACTED]");
      expect(out.cookie).toBe("[REDACTED]");
   });
});

describe("buildAxiosErrorLog", () => {
   it("redacts response data and headers on a server-side error", () => {
      const error = {
         response: {
            config: { url: "http://worker/api" },
            status: 500,
            headers: { authorization: "Bearer abc", "content-type": "json" },
            data: { connection: { serviceAccountKeyJson: "PRIVATE_KEY" } },
         },
      } as unknown as AxiosError;
      const { message, meta } = buildAxiosErrorLog(error);
      expect(message).toBe("Axios server-side error");
      expect(meta).toEqual({
         url: "http://worker/api",
         status: 500,
         headers: { authorization: "[REDACTED]", "content-type": "json" },
         data: { connection: { serviceAccountKeyJson: "[REDACTED]" } },
      });
   });

   it("never logs the raw request object on a client-side error", () => {
      const error = {
         request: { _header: "GET / HTTP/1.1\r\nAuthorization: Bearer abc" },
         config: { method: "get", url: "http://worker/api" },
         code: "ECONNABORTED",
      } as unknown as AxiosError;
      const { message, meta } = buildAxiosErrorLog(error);
      expect(message).toBe("Axios client-side error");
      expect(meta).toEqual({
         method: "get",
         url: "http://worker/api",
         code: "ECONNABORTED",
      });
      expect(JSON.stringify(meta)).not.toContain("Bearer abc");
   });

   it("logs only the message on a setup error", () => {
      const error = {
         message: "getaddrinfo ENOTFOUND",
         config: { headers: { Authorization: "Bearer abc" } },
      } as unknown as AxiosError;
      const { message, meta } = buildAxiosErrorLog(error);
      expect(message).toBe("Axios unknown error");
      expect(meta).toEqual({ message: "getaddrinfo ENOTFOUND" });
      expect(JSON.stringify(meta)).not.toContain("Bearer abc");
   });
});

describe("loggerMiddleware", () => {
   const TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";

   /** Run one request through the middleware; the access-log metadata it wrote, if any. */
   function accessLog(
      url: string,
      {
         headers = {},
         body,
      }: { headers?: Record<string, string>; body?: unknown } = {},
   ): Record<string, unknown> | undefined {
      const info = spyOn(logger, "info").mockImplementation(
         (() => logger) as never,
      );
      try {
         const req = {
            method: "POST",
            url,
            headers,
            body,
            params: {},
            query: {},
         };
         const res = Object.assign(new EventEmitter(), {
            statusCode: 200,
            locals: {},
            json: (payload: unknown) => payload,
         });
         loggerMiddleware(
            req as unknown as Request,
            res as unknown as Response,
            () => {},
         );
         res.emit("finish");
         const [line] = linesMentioning(info, url);
         return line?.[1] as Record<string, unknown> | undefined;
      } finally {
         info.mockRestore();
      }
   }

   function atLevel<T>(level: string, fn: () => T): T {
      const previous = logger.level;
      logger.level = level;
      try {
         return fn();
      } finally {
         logger.level = previous;
      }
   }

   const body = { query: "run: x", password: "hunter2" };

   it("logs the status and the duration of a request", () => {
      const meta = atLevel("info", () => accessLog("/logger-spec/a", { body }));
      expect(meta).toMatchObject({ statusCode: 200, params: {}, query: {} });
      expect(meta?.duration).toMatch(/^\d+\.\d\dms$/);
      expect(typeof meta?.durationMs).toBe("number");
      expect(meta).not.toHaveProperty("payload");
   });

   it("adds the redacted request body at debug", () => {
      const meta = atLevel("debug", () =>
         accessLog("/logger-spec/e", { body }),
      );
      expect(meta?.payload).toEqual({
         query: "run: x",
         password: "[REDACTED]",
      });
   });

   it("logs a large body as its length and head, not whole", () => {
      const big = { source: "x".repeat(MAX_LOGGED_BODY_CHARS * 4) };
      const meta = atLevel("debug", () =>
         accessLog("/logger-spec/f", { body: big }),
      );
      const payload = meta?.payload as {
         truncated: boolean;
         length: number;
         head: string;
      };
      expect(payload.truncated).toBe(true);
      expect(payload.length).toBe(JSON.stringify(big).length);
      expect(payload.head).toHaveLength(MAX_LOGGED_BODY_CHARS);
      expect(payload.head.startsWith('{"source":"xxx')).toBe(true);
   });

   it("carries the trace id of an incoming traceparent", () => {
      const meta = accessLog("/logger-spec/b", {
         headers: { traceparent: `00-${TRACE_ID}-00f067aa0ba902b7-01` },
      });
      expect(meta?.traceId).toBe(TRACE_ID);
   });

   describe("inside an active span", () => {
      const SPAN_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
      let registered = false;
      beforeAll(() => {
         registered = context.setGlobalContextManager(
            new StackContextManager(),
         );
      });
      afterAll(() => {
         if (registered) context.disable();
      });

      function inSpan<T>(fn: () => T): T {
         const span = trace.wrapSpanContext({
            traceId: SPAN_TRACE_ID,
            spanId: "b7ad6b7169203331",
            traceFlags: TraceFlags.SAMPLED,
         });
         return context.with(trace.setSpan(ROOT_CONTEXT, span), fn);
      }

      it("takes the trace id from the span, over the header", () => {
         const meta = inSpan(() =>
            accessLog("/logger-spec/c", {
               headers: { traceparent: `00-${TRACE_ID}-00f067aa0ba902b7-01` },
            }),
         );
         expect(meta?.traceId).toBe(SPAN_TRACE_ID);
      });

      it("takes it from the span when there is no header", () => {
         expect(inSpan(() => accessLog("/logger-spec/d"))?.traceId).toBe(
            SPAN_TRACE_ID,
         );
      });
   });

   it("writes nothing for a probe", () => {
      expect(accessLog("/health")).toBeUndefined();
   });
});

/** A synchronous context manager, enough for code that reads the active span before it awaits. */
class StackContextManager implements ContextManager {
   private stack: Context[] = [];
   active(): Context {
      return this.stack[this.stack.length - 1] ?? ROOT_CONTEXT;
   }
   with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(
      ctx: Context,
      fn: F,
      thisArg?: ThisParameterType<F>,
      ...args: A
   ): ReturnType<F> {
      this.stack.push(ctx);
      try {
         return fn.call(thisArg, ...args);
      } finally {
         this.stack.pop();
      }
   }
   bind<T>(_ctx: Context, target: T): T {
      return target;
   }
   enable(): this {
      return this;
   }
   disable(): this {
      this.stack = [];
      return this;
   }
}
