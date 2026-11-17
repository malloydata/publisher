// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The log mask's masking gaps for nested errors, class-instance cycles and
 * URL objects, and the rest of its review: each case runs through the
 * server's own log format in both its modes, the readable line and JSON.
 * Every case also requires that the log call does not throw.
 */

import { describe, expect, it } from "bun:test";
import { Writable } from "stream";
import winston from "winston";
import { guardedEntries, maskedForLog, publisherLogFormat } from "./logger";
import { redactSignedUrlParams } from "./pg_helpers";

const SECRET = "deadbeef1234";
const SIGNED = `https://b.s3.amazonaws.com/k?X-Amz-Credential=AKIAX&X-Amz-Signature=${SECRET}`;

function capture(json: boolean): { log: winston.Logger; text: () => string } {
   const lines: string[] = [];
   // As the server's own logger is made: its format, its guarded entries.
   const log = guardedEntries(
      winston.createLogger({
         level: "debug",
         format: publisherLogFormat(json),
         transports: [
            new winston.transports.Stream({
               stream: new Writable({
                  write(chunk, _encoding, done) {
                     lines.push(String(chunk));
                     done();
                  },
               }),
            }),
         ],
      }),
   );
   return { log, text: () => lines.join("\n") };
}

/** Log with `fn` in each mode: no throw, no secret, and the text for more checks. */
function logged(fn: (log: winston.Logger) => void): string[] {
   return [false, true].map((json) => {
      const { log, text } = capture(json);
      expect(() => fn(log)).not.toThrow();
      expect(text()).not.toContain(SECRET);
      expect(text()).not.toContain("AKIAX");
      return text();
   });
}

describe("nested errors", () => {
   it("print their message, masked, never an empty object", () => {
      for (const text of logged((l) =>
         l.error("Failed", { error: new Error(SIGNED) }),
      )) {
         expect(text).toContain("X-Amz-Signature=***");
         expect(text).not.toContain('"error":{}');
      }
      for (const text of logged((l) =>
         l.error("Failed", { error: new Error("plain message") }),
      )) {
         expect(text).toContain("plain message");
      }
   });

   it("of a class with private fields read by toJSON or a getter, without throwing", () => {
      class WithPrivate extends Error {
         #detail = "d";
         toJSON() {
            return { message: this.message, detail: this.#detail };
         }
         get detail() {
            return this.#detail;
         }
      }
      for (const text of logged((l) =>
         l.error("Failed", { error: new WithPrivate("plain") }),
      )) {
         expect(text).toContain("plain");
      }
      logged((l) =>
         l.error("Failed", {
            error: new DOMException("aborted", "AbortError"),
         }),
      );
   });

   it("keep their cause chain masked, as the whole entry or as metadata", () => {
      logged((l) =>
         l.error(new Error("a", { cause: new Error("b", { cause: SIGNED }) })),
      );
      logged((l) => l.error("x", new Error("a", { cause: new Error(SIGNED) })));
      logged((l) =>
         l.error("x", { e: new AggregateError([new Error(SIGNED)], "agg") }),
      );
   });
});

describe("class-instance cycles", () => {
   it("print the copy, never the original's strings", () => {
      class Node {
         url = SIGNED;
         self: unknown = this;
      }
      logged((l) => l.warn("x", { n: new Node() }));
      class Node2 {
         self: unknown = this;
         url = SIGNED;
      }
      logged((l) => l.warn("x", new Node2()));
   });
});

describe("URL objects and other toJSON values", () => {
   it("are logged as their masked JSON", () => {
      for (const text of logged((l) => l.warn("x", { u: new URL(SIGNED) }))) {
         expect(text).toContain("b.s3.amazonaws.com");
      }
      class Wrapped {
         #url = SIGNED;
         toJSON() {
            return { u: this.#url };
         }
      }
      logged((l) => l.warn("x", { t: new Wrapped() }));
   });
});

describe("depth and cost", () => {
   it("never overflows the stack, however deep", () => {
      let deep: Record<string, unknown> = { u: SIGNED };
      for (let i = 0; i < 200_000; i++) deep = { n: deep };
      expect(() => maskedForLog(deep)).not.toThrow();
      let shallow: Record<string, unknown> = { u: SIGNED };
      for (let i = 0; i < 500; i++) shallow = { n: shallow };
      expect(JSON.stringify(maskedForLog(shallow))).not.toContain(SECRET);
   });

   it("returns what it does not change as itself, without copying", () => {
      const rows = Array.from({ length: 50_000 }, (_, i) => ({
         id: i,
         name: `row${i}`,
         nested: { a: [1, 2, 3] },
      }));
      const started = performance.now();
      expect(maskedForLog(rows)).toBe(rows);
      expect(performance.now() - started).toBeLessThan(500);
      const meta = { statusCode: 200, params: { a: "b" }, query: { x: "y" } };
      expect(maskedForLog(meta)).toBe(meta);
      const buf = Buffer.alloc(1 << 20);
      const t = performance.now();
      expect(maskedForLog(buf)).toBe(buf);
      expect(performance.now() - t).toBeLessThan(50);
   });
});

describe("the caller's objects", () => {
   it("are never altered by a log call", () => {
      const err = new Error(SIGNED);
      const stack = err.stack;
      logged((l) => l.error(err));
      expect(err.message).toBe(SIGNED);
      expect(err.stack).toBe(stack);
      const entry = { message: "m", url: SIGNED };
      logged((l) => l.warn(entry));
      expect(entry.url).toBe(SIGNED);
   });
});

describe("separators and echoes", () => {
   it("masks every way a separator is written, and S3's signature echo", () => {
      for (const line of [
         `GET /p?u=https%3A%2F%2Fb%2Fk%3fX-Amz-Signature%3d${SECRET}`,
         `GET /p?u=x%253FX-Amz-Signature%253D${SECRET}%2526a%253D1`,
         `GET /p?u=x%25253FX-Amz-Signature%25253D${SECRET}`,
         `https://b/k?a=1&amp;X-Amz-Signature=${SECRET}`,
         `https://b/k?a=1;X-Amz-Signature=${SECRET}`,
         `https://b/k?a=1\\u0026X-Amz-Signature=${SECRET}`,
         `<Error><SignatureProvided>${SECRET}</SignatureProvided></Error>`,
      ]) {
         logged((l) => l.info(line));
      }
   });

   it("masks an encoded separator inside a value as part of the value", () => {
      expect(
         redactSignedUrlParams(`https://x/?token=abc%26${SECRET}&x=1`),
      ).toBe("https://x/?token=***&x=1");
      // A parameter that only starts like one is left alone.
      expect(redactSignedUrlParams("https://x/?setting=1&sig_count=3")).toBe(
         "https://x/?setting=1&sig_count=3",
      );
   });

   it("stays linear on hostile input", () => {
      for (const input of [
         "a".repeat(1_000_000) + "%26".repeat(100_000),
         "?token=" + "%2".repeat(300_000),
         "%26token%3D" + "%25".repeat(300_000),
      ]) {
         const started = performance.now();
         redactSignedUrlParams(input);
         expect(performance.now() - started).toBeLessThan(500);
      }
   });
});

describe("a Postgres password", () => {
   it("never reaches the log", () => {
      for (const text of logged((l) =>
         l.error("Error initializing environment store", {
            error: "connect postgres://u:hunter2@h/db failed",
         }),
      )) {
         expect(text).not.toContain("hunter2");
      }
   });
});

describe("a log call never throws", () => {
   it("on a revoked proxy, a throwing getter, a frozen Error or an Error's own getter", () => {
      const revocable = Proxy.revocable({}, {});
      revocable.revoke();
      logged((l) => l.warn("x", { p: revocable.proxy }));
      logged((l) =>
         l.warn("x", {
            o: {
               get u() {
                  throw new Error("getter");
               },
            },
         }),
      );
      const frozen = new Error(SIGNED);
      Object.freeze(frozen);
      logged((l) => l.error(frozen));
      const withGetter = new Error("b");
      Object.defineProperty(withGetter, "detail", {
         get: () => SIGNED,
         enumerable: true,
      });
      logged((l) => l.error("x", { err: withGetter }));
      logged((l) => l.error(withGetter));
      logged((l) => l.error("x", withGetter));
   });
});
