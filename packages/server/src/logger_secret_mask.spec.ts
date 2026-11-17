// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P6-21 (F-27 review): a signed URL's signature, credential or token never
 * reaches the server log, whichever line quotes it: the logger's own format
 * step masks every string of every line. Each case runs through the
 * logger's format in both its modes, the readable line and JSON.
 */

import { describe, expect, it } from "bun:test";
import { Writable } from "stream";
import winston from "winston";
import { logger, maskedForLog, publisherLogFormat } from "./logger";
import { initFailedLine } from "./pg_helpers";

const SECRET = "deadbeef1234";
const SIGNED = `HTTP GET error on 'https://b.s3.amazonaws.com/org_1/t.parquet?X-Amz-Credential=AKIAX&X-Amz-Signature=${SECRET}' (HTTP 403)`;

/** A logger with the server's format in one mode, writing to `lines`. */
function capture(json: boolean): { log: winston.Logger; text: () => string } {
   const lines: string[] = [];
   const log = winston.createLogger({
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
   });
   return { log, text: () => lines.join("\n") };
}

for (const [mode, json] of [
   ["the readable line", false],
   ["JSON", true],
] as const) {
   describe(`P6-21: the log masks signed URL secrets (${mode})`, () => {
      it("on every string of a line, and leaves the caller's object alone", () => {
         const { log, text } = capture(json);
         const meta = { error: { message: SIGNED }, list: [SIGNED] };
         log.warn(`Error loading package: ${SIGNED}`, meta);
         expect(text()).not.toContain(SECRET);
         expect(text()).not.toContain("AKIAX");
         expect(text()).toContain("X-Amz-Signature=***");
         // The bucket and path stay, for the operator.
         expect(text()).toContain("b.s3.amazonaws.com/org_1/t.parquet");
         expect(meta.error.message).toBe(SIGNED);
      });

      it("through a cycle, never printing the original's strings", () => {
         const { log, text } = capture(json);
         const circ: Record<string, unknown> = { url: SIGNED };
         circ.self = circ;
         log.warn("cyclic", { circ });
         expect(text()).not.toContain(SECRET);
         expect(circ.url).toBe(SIGNED);
      });

      it("on an Error logged as the whole entry", () => {
         const { log, text } = capture(json);
         log.error(new Error(SIGNED));
         expect(text()).not.toContain(SECRET);
         expect(text()).toContain("X-Amz-Signature=***");
      });

      it("at any depth", () => {
         const { log, text } = capture(json);
         let deep: Record<string, unknown> = { url: SIGNED };
         for (let i = 0; i < 30; i++) deep = { next: deep };
         log.warn("deep", deep);
         expect(text()).not.toContain(SECRET);
      });

      it("behind percent-encoded separators (an access log's request line)", () => {
         const { log, text } = capture(json);
         log.info(
            `GET /proxy?u=https%3A%2F%2Fb.s3.amazonaws.com%2Fk%3FX-Amz-Credential%3DAKIAX%26X-Amz-Signature%3D${SECRET}%26X-Amz-Date%3D1 200`,
         );
         expect(text()).not.toContain(SECRET);
         expect(text()).not.toContain("AKIAX");
         expect(text()).toContain("X-Amz-Date%3D1");
      });

      it("keeps an Error's message and details, never [object Object]", () => {
         const { log, text } = capture(json);
         const err = Object.assign(new Error(SIGNED), { code: "E_STORAGE" });
         log.error("Request refused", err);
         expect(text()).not.toContain(SECRET);
         expect(text()).not.toContain("[object Object]");
         expect(text()).toContain("X-Amz-Signature=***");
         expect(text()).toContain("E_STORAGE");
      });

      it("passes binary data through, and still masks the line's text", () => {
         // The mask's own cost on a large buffer is timed below; here, a
         // small one, since the JSON format serializes a Buffer's bytes.
         const { log, text } = capture(json);
         log.debug("binary", { buf: Buffer.from("abc"), note: SIGNED });
         expect(text()).not.toContain(SECRET);
      });
   });
}

describe("maskedForLog", () => {
   it("keeps an Error an Error of its class, with its cause and code masked and kept", () => {
      class StorageError extends Error {}
      const err = Object.assign(
         new StorageError(SIGNED, { cause: new Error(SIGNED) }),
         { code: "E_STORAGE" },
      );
      const out = maskedForLog(err) as StorageError & {
         code: string;
         cause: Error;
      };
      expect(out).toBeInstanceOf(StorageError);
      expect(out).not.toBe(err);
      expect(out.message).not.toContain(SECRET);
      expect(out.stack ?? "").not.toContain(SECRET);
      expect(out.code).toBe("E_STORAGE");
      expect(out.cause).toBeInstanceOf(Error);
      expect(out.cause.message).not.toContain(SECRET);
      // The original is untouched.
      expect(err.message).toBe(SIGNED);
   });

   it("returns binary data as itself, at once", () => {
      const buf = Buffer.alloc(1 << 20);
      const started = performance.now();
      expect(maskedForLog(buf)).toBe(buf);
      expect(maskedForLog(new Uint8Array(8))).toBeInstanceOf(Uint8Array);
      expect(performance.now() - started).toBeLessThan(50);
   });

   it("masks the server's own logger too", () => {
      const lines: string[] = [];
      const transport = new winston.transports.Stream({
         stream: new Writable({
            write(chunk, _encoding, done) {
               lines.push(String(chunk));
               done();
            },
         }),
      });
      logger.add(transport);
      try {
         logger.warn(`Error loading package: ${SIGNED}`);
      } finally {
         logger.remove(transport);
      }
      expect(lines.join("\n")).not.toContain(SECRET);
   });
});

describe("the PUBLISHER_INIT_FAILED stderr line", () => {
   it("masks a signed URL's secrets and a Postgres password", () => {
      const line = initFailedLine(
         `${SIGNED}; postgres://admin:hunter2@db.internal/x`,
      );
      expect(line.startsWith("PUBLISHER_INIT_FAILED error=")).toBe(true);
      expect(line).not.toContain(SECRET);
      expect(line).not.toContain("hunter2");
   });
});
