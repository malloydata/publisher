// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P6-21 (F-27 review): a signed URL's signature, credential or token never
 * reaches the server log, whichever line quotes it (a package load, a
 * publish, a refused request, the HTTP access log): the logger's own format
 * step masks every string of every line.
 */

import { describe, expect, it } from "bun:test";
import { Writable } from "stream";
import winston from "winston";
import { logger, maskedForLog } from "./logger";

const SIGNED =
   "HTTP GET error on 'https://b.s3.amazonaws.com/org_1/t.parquet?X-Amz-Credential=AKIAX&X-Amz-Signature=deadbeef1234' (HTTP 403)";

describe("P6-21: the log masks signed URL secrets", () => {
   it("on every string of a line, its message and nested metadata alike", () => {
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
         const meta = { error: { message: SIGNED }, list: [SIGNED] };
         logger.warn(`Error loading package: ${SIGNED}`, meta);
         logger.error("Request refused", new Error(SIGNED));
         const out = lines.join("\n");
         expect(out).not.toContain("deadbeef1234");
         expect(out).not.toContain("AKIAX");
         expect(out).toContain("X-Amz-Signature=***");
         // The bucket and path stay, for the operator.
         expect(out).toContain("b.s3.amazonaws.com/org_1/t.parquet");
         // The caller's own object is not altered.
         expect(meta.error.message).toBe(SIGNED);
      } finally {
         logger.remove(transport);
      }
   });

   it("leaves a value with nothing to mask as the same object", () => {
      const value = { a: "plain", b: [1, "two"] };
      expect(maskedForLog(value)).toBe(value);
   });
});
