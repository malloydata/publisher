// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type { S3 } from "@aws-sdk/client-s3";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { S3PackageSource } from "./s3_source";

/**
 * An S3 download streams each object's body to disk. An error mid-transfer
 * must reject the fetch, as any other download failure does, and never
 * escape as an unhandled rejection (which crashes the process under Node's
 * default).
 */

const encoder = new TextEncoder();

/** A body that yields `text`, then errors when `fail` is set. */
function body(text: string, fail: boolean) {
   return {
      transformToWebStream: () => {
         let sent = false;
         return new ReadableStream<Uint8Array>({
            pull(controller) {
               if (!sent) {
                  sent = true;
                  controller.enqueue(encoder.encode(text));
                  return;
               }
               if (fail) {
                  controller.error(new Error("connection reset mid-stream"));
               } else {
                  controller.close();
               }
            },
         });
      },
   };
}

function client(fail: boolean): () => S3 {
   return () =>
      ({
         listObjectsV2: async () => ({
            Contents: [{ Key: "pkg/model.malloy" }],
         }),
         send: async () => ({
            Body: body("source: x is duckdb.sql('select 1')\n", fail),
         }),
      }) as unknown as S3;
}

let root: string;
const unhandled: unknown[] = [];
const onUnhandled = (reason: unknown) => {
   unhandled.push(reason);
};

beforeEach(() => {
   root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "s3-src-")));
   unhandled.length = 0;
   process.on("unhandledRejection", onUnhandled);
});

afterEach(() => {
   process.off("unhandledRejection", onUnhandled);
   fs.rmSync(root, { recursive: true, force: true });
});

/** Let any stray rejection reach the process before asserting there was none. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

describe("S3PackageSource", () => {
   it("writes every object under the prefix", async () => {
      const target = path.join(root, "pkg");
      await new S3PackageSource(client(false)).fetch(
         "s3://bucket/pkg",
         target,
         {
            environmentName: "env",
         } as never,
      );
      expect(
         fs.readFileSync(path.join(target, "model.malloy"), "utf-8"),
      ).toContain("source: x");
   });

   it("rejects a directory download whose body errors mid-stream", async () => {
      const target = path.join(root, "pkg");
      await expect(
         new S3PackageSource(client(true)).fetch("s3://bucket/pkg", target, {
            environmentName: "env",
         } as never),
      ).rejects.toThrow("connection reset mid-stream");
      await settle();
      expect(unhandled).toEqual([]);
   });

   it("rejects a zip download whose body errors mid-stream", async () => {
      const target = path.join(root, "pkg");
      await expect(
         new S3PackageSource(client(true)).fetch(
            "s3://bucket/pkg.zip",
            target,
            { environmentName: "env" } as never,
         ),
      ).rejects.toThrow("connection reset mid-stream");
      await settle();
      expect(unhandled).toEqual([]);
   });
});
