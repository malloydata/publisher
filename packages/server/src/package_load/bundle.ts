// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A compiled package bundle: the package-load worker's outcome, written to a
 * file so another process can hydrate the package without compiling it and
 * without touching a connection.
 *
 * Layout: one line of JSON (the stamp), a newline, then
 * `gzip(v8.serialize(result))`. v8's ValueSerializer is the structured-clone
 * format `postMessage` uses, so object aliasing inside the outcome survives
 * the file exactly as it survives the worker boundary (see protocol.ts on why
 * `#(authorize)` cares).
 *
 * The stamp is what decides whether a bundle may be hydrated here: the same
 * compiler produced it (a `ModelDef` has no cross-version guarantee), and the
 * same source tree went into it. Anything else is a miss, never an error.
 */
import { createHash } from "crypto";
import * as fs from "fs";
import { createRequire } from "module";
import * as path from "path";
import * as v8 from "v8";
import * as zlib from "zlib";
import type { LoadPackageResult } from "./protocol";
import { SERVER_VERSION } from "../version";

export const BUNDLE_FORMAT = "malloy-package-bundle/1";

export interface BundleStamp {
   format: typeof BUNDLE_FORMAT;
   /** `@malloydata/malloy` version that compiled the outcome. */
   compilerVersion: string;
   publisherVersion: string;
   packageName: string;
   /** {@link computeSourceTreeSha} of the package directory that was compiled. */
   sourceTreeSha: string;
   /** `LoadPackageResult.protocolVersion` of the outcome inside, so a shape change is a miss. */
   protocolVersion: number;
   modelCount: number;
   createdAt: string;
}

/** How a load got its models, reported on the package and counted. */
export type BundleLoadStatus =
   | "hit"
   | "absent"
   | "unreadable"
   | "stale_compiler"
   | "stale_source"
   | "compiled";

const require = createRequire(import.meta.url);

export function compilerVersion(): string {
   return (require("@malloydata/malloy/package.json") as { version: string })
      .version;
}

export function publisherVersion(): string {
   // The static import `src/version.ts` already carries, so it survives the
   // dist bundle; a relative require of package.json does not.
   return SERVER_VERSION;
}

/**
 * SHA-256 over every regular file under `packagePath` (relative path, NUL,
 * bytes), sorted, dot-directories skipped. A stand-in for the archive digest a
 * control plane already holds; here it is what makes "the same source tree" a
 * check rather than a hope.
 */
export function computeSourceTreeSha(packagePath: string): string {
   const files: string[] = [];
   const walk = (dir: string) => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
         if (entry.name.startsWith(".")) continue;
         // The root publisher.json is rewritten by an install with serving
         // metadata (location, manifestLocation, modelBundleLocation), so the
         // served tree never hashes equal to the compiled one if it is
         // counted. A manifest edit reaches a server as a new archive, whose
         // digest is the key a control plane supplies; this hash is the
         // standalone stand-in.
         if (dir === packagePath && entry.name === "publisher.json") continue;
         const abs = path.join(dir, entry.name);
         if (entry.isDirectory()) walk(abs);
         else if (entry.isFile()) files.push(abs);
      }
   };
   walk(packagePath);
   const hash = createHash("sha256");
   for (const abs of files.sort()) {
      hash.update(path.relative(packagePath, abs).split(path.sep).join("/"));
      hash.update("\0");
      hash.update(fs.readFileSync(abs));
      hash.update("\0");
   }
   return hash.digest("hex");
}

export function encodeBundle(
   stamp: BundleStamp,
   result: LoadPackageResult,
): Buffer {
   const header = Buffer.from(JSON.stringify(stamp) + "\n", "utf8");
   const body = zlib.gzipSync(v8.serialize(result));
   return Buffer.concat([header, body]);
}

export function decodeBundleStamp(buf: Buffer): BundleStamp {
   const nl = buf.indexOf(0x0a);
   if (nl < 0) throw new Error("bundle has no stamp line");
   const stamp = JSON.parse(
      buf.subarray(0, nl).toString("utf8"),
   ) as BundleStamp;
   if (stamp.format !== BUNDLE_FORMAT) {
      throw new Error(
         `bundle format ${String(stamp.format)} is not ${BUNDLE_FORMAT}`,
      );
   }
   return stamp;
}

export function decodeBundle(buf: Buffer): {
   stamp: BundleStamp;
   result: LoadPackageResult;
} {
   const stamp = decodeBundleStamp(buf);
   const nl = buf.indexOf(0x0a);
   const result = v8.deserialize(
      zlib.gunzipSync(buf.subarray(nl + 1)),
   ) as LoadPackageResult;
   return { stamp, result };
}

/** Temp file then rename, so a reader never sees a partial bundle. */
export function writeBundleFile(outPath: string, bytes: Buffer): void {
   fs.mkdirSync(path.dirname(outPath), { recursive: true });
   const tmp = `${outPath}.${process.pid}.tmp`;
   fs.writeFileSync(tmp, bytes);
   fs.renameSync(tmp, outPath);
}

/**
 * Whether a bundle may be hydrated by this process for this package tree.
 * Returns the miss reason otherwise.
 */
export function checkBundleStamp(
   stamp: BundleStamp,
   expected: {
      compilerVersion: string;
      sourceTreeSha: string;
      protocolVersion: number;
   },
): Exclude<BundleLoadStatus, "absent" | "unreadable" | "compiled"> {
   if (stamp.protocolVersion !== expected.protocolVersion)
      return "stale_compiler";
   if (stamp.compilerVersion !== expected.compilerVersion)
      return "stale_compiler";
   if (stamp.sourceTreeSha !== expected.sourceTreeSha) return "stale_source";
   return "hit";
}
