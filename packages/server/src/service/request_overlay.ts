// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A request's `files` overlay: package files a compile or query reads instead
 * of, or in addition to, the package's own, for that one request. Nothing is
 * written to disk or kept: the overlay is served to the compiler by its file
 * URL and dropped with the request.
 *
 * Overlay files are author content, read exactly as the package's own files:
 * they are not restricted like `append` text or an ad-hoc query, and their
 * `#(authorize)` and `#(access_filter)` annotations gate as written. A host
 * must therefore offer `files` only to callers who may edit the package.
 */

import * as path from "path";
import { pathToFileURL } from "url";
import { getMaxOverlayBytes, getMaxOverlayFiles } from "../config";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../constants";
import { BadRequestError, PayloadTooLargeError } from "../errors";
import { assertSafeRelativeModelPath } from "../path_safety";
import { lazyCounter } from "../telemetry";

/** One overlaid file: its package-relative path and its whole text. */
export interface OverlayFile {
   path: string;
   content: string;
}

/** The routes that take an overlay, as `publisher_request_overlay_total` labels them. */
export type OverlayRoute = "compile" | "query";

const overlayRequests = lazyCounter(
   "publisher_request_overlay_total",
   "Compile and query requests that carried a `files` overlay. Labels: route (compile|query), outcome (accepted | refused_entry = a malformed or unsafe entry, 400 | refused_bounds = past PUBLISHER_MAX_OVERLAY_FILES or PUBLISHER_MAX_OVERLAY_BYTES, 413).",
);

/**
 * The request's `files`, checked: undefined when absent. A malformed entry, an
 * unsafe or non-model path, or a path given twice is a 400; more files or bytes
 * than the server's bounds is a 413. Counted either way.
 */
export function parseOverlayFiles(
   input: unknown,
   route: OverlayRoute,
): OverlayFile[] | undefined {
   if (input === undefined || input === null) return undefined;
   try {
      const files = checkOverlayFiles(input);
      overlayRequests().add(1, { route, outcome: "accepted" });
      return files;
   } catch (error) {
      overlayRequests().add(1, {
         route,
         outcome:
            error instanceof PayloadTooLargeError
               ? "refused_bounds"
               : "refused_entry",
      });
      throw error;
   }
}

function checkOverlayFiles(input: unknown): OverlayFile[] {
   if (!Array.isArray(input)) {
      throw new BadRequestError(
         "`files` must be an array of { path, content } entries.",
      );
   }
   const maxFiles = getMaxOverlayFiles();
   if (input.length > maxFiles) {
      throw new PayloadTooLargeError(
         `\`files\` holds ${input.length} files; this server takes at most ${maxFiles} per request (PUBLISHER_MAX_OVERLAY_FILES).`,
      );
   }
   const maxBytes = getMaxOverlayBytes();
   const seen = new Set<string>();
   const files: OverlayFile[] = [];
   let bytes = 0;
   input.forEach((entry: unknown, index: number) => {
      const where = `\`files[${index}]\``;
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
         throw new BadRequestError(
            `${where} must be an object with \`path\` and \`content\`.`,
         );
      }
      const { path: filePath, content } = entry as Record<string, unknown>;
      if (typeof content !== "string") {
         throw new BadRequestError(`${where}.content must be a string.`);
      }
      try {
         assertSafeRelativeModelPath(filePath);
      } catch {
         throw new BadRequestError(
            `${where}.path must be a package-relative path with no "." or ".." segment, no leading "/" and no backslash (got ${JSON.stringify(filePath)}).`,
         );
      }
      if (
         !filePath.endsWith(MODEL_FILE_SUFFIX) &&
         !filePath.endsWith(NOTEBOOK_FILE_SUFFIX)
      ) {
         throw new BadRequestError(
            `${where}.path must name a ${MODEL_FILE_SUFFIX} or ${NOTEBOOK_FILE_SUFFIX} file (got "${filePath}"); other files are read from the package as published.`,
         );
      }
      if (seen.has(filePath)) {
         throw new BadRequestError(
            `${where}.path "${filePath}" appears more than once in \`files\`.`,
         );
      }
      seen.add(filePath);
      bytes += Buffer.byteLength(content, "utf8");
      if (bytes > maxBytes) {
         throw new PayloadTooLargeError(
            `\`files\` holds more than ${maxBytes} bytes of text; this server takes at most that per request (PUBLISHER_MAX_OVERLAY_BYTES).`,
         );
      }
      files.push({ path: filePath, content });
   });
   return files;
}

/**
 * The overlay keyed as the compiler asks for files: each file's absolute
 * `file://` URL (`href`) under `packagePath`.
 */
export function overlayByUrl(
   packagePath: string,
   files: readonly OverlayFile[] | undefined,
): Map<string, string> {
   const byUrl = new Map<string, string>();
   for (const file of files ?? []) {
      byUrl.set(
         pathToFileURL(path.join(packagePath, file.path)).href,
         file.content,
      );
   }
   return byUrl;
}

/** The overlaid text of `filePath`, if the overlay holds it. */
export function overlaidText(
   files: readonly OverlayFile[] | undefined,
   filePath: string,
): string | undefined {
   return files?.find((file) => file.path === filePath)?.content;
}
