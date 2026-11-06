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
import { getMaxOverlayBytes, getMaxOverlayFiles } from "../config";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../constants";
import {
   BadRequestError,
   FeatureDisabledError,
   PayloadTooLargeError,
} from "../errors";
import { assertSafeRelativeModelPath } from "../path_safety";
import { lazyCounter } from "../telemetry";
import { overlayKeyOfPath } from "./overlay_key";
import { settings } from "../settings";

/** One overlaid file: its package-relative path and its whole text. */
export interface OverlayFile {
   path: string;
   content: string;
}

/** The routes that take an overlay, as `publisher_request_overlay_total` labels them. */
export type OverlayRoute = "compile" | "query";

const overlayRequests = lazyCounter(
   "publisher_request_overlay_total",
   "Compile and query requests that carried a `files` overlay. Labels: route (compile|query), outcome (accepted | refused_entry = a malformed or unsafe entry, 400 | refused_bounds = past PUBLISHER_MAX_OVERLAY_FILES or PUBLISHER_MAX_OVERLAY_BYTES, 413 | refused_disabled = the server does not accept an overlay (query.filesOverlayEnabled off), 403).",
);

/**
 * The request's `files`, checked: undefined when absent or empty. A malformed
 * entry, an unsafe or non-model path, or a path given twice is a 400; more
 * files or bytes than the server's bounds is a 413; any overlay on a server
 * without `query.filesOverlayEnabled` is a 403. Counted either way.
 */
export function parseOverlayFiles(
   input: unknown,
   route: OverlayRoute,
): OverlayFile[] | undefined {
   if (input === undefined || input === null) return undefined;
   try {
      const files = checkOverlayFiles(input);
      // An empty overlay overlays nothing: the request is an ordinary one.
      if (files.length === 0) return undefined;
      if (!settings.get("query.filesOverlayEnabled")) {
         throw new FeatureDisabledError(
            "`files` is not accepted by this server: its overlay files are package content whose gates apply as written. A deployment turns it on with PUBLISHER_QUERY_FILES_OVERLAY_ENABLED=true, behind a host that offers it only to callers who may edit the package.",
         );
      }
      overlayRequests().add(1, { route, outcome: "accepted" });
      return files;
   } catch (error) {
      overlayRequests().add(1, {
         route,
         outcome:
            error instanceof PayloadTooLargeError
               ? "refused_bounds"
               : error instanceof FeatureDisabledError
                 ? "refused_disabled"
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
         overlayKeyOfPath(path.join(packagePath, file.path)),
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
