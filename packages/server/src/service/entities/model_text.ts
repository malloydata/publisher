// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Malloy text a compiled source or field was declared with, read from the
 * file its location names.
 */

import type {
   Explore as MalloyExplore,
   Field as MalloyField,
} from "@malloydata/malloy";
import fs from "fs";
import { AsyncLocalStorage } from "node:async_hooks";
import { fileURLToPath } from "url";
import { logger } from "../../logger";

const modelLines = new AsyncLocalStorage<Map<string, string[]>>();

/**
 * Runs `fn` with each model file's lines split at most once. `texts` (by
 * absolute path) are the texts the models compiled from, so a field's code is
 * sliced from the revision its range describes; any other file is read from
 * disk once. The cache is scoped to one run, so concurrent loads never share it.
 */
export function withModelTextCache<T>(
   fn: () => T,
   texts: ReadonlyMap<string, string> = new Map(),
): T {
   const cache = new Map<string, string[]>();
   for (const [filePath, text] of texts) cache.set(filePath, text.split("\n"));
   return modelLines.run(cache, fn);
}

function readModelLines(filePath: string): string[] {
   const cache = modelLines.getStore();
   const cached = cache?.get(filePath);
   if (cached !== undefined) return cached;
   const lines = fs.readFileSync(filePath, "utf8").split("\n");
   cache?.set(filePath, lines);
   return lines;
}

/**
 * Extracts the Malloy code from an entity based on location.
 */
export function extractCodeFromEntity(
   entity: MalloyExplore | MalloyField,
   docString: string,
): string {
   const codeRange = entity.location?.range;
   if (!codeRange) {
      return "";
   }
   // If the code comes from another file, we need to load that file
   let lines: string[];
   const locationUrl = entity.location?.url;
   if (locationUrl && !locationUrl.endsWith("internal.malloy")) {
      const filePath = fileURLToPath(locationUrl);
      try {
         lines = readModelLines(filePath);
      } catch (error) {
         logger.warn(
            `Failed to read file for entity ${entity.name}: ${filePath}`,
            error,
         );
         return "";
      }
   } else {
      lines = docString.split("\n");
   }
   const keep: string[] = [];
   for (let i = codeRange.start.line; i <= codeRange.end.line; i++) {
      const line = lines[i];
      // A blank line inside the declaration is `""`, not out of bounds.
      if (i < 0 || i >= lines.length || line === undefined) {
         logger.warn("Trying to get code out of bounds");
         break;
      }

      if (i === codeRange.start.line && i === codeRange.end.line) {
         keep.push(
            line.slice(codeRange.start.character, codeRange.end.character),
         );
      } else if (i === codeRange.start.line) {
         keep.push(line.slice(codeRange.start.character));
      } else if (i === codeRange.end.line) {
         keep.push(line.slice(0, codeRange.end.character));
      } else {
         keep.push(line);
      }
   }
   return keep.join("\n");
}
