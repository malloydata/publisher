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

const modelTexts = new AsyncLocalStorage<Map<string, string>>();

/**
 * Runs `fn` with each model file read from disk at most once. The extraction
 * reads a field's declaring file for its code several times per field; the
 * cache is scoped to one run, so concurrent loads never share it.
 */
export function withModelTextCache<T>(fn: () => T): T {
   return modelTexts.run(new Map(), fn);
}

function readModelText(filePath: string): string {
   const cache = modelTexts.getStore();
   const cached = cache?.get(filePath);
   if (cached !== undefined) return cached;
   const text = fs.readFileSync(filePath, "utf8");
   cache?.set(filePath, text);
   return text;
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
   const locationUrl = entity.location?.url;
   if (locationUrl && !locationUrl.endsWith("internal.malloy")) {
      const filePath = fileURLToPath(locationUrl);
      try {
         docString = readModelText(filePath);
      } catch (error) {
         logger.warn(
            `Failed to read file for entity ${entity.name}: ${filePath}`,
            error,
         );
         return "";
      }
   }
   const lines = docString.split("\n");
   const keep: string[] = [];
   for (let i = codeRange.start.line; i <= codeRange.end.line; i++) {
      const line = lines[i];
      if (i < 0 || i >= lines.length || !line) {
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
