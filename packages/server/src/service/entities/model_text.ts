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
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../logger";
import { readPackageURLSync } from "../../utils";
import { lexAll, statementStarts, type Token } from "../malloy_lexer_core";

const modelLines = new AsyncLocalStorage<{
   lines: Map<string, string[]>;
   code: Map<string, ModelCode | undefined>;
   /** The package root a file not in `lines` must lie inside to be read. */
   packageRoot?: string;
}>();

/** A model file's text, its statement tokens and which of them start a statement. */
export interface ModelCode {
   text: string;
   tokens: Token[];
   /** Indexes in `tokens` of each statement's keyword. */
   statementKeywords: ReadonlySet<number>;
   lineStarts: number[];
}

/**
 * Runs `fn` with each model file's lines split at most once. `texts` (by
 * absolute path) are the texts the models compiled from, so a field's code is
 * sliced from the revision its range describes; any other file is read from
 * disk once. The cache is scoped to one run, so concurrent loads never share it.
 */
export function withModelTextCache<T>(
   fn: () => T,
   texts: ReadonlyMap<string, string> = new Map(),
   packageRoot?: string,
): T {
   const lines = new Map<string, string[]>();
   for (const [filePath, text] of texts) lines.set(filePath, text.split("\n"));
   return modelLines.run({ lines, code: new Map(), packageRoot }, fn);
}

function readModelLines(filePath: string): string[] {
   const store = modelLines.getStore();
   const cached = store?.lines.get(filePath);
   if (cached !== undefined) return cached;
   // Inside the package only, when the run names it.
   const text =
      store?.packageRoot !== undefined
         ? readPackageURLSync(store.packageRoot, pathToFileURL(filePath))
         : fs.readFileSync(filePath, "utf8");
   const lines = text.split("\n");
   store?.lines.set(filePath, lines);
   return lines;
}

/**
 * The model file at `url` as the run compiled it, lexed once per run;
 * undefined when that file cannot be read or lexed.
 */
export function modelCode(url: string): ModelCode | undefined {
   let filePath: string;
   let lines: string[];
   try {
      filePath = fileURLToPath(url);
      lines = readModelLines(filePath);
   } catch {
      return undefined;
   }
   const store = modelLines.getStore();
   if (store?.code.has(filePath)) return store.code.get(filePath);
   const model = modelCodeOf(lines.join("\n"));
   if (!model) {
      logger.warn("Model file does not lex; reading its notes by line", {
         filePath,
      });
   }
   store?.code.set(filePath, model);
   return model;
}

/** `text` lexed as a model file; undefined when it does not lex. */
export function modelCodeOf(text: string): ModelCode | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const lineStarts = [0];
   for (let at = text.indexOf("\n"); at >= 0; at = text.indexOf("\n", at + 1)) {
      lineStarts.push(at + 1);
   }
   return {
      text,
      tokens,
      statementKeywords: new Set(statementStarts(tokens)),
      lineStarts,
   };
}

/** The offset of a Malloy position, whose `character` counts code points; undefined when off the text. */
export function modelOffset(
   model: ModelCode,
   pos: { line?: number; character?: number } | undefined,
): number | undefined {
   if (pos?.line === undefined || pos.character === undefined) {
      return undefined;
   }
   let at = model.lineStarts[pos.line];
   if (at === undefined) return undefined;
   for (let k = 0; k < pos.character; k++) {
      if (at >= model.text.length) return undefined;
      const c = model.text.charCodeAt(at);
      at += c >= 0xd800 && c <= 0xdbff ? 2 : 1;
   }
   return at;
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
