import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import { PER_MODE_COLOR_KEYS } from "./config";

/**
 * The per-mode palette key list is hand-copied in three places: this server
 * copy (config.ts), the SDK copy (packages/sdk/src/theme/keys.ts), and the
 * api-doc.yaml Theme.palette schema. The server and the SDK are intentionally
 * decoupled (neither imports the other), and api-doc.yaml is their only shared
 * contract, so nothing but this test keeps the three in step. A silent drift
 * drops a colour: the server sanitizer or the SDK resolver ignores a key that
 * is not in its own copy.
 */

function extractTsArray(source: string, constName: string): string[] {
   const decl = source.indexOf(`${constName} = [`);
   if (decl === -1) throw new Error(`${constName} array not found`);
   const open = source.indexOf("[", decl);
   const close = source.indexOf("]", open);
   return [...source.slice(open + 1, close).matchAll(/"([^"]+)"/g)].map(
      (m) => m[1],
   );
}

function extractApiDocPaletteKeys(apiDoc: string): string[] {
   const start = apiDoc.indexOf("\n        palette:");
   if (start === -1) throw new Error("palette block not found in api-doc.yaml");
   const end = apiDoc.indexOf("\n        font:", start);
   const block = apiDoc.slice(start, end === -1 ? undefined : end);
   // Direct children of `palette.properties` sit at 12-space indentation;
   // deeper keys (type, properties, light, dark) are more indented and skipped.
   return [...block.matchAll(/^ {12}(\w+):$/gm)].map((m) => m[1]);
}

describe("per-mode palette key parity", () => {
   const serverKeys = [...PER_MODE_COLOR_KEYS];

   it("server config.ts matches the SDK keys.ts copy", () => {
      const sdk = readFileSync(
         resolve(import.meta.dir, "../../sdk/src/theme/keys.ts"),
         "utf8",
      );
      expect(extractTsArray(sdk, "PER_MODE_COLOR_KEYS")).toEqual(serverKeys);
   });

   it("server config.ts matches the api-doc.yaml Theme.palette schema", () => {
      const apiDoc = readFileSync(
         resolve(import.meta.dir, "../../../api-doc.yaml"),
         "utf8",
      );
      // palette also declares `series` (a shared array, not a per-mode object).
      expect(new Set(extractApiDocPaletteKeys(apiDoc))).toEqual(
         new Set([...serverKeys, "series"]),
      );
   });
});
