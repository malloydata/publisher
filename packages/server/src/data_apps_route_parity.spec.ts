import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";

/**
 * The data-app listing path is written twice: as an Express route in server.ts
 * and as a path key in api-doc.yaml, which is what every generated client is
 * built from. Nothing else keeps the two in step, and a drift is silent in a
 * way that survives the whole suite: the spec and the route each stay
 * internally consistent, every generated client compiles, and typecheck passes,
 * while the shipped SDK requests a path the server does not serve. The UI hides
 * it too, because Package.tsx deliberately swallows a 404 from this endpoint and
 * renders the package without a Data Apps section. Verified by reverting the
 * path in api-doc.yaml alone: unit and integration both stayed fully green.
 */

const REPO_ROOT = resolve(import.meta.dir, "../../..");

/** The Express route string, with `:param` rewritten to the spec's `{param}`. */
function expressRoute(serverSource: string): string {
   const m = serverSource.match(
      /`\$\{API_PREFIX\}(\/environments\/[^`]*\/data-apps)`/,
   );
   if (!m) throw new Error("data-apps route not found in server.ts");
   return m[1].replace(/:(\w+)/g, "{$1}");
}

/** Path keys in api-doc.yaml that end in `/data-apps`. */
function specPaths(apiDoc: string): string[] {
   return [...apiDoc.matchAll(/^ {2}(\/\S*\/data-apps):$/gm)].map((x) => x[1]);
}

describe("data-app listing route parity", () => {
   const serverSource = readFileSync(
      resolve(import.meta.dir, "server.ts"),
      "utf8",
   );
   const apiDoc = readFileSync(resolve(REPO_ROOT, "api-doc.yaml"), "utf8");

   it("api-doc.yaml declares exactly one data-apps path", () => {
      expect(specPaths(apiDoc)).toHaveLength(1);
   });

   it("the spec path matches the route server.ts registers", () => {
      expect(specPaths(apiDoc)[0]).toBe(expressRoute(serverSource));
   });

   it("no /pages listing path or operationId survives in the spec", () => {
      expect(apiDoc).not.toContain("/pages:");
      expect(apiDoc).not.toContain("operationId: list-pages");
   });
});
