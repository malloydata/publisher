import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import * as path from "node:path";

import {
   MIN_NODE_MAJOR,
   nodeVersionWarning,
   REQUIRED_NODE_RANGE,
} from "./node_version";

describe("nodeVersionWarning", () => {
   it("names the requirement and what is actually running", () => {
      // The failure it warns about names neither.
      const warning = nodeVersionWarning({ nodeVersion: "v18.20.8" });
      expect(warning).toContain("Node.js");
      expect(warning).toContain(String(MIN_NODE_MAJOR));
      expect(warning).toContain("v18.20.8");
   });

   it("does not claim npm start will refuse to boot", () => {
      // The pinned SERVER_VERSION may predate the server's own boot check, in
      // which case the failure is still a 500 on the first query. Promising a
      // refusal would be false for that version.
      const warning = nodeVersionWarning({ nodeVersion: "v18.20.8" }) ?? "";
      expect(warning.toLowerCase()).not.toContain("refuse");
   });

   it("warns below the floor only, and never on an unreadable version or Bun", () => {
      const cases: [string, string | undefined, boolean][] = [
         [`v${MIN_NODE_MAJOR - 1}.99.99`, undefined, true],
         [`v${MIN_NODE_MAJOR}.0.0`, undefined, false],
         ["v24.15.0", undefined, false],
         ["not-a-version", undefined, false],
         // Bun reports a Node version that says nothing about what it provides.
         ["v18.20.8", "1.3.13", false],
      ];
      for (const [nodeVersion, bunVersion, warns] of cases) {
         expect(
            nodeVersionWarning({ nodeVersion, bunVersion }) !== undefined,
            `${nodeVersion} bun=${bunVersion}`,
         ).toBe(warns);
      }
   });
});

describe("engines contract", () => {
   // Pins this package's declared floor to the one it warns about. The server's
   // node_version_check.spec.ts pins this same manifest to the server's floor,
   // so the two packages cannot drift apart without a test failing.
   const pkg = JSON.parse(
      readFileSync(path.join(import.meta.dir, "..", "package.json"), "utf8"),
   ) as { engines?: { node?: string } };

   it("declares the same floor it warns about", () => {
      expect(pkg.engines?.node).toBe(REQUIRED_NODE_RANGE);
   });
});
