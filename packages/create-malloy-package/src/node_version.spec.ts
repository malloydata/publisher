import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import * as path from "node:path";

import {
   MIN_NODE_MAJOR,
   nodeVersionWarning,
   REQUIRED_NODE_RANGE,
} from "./node_version";

describe("nodeVersionWarning", () => {
   it("warns on the Node version that produced the original defect", () => {
      const warning = nodeVersionWarning({ nodeVersion: "v18.20.8" });
      expect(warning).toBeDefined();
      // Names the requirement and what is actually running, which the failure
      // it is warning about does neither.
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

   it("stays quiet on a supported Node", () => {
      expect(nodeVersionWarning({ nodeVersion: "v24.15.0" })).toBeUndefined();
      expect(
         nodeVersionWarning({ nodeVersion: `v${MIN_NODE_MAJOR}.0.0` }),
      ).toBeUndefined();
   });

   it("warns on the major immediately below the floor", () => {
      expect(
         nodeVersionWarning({ nodeVersion: `v${MIN_NODE_MAJOR - 1}.99.99` }),
      ).toBeDefined();
   });

   it("stays quiet under Bun", () => {
      expect(
         nodeVersionWarning({
            nodeVersion: "v18.20.8",
            bunVersion: "1.3.13",
         }),
      ).toBeUndefined();
   });

   it("stays quiet on a version it cannot parse", () => {
      for (const nodeVersion of ["", "not-a-version", "v"]) {
         expect(nodeVersionWarning({ nodeVersion })).toBeUndefined();
      }
   });
});

describe("engines contract", () => {
   // Pins this package's declared floor to the one it warns about, so the two
   // cannot drift apart silently.
   const pkg = JSON.parse(
      readFileSync(path.join(import.meta.dir, "..", "package.json"), "utf8"),
   ) as { engines?: { node?: string } };

   it("declares the same floor it warns about", () => {
      expect(pkg.engines?.node).toBe(REQUIRED_NODE_RANGE);
   });
});
