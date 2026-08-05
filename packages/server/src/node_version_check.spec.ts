import { describe, expect, it, mock } from "bun:test";
import { readFileSync } from "fs";
import * as path from "path";

import {
   assertSupportedNodeVersion,
   evaluateRuntime,
   MIN_NODE_MAJOR,
   REQUIRED_NODE_RANGE,
   UNSUPPORTED_NODE_TOKEN,
} from "./node_version_check";

describe("evaluateRuntime", () => {
   it("supports the floor and above, refuses below it, and never blocks on an unreadable version", () => {
      const cases: [string, boolean][] = [
         [`v${MIN_NODE_MAJOR - 1}.99.99`, false],
         [`v${MIN_NODE_MAJOR}.0.0`, true],
         ["v24.15.0", true],
         // Only guard a version we can read: refusing a working runtime over an
         // odd version string would be worse than the failure this prevents.
         ["", true],
         ["not-a-version", true],
         ["v", true],
      ];
      for (const [nodeVersion, supported] of cases) {
         expect(evaluateRuntime({ nodeVersion }).supported).toBe(supported);
      }
   });

   it("exempts Bun even when it reports an old Node version", () => {
      // Load-bearing: the Docker image runs the bundle under Bun
      // (CMD ["bun", "run", ...]), so without this the image refuses to boot.
      expect(
         evaluateRuntime({ nodeVersion: "v18.20.8", bunVersion: "1.3.13" })
            .supported,
      ).toBe(true);
   });

   it("names the requirement and the detected version, behind a greppable token", () => {
      // The entire point of the change: the failure it replaces named neither,
      // and the token lets a script treat this as terminal like PUBLISHER_READY.
      const message =
         evaluateRuntime({ nodeVersion: "v18.20.8" }).message ?? "";
      expect(message.split("\n")[0]).toBe(
         `${UNSUPPORTED_NODE_TOKEN} required=${REQUIRED_NODE_RANGE} detected=v18.20.8`,
      );
      expect(message).toContain("Node");
      expect(message).toContain("v18.20.8");
   });

   it("states the floor as policy rather than naming a mechanism", () => {
      // The crash that exposed the floor (the missing `crypto` global on the
      // query path) is fixed in service/query_metadata.ts, so text blaming it
      // would describe something that no longer happens on any Node.
      const message =
         evaluateRuntime({ nodeVersion: "v18.20.8" }).message ?? "";
      expect(message).not.toContain("crypto");
      expect(message).toContain("declare Node");
   });
});

describe("assertSupportedNodeVersion", () => {
   it("writes the message and exits non-zero on an unsupported Node", () => {
      const write = mock((_text: string) => undefined);
      const exit = mock((_code: number) => undefined as never);

      assertSupportedNodeVersion({
         nodeVersion: "v18.20.8",
         bunVersion: undefined,
         write,
         exit,
      });

      expect(write).toHaveBeenCalledTimes(1);
      expect(exit).toHaveBeenCalledTimes(1);
      expect(exit.mock.calls[0]?.[0]).toBe(1);
      expect(String(write.mock.calls[0]?.[0])).toContain(
         UNSUPPORTED_NODE_TOKEN,
      );
   });

   it("does nothing on a supported Node", () => {
      const write = mock((_text: string) => undefined);
      const exit = mock((_code: number) => undefined as never);

      assertSupportedNodeVersion({
         nodeVersion: "v24.15.0",
         bunVersion: undefined,
         write,
         exit,
      });

      expect(write).not.toHaveBeenCalled();
      expect(exit).not.toHaveBeenCalled();
   });

   it("still exits when the stderr write throws", () => {
      const exit = mock((_code: number) => undefined as never);
      assertSupportedNodeVersion({
         nodeVersion: "v18.20.8",
         bunVersion: undefined,
         write: () => {
            throw new Error("stderr closed");
         },
         exit,
      });
      expect(exit).toHaveBeenCalledTimes(1);
   });
});

describe("engines contract", () => {
   // Six declarations of one floor: this constant, the scaffolder's, and
   // `engines.node` in the server, root, scaffolder and CLI manifests. This
   // block asserts all four manifests against MIN_NODE_MAJOR, and the
   // scaffolder's own spec pins its constant to its manifest, which closes the
   // chain: no declaration can drift without a test failing. Silent drift
   // between two declarations of the same requirement is the class of defect
   // this module exists to prevent, so it must not reintroduce it one level up.
   //
   // Read from disk rather than imported: a spec-time file read keeps the
   // packages independent at build and run time, which importing a shared
   // constant across workspace packages would not.
   const manifests = {
      "server package": ["package.json"],
      "repo root": ["..", "..", "package.json"],
      scaffolder: ["..", "create-malloy-package", "package.json"],
      cli: ["..", "cli", "package.json"],
   };

   it("declares one floor across every published manifest", () => {
      for (const [name, segments] of Object.entries(manifests)) {
         const pkg = JSON.parse(
            readFileSync(path.join(import.meta.dir, "..", ...segments), "utf8"),
         ) as { engines?: { node?: string } };
         expect(pkg.engines?.node, `${name} engines.node`).toBe(
            REQUIRED_NODE_RANGE,
         );
      }
   });
});
