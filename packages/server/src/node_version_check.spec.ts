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
   it("rejects the Node version that produced the original defect", () => {
      const verdict = evaluateRuntime({ nodeVersion: "v18.20.8" });
      expect(verdict.supported).toBe(false);
   });

   it("names Node, the requirement, and the detected version", () => {
      // The entire point of the change: the failure it replaces named none of
      // these three things.
      const message = evaluateRuntime({ nodeVersion: "v18.20.8" }).message;
      expect(message).toBeDefined();
      expect(message).toContain("Node");
      expect(message).toContain(REQUIRED_NODE_RANGE);
      expect(message).toContain("v18.20.8");
   });

   it("emits a greppable token as the first line, like PUBLISHER_READY", () => {
      const message =
         evaluateRuntime({ nodeVersion: "v18.20.8" }).message ?? "";
      expect(message.split("\n")[0]).toBe(
         `${UNSUPPORTED_NODE_TOKEN} required=${REQUIRED_NODE_RANGE} detected=v18.20.8`,
      );
   });

   it("does not claim the detected version lacks global crypto when it does not", () => {
      // Node 19 shipped the global Web Crypto API unflagged but is still below
      // the floor, so a message templated as "<detected> does not provide the
      // global Web Crypto API" would be false for it. The mechanism has to be
      // attributed to the versions it actually applies to.
      const message = evaluateRuntime({ nodeVersion: "v19.9.0" }).message ?? "";
      expect(message).not.toContain("v19.9.0 does not provide");
   });

   it("accepts the minimum supported major at its .0.0 boundary", () => {
      expect(
         evaluateRuntime({ nodeVersion: `v${MIN_NODE_MAJOR}.0.0` }).supported,
      ).toBe(true);
   });

   it("rejects the major immediately below the floor", () => {
      expect(
         evaluateRuntime({ nodeVersion: `v${MIN_NODE_MAJOR - 1}.99.99` })
            .supported,
      ).toBe(false);
   });

   it("accepts a current Node", () => {
      expect(evaluateRuntime({ nodeVersion: "v24.15.0" }).supported).toBe(true);
   });

   it("exempts Bun even when it reports an old Node version", () => {
      // Load-bearing: the Docker image runs the bundle under Bun
      // (CMD ["bun", "run", ...]), and Bun has global crypto regardless of the
      // Node version it reports. Without this the image would refuse to boot.
      expect(
         evaluateRuntime({ nodeVersion: "v18.20.8", bunVersion: "1.3.13" })
            .supported,
      ).toBe(true);
   });

   it("treats an unparseable version as supported rather than blocking boot", () => {
      for (const nodeVersion of ["", "not-a-version", "v"]) {
         expect(evaluateRuntime({ nodeVersion }).supported).toBe(true);
      }
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
   // The published package is what a user installs, so `engines` has to travel
   // with it. This spec is the pin: deleting the field, or letting it drift
   // from MIN_NODE_MAJOR, fails the unit suite rather than shipping silently.
   // That drift is the whole defect, npm had nothing to enforce.
   const pkg = JSON.parse(
      readFileSync(path.join(import.meta.dir, "..", "package.json"), "utf8"),
   ) as { engines?: { node?: string } };

   it("declares engines.node in the published package", () => {
      expect(pkg.engines?.node).toBeDefined();
   });

   it("declares the same floor the boot check enforces", () => {
      expect(pkg.engines?.node).toBe(REQUIRED_NODE_RANGE);
   });
});
