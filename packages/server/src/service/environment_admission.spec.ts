import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { ServiceUnavailableError } from "../errors";
import { buildEnvironmentMalloyConfig } from "./connection";
import { Environment } from "./environment";
import type { PackageMemoryGovernor } from "./package_memory_governor";

/**
 * Minimal subset of {@link PackageMemoryGovernor} that
 * `Environment.assertCanAdmitNewPackage` actually consults. Allows us
 * to drive the gate from the tests without spinning the real OTel
 * instrumentation pipeline.
 */
class StubGovernor {
   public backpressured = false;
   isBackpressured(): boolean {
      return this.backpressured;
   }
}

function makeEnvironment(envPath: string): Environment {
   const malloyConfig = buildEnvironmentMalloyConfig([], envPath);
   return new Environment("test-env", envPath, malloyConfig, []);
}

describe("Environment admission gate (memory governor choke point)", () => {
   let envDir: string;

   beforeEach(() => {
      envDir = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-env-admission-"),
      );
   });

   afterEach(() => {
      fs.rmSync(envDir, { recursive: true, force: true });
   });

   it("admits new packages when no governor is attached (legacy behaviour)", async () => {
      const env = makeEnvironment(envDir);
      // No governor set; the gate must be a pure no-op. The package
      // doesn't exist on disk, so addPackage rejects with
      // PackageNotFoundError — that we get any error other than 503
      // is the assertion.
      let caught: unknown;
      try {
         await env.addPackage("does-not-exist");
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeDefined();
      expect(caught).not.toBeInstanceOf(ServiceUnavailableError);
   });

   it("rejects getPackage cache-miss with 503 when back-pressured", async () => {
      const env = makeEnvironment(envDir);
      const governor = new StubGovernor();
      env.setMemoryGovernor(governor as unknown as PackageMemoryGovernor);
      governor.backpressured = true;

      // No package by this name has ever been loaded, so this is the
      // exact "lazy-load on cache miss" path Monty flagged. The gate
      // must throw before Package.create touches the disk.
      await expect(env.getPackage("ghost", false)).rejects.toBeInstanceOf(
         ServiceUnavailableError,
      );
   });

   it("rejects getPackage reload=true with 503 when back-pressured", async () => {
      const env = makeEnvironment(envDir);
      const governor = new StubGovernor();
      env.setMemoryGovernor(governor as unknown as PackageMemoryGovernor);
      governor.backpressured = true;

      await expect(env.getPackage("ghost", true)).rejects.toBeInstanceOf(
         ServiceUnavailableError,
      );
   });

   it("rejects addPackage with 503 when back-pressured (after the 404 check passes)", async () => {
      // Create a real (empty) package directory so the existence
      // check passes and the gate gets to run. Without this, the
      // PackageNotFoundError would mask the 503 we want to assert.
      const pkgName = "real-pkg";
      fs.mkdirSync(path.join(envDir, pkgName));

      const env = makeEnvironment(envDir);
      const governor = new StubGovernor();
      env.setMemoryGovernor(governor as unknown as PackageMemoryGovernor);
      governor.backpressured = true;

      await expect(env.addPackage(pkgName)).rejects.toBeInstanceOf(
         ServiceUnavailableError,
      );
   });

   it("returns 404 (not 503) when the package directory does not exist, even under pressure", async () => {
      // 404 must take precedence over 503: a permanent "you forgot to
      // upload the package" error should not be masked as a transient
      // "retry later" — otherwise operators chase phantom memory
      // problems while the real fix is a missing artifact.
      const env = makeEnvironment(envDir);
      const governor = new StubGovernor();
      env.setMemoryGovernor(governor as unknown as PackageMemoryGovernor);
      governor.backpressured = true;

      let caught: unknown;
      try {
         await env.addPackage("never-existed");
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeDefined();
      expect(caught).not.toBeInstanceOf(ServiceUnavailableError);
   });

   it("allowAdmission=true bypasses the gate (for future warmup/probe callers)", async () => {
      const env = makeEnvironment(envDir);
      const governor = new StubGovernor();
      env.setMemoryGovernor(governor as unknown as PackageMemoryGovernor);
      governor.backpressured = true;

      // With the bypass, the gate must not fire — the call should
      // proceed to the real loader and fail there with some other
      // error (PackageNotFoundError-equivalent from Package.create on
      // a non-existent directory). The assertion is "not 503".
      let caught: unknown;
      try {
         await env.getPackage("ghost", false, { allowAdmission: true });
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeDefined();
      expect(caught).not.toBeInstanceOf(ServiceUnavailableError);
   });

   it("clearing back-pressure on the governor immediately re-admits new loads", async () => {
      const env = makeEnvironment(envDir);
      const governor = new StubGovernor();
      env.setMemoryGovernor(governor as unknown as PackageMemoryGovernor);

      governor.backpressured = true;
      await expect(env.getPackage("ghost", false)).rejects.toBeInstanceOf(
         ServiceUnavailableError,
      );

      // Flip the flag (simulating the periodic poller crossing the
      // low-water mark) and verify the next call no longer 503s.
      governor.backpressured = false;
      let caught: unknown;
      try {
         await env.getPackage("ghost", false);
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeDefined();
      expect(caught).not.toBeInstanceOf(ServiceUnavailableError);
   });

   it("detaching the governor (set null) reverts to legacy admit-everything", async () => {
      const env = makeEnvironment(envDir);
      const governor = new StubGovernor();
      env.setMemoryGovernor(governor as unknown as PackageMemoryGovernor);
      governor.backpressured = true;

      env.setMemoryGovernor(null);

      let caught: unknown;
      try {
         await env.getPackage("ghost", false);
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeDefined();
      expect(caught).not.toBeInstanceOf(ServiceUnavailableError);
   });
});
