import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import { Server } from "http";
import { performGracefulShutdownAfterDrain } from "./health";
import { logger } from "./logger";
import { __setPackageLoadPoolForTests } from "./package_load/package_load_pool";

// Regression test for the graceful-shutdown ordering bug that caused
//   [winston] Attempt to write logs with no transports: {"message":"Waiting 50 seconds..."}
// to appear in production logs. logger.close() must run after every
// logger.* call, including the "Waiting ... seconds after server close
// before exit..." message.
//
// Tests call performGracefulShutdownAfterDrain directly rather than
// emitting SIGTERM, so module-level operationalState is not mutated
// and the spec stays isolated from sibling tests in the same process.
describe("performGracefulShutdownAfterDrain: shutdown ordering", () => {
   const originalExit = process.exit;
   let callOrder: string[];

   afterAll(async () => {
      // performGracefulShutdownAfterDrain drains the package-load worker pool
      // via getPackageLoadPool().shutdown() — which lazily CREATES the global
      // singleton and leaves it installed in its shut-down state. In prod the
      // process exits right after, so that's fine; in this shared test process
      // it poisons every later spec that touches the worker path
      // (Package.create → "PackageLoadPool is shutting down"), with failures
      // that come and go with bun's platform-dependent file order. Reset the
      // singleton so the next user lazily creates a fresh pool.
      await __setPackageLoadPoolForTests(null);
   });

   beforeEach(() => {
      callOrder = [];

      spyOn(logger, "info").mockImplementation(((msg: string) => {
         callOrder.push(`info:${msg}`);
         return logger;
      }) as never);
      spyOn(logger, "close").mockImplementation((() => {
         callOrder.push("close");
         return logger;
      }) as never);
      // Silence warn/error calls so spec output stays clean. They are
      // not load-bearing for these assertions.
      spyOn(logger, "warn").mockImplementation((() => logger) as never);
      spyOn(logger, "error").mockImplementation((() => logger) as never);

      process.exit = ((_code?: number) => {
         callOrder.push("exit");
      }) as never;
   });

   afterEach(() => {
      process.exit = originalExit;
   });

   const fakeServer = (): Server => ({ listening: false }) as unknown as Server;

   it("logs the 'Waiting ...' message before closing the logger", async () => {
      await performGracefulShutdownAfterDrain(fakeServer(), fakeServer(), 0.05);

      const waitingIdx = callOrder.findIndex((entry) =>
         entry.startsWith("info:Waiting"),
      );
      const closeIdx = callOrder.indexOf("close");
      const exitIdx = callOrder.indexOf("exit");

      expect(waitingIdx).toBeGreaterThanOrEqual(0);
      expect(closeIdx).toBeGreaterThanOrEqual(0);
      expect(exitIdx).toBeGreaterThanOrEqual(0);
      expect(waitingIdx).toBeLessThan(closeIdx);
      expect(closeIdx).toBeLessThan(exitIdx);
   });

   it("emits no logger.info calls after logger.close", async () => {
      await performGracefulShutdownAfterDrain(fakeServer(), fakeServer(), 0.05);

      const closeIdx = callOrder.indexOf("close");
      const lateInfoIdx = callOrder.findIndex(
         (entry, idx) => idx > closeIdx && entry.startsWith("info:"),
      );
      expect(closeIdx).toBeGreaterThanOrEqual(0);
      expect(lateInfoIdx).toBe(-1);
   });

   it("closes the logger exactly once", async () => {
      await performGracefulShutdownAfterDrain(fakeServer(), fakeServer(), 0.05);

      const closes = callOrder.filter((entry) => entry === "close").length;
      expect(closes).toBe(1);
   });

   it("skips the 'Waiting ...' message when gracefulCloseTimeoutSeconds is 0", async () => {
      await performGracefulShutdownAfterDrain(fakeServer(), fakeServer(), 0);

      const waitingCalls = callOrder.filter((entry) =>
         entry.startsWith("info:Waiting"),
      );
      expect(waitingCalls.length).toBe(0);
      expect(callOrder.indexOf("close")).toBeGreaterThanOrEqual(0);
      expect(callOrder.indexOf("exit")).toBeGreaterThanOrEqual(0);
   });
});
