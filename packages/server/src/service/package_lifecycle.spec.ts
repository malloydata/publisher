// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";

import {
   IllegalPackageTransitionError,
   PackageLifecycle,
   PackageLoadTracker,
   PackageStatus,
   setPackageLifecycleStrictForTesting,
} from "./package_lifecycle";

const STALE = { message: "boom", failedAt: "2026-01-01T00:00:00.000Z" };

describe("PackageLifecycle", () => {
   afterEach(() => setPackageLifecycleStrictForTesting(undefined));

   it("walks configured -> loading -> serving and reports SERVING for both ends", () => {
      const life = new PackageLifecycle("e");
      expect(life.phaseOf("p")).toBe("absent");
      life.apply("p", { type: "configure" });
      expect(life.phaseOf("p")).toBe("configured");
      expect(life.statusOf("p")?.status).toBe(PackageStatus.SERVING);
      const registeredAt = life.statusOf("p")!.loadTimestamp;
      life.apply("p", { type: "beginLoad" });
      expect(life.statusOf("p")?.status).toBe(PackageStatus.LOADING);
      life.apply("p", { type: "loaded" });
      expect(life.phaseOf("p")).toBe("serving");
      expect(life.statusOf("p")?.loadTimestamp).toBe(registeredAt);
   });

   it("keeps a stale package serving through its next reload until one succeeds", () => {
      const life = new PackageLifecycle("e");
      life.apply("p", { type: "beginLoad" });
      life.apply("p", { type: "loaded" });
      life.apply("p", { type: "beginLoad" });
      life.apply("p", { type: "reloadFailed", stale: STALE });
      expect(life.phaseOf("p")).toBe("serving");
      expect([...life.staleRecords()]).toEqual([["p", STALE]]);
      life.apply("p", { type: "beginLoad" });
      expect(life.staleRecords().size).toBe(1);
      life.apply("p", { type: "loaded" });
      expect(life.staleRecords().size).toBe(0);
   });

   it("keeps an unregistered name only while it carries a problem", () => {
      const life = new PackageLifecycle("e");
      life.apply("p", { type: "configure" });
      life.apply("p", { type: "recordLoadError", message: "no manifest" });
      life.apply("p", { type: "unregister" });
      expect(life.phaseOf("p")).toBe("failed");
      expect(life.isRegistered("p")).toBe(false);
      expect(life.statusOf("p")).toBeUndefined();
      expect(life.registeredNames()).toEqual([]);
      life.apply("p", { type: "clearProblems" });
      expect(life.phaseOf("p")).toBe("absent");
   });

   it("lists registered names in the order they last became registered", () => {
      const life = new PackageLifecycle("e");
      life.apply("b", { type: "recordMountError", message: "x" });
      life.apply("a", { type: "configure" });
      life.apply("b", { type: "configure" });
      life.apply("c", { type: "configure" });
      life.apply("a", { type: "unregister" });
      life.apply("a", { type: "configure" });
      expect(life.registeredNames()).toEqual(["b", "c", "a"]);
   });

   it("beginUnload only moves a serving or configured package", () => {
      const life = new PackageLifecycle("e");
      life.apply("p", { type: "configure" });
      life.apply("p", { type: "beginUnload" });
      expect(life.phaseOf("p")).toBe("unloading");
      life.apply("q", { type: "beginLoad" });
      life.apply("q", { type: "beginUnload" });
      expect(life.phaseOf("q")).toBe("loading");
      life.apply("r", { type: "beginUnload" });
      expect(life.phaseOf("r")).toBe("absent");
   });

   it("rejects an illegal move in strict mode, leaving the state unchanged", () => {
      setPackageLifecycleStrictForTesting(true);
      const life = new PackageLifecycle("e");
      life.apply("p", { type: "configure" });
      expect(() => life.apply("p", { type: "loaded" })).toThrow(
         IllegalPackageTransitionError,
      );
      expect(() =>
         life.apply("p", { type: "reloadFailed", stale: STALE }),
      ).toThrow("cannot reloadFailed while configured");
      expect(life.phaseOf("p")).toBe("configured");
      expect(life.staleRecords().size).toBe(0);

      life.apply("p", { type: "beginUnload" });
      expect(() => life.apply("p", { type: "beginLoad" })).toThrow(
         IllegalPackageTransitionError,
      );
      expect(() => life.apply("p", { type: "configure" })).toThrow(
         IllegalPackageTransitionError,
      );
      expect(life.phaseOf("p")).toBe("unloading");
   });

   it("applies an illegal move in production as the status setter did", () => {
      setPackageLifecycleStrictForTesting(false);
      const life = new PackageLifecycle("e");
      life.apply("p", { type: "configure" });
      life.apply("p", { type: "reloadFailed", stale: STALE });
      expect(life.statusOf("p")?.status).toBe(PackageStatus.SERVING);
      expect(life.staleRecords().size).toBe(1);
      life.apply("p", { type: "beginUnload" });
      life.apply("p", { type: "configure" });
      expect(life.statusOf("p")?.status).toBe(PackageStatus.SERVING);
   });

   it("caps runtime add failures but never evicts boot mount errors", () => {
      const life = new PackageLifecycle("e");
      life.apply("boot", { type: "recordMountError", message: "boot" });
      for (let i = 0; i < 101; i++) life.recordAddFailure(`a${i}`, `m${i}`);
      const failures = life.failures();
      expect(failures.has("a0")).toBe(false);
      expect(failures.get("boot")).toBe("boot");
      expect(failures.size).toBe(101);
      // A cleared add failure frees its slot.
      life.apply("a1", { type: "clearProblems" });
      life.recordAddFailure("z", "z");
      expect(life.failures().has("a2")).toBe(true);
   });
});

describe("PackageLoadTracker", () => {
   it("stays in flight until the outermost of nested loads settles", async () => {
      const tracker = new PackageLoadTracker();
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const outer = tracker.track("p", async () => {
         await tracker.track("p", async () => {});
         expect(tracker.has("p")).toBe(true);
         await gate;
      });
      const since = tracker.since("p");
      expect(since).toBeNumber();
      let settled = false;
      const waiting = tracker.awaitSettled("p").then(() => (settled = true));
      await Promise.resolve();
      expect(settled).toBe(false);
      release();
      await outer;
      await waiting;
      expect(tracker.has("p")).toBe(false);
      expect([...tracker.names()]).toEqual([]);
   });
});
