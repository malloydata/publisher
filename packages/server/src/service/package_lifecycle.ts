// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../logger";
import { externalEnv } from "../settings/external";

/**
 * The registration status a package reports through
 * `Environment.getPackageStatus`. A projection of {@link PackagePhase}: both
 * `configured` and `serving` report SERVING, as they always have.
 */
export enum PackageStatus {
   LOADING = "loading",
   SERVING = "serving",
   UNLOADING = "unloading",
}

export interface PackageInfo {
   name: string;
   loadTimestamp: number;
   status: PackageStatus;
}

/** Why a SERVING package's latest reload failed to compile. */
export interface StaleCompileError {
   message: string;
   failedAt: string;
}

/**
 * Where one package is in its life in an environment. Exactly one per name.
 *
 * - `configured`: registered from config or the database, not loaded here yet.
 *   It is listed, and the first listing or request loads it.
 * - `loading`: a load, reload or reinstall holds the package lock and is
 *   building a copy. A reload keeps the previous copy resident meanwhile.
 * - `serving`: a load committed (or a version loaded); the package answers.
 * - `unloading`: a delete or unload is retiring the resident copy.
 * - `failed`: not registered, kept only for a recorded problem (a load or mount
 *   failure /status reports). A name with neither is not tracked at all.
 *
 * Staleness is not a phase: a stale package is `serving` (it answers from its
 * last good model) and stays stale through the next reload's `loading`, so it
 * is a {@link PackageProblems} record carried by the entry, as are load and
 * mount failures, which a registered package can also carry (a configured
 * package whose location never mounted is listed AND reported failed). There
 * is no `mounting` phase: mounting happens on disk before the Environment
 * exists, and only its outcome (a mount error) is recorded here.
 */
export type PackagePhase =
   | { kind: "configured"; registeredAt: number }
   | { kind: "loading"; registeredAt: number }
   | { kind: "serving"; registeredAt: number }
   | { kind: "unloading"; registeredAt: number }
   | { kind: "failed" };

export type PackagePhaseKind = PackagePhase["kind"];
type RegisteredPhaseKind = Exclude<PackagePhaseKind, "failed">;

/** Problems a package carries, each stamped with when it was first recorded. */
interface PackageProblems {
   /** A load reached from a listing failed (what `listPackages` records). */
   loadError?: Recorded<string>;
   /** The configured location never mounted, or a runtime add failed. */
   mountError?: Recorded<string>;
   /** The latest reload failed to compile; the last good model serves. */
   stale?: Recorded<StaleCompileError>;
}

/**
 * `order` is when the record was first set, kept while it is overwritten: the
 * reports list records in that order, as the per-kind maps they replace did.
 */
interface Recorded<T> {
   value: T;
   order: number;
}

interface PackageEntry {
   phase: PackagePhase;
   /** When the package last became registered; orders listings. */
   registeredOrder: number;
   problems: PackageProblems;
}

/**
 * Every move a package's lifecycle makes. Problem records go through here too,
 * so one function owns every change to a package's state.
 */
export type PackageLifecycleEvent =
   /** Boot (config or database) registers a package it will load lazily. */
   | { type: "configure" }
   /** A load, reload or reinstall starts building a copy. */
   | { type: "beginLoad" }
   /** The built copy was committed: it serves, and every problem clears. */
   | { type: "loaded" }
   /** A reload failed: the previous copy keeps serving, now stale. */
   | { type: "reloadFailed"; stale: StaleCompileError }
   /** A version of the package loaded. Its problems are the caller's to clear. */
   | { type: "versionServing" }
   /** A failed reinstall put the previous copy back: it keeps serving. */
   | { type: "restored" }
   /** A delete or unload begins retiring the resident copy. */
   | { type: "beginUnload" }
   /** No copy is registered any more. Recorded problems are kept. */
   | { type: "unregister" }
   /** Forget every recorded problem; the registration is unchanged. */
   | { type: "clearProblems" }
   | { type: "recordLoadError"; message: string }
   | { type: "recordMountError"; message: string };

type EventType = PackageLifecycleEvent["type"];

const ALL_PHASES: readonly (PackagePhaseKind | "absent")[] = [
   "absent",
   "failed",
   "configured",
   "loading",
   "serving",
   "unloading",
];

/**
 * The phases each event may be applied in ("absent": the name is not
 * tracked). A move from any other phase is a bug in the caller.
 */
const LEGAL_FROM: Record<EventType, readonly (PackagePhaseKind | "absent")[]> =
   {
      // Boot registers into a fresh environment; registering a name that is
      // already registered and not mid-operation leaves it as it is.
      configure: ["absent", "failed", "configured", "serving"],
      // `loading` too: a load that threw outside its rollback leaves the phase
      // there, and the next load under the lock starts over.
      beginLoad: ["absent", "failed", "configured", "serving", "loading"],
      loaded: ["loading"],
      reloadFailed: ["loading"],
      versionServing: ["absent", "failed", "configured", "serving"],
      // A reinstall that fails before its load started restores from
      // `serving` itself.
      restored: ["loading", "serving"],
      // A no-op unless the package is registered as serving.
      beginUnload: ALL_PHASES,
      unregister: ALL_PHASES,
      clearProblems: ALL_PHASES,
      recordLoadError: ALL_PHASES,
      recordMountError: ALL_PHASES,
   };

/**
 * Whether an illegal move throws. On in tests, so a caller that makes one
 * fails the suite; off in production, where the move is logged and applied
 * as the status setter it replaces applied it, so a missed edge degrades
 * exactly as it did before rather than failing a request.
 */
let strict: boolean | undefined;

function isStrict(): boolean {
   return strict ?? externalEnv("NODE_ENV") === "test";
}

/** Visible for tests: force (or, with undefined, un-force) strict mode. */
export function setPackageLifecycleStrictForTesting(
   value: boolean | undefined,
): void {
   strict = value;
}

export class IllegalPackageTransitionError extends Error {
   constructor(
      readonly packageName: string,
      readonly from: PackagePhaseKind | "absent",
      readonly event: EventType,
   ) {
      super(
         `Internal error: package ${packageName} cannot ${event} while ${from}`,
      );
      this.name = "IllegalPackageTransitionError";
   }
}

/** Cap on runtime add failures kept per environment for /status. */
export const MAX_RECORDED_ADD_FAILURES = 100;

/**
 * Every package an environment knows of, by name, each in one
 * {@link PackagePhase} with the problems it carries. {@link apply} is the only
 * way to change one. Synchronization (the package lock, loads in flight) is
 * not state and lives in {@link PackageLoadTracker} and the Environment.
 */
export class PackageLifecycle {
   private readonly entries = new Map<string, PackageEntry>();
   /** Runtime add failures recorded as mount errors, oldest first. */
   private readonly recordedAddFailures: string[] = [];
   private nextOrder = 0;

   constructor(private readonly environmentName: string) {}

   /** The package's phase, or "absent" when the name is not tracked. */
   phaseOf(packageName: string): PackagePhaseKind | "absent" {
      return this.entries.get(packageName)?.phase.kind ?? "absent";
   }

   /** Whether a copy is registered: any phase but `failed`. */
   isRegistered(packageName: string): boolean {
      const kind = this.phaseOf(packageName);
      return kind !== "absent" && kind !== "failed";
   }

   /** The legacy registration record, undefined when not registered. */
   statusOf(packageName: string): PackageInfo | undefined {
      const phase = this.entries.get(packageName)?.phase;
      if (phase === undefined || phase.kind === "failed") return undefined;
      return {
         name: packageName,
         loadTimestamp: phase.registeredAt,
         status: STATUS_OF[phase.kind],
      };
   }

   /** Registered names, in the order they became registered. */
   registeredNames(): string[] {
      return [...this.entries]
         .filter(([, entry]) => entry.phase.kind !== "failed")
         .sort(([, a], [, b]) => a.registeredOrder - b.registeredOrder)
         .map(([name]) => name);
   }

   /**
    * Packages that did not load, and why. Load failures first, then names with
    * only a mount error; a mount error's message wins for a name with both,
    * since it is the specific cause the lazy load's manifest error would hide.
    */
   failures(): ReadonlyMap<string, string> {
      const loadErrors = this.recordsOf("loadError");
      const failures = new Map(loadErrors);
      for (const [name, message] of this.recordsOf("mountError")) {
         failures.set(name, message);
      }
      return failures;
   }

   /** Packages serving a model older than their files, and why. */
   staleRecords(): ReadonlyMap<string, StaleCompileError> {
      return new Map(this.recordsOf("stale"));
   }

   /** Registered packages minus failed ones, so the two never overlap. */
   servingCount(): number {
      const failed = this.failures();
      return this.registeredNames().filter((name) => !failed.has(name)).length;
   }

   /**
    * Record a runtime add that failed before the package could serve. Skipped
    * while the name is registered (a failed re-install rolled back, so nothing
    * failed). Capped at {@link MAX_RECORDED_ADD_FAILURES}, oldest dropped
    * first; boot-time mount errors are not capped.
    */
   recordAddFailure(packageName: string, message: string): void {
      if (this.isRegistered(packageName)) return;
      if (this.entries.get(packageName)?.problems.mountError === undefined) {
         this.recordedAddFailures.push(packageName);
         while (this.recordedAddFailures.length > MAX_RECORDED_ADD_FAILURES) {
            const evicted = this.recordedAddFailures.shift();
            if (evicted !== undefined) this.dropMountError(evicted);
         }
      }
      this.apply(packageName, { type: "recordMountError", message });
   }

   /** Drop every registration; recorded problems are kept. */
   unregisterAll(): void {
      for (const name of [...this.entries.keys()]) {
         this.apply(name, { type: "unregister" });
      }
   }

   /**
    * The one transition function. An event applied in a phase
    * {@link LEGAL_FROM} does not allow is logged, and thrown in tests.
    */
   apply(packageName: string, event: PackageLifecycleEvent): void {
      const entry = this.entries.get(packageName);
      const from: PackagePhaseKind | "absent" = entry?.phase.kind ?? "absent";
      if (!LEGAL_FROM[event.type].includes(from)) {
         const error = new IllegalPackageTransitionError(
            packageName,
            from,
            event.type,
         );
         logger.error("Illegal package lifecycle transition", {
            environmentName: this.environmentName,
            packageName,
            from,
            event: event.type,
         });
         if (isStrict()) throw error;
      }
      const next: PackageEntry = entry
         ? { ...entry, problems: { ...entry.problems } }
         : {
              phase: { kind: "failed" },
              registeredOrder: 0,
              problems: {},
           };
      switch (event.type) {
         case "configure":
            // Registering again leaves a serving copy serving.
            this.register(next, from === "serving" ? "serving" : "configured");
            break;
         case "beginLoad":
            this.register(next, "loading");
            break;
         case "loaded":
            this.register(next, "serving");
            next.problems = {};
            break;
         case "reloadFailed":
            this.register(next, "serving");
            next.problems.stale = this.record(next.problems.stale, event.stale);
            break;
         case "versionServing":
         case "restored":
            this.register(next, "serving");
            break;
         case "beginUnload":
            if (
               next.phase.kind === "serving" ||
               next.phase.kind === "configured"
            )
               this.register(next, "unloading");
            break;
         case "unregister":
            next.phase = { kind: "failed" };
            break;
         case "clearProblems":
            next.problems = {};
            break;
         case "recordLoadError":
            next.problems.loadError = this.record(
               next.problems.loadError,
               event.message,
            );
            break;
         case "recordMountError":
            next.problems.mountError = this.record(
               next.problems.mountError,
               event.message,
            );
            break;
      }
      if (next.problems.mountError === undefined) {
         const recorded = this.recordedAddFailures.indexOf(packageName);
         if (recorded !== -1) this.recordedAddFailures.splice(recorded, 1);
      }
      this.store(packageName, next);
   }

   private register(entry: PackageEntry, kind: RegisteredPhaseKind): void {
      if (entry.phase.kind === "failed") {
         entry.registeredOrder = this.nextOrder++;
         entry.phase = { kind, registeredAt: Date.now() };
      } else {
         entry.phase = { kind, registeredAt: entry.phase.registeredAt };
      }
   }

   private record<T>(previous: Recorded<T> | undefined, value: T): Recorded<T> {
      return { value, order: previous?.order ?? this.nextOrder++ };
   }

   private dropMountError(packageName: string): void {
      const entry = this.entries.get(packageName);
      if (entry?.problems.mountError === undefined) return;
      const problems = { ...entry.problems };
      delete problems.mountError;
      this.store(packageName, { ...entry, problems });
   }

   /** Keep an entry only while it is registered or carries a problem. */
   private store(packageName: string, entry: PackageEntry): void {
      const { loadError, mountError, stale } = entry.problems;
      if (
         entry.phase.kind === "failed" &&
         loadError === undefined &&
         mountError === undefined &&
         stale === undefined
      ) {
         this.entries.delete(packageName);
      } else {
         this.entries.set(packageName, entry);
      }
   }

   private recordsOf<K extends keyof PackageProblems>(
      kind: K,
   ): Array<[string, NonNullable<PackageProblems[K]>["value"]]> {
      const records: Array<[string, Recorded<unknown>]> = [];
      for (const [name, entry] of this.entries) {
         const recorded = entry.problems[kind];
         if (recorded !== undefined) records.push([name, recorded]);
      }
      return records
         .sort(([, a], [, b]) => a.order - b.order)
         .map(([name, recorded]) => [
            name,
            recorded.value as NonNullable<PackageProblems[K]>["value"],
         ]);
   }
}

const STATUS_OF: Record<RegisteredPhaseKind, PackageStatus> = {
   configured: PackageStatus.SERVING,
   loading: PackageStatus.LOADING,
   serving: PackageStatus.SERVING,
   unloading: PackageStatus.UNLOADING,
};

/**
 * Packages with a load, reinstall or recompile in progress, with when the
 * first began; what `Package.status.loading` reports. Synchronization, not
 * lifecycle state: a reload runs while the previous copy keeps serving, and a
 * version publish is tracked here without a `loading` phase. Reference-counted
 * so a nested operation stays marked until the outermost finishes.
 */
export class PackageLoadTracker {
   private readonly loads = new Map<
      string,
      {
         count: number;
         since: number;
         /** Resolves once every load sharing this entry has finished. */
         settled: Promise<void>;
         settle: () => void;
      }
   >();

   /** When the in-flight load of `packageName` began, if one is running. */
   since(packageName: string): number | undefined {
      return this.loads.get(packageName)?.since;
   }

   has(packageName: string): boolean {
      return this.loads.has(packageName);
   }

   names(): IterableIterator<string> {
      return this.loads.keys();
   }

   /** Resolve once no load of the package is in flight. */
   async awaitSettled(packageName: string): Promise<void> {
      let entry = this.loads.get(packageName);
      while (entry !== undefined) {
         await entry.settled;
         entry = this.loads.get(packageName);
      }
   }

   /** Run `fn` with the package marked as loading. */
   async track<T>(packageName: string, fn: () => Promise<T>): Promise<T> {
      const current = this.loads.get(packageName);
      let settle = current?.settle;
      const settled =
         current?.settled ??
         new Promise<void>((resolve) => {
            settle = resolve;
         });
      this.loads.set(packageName, {
         count: (current?.count ?? 0) + 1,
         since: current?.since ?? Date.now(),
         settled,
         settle: settle!,
      });
      try {
         return await fn();
      } finally {
         const entry = this.loads.get(packageName);
         if (entry !== undefined && entry.count > 1) {
            entry.count -= 1;
         } else {
            this.loads.delete(packageName);
            entry?.settle();
         }
      }
   }
}
