// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a persist source's table lives and what it is keyed by: the build
 * order Malloy's `getBuildTargets` reports, the build target each source maps
 * onto, the content address (`sourceEntityId`) every plan, build and serve
 * path agrees on, the compiler's own key, and the connection digests both are
 * hashed with. The one place an address is computed. Split out of
 * build_plan.ts; behaviour unchanged.
 */

import type {
   BuildManifest,
   BuildTarget,
   ConnectionBuild,
   PersistSource,
   UnbuildableSource,
} from "@malloydata/malloy";
import { makeDigest } from "@malloydata/malloy";
import {
   getPersistAddressIncludesDestination,
   getPersistStorageMode,
} from "../../../config";
import { resolvePartitionColumns } from "./persist_partition";
import {
   declaredStorage,
   deriveAnnotationFields,
} from "./persist_source_fields";

/**
 * One connection's persist sources in the order they build, from Malloy's
 * `Runtime.getBuildTargets`: every source on every table, the tables a source
 * reads before it. Sources whose SQL does not compile on their own come last;
 * they have no table, and are refused per source like any other.
 *
 * Holds sourceIDs, resolved against a plan's `sources` when walked, so a
 * pre-aggregation companion that reaches an author's persisted source walks
 * the author's own `PersistSource` for it.
 */
export interface SourceBuild {
   connectionName: string;
   /** sourceIDs, dependencies first; the sources of one table are adjacent */
   order: string[];
   /**
    * sourceID -> the sourceIDs on the tables it reads. Sources on one table
    * read none of each other: an extension is its base's table, not a reader
    * of it. An unbuildable source lists none.
    */
   dependsOn: Record<string, string[]>;
}

/** Project Malloy's targets for one connection into a {@link SourceBuild}. */
export function sourceBuildOf(
   build: ConnectionBuild,
   unbuildable: UnbuildableSource[],
): SourceBuild {
   const order: string[] = [];
   const dependsOn: Record<string, string[]> = {};
   for (const target of build.targets) {
      const reads = target.dependsOn.flatMap((t) =>
         t.sources.map((s) => s.sourceID),
      );
      for (const source of target.sources) {
         order.push(source.sourceID);
         dependsOn[source.sourceID] = reads;
      }
   }
   // Malloy reports an unbuildable source without the tables it reads.
   for (const { source } of unbuildable) {
      if (source.connectionName !== build.connectionName) continue;
      order.push(source.sourceID);
      dependsOn[source.sourceID] = [];
   }
   return { connectionName: build.connectionName, order, dependsOn };
}

/**
 * {@link sourceBuildOf} for every connection `getBuildTargets` answered,
 * including one whose only persist sources were unbuildable.
 */
export function sourceBuildsOf(targets: {
   connections: ConnectionBuild[];
   unbuildable: UnbuildableSource[];
}): SourceBuild[] {
   const builds = targets.connections.map((c) =>
      sourceBuildOf(c, targets.unbuildable),
   );
   const planned = new Set(builds.map((b) => b.connectionName));
   for (const { source } of targets.unbuildable) {
      if (planned.has(source.connectionName)) continue;
      planned.add(source.connectionName);
      builds.push(
         sourceBuildOf(
            { connectionName: source.connectionName, targets: [] },
            targets.unbuildable,
         ),
      );
   }
   return builds;
}

/** Every persist source `getBuildTargets` named, planned or unbuildable. */
export function plannedSources(targets: {
   connections: ConnectionBuild[];
   unbuildable: UnbuildableSource[];
}): PersistSource[] {
   for (const connection of targets.connections) {
      for (const target of connection.targets) {
         for (const source of target.sources) {
            buildTargets.set(source, target);
         }
      }
   }
   for (const { source, problem } of targets.unbuildable) {
      unkeyable.set(source, problem.message);
   }
   return [
      ...targets.connections.flatMap((c) =>
         c.targets.flatMap((t) => t.sources),
      ),
      ...targets.unbuildable.map((u) => u.source),
   ];
}

/**
 * The build target each planned source maps onto, as {@link plannedSources}
 * paired them. A plan carries its targets explicitly
 * ({@link CompiledBuildPlan.targets}); this is the fallback lookup for a
 * caller holding only the source, which works for every source that came
 * from `plannedSources` (every production path does). Weak, so a model's
 * sources and targets are collected with it.
 */
const buildTargets = new WeakMap<PersistSource, BuildTarget>();

/** Why `getBuildTargets` could not key a source, as it reported it. */
const unkeyable = new WeakMap<PersistSource, string>();

/**
 * Test doubles' chosen addresses, keyed by their TARGET. A fake target names
 * the address its fixture asserts, where a real one is hashed; a partitioned
 * source still folds its layout over it (see {@link computeSourceEntityId}),
 * so a partition regression is not hidden. Set only by
 * `test_helpers/materialization_test_fixtures`, never on a real target.
 */
const testAddresses = new WeakMap<BuildTarget, string>();

/** See {@link testAddresses}. For test fixtures only. */
export function setTestAddressForTarget(
   target: BuildTarget,
   address: string,
): void {
   testAddresses.set(target, address);
}

/**
 * The build target `source` maps onto: `target` when the caller holds it (a
 * plan's {@link CompiledBuildPlan.targets}), else the one
 * {@link plannedSources} paired with this source. Throws for a source with
 * none, naming why: one `getBuildTargets` could not key (an unbound parameter,
 * a given with no value), which the deprecated `getSQL()` also threw for and
 * every caller treats as unaddressable; or one that never came through
 * `plannedSources`, which is a bug in the caller.
 */
export function buildTargetOf(
   source: PersistSource,
   target?: BuildTarget,
): BuildTarget {
   const found = target ?? buildTargets.get(source);
   if (found !== undefined) return found;
   const problem = unkeyable.get(source);
   if (problem !== undefined) {
      throw new Error(
         `Persisted source '${source.name}' cannot be keyed: ${problem}`,
      );
   }
   throw new Error(
      `Persisted source '${source.name}' was never planned: it did not come ` +
         "from plannedSources(), so no build target is known for it.",
   );
}

/**
 * The SQL `source`'s table is keyed by: the target's fully inlined `sql`, with
 * no manifest substitution, so it describes what the source computes whatever
 * else has been built.
 */
export function addressSQL(
   source: PersistSource,
   target?: BuildTarget,
): string {
   return buildTargetOf(source, target).sql;
}

/**
 * The SQL to execute for `source`'s table: the target's `buildSQL`, which reads
 * the dependencies `manifest` holds from their tables and is generated under
 * the same givens and `virtualMap` as {@link addressSQL}.
 */
export function buildSQLOf(
   source: PersistSource,
   manifest: BuildManifest,
   target?: BuildTarget,
): string {
   return buildTargetOf(source, target).buildSQL(manifest);
}

/** Record `source`'s target on `parts`, when it has one (an unkeyable source has none). */
export function recordTarget(
   parts: { targets: Record<string, BuildTarget> },
   sourceID: string,
   source: PersistSource,
): void {
   const target = buildTargets.get(source);
   if (target !== undefined) parts.targets[sourceID] = target;
}

/** The sources of `build` in build order, skipping any absent from `sources`. */
export function* sourcesInBuildOrder(
   build: SourceBuild,
   sources: Record<string, PersistSource>,
): Iterable<PersistSource> {
   for (const sourceID of build.order) {
      const source = sources[sourceID];
      if (source) yield source;
   }
}

/**
 * The sourceEntityId for a persist source: a stable content address of its
 * connection identity and canonical SQL. The only place the publisher derives
 * the id, so planning, build and serve-time manifest resolution all agree.
 *
 * The connection digest is fingerprint-aware (see applyConnectionFingerprint in
 * connection.ts), so ids survive credential rotation.
 */
export function computeSourceEntityId(
   source: PersistSource,
   connectionDigests: Record<string, string>,
   target?: BuildTarget,
): string {
   // The target's `sql` is load-bearing: this address keys the covered_through
   // ledger (incremental_build.ts), so it must describe WHAT the source
   // computes, never HOW FAR a run got. A boundary value in this SQL would
   // re-address the table every refresh and re-seed forever. A declaration alone
   // must not move it either (fact 1 in incremental_compiler_contract.spec.ts).
   //
   // Hashed the way the compiler hashes a BuildID (`makeDigest(digest, sql)`)
   // over the target's `sql`, with Publisher's digest: equal to the target's
   // own `buildId` (build_targets_address_equality pins it), and the digest
   // stays the caller's, so the same SQL on another connection is another
   // table.
   const resolved = buildTargetOf(source, target);
   const chosen = testAddresses.get(resolved);
   if (chosen !== undefined) {
      const layout = addressText(source, "");
      return layout === "" ? chosen : makeDigest(chosen, layout);
   }
   return makeDigest(
      connectionDigests[source.connectionName],
      addressText(source, resolved.sql),
   );
}

/**
 * The key the compiler itself files a source's table under: its target's
 * `buildId`, which the compiler computed and looks tables up by. Like
 * {@link computeSourceEntityId} but without the partition layout, so for a
 * partitioned source the two differ and a manifest handed to the compiler
 * carries the entry under both (see `manifestForCompiler`).
 *
 * `connectionDigests` is unused: the target's key was computed under the
 * runtime's own digests, for the same connections Publisher resolves
 * (`build_targets_address_equality` pins the two forms equal). Kept so the
 * signature, and its callers, are unchanged.
 */
export function compilerBuildId(
   source: PersistSource,
   _connectionDigests: Record<string, string>,
   target?: BuildTarget,
): string {
   const resolved = buildTargetOf(source, target);
   return testAddresses.get(resolved) ?? resolved.buildId;
}

/**
 * The storage destination folded into `source`'s address, or undefined: the
 * destination it declares with `storage=`, when
 * `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION` is on and
 * `PERSIST_STORAGE_MODE` is not `off` (off, `storage=` is inert and the source
 * builds colocated). Undefined for every colocated source, so turning the
 * switch on moves no colocated address (materialization v2's P12).
 */
export function addressDestination(source: PersistSource): string | undefined {
   if (!getPersistAddressIncludesDestination()) return undefined;
   if (getPersistStorageMode() === "off") return undefined;
   try {
      return declaredStorage(source);
   } catch {
      // An unreadable annotation addresses as colocated, as partition does.
      return undefined;
   }
}

/**
 * Fold what the target's `sql` cannot carry into the text a source's address
 * is computed from: its `partition=` layout, and (P12, behind a switch) the
 * storage destination it builds into. The target's `sql` carries no
 * annotation bytes, so without this a `partition=` change would be skipped as
 * unchanged and the table would keep the old layout, and a source moved to
 * `storage=` would keep its colocated address (#7087).
 *
 * Derived here rather than passed in so every caller agrees
 * (`build_targets_address_equality` pins it). Nothing to fold appends
 * nothing, keeping such addresses stable; a change moves the address once,
 * which is the rebuild the author asked for.
 */
export function addressText(source: PersistSource, sql: string): string {
   let columns: string[] = [];
   try {
      const resolved = resolvePartitionColumns(
         source,
         deriveAnnotationFields(source),
      );
      // A refusal contributes nothing: such a source cannot build at all.
      if (resolved.ok) columns = resolved.columns;
   } catch {
      // An unreadable annotation addresses as unpartitioned.
   }
   let text = sql;
   if (columns.length > 0) text += `\n-- partitioned by: ${columns.join(", ")}`;
   const destination = addressDestination(source);
   if (destination !== undefined) text += `\n-- stored in: ${destination}`;
   return text;
}

/**
 * Digest each connection the builds run on. A connection that does not
 * resolve is reported through `onSkipped` and left out, so its sources'
 * entity ids are computed without a digest.
 */
export async function resolveConnectionDigests(
   connections: Map<string, { getDigest(): string | Promise<string> }>,
   builds: { connectionName: string }[],
   onSkipped: (connectionName: string) => void,
): Promise<Record<string, string>> {
   const connectionDigests: Record<string, string> = {};
   for (const build of builds) {
      const conn = connections.get(build.connectionName);
      if (!conn) {
         onSkipped(build.connectionName);
         continue;
      }
      if (!connectionDigests[build.connectionName]) {
         // Fingerprint-aware (applyConnectionFingerprint), so credential
         // rotation does not re-address sources.
         connectionDigests[build.connectionName] = await conn.getDigest();
      }
   }
   return connectionDigests;
}
