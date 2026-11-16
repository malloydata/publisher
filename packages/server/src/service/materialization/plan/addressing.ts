// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a persist source's table lives and what it is keyed by: the build
 * order Malloy's `getBuildTargets` reports, the build target each source maps
 * onto, the content address (`sourceEntityId`) every plan, build and serve
 * path agrees on, the compiler's own key, and the connection digests both are
 * hashed with. The one place an address is computed.
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
import { tableNamesFoldCase } from "../domain/storage_table_path";
import { tableNamesMeet } from "../domain/table_name";
import {
   declaredStorage,
   deriveAnnotationFields,
} from "./persist_source_fields";
import type { SourceBuild } from "../domain/compiled_build_plan";
import { notesDeclarePersist } from "./declaration";

/** Project Malloy's targets for one connection into a {@link SourceBuild}. */
function sourceBuildOf(
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
 * Whether `getBuildTargets` could not key `source` (it came from
 * {@link plannedSources} as unbuildable), so addressing it throws.
 */
export function isUnkeyable(source: PersistSource): boolean {
   return unkeyable.has(source);
}

/**
 * Test doubles' chosen addresses, keyed by their TARGET. A fake target names
 * the address its fixture asserts, where a real one is hashed; a source with a
 * destination folded into its address still folds it over the chosen one (see
 * {@link computeSourceEntityId}), so a regression there is not hidden. Set only by
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
 * no compiler manifest substitution, so it describes what the source computes whatever
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

/**
 * `source` presented under another identity: its `name` and `sourceID` are
 * `identity`'s, and everything else (its SQL, annotations, connection and
 * build target) is its own. A generated `#(materialize)` build source
 * (`<src>__mat` in the companion file) is presented as the author's source it
 * builds, so no outward surface (the wire plan, warnings, manifest entries, a
 * name-scoped run) shows the generated name. Its address is computed from its
 * build target's SQL, so it does not move.
 */
export function presentAs(
   source: PersistSource,
   identity: { name: string; sourceID: string },
): PersistSource {
   const presented = Object.create(source, {
      name: { get: () => identity.name },
      sourceID: { get: () => identity.sourceID },
   }) as PersistSource;
   const target = buildTargets.get(source);
   if (target !== undefined) buildTargets.set(presented, target);
   const problem = unkeyable.get(source);
   if (problem !== undefined) unkeyable.set(presented, problem);
   return presented;
}

/** `build` with each sourceID `rename` maps replaced, everywhere it appears. */
export function renameBuildSourceIDs(
   build: SourceBuild,
   rename: ReadonlyMap<string, string>,
): SourceBuild {
   const to = (id: string) => rename.get(id) ?? id;
   return {
      connectionName: build.connectionName,
      order: build.order.map(to),
      dependsOn: Object.fromEntries(
         Object.entries(build.dependsOn).map(([id, reads]) => [
            to(id),
            reads.map(to),
         ]),
      ),
   };
}

/** `build` without the sourceIDs `drop` names, wherever they appear. */
export function withoutSourceIDs(
   build: SourceBuild,
   drop: (sourceID: string) => boolean,
): SourceBuild {
   return {
      connectionName: build.connectionName,
      order: build.order.filter((id) => !drop(id)),
      dependsOn: Object.fromEntries(
         Object.entries(build.dependsOn)
            .filter(([id]) => !drop(id))
            .map(([id, reads]) => [id, reads.filter((r) => !drop(r))]),
      ),
   };
}

/** `source`'s build target, when it has one (an unkeyable source has none). */
export function plannedTargetOf(
   source: PersistSource,
): BuildTarget | undefined {
   return buildTargets.get(source);
}

/**
 * The sourceEntityId for a persist source: a stable content address of its
 * connection identity and canonical SQL. The only place the publisher derives
 * the id, so planning, build and serve-time run manifest resolution all
 * agree.
 *
 * The connection digest is fingerprint-aware (see applyConnectionFingerprint in
 * connection.ts), so ids survive credential rotation.
 */
export function computeSourceEntityId(
   source: PersistSource,
   connectionDigests: Record<string, string>,
   target?: BuildTarget,
   /** The plan's {@link TableFolds}; absent, nothing is folded. */
   tableFolds?: TableFolds,
): string {
   // The target's `sql` is load-bearing: this address keys the covered_through
   // ledger (incremental_build.ts), so it must describe WHAT the source
   // computes, never HOW FAR a run got. A boundary value in this SQL would
   // re-address the table every refresh and re-seed forever. A declaration alone
   // must not move it either (fact 1 in incremental_compiler_contract.spec.ts).
   //
   // Hashed the way the compiler hashes a BuildID (`makeDigest(digest, sql)`)
   // over `addressText`: the target's `sql`, plus, with
   // PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION on, the `storage=`
   // destination. Without it, it equals the target's own `buildId`
   // (build_targets_address_equality pins that); otherwise the two differ and
   // `manifestForCompiler` carries an entry under both. The digest stays the
   // caller's, so the same SQL on another connection is another table.
   const resolved = buildTargetOf(source, target);
   const fold = tableFolds?.[source.sourceID];
   const chosen = testAddresses.get(resolved);
   if (chosen !== undefined) {
      const layout = addressText(source, "", fold);
      return layout === "" ? chosen : makeDigest(chosen, layout);
   }
   return makeDigest(
      connectionDigests[source.connectionName],
      addressText(source, resolved.sql, fold),
   );
}

/**
 * The key the compiler itself files a source's table under: its target's
 * `buildId`, which the compiler computed and looks tables up by. Like
 * {@link computeSourceEntityId} but without what {@link addressText} folds in
 * (a destination, a distinct `name=` or projection), so for a source that has
 * one the two differ and a manifest handed to the compiler carries the entry
 * under both (see `manifestForCompiler`). Sources folded apart share one
 * compiler key, whose entry is then either of their tables: the same rows.
 *
 * The target's key was computed under the runtime's own digests, for the
 * same connections Publisher resolves (`build_targets_address_equality` pins
 * the two forms equal).
 */
export function compilerBuildId(
   source: PersistSource,
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
 * switch on moves no colocated address (P12).
 */
export function addressDestination(source: PersistSource): string | undefined {
   if (!getPersistAddressIncludesDestination()) return undefined;
   if (getPersistStorageMode() === "off") return undefined;
   try {
      return declaredStorage(source);
   } catch {
      // An unreadable annotation addresses as colocated.
      return undefined;
   }
}

/**
 * Fold what the target's `sql` cannot carry into the text a source's address
 * is computed from: (P12, behind a switch) the storage destination it builds
 * into, and the author's `name=` (or stored projection) where another source
 * of the same build names (or stores) another table
 * ({@link foldDistinctTableNames}). The target's `sql` carries no annotation
 * bytes, so without this a source moved to `storage=` would keep its
 * colocated address, and two `name=`s on one build would get one
 * table.
 *
 * The destination is derived here rather than passed in so every caller
 * agrees (`build_targets_address_equality` pins it); the table fold is the
 * plan's, which every caller addressing a plan's source passes. Nothing to fold appends
 * nothing, keeping such addresses stable; a change moves the address once,
 * which is the rebuild the author asked for.
 */
export function addressText(
   source: PersistSource,
   sql: string,
   /** The source's entry in its plan's {@link TableFolds}. */
   tableFold?: string,
): string {
   let text = sql;
   const destination = addressDestination(source);
   if (destination !== undefined) text += `\n-- stored in: ${destination}`;
   if (tableFold) text += `\n-- table: ${tableFold}`;
   return text;
}

/**
 * Where `source`'s `#@` persist declaration is written: an extension that
 * inherits its base's declaration names the base's line. Its sourceID when it
 * carries no located note.
 */
function declarationOf(source: PersistSource): string {
   try {
      const note = source.annotations.forRoute("@")[0] as
         | { at?: { url: string; range: { start: { line: number } } } }
         | undefined;
      if (note?.at) return `${note.at.url}:${note.at.range.start.line}`;
   } catch {
      // Unreadable: its own declaration.
   }
   return source.sourceID;
}

/**
 * Whether `source` writes its own `#@ persist` declaration, rather than
 * inheriting its base's (an extension). A source whose annotation cannot be
 * read counts as declaring.
 */
export function declaresOwnPersist(source: PersistSource): boolean {
   try {
      const own = source.annotation;
      if (!own) return true;
      return notesDeclarePersist([
         ...(own.blockNotes ?? []),
         ...(own.notes ?? []),
      ]);
   } catch {
      return true;
   }
}

/**
 * The columns a `storage=` build of `source` stores (as
 * projectToStoredColumns writes them), sorted; "" for a colocated source,
 * whose build writes its whole query, or one whose columns cannot be read
 * (its storage build refuses on its own).
 */
function storedProjection(
   source: PersistSource,
   storedColumns: (source: PersistSource) => string[],
): string {
   try {
      if (!declaredStorage(source)) return "";
      return [...storedColumns(source)].sort().join(",");
   } catch {
      return "";
   }
}

/**
 * What {@link foldDistinctTableNames} folds into each moved source's address,
 * by sourceID: its name's identity, and its stored columns where projections
 * differ. A source not listed keeps its plain address. Carried on the plan
 * (`CompiledBuildPlan.tableFolds`) and handed to every
 * {@link computeSourceEntityId} that addresses a plan's source.
 */
export type TableFolds = Readonly<Record<string, string>>;

/**
 * Each planned source's content address, its table fold applied, by
 * sourceID, or why it cannot be addressed (it is refused on its own path).
 * Computed once, over the whole plan, once its folds are decided
 * (`CompiledBuildPlan.addresses`).
 */
export type PlanAddresses = Readonly<
   Record<string, string | { unaddressable: string }>
>;

/** {@link PlanAddresses} for `plan`. */
export function planAddresses(plan: {
   tables: Readonly<
      Record<string, { source: PersistSource; target?: BuildTarget }>
   >;
   connectionDigests: Record<string, string>;
   tableFolds?: TableFolds;
}): PlanAddresses {
   const addresses: Record<string, string | { unaddressable: string }> = {};
   for (const [sourceID, { source, target }] of Object.entries(plan.tables)) {
      try {
         addresses[sourceID] = computeSourceEntityId(
            source,
            plan.connectionDigests,
            target,
            plan.tableFolds,
         );
      } catch (err) {
         addresses[sourceID] = {
            unaddressable: err instanceof Error ? err.message : String(err),
         };
      }
   }
   return addresses;
}

/**
 * The content address of the plan's source `sourceID`, read from its
 * {@link PlanAddresses}. A source that cannot be addressed throws why.
 */
export function planAddressOf(
   plan: { addresses: PlanAddresses },
   sourceID: string,
): string {
   const address = plan.addresses[sourceID];
   if (typeof address === "string") return address;
   throw new Error(
      address?.unaddressable ?? `'${sourceID}' has no planned address`,
   );
}

/**
 * Give each author `name=` its own table: two sources that compile to one
 * build (one address, say gated differently at read) but name different
 * tables are each addressed with their name folded in, so every plan, build,
 * manifest and bind path keys two tables. A source with no `name=` of its
 * own (a defaulted readable name, a rollup) names no table: it never moves,
 * keeping the plain address and its own readable table, and beside a source
 * that does name one it is still told apart, so the named source moves to an
 * address of its own (a defaulted name is Publisher's table, a `name=` the
 * author's; see hasAuthorTableName). Only an address whose sources all name
 * one table, or all name none, keeps every source on the plain address: the
 * same source reached from two models, or identical SQL with no `name=`.
 *
 * Likewise two `storage=` declarations of one build that store different
 * columns (one keeps a column `private:`, the other drops it with `except:`):
 * each is addressed with its declaring source's projection folded in, so a
 * column one drops is never at rest in the other's table. An extension that
 * inherits a declaration is on its table. Two such sources with one `name=`
 * then sit on two addresses at one table, which is the author's collision,
 * reported, and refused by an auto-run (see sharedTables).
 *
 * So only an address that held sources naming or storing different tables
 * moves, and the result does not depend on build order. A source moves when
 * a same-build source naming another table is added or removed, so its table
 * is rebuilt once then.
 *
 * Names are compared as persistTableTarget compares them: case folded where
 * the destination folds it, and two names that may be one table
 * ({@link tableNamesMeet}: `main.t` and `t`) are one, so they are never
 * split onto two addresses writing it; `s1.t` and `s2.t` are two.
 *
 * Decided over the package's whole plan, before any source is refused, and
 * carried on the plan, so a plan less its refused sources keeps the
 * addresses.
 */
export function foldDistinctTableNames(
   plan: {
      tables: Readonly<
         Record<
            string,
            {
               source: PersistSource;
               target?: BuildTarget;
               rollup?: unknown;
               materialize?: { nameDefaulted?: true };
            }
         >
      >;
      connectionDigests: Record<string, string>;
   },
   /**
    * The columns a `storage=` build of a source writes
    * (build_plan's storedColumnNames); a throw reads as none.
    */
   storedColumns: (source: PersistSource) => string[],
): TableFolds {
   const groups = new Map<
      string,
      {
         sourceID: string;
         name: string;
         defaulted: boolean;
         stores: string;
         declaration: string;
         declares: boolean;
      }[]
   >();
   for (const [sourceID, table] of Object.entries(plan.tables)) {
      const { source } = table;
      let address: string;
      try {
         address = computeSourceEntityId(
            source,
            plan.connectionDigests,
            table.target,
         );
      } catch {
         continue; // Unaddressable: refused on its own path.
      }
      const rollup = !!table.rollup;
      const written = deriveAnnotationFields(source).name?.trim();
      // A rollup's leaf is Publisher's own, so only its container (its
      // `name=`'s, or its base's) tells its tables apart: one grain placed
      // in two containers is two tables, never one written twice.
      const declared =
         rollup && written !== undefined
            ? written.slice(0, Math.max(0, written.lastIndexOf("."))) ||
              undefined
            : written;
      const materialize = table.materialize;
      const defaulted = materialize ? !!materialize.nameDefaulted : !declared;
      const group = groups.get(address) ?? [];
      group.push({
         sourceID,
         // As persistTableTarget compares names: case folded where the
         // destination folds it, so `T` and `t` are one table there.
         name: tableNamesFoldCase(
            source.dialectName,
            !!deriveAnnotationFields(source).storage?.trim(),
         )
            ? (declared ?? "").toLowerCase()
            : (declared ?? ""),
         defaulted,
         stores: rollup ? "" : storedProjection(source, storedColumns),
         declaration: declarationOf(source),
         declares: declaresOwnPersist(source),
      });
      groups.set(address, group);
   }
   const folds: Record<string, string> = {};
   for (const group of groups.values()) {
      const tables = tableClusters(
         group.filter((m) => !m.defaulted).map((m) => m.name),
      );
      const nameOf = (m: (typeof group)[number]) =>
         m.defaulted ? "" : tables.get(m.name)!;
      // A colocated build stores its whole query, so only `storage=`
      // sources are told apart by what they store, and only one declaration
      // from another: an extension that inherits its base's `#@ persist` is
      // its base's table, whatever it renames or adds.
      // A declaration stores what its declaring source projects; an
      // extension reads that table. With no declaring source among them (a
      // hand-made plan), the first by sourceID.
      const declared = new Map<string, { stores: string; declares: boolean }>();
      for (const m of [...group].sort((a, b) =>
         a.sourceID.localeCompare(b.sourceID),
      )) {
         if (m.stores === "") continue;
         const held = declared.get(m.declaration);
         if (held === undefined || (m.declares && !held.declares)) {
            declared.set(m.declaration, {
               stores: m.stores,
               declares: m.declares,
            });
         }
      }
      const storesOf = (m: (typeof group)[number]) =>
         m.stores === "" ? "" : declared.get(m.declaration)!.stores;
      const projections = new Set(group.map(storesOf).filter((p) => p !== ""));
      const keyOf = (m: (typeof group)[number]) =>
         storesOf(m) !== "" && projections.size > 1
            ? `${nameOf(m)}\u0000${storesOf(m)}`
            : nameOf(m);
      if (new Set(group.map(keyOf)).size < 2) continue;
      for (const member of group) {
         const key = keyOf(member);
         if (key) folds[member.sourceID] = key;
      }
   }
   return folds;
}

/**
 * Each written name's table, as the smallest name of the names that are one
 * table with it: names merge only where every pair of them meets
 * ({@link tableNamesMeet}). `main.t` and `t` are one table, and `s1.t` and
 * `s2.t` two. A name that meets two names which do not meet each other (a
 * bare `t` beside `s1.t` and `s2.t`) merges with none: it is left on an
 * address of its own, for the collision check to refuse, so it never stands
 * for either of them and leaves the other unwritten.
 */
function tableClusters(names: string[]): Map<string, string> {
   const distinct = [...new Set(names)].sort();
   // A name whose every meeting name meets every other: only it may merge.
   const settled = new Set(
      distinct.filter((n) => {
         const near = distinct.filter((m) => m !== n && tableNamesMeet(n, m));
         return near.every((a, i) =>
            near.slice(i + 1).every((b) => tableNamesMeet(a, b)),
         );
      }),
   );
   const root = new Map(distinct.map((n) => [n, n]));
   const find = (n: string): string => {
      const r = root.get(n)!;
      return r === n ? n : find(r);
   };
   for (let i = 0; i < distinct.length; i++) {
      for (let j = i + 1; j < distinct.length; j++) {
         const [x, y] = [distinct[i], distinct[j]];
         // Both settled: every component is then a set of names that all
         // meet one another.
         if (!settled.has(x) || !settled.has(y) || !tableNamesMeet(x, y)) {
            continue;
         }
         const [a, b] = [find(x), find(y)];
         if (a !== b) root.set(a < b ? b : a, a < b ? a : b);
      }
   }
   return new Map(distinct.map((n) => [n, find(n)]));
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
