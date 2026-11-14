// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The join_one uniqueness check (materialization v2, P8): before a
 * `#(materialize)` source that inlines a join_one is built, its joined source
 * is grouped on the join's key, and any key held twice fails the build. A
 * duplicate would store the parent row once per match, so every count over
 * the table would come up high while live, deduplicating through the join,
 * stayed right.
 *
 * The check's SQL is generated and compiled with the build companion at plan
 * time (build_file_generator.ts); here it only runs.
 *
 * It is skipped only when its answer provably cannot have changed since it
 * last passed (see {@link UniquenessLedger}): the joined source is itself a
 * persisted table the parent's build reads, that table was not written since
 * a check that ran over it passed, and the check's own SQL is the same. A
 * joined source that is a warehouse table (or anything the build reads live)
 * is checked on every build, because its rows change under a stable address.
 */

import { createHash } from "node:crypto";
import { tableIdentity } from "../../versions/materialization_scope";
import type {
   BuildManifest,
   Connection as MalloyConnection,
} from "@malloydata/malloy";
import type { KeyUniqueness, ManifestEntry } from "../types";
import type {
   InlineUniquenessCheck,
   MaterializePlan,
} from "../plan/build_file_generator";

/**
 * A `#(materialize)` source inlines a join_one (`inline=["j"]`) whose joined
 * source holds one key more than once (P8), so its build would store a parent
 * row once per match and every count over the table would come up high. The
 * build fails before writing anything; the message names the join.
 */
export class InlinedJoinNotUniqueError extends Error {
   get kind(): "inlined_join_not_unique" {
      return "inlined_join_not_unique";
   }

   constructor(
      readonly sourceName: string,
      readonly join: string,
      readonly keyColumn: string,
      /**
       * True when the plan carries no compiled check for the join (a plan from
       * a Publisher that did not compile one): refused rather than built
       * unchecked, and said so rather than claimed a duplicate.
       */
      readonly unchecked = false,
      /**
       * Set for a pair table's key check: the level of the path whose key
       * repeats or is null, 0 for the source's own (`join` is then empty).
       */
      readonly pairLevel?: number,
   ) {
      super(
         pairLevel !== undefined
            ? pairKeyMessage(sourceName, join, keyColumn, unchecked)
            : unchecked
              ? `'${sourceName}' is not built: it inlines the join_one '${join}', ` +
                `and its build plan carries no uniqueness check for it, so a ` +
                `duplicate '${keyColumn}' could not be ruled out. Reload the ` +
                `package to plan the check.`
              : `'${sourceName}' is not built: it inlines the join_one '${join}', ` +
                `but the joined source holds a '${keyColumn}' value more than once, ` +
                `so a row would be stored once per match. Make '${keyColumn}' ` +
                `unique in the joined source, or leave '${join}' out of inline=.`,
      );
      this.name = "InlinedJoinNotUniqueError";
   }
}

/** {@link InlinedJoinNotUniqueError}'s message for a pair table's key check. */
function pairKeyMessage(
   sourceName: string,
   join: string,
   keyColumn: string,
   unchecked: boolean,
): string {
   const whose = join
      ? `the source '${join}' joins through its pair table`
      : `'${sourceName}' itself`;
   return unchecked
      ? `'${sourceName}' is not built: its pair table keys one row flag on ` +
           `'${keyColumn}' of ${whose}, and its build plan carries no check ` +
           `of that key, so a repeated or null '${keyColumn}' could not be ` +
           `ruled out. Reload the package to plan the check.`
      : `'${sourceName}' is not built: its pair table keys one row flag on ` +
           `'${keyColumn}' of ${whose}, which holds a value more than once or ` +
           `a null, so that flag would mark one row where live counts each. ` +
           `Make '${keyColumn}' unique and never null, or leave that path out ` +
           `of inline=.`;
}

/**
 * The run metadata key a run records its uniqueness checks under: the passes
 * a later run may rely on, and what became of each check this run.
 */
export const INLINE_UNIQUENESS_KEY = "inlineUniquenessChecks";

/**
 * One passing check a later run may rely on, keyed by {@link checkDigest}.
 * Recorded only for a check that read a persisted joined table (a
 * {@link StoredJoin}); a check over anything read live is never relied on.
 */
interface UniquenessPass {
   /** The joined source's content address (its sourceEntityId). */
   joinedAddress: string;
   /** The table the check read: its connection and physical name. */
   joinedTable: string;
   /** The run whose check actually ran and passed (a skip carries it). */
   checkedInRun?: string;
}

/** What became of one check in a run, for the run's metadata. */
interface UniquenessCheckOutcome {
   sourceName: string;
   join: string;
   /** `ran`: the check ran (and passed). `skipped`: a recorded pass held. */
   outcome: "ran" | "skipped";
}

/**
 * What a run's rollup key check found (known limitation 29): whether the
 * rows a `#(preaggregate)` rollup grouped held its one grain dimension (a
 * join_one's key) once per value. Keyed in the run metadata by the rollup's
 * content address; it describes the table the run that checked wrote, so it
 * is carried to a later run only while that table is not written again and
 * the check is the same ({@link UniquenessLedger.carryRollupKey}).
 */
export interface RollupKeyRecord {
   /** The rollup's table: its connection and physical name. */
   table: string;
   keyColumn: string;
   /** {@link checkDigest} of the table and the check's SQL. */
   digest: string;
   unique: boolean;
   /** The run whose check ran (a carried record keeps it). */
   checkedInRun?: string;
   /**
    * Set on a query source's primary key check (see `ParentKeyCheck`): the
    * source whose key it read, beside the rollup at the record's address.
    * Absent on the rollup's own key check. `unique` then says the key was
    * held once per row and never NULL.
    */
   sourceID?: string;
}

/**
 * The key a rollup key record is held under: the rollup's content address,
 * and for a query source's primary key check the source too. Every record
 * under an address describes the rollup's table, so writing it loses them all.
 */
export function rollupKeyRecordKey(address: string, sourceID?: string): string {
   return sourceID === undefined ? address : `${address}#${sourceID}`;
}

/**
 * The joined source of a check as the parent's build reads it: a persisted
 * table, substituted for the source in the parent's build SQL. Resolved by
 * the build loop (instructed_build.ts), which knows the run's manifest; any
 * doubt there resolves to undefined, and the check runs as compiled.
 */
export interface StoredJoin {
   /** The check compiled over the stored table: what this run runs. */
   sql: string;
   /** The joined source's content address. */
   address: string;
   /** The table's identity: its connection and physical name. */
   table: string;
}

/**
 * The identity a pass is keyed by: the table the check reads (its connection
 * and name) and the exact SQL it runs, which names the join's key.
 */
export function checkDigest(table: string, sql: string): string {
   return createHash("sha256")
      .update(table)
      .update("\u0000")
      .update(sql)
      .digest("hex");
}

/**
 * The passes a run may rely on, read from the metadata of the run before it,
 * and this run's own, for its metadata.
 *
 * A pass is relied on only when the previous run is the one every other run
 * of the package finished before (see `priorUniquenessPasses` in
 * materialization_service.ts): then nothing Publisher runs wrote the joined
 * table between that run's check and this one, unless this run did. Every
 * table this run starts to write is marked ({@link markWritten}): a check over
 * it then runs, and a pass recorded against it before loses its place, so the
 * next run checks the new generation.
 */
export class UniquenessLedger {
   private readonly passes = new Map<string, UniquenessPass>();
   private readonly outcomes: UniquenessCheckOutcome[] = [];
   private readonly writtenAddresses = new Set<string>();
   private readonly writtenTables = new Set<string>();
   private readonly rollupKeys = new Map<string, RollupKeyRecord>();

   constructor(
      /** The previous run's passes; undefined when none may be relied on. */
      private readonly prior:
         | Readonly<Record<string, UniquenessPass>>
         | undefined,
      private readonly runId?: string,
      /**
       * The previous run's rollup key records, under the same proof as its
       * passes; undefined when none may be relied on.
       */
      private readonly priorRollupKeys?: Readonly<
         Record<string, RollupKeyRecord>
      >,
   ) {}

   /**
    * This run checked the rollup at `address` after writing its table: the
    * record stands for that table until it is written again.
    */
   recordRollupKey(address: string, record: RollupKeyRecord): void {
      this.rollupKeys.set(rollupKeyRecordKey(address, record.sourceID), {
         ...record,
         ...(this.runId ? { checkedInRun: this.runId } : {}),
      });
   }

   /**
    * Keep the previous run's record for a rollup this run did not build (its
    * entry carried): only when this run wrote neither its address nor its
    * table, and the record names the same table and the same check. With
    * `sourceID`, the record of that query source's primary key check.
    */
   carryRollupKey(
      address: string,
      table: string,
      digest: string,
      sourceID?: string,
   ): void {
      const key = rollupKeyRecordKey(address, sourceID);
      if (
         this.rollupKeys.has(key) ||
         this.writtenAddresses.has(address) ||
         this.writtenTables.has(table)
      ) {
         return;
      }
      const prior = this.priorRollupKeys?.[key];
      if (
         prior &&
         prior.table === table &&
         prior.digest === digest &&
         prior.sourceID === sourceID
      ) {
         this.rollupKeys.set(key, prior);
      }
   }

   /** The rollup key records this run stands behind, by content address. */
   rollupKeyRecords(): Record<string, RollupKeyRecord> {
      return Object.fromEntries(this.rollupKeys);
   }

   /** The recorded pass for `digest` that still describes `stored`, if any. */
   heldPass(digest: string, stored: StoredJoin): UniquenessPass | undefined {
      if (
         this.writtenAddresses.has(stored.address) ||
         this.writtenTables.has(stored.table)
      ) {
         return undefined;
      }
      const pass = this.prior?.[digest];
      if (
         !pass ||
         pass.joinedAddress !== stored.address ||
         pass.joinedTable !== stored.table
      ) {
         return undefined;
      }
      return pass;
   }

   record(digest: string, pass: UniquenessPass): void {
      this.passes.set(digest, pass);
   }

   outcome(outcome: UniquenessCheckOutcome): void {
      this.outcomes.push(outcome);
   }

   /**
    * This run is about to write `table` (a build of the source at `address`),
    * whether or not that build succeeds: no pass recorded so far holds for
    * either, and none is relied on for them for the rest of the run.
    */
   markWritten(address: string, table: string): void {
      this.writtenAddresses.add(address);
      this.writtenTables.add(table);
      for (const [digest, pass] of this.passes) {
         if (pass.joinedAddress === address || pass.joinedTable === table) {
            this.passes.delete(digest);
         }
      }
      for (const [key, record] of this.rollupKeys) {
         if (
            key === address ||
            key.startsWith(`${address}#`) ||
            record.table === table
         ) {
            this.rollupKeys.delete(key);
         }
      }
   }

   get id(): string | undefined {
      return this.runId;
   }

   /** The run metadata value, or undefined when no check was reached. */
   toMetadata():
      | {
           passes: Record<string, UniquenessPass>;
           checks: UniquenessCheckOutcome[];
           rollupKeys?: Record<string, RollupKeyRecord>;
        }
      | undefined {
      if (
         this.outcomes.length === 0 &&
         this.passes.size === 0 &&
         this.rollupKeys.size === 0
      ) {
         return undefined;
      }
      return {
         passes: Object.fromEntries(this.passes),
         checks: [...this.outcomes],
         ...(this.rollupKeys.size > 0
            ? { rollupKeys: Object.fromEntries(this.rollupKeys) }
            : {}),
      };
   }
}

/**
 * The passes a run's metadata records, or undefined when it records none or
 * the value is not the shape this Publisher writes.
 */
export function uniquenessPassesIn(
   metadata: Record<string, unknown> | null | undefined,
): Record<string, UniquenessPass> | undefined {
   const value = metadata?.[INLINE_UNIQUENESS_KEY] as
      | { passes?: unknown }
      | undefined;
   const passes = value?.passes;
   if (!passes || typeof passes !== "object" || Array.isArray(passes)) {
      return undefined;
   }
   const out: Record<string, UniquenessPass> = {};
   for (const [digest, pass] of Object.entries(passes)) {
      const p = pass as Partial<UniquenessPass> | null;
      if (
         p &&
         typeof p.joinedAddress === "string" &&
         typeof p.joinedTable === "string"
      ) {
         out[digest] = {
            joinedAddress: p.joinedAddress,
            joinedTable: p.joinedTable,
            ...(typeof p.checkedInRun === "string"
               ? { checkedInRun: p.checkedInRun }
               : {}),
         };
      }
   }
   return out;
}

/**
 * The rollup key records a run's metadata holds (known limitation 29), or
 * undefined when it holds none or the value is not the shape this Publisher
 * writes. A record that does not read whole is left out.
 */
export function rollupKeyRecordsIn(
   metadata: Record<string, unknown> | null | undefined,
): Record<string, RollupKeyRecord> | undefined {
   const value = metadata?.[INLINE_UNIQUENESS_KEY] as
      | { rollupKeys?: unknown }
      | undefined;
   const records = value?.rollupKeys;
   if (!records || typeof records !== "object" || Array.isArray(records)) {
      return undefined;
   }
   const out: Record<string, RollupKeyRecord> = {};
   for (const [address, record] of Object.entries(records)) {
      const r = record as Partial<RollupKeyRecord> | null;
      if (
         r &&
         typeof r.table === "string" &&
         typeof r.keyColumn === "string" &&
         typeof r.digest === "string" &&
         typeof r.unique === "boolean"
      ) {
         out[address] = {
            table: r.table,
            keyColumn: r.keyColumn,
            digest: r.digest,
            unique: r.unique,
            ...(typeof r.checkedInRun === "string"
               ? { checkedInRun: r.checkedInRun }
               : {}),
            ...(typeof r.sourceID === "string" ? { sourceID: r.sourceID } : {}),
         };
      }
   }
   return out;
}

/**
 * `metadata` without the passes it records, keeping what became of each
 * check, or undefined when it records none. A pass is relied on only while
 * its run is the newest of the package and no other run overlapped it, which
 * the run records alone prove; deleting a record takes that proof away (a
 * newer run that rewrote the table, or an older one that overlapped), so the
 * passes left behind are forgotten first (`deleteMaterialization`).
 */
export function withoutUniquenessPasses(
   metadata: Record<string, unknown> | null | undefined,
): Record<string, unknown> | undefined {
   const value = metadata?.[INLINE_UNIQUENESS_KEY];
   if (!metadata || !value || typeof value !== "object") return undefined;
   const { passes, rollupKeys, ...rest } = value as {
      passes?: unknown;
      rollupKeys?: unknown;
   };
   const holds = (v: unknown) =>
      !!v && typeof v === "object" && Object.keys(v as object).length > 0;
   // A rollup key record is relied on under the same proof as a pass, so it
   // is forgotten with them.
   if (!holds(passes) && !holds(rollupKeys)) return undefined;
   return { ...metadata, [INLINE_UNIQUENESS_KEY]: { ...rest, passes: {} } };
}

/**
 * Compiles a check against a build manifest, so it reads the persisted
 * tables the manifest names. Registered per check at plan time, where the
 * companion's materializer is (materialize_compile.ts); a check without one
 * (a plan from elsewhere) is never compiled over a stored table.
 */
type UniquenessCheckCompiler = (
   buildManifest: BuildManifest,
   connectionDigests: Record<string, string>,
) => Promise<string>;

const checkCompilers = new WeakMap<
   InlineUniquenessCheck,
   UniquenessCheckCompiler
>();

export function registerUniquenessCheckCompiler(
   check: InlineUniquenessCheck,
   compile: UniquenessCheckCompiler,
): void {
   checkCompilers.set(check, compile);
}

export function uniquenessCheckCompiler(
   check: InlineUniquenessCheck,
): UniquenessCheckCompiler | undefined {
   return checkCompilers.get(check);
}

/**
 * Run every uniqueness check `plan` carries on `connection`, throwing
 * {@link InlinedJoinNotUniqueError} for the first join whose key repeats. A
 * plan with no inlined join checks nothing.
 *
 * With `ledger` and `storedJoin` (a build run), a check whose joined source
 * the parent's build reads as a persisted table runs over that table, and is
 * skipped when the ledger holds a pass that still describes it. Every other
 * check runs as compiled.
 */
export async function assertInlinedJoinsUnique(
   plan: MaterializePlan | undefined,
   connection: Pick<MalloyConnection, "runSQL">,
   ledger?: UniquenessLedger,
   storedJoin?: (
      check: InlineUniquenessCheck,
   ) => Promise<StoredJoin | undefined>,
): Promise<void> {
   for (const check of plan?.uniquenessChecks ?? []) {
      // A plan from a Publisher that did not compile the check: refuse rather
      // than build unchecked.
      if (!check.sql) {
         throw new InlinedJoinNotUniqueError(
            plan?.sourceName ?? "",
            check.join,
            check.keyColumn,
            true,
            check.pairLevel,
         );
      }
      const stored = ledger && storedJoin ? await storedJoin(check) : undefined;
      const sql = stored?.sql ?? check.sql;
      const digest = stored ? checkDigest(stored.table, stored.sql) : undefined;
      const held =
         ledger && stored && digest
            ? ledger.heldPass(digest, stored)
            : undefined;
      if (ledger && digest && held) {
         ledger.record(digest, held);
         ledger.outcome({
            sourceName: plan?.sourceName ?? "",
            join: check.join,
            outcome: "skipped",
         });
         continue;
      }
      const result = await connection.runSQL(sql);
      if (result.rows.length > 0) {
         throw new InlinedJoinNotUniqueError(
            plan?.sourceName ?? "",
            check.join,
            check.keyColumn,
            false,
            check.pairLevel,
         );
      }
      if (ledger) {
         if (stored && digest) {
            ledger.record(digest, {
               joinedAddress: stored.address,
               joinedTable: stored.table,
               ...(ledger.id ? { checkedInRun: ledger.id } : {}),
            });
         }
         ledger.outcome({
            sourceName: plan?.sourceName ?? "",
            join: check.join,
            outcome: "ran",
         });
      }
   }
}

/**
 * The SQL of a stored table's primary key check, over `quotedTable` (a
 * DuckDB path, as the destination's session reads it): one row when a value
 * of `keyColumn` repeats or is NULL.
 */
export function storedKeyCheckSql(
   quotedTable: string,
   keyColumn: string,
): string {
   const k = `"${keyColumn.replace(/"/g, '""')}"`;
   return `SELECT ${k} FROM ${quotedTable} GROUP BY ${k} HAVING COUNT(*) > 1 OR ${k} IS NULL LIMIT 1`;
}

/**
 * Run a rollup's key check (known limitation 29) on `connection` and say what
 * it found: unique when no non-NULL key value repeats. Undefined when the
 * check could not run, which the join-to-rollup leg reads as unknown and
 * declines as it did before the check existed; the build itself never fails
 * for it.
 */
export async function runRollupKeyCheck(
   check: { keyColumn: string; sql: string },
   connection: Pick<MalloyConnection, "runSQL">,
   table: string,
): Promise<Omit<RollupKeyRecord, "checkedInRun"> | undefined> {
   try {
      const result = await connection.runSQL(check.sql);
      return {
         table,
         keyColumn: check.keyColumn,
         digest: checkDigest(table, check.sql),
         unique: result.rows.length === 0,
      };
   } catch {
      return undefined;
   }
}

/** A run that can write nothing more. */
const TERMINAL_RUN_STATUSES: ReadonlySet<string> = new Set([
   "MANIFEST_FILE_READY",
   "FAILED",
   "CANCELLED",
]);

/**
 * The newest of a package's runs, when its records may be relied on: it is
 * the most recently created, it committed, it was an auto-run, and every
 * other run was terminal and had finished before it began. Then nothing
 * Publisher runs wrote a table between that run's checks and now, so a table
 * it holds unchanged is the table its checks read or wrote. Undefined on any
 * doubt (a run still active, one that overlapped, an orchestrated one, an
 * unreadable time).
 */
export function uncontestedAutoRun<
   R extends {
      status: string;
      metadata?: Record<string, unknown> | null;
      createdAt?: Date | string | null;
      startedAt?: Date | string | null;
      completedAt?: Date | string | null;
      updatedAt?: Date | string | null;
   },
>(runs: readonly R[]): R | undefined {
   if (runs.length === 0) return undefined;
   const at = (d: Date | string | null | undefined) =>
      d ? new Date(d).getTime() : NaN;
   const previous = runs.reduce((a, b) =>
      at(b.createdAt) > at(a.createdAt) ? b : a,
   );
   if (
      previous.status !== "MANIFEST_FILE_READY" ||
      previous.metadata?.mode !== "auto"
   ) {
      return undefined;
   }
   const began = at(previous.startedAt ?? previous.createdAt);
   for (const other of runs) {
      if (other === previous) continue;
      if (!TERMINAL_RUN_STATUSES.has(other.status)) return undefined;
      const ended = at(other.completedAt ?? other.updatedAt);
      if (!(ended <= began)) return undefined;
   }
   return previous;
}

/**
 * The identity a uniqueness record names a table by: where it lives and its
 * name as {@link tableIdentity} reads it. Two names a warehouse may read as
 * one table get one identity, so a write to either loses the record: the
 * safe direction.
 */
export function tableIdentityKey(
   kind: "connection" | "destination",
   where: string,
   physicalTableName: string,
): string {
   return JSON.stringify([kind, where, tableIdentity(physicalTableName)]);
}

/**
 * What a rollup key record says of the colocated table `entry` names, or
 * undefined when it names another table (a record never describes a table
 * it was not taken over).
 */
export function keyUniquenessOf(
   record: RollupKeyRecord | undefined,
   entry: Pick<ManifestEntry, "physicalTableName" | "connectionName">,
): KeyUniqueness | undefined {
   if (
      !record ||
      !entry.physicalTableName ||
      !entry.connectionName ||
      record.sourceID !== undefined ||
      record.table !==
         tableIdentityKey(
            "connection",
            entry.connectionName,
            entry.physicalTableName,
         )
   ) {
      return undefined;
   }
   return { keyColumn: record.keyColumn, unique: record.unique };
}

/**
 * Everything `records` say of the colocated table of the rollup at `address`:
 * its own key check ({@link keyUniquenessOf}) and each query source's primary
 * key check taken at its build, by sourceID. A record over another table is
 * left out; undefined when none describes this one.
 */
export function keyUniquenessAt(
   records: Readonly<Record<string, RollupKeyRecord>> | undefined,
   address: string,
   entry: Pick<
      ManifestEntry,
      "physicalTableName" | "connectionName" | "storageDestinationName"
   >,
): KeyUniqueness | undefined {
   if (!records || !entry.physicalTableName) return undefined;
   // A stored table carries only its own source's primary key check; a
   // colocated one, its rollup's key check and each query source's.
   const table = entry.storageDestinationName
      ? tableIdentityKey(
           "destination",
           entry.storageDestinationName,
           entry.physicalTableName,
        )
      : entry.connectionName
        ? tableIdentityKey(
             "connection",
             entry.connectionName,
             entry.physicalTableName,
          )
        : undefined;
   if (table === undefined) return undefined;
   const own = entry.storageDestinationName
      ? undefined
      : keyUniquenessOf(records[address], entry);
   const parentKeys: Record<string, boolean> = {};
   for (const [key, record] of Object.entries(records)) {
      if (
         record.sourceID !== undefined &&
         key === rollupKeyRecordKey(address, record.sourceID) &&
         record.table === table
      ) {
         parentKeys[record.sourceID] = record.unique;
      }
   }
   const anyParent = Object.keys(parentKeys).length > 0;
   if (!own && !anyParent) return undefined;
   return { ...(own ?? {}), ...(anyParent ? { parentKeys } : {}) };
}

/**
 * `entries` with each colocated rollup entry carrying what `run`'s key check
 * records say of its table (see {@link keyUniquenessOf}), for a bind that
 * restores serving from the store. Only an {@link uncontestedAutoRun}'s
 * records are read; any doubt leaves the entries as they are, unknown.
 */
export function withRollupKeyRecords<E extends ManifestEntry>(
   entries: Record<string, E>,
   runs: readonly Parameters<typeof uncontestedAutoRun>[0][number][],
): Record<string, E & { keyUniqueness?: KeyUniqueness }> {
   const records = rollupKeyRecordsIn(uncontestedAutoRun(runs)?.metadata);
   if (!records || Object.keys(records).length === 0) return entries;
   const out: Record<string, E & { keyUniqueness?: KeyUniqueness }> = {};
   for (const [address, entry] of Object.entries(entries)) {
      const keyUniqueness = keyUniquenessAt(records, address, entry);
      out[address] = keyUniqueness ? { ...entry, keyUniqueness } : entry;
   }
   return out;
}
