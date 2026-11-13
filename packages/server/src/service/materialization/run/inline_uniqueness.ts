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
import type {
   BuildManifest,
   Connection as MalloyConnection,
} from "@malloydata/malloy";
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
export interface UniquenessPass {
   /** The joined source's content address (its sourceEntityId). */
   joinedAddress: string;
   /** The table the check read: its connection and physical name. */
   joinedTable: string;
   /** The run whose check actually ran and passed (a skip carries it). */
   checkedInRun?: string;
}

/** What became of one check in a run, for the run's metadata. */
export interface UniquenessCheckOutcome {
   sourceName: string;
   join: string;
   /** `ran`: the check ran (and passed). `skipped`: a recorded pass held. */
   outcome: "ran" | "skipped";
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

   constructor(
      /** The previous run's passes; undefined when none may be relied on. */
      private readonly prior:
         | Readonly<Record<string, UniquenessPass>>
         | undefined,
      private readonly runId?: string,
   ) {}

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
   }

   get id(): string | undefined {
      return this.runId;
   }

   /** The run metadata value, or undefined when no check was reached. */
   toMetadata():
      | {
           passes: Record<string, UniquenessPass>;
           checks: UniquenessCheckOutcome[];
        }
      | undefined {
      if (this.outcomes.length === 0 && this.passes.size === 0)
         return undefined;
      return {
         passes: Object.fromEntries(this.passes),
         checks: [...this.outcomes],
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
   const { passes, ...rest } = value as { passes?: unknown };
   if (
      !passes ||
      typeof passes !== "object" ||
      Object.keys(passes as object).length === 0
   ) {
      return undefined;
   }
   return { ...metadata, [INLINE_UNIQUENESS_KEY]: { ...rest, passes: {} } };
}

/**
 * Compiles a check against a build manifest, so it reads the persisted
 * tables the manifest names. Registered per check at plan time, where the
 * companion's materializer is (materialize_compile.ts); a check without one
 * (a plan from elsewhere) is never compiled over a stored table.
 */
export type UniquenessCheckCompiler = (
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
