// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A materialized table's column contract (`ManifestEntry.contract`): what
 * its build recorded about each column, read back at bind and by every
 * downstream build instead of being probed again.
 *
 * The rules a reader follows, the same on every replica and every release:
 *
 * - An entry with no contract, an empty one, or one with no `v` HAS NO
 *   contract ({@link parseContract} answers undefined): its table is never
 *   served from, and the next build rebuilds it in full
 *   (`contract_missing`). Nor is one that lists no column (its capture
 *   read nothing): {@link servableContract}.
 * - A column the contract does not list is unknown: compared `none`, its
 *   gate not exact ({@link contractColumn}).
 * - Keys a reader does not know are ignored at every level; an unknown
 *   `compare` reads as `none`.
 *
 * The contract is stored by a host as text and echoed back unparsed, so it
 * is read here from `unknown`, never trusted to be well-formed.
 *
 * Pure.
 */

import { createHash } from "crypto";
import {
   BINARY_COLLATION,
   type Collation,
   collationOf,
   type TableCollations,
} from "./collation";
import { gateColumnTypeExact } from "./gate_policy";
import type { Tier } from "./vocabulary";

/** The contract shape this release writes and reads. */
const CONTRACT_VERSION = 1;

/**
 * How a stored read of a column may stand in for live: `exact` (equality,
 * grouping, filters and ordering match), `equality` (equality, grouping,
 * equality filters and joins match; ordering and ranges may not), `none`.
 */
type ContractCompare = "exact" | "equality" | "none";

/** One column a build recorded. */
type ContractColumn = {
   name: string;
   /** Its type as the holding engine's catalog names it. */
   nativeType: string;
   /** The stored column's normalised collation identity; null: not a string. */
   collation: string | null;
   compare: ContractCompare;
   /** Whether a row-level gate over it reads the same rows as live (R-10). */
   gateExact: boolean;
   /**
    * The Malloy type the build source declared for it when it was built: a
    * later bind whose source declares another type finds the table's values
    * read as a type they no longer are (`declared_moved`).
    */
   declaredType?: string;
};

/**
 * One key check a build ran over its table (`ColumnContract.keys`), or one
 * it was asked for and could not run (`unverified`, with `unique: false`):
 * recorded so a later run knows the key was the one the table was built
 * under, and still never read as passed. `rebuiltToVerify` marks an
 * `unverified` check of a table that was itself rebuilt to run it (the
 * table before it recorded the check `unverified` too): a standalone run
 * rebuilds a pair table once for a leaf check that could not run, and not
 * again for this table.
 */
export type ContractKeyCheck = {
   unique: boolean;
   level?: string;
   unverified?: true;
   rebuiltToVerify?: true;
};

/**
 * What a key check in `ColumnContract.keys` checked: a pair table's leaf
 * key (`leaf`), a rollup's own key, its one grain dimension (`rollup`), the
 * primary key of a query source that reads a rollup through a join_many on
 * it (`parent`), and a stored source's own primary key (`stored`).
 */
type ContractKeyLevel = "leaf" | "rollup" | "parent" | "stored";

/**
 * The name a key check is recorded under in `ColumnContract.keys`:
 * `<level>:<subject>.<keyColumn>`, or `<level>:<keyColumn>` with no subject.
 * The subject is the join path to a pair leaf (`leaf:items.options.option_id`)
 * and the package-relative sourceID of a checked source
 * (`parent:orders@models/orders.malloy.id`); a rollup's own key has none
 * (`rollup:category`). A reader builds the name it needs and looks it up,
 * never parsing one, so a subject may hold dots.
 */
export function contractKeyName(
   level: ContractKeyLevel,
   subject: string,
   keyColumn: string,
): string {
   return `${level}:${subject ? `${subject}.` : ""}${keyColumn}`;
}

/**
 * A key check's name without its level, when it is recorded at `level`
 * (`leaf:items.item_id` → `items.item_id`), else undefined: how a reader
 * shows a check as the model names it, or reads a rollup's own key column.
 */
function contractKeyWithoutLevel(
   name: string,
   level: ContractKeyLevel,
): string | undefined {
   const prefix = `${level}:`;
   return name.startsWith(prefix) ? name.slice(prefix.length) : undefined;
}

/** A source a bind may find a key check recorded for, as the current model declares it. */
export interface KeyedSource {
   sourceID: string;
   /** Its sourceID with its file made relative to the package. */
   relativeID: string;
   primaryKey: string;
}

/**
 * The key checks `contract` records, in the shape a bind gives a table
 * (`KeyUniqueness`): its own key check (`rollup:`), the checks of `sources`
 * it holds under their current primary key (`parent:` and `stored:`, by
 * sourceID), and the check of each pair leaf `leaves` names (`leaf:` under
 * its join path, by the leaf's sourceID), also under the leaf's current
 * primary key. A check of another key, or of a source the model no longer
 * declares, is not one; one that could not run (`unverified`) reads as not
 * passed. Undefined when it records none.
 */
export function keyUniquenessOfContract(
   contract: unknown,
   sources: readonly KeyedSource[],
   leaves: readonly { joinPath: string; sourceID: string }[] = [],
):
   | {
        keyColumn?: string;
        unique?: boolean;
        parentKeys?: Record<string, boolean>;
        parentKeyColumns?: Record<string, string>;
     }
   | undefined {
   const keys = parseContract(contract)?.keys;
   if (!keys) return undefined;
   const own = Object.entries(keys).find(
      ([name, check]) =>
         check.level === "rollup" &&
         contractKeyWithoutLevel(name, "rollup") !== undefined,
   );
   const parentKeys: Record<string, boolean> = {};
   const parentKeyColumns: Record<string, string> = {};
   const read = (
      level: ContractKeyLevel,
      subject: string,
      source: KeyedSource,
   ) => {
      const check = keys[contractKeyName(level, subject, source.primaryKey)];
      if (check?.level !== level) return;
      // Every check of a source must pass: one checked twice is unique only if both say so.
      parentKeys[source.sourceID] =
         (parentKeys[source.sourceID] ?? true) && passed(check);
      parentKeyColumns[source.sourceID] = source.primaryKey;
   };
   for (const s of sources) {
      read("parent", s.relativeID, s);
      read("stored", s.relativeID, s);
   }
   const bySourceID = new Map(sources.map((s) => [s.sourceID, s]));
   for (const leaf of leaves) {
      const source = bySourceID.get(leaf.sourceID);
      if (source) read("leaf", leaf.joinPath, source);
   }
   const anyParent = Object.keys(parentKeys).length > 0;
   if (!own && !anyParent) return undefined;
   return {
      ...(own
         ? {
              keyColumn: contractKeyWithoutLevel(own[0], "rollup")!,
              unique: passed(own[1]),
           }
         : {}),
      ...(anyParent ? { parentKeys, parentKeyColumns } : {}),
   };
}

/** Whether `check` ran and found its key unique and never NULL. */
function passed(check: ContractKeyCheck): boolean {
   return check.unique && check.unverified !== true;
}

/**
 * `raw` (a contract as a host or the store holds it) without the key checks
 * that passed, everything else as it was: so a bind finds no verdict in it
 * that would serve. A check that found its key repeated, or could not run,
 * stays, so what it declined still declines. `raw` itself when it records no
 * passed check, or is not a contract.
 */
export function contractWithoutPassedKeys(raw: unknown): unknown {
   let value = raw;
   if (typeof value === "string") {
      try {
         value = JSON.parse(value);
      } catch {
         return raw;
      }
   }
   if (!isRecord(value) || !isRecord(value.keys)) return raw;
   const all = Object.entries(value.keys);
   const kept = all.filter(
      ([, check]) =>
         !(
            isRecord(check) &&
            check.unique === true &&
            check.unverified !== true
         ),
   );
   if (kept.length === all.length) return raw;
   const { keys: _dropped, ...rest } = value;
   return kept.length > 0 ? { ...rest, keys: Object.fromEntries(kept) } : rest;
}

/** A table's contract, as {@link parseContract} reads it. */
export type ColumnContract = {
   v: number;
   producer?: string;
   dialect: string;
   defaultCollation?: string;
   columns: ContractColumn[];
   keys?: Record<string, ContractKeyCheck>;
};

const COMPARES: ReadonlySet<string> = new Set(["exact", "equality", "none"]);

function isRecord(v: unknown): v is Record<string, unknown> {
   return typeof v === "object" && v !== null && !Array.isArray(v);
}

/**
 * The contract `raw` holds, or undefined when it holds none: absent, not an
 * object (a host's text that does not parse as JSON included), empty, or
 * without a numeric `v`. A column without a name or a native type is
 * dropped (it is then unknown, as an unlisted column is); an unknown
 * `compare` reads as `none`; a non-boolean `gateExact` as false.
 */
export function parseContract(raw: unknown): ColumnContract | undefined {
   let value = raw;
   if (typeof value === "string") {
      try {
         value = JSON.parse(value);
      } catch {
         return undefined;
      }
   }
   if (!isRecord(value) || typeof value.v !== "number") return undefined;
   const columns: ContractColumn[] = [];
   for (const c of Array.isArray(value.columns) ? value.columns : []) {
      if (!isRecord(c)) continue;
      if (typeof c.name !== "string" || c.name === "") continue;
      if (typeof c.nativeType !== "string" || c.nativeType === "") continue;
      columns.push({
         name: c.name,
         nativeType: c.nativeType,
         collation: typeof c.collation === "string" ? c.collation : null,
         compare:
            typeof c.compare === "string" && COMPARES.has(c.compare)
               ? (c.compare as ContractCompare)
               : "none",
         gateExact: c.gateExact === true,
         ...(typeof c.declaredType === "string" && c.declaredType !== ""
            ? { declaredType: c.declaredType }
            : {}),
      });
   }
   const keys: Record<string, ContractKeyCheck> = {};
   if (isRecord(value.keys)) {
      for (const [column, check] of Object.entries(value.keys)) {
         if (!isRecord(check) || typeof check.unique !== "boolean") continue;
         keys[column] = {
            unique: check.unique,
            ...(typeof check.level === "string" ? { level: check.level } : {}),
            ...(check.unverified === true ? { unverified: true } : {}),
            ...(check.unverified === true && check.rebuiltToVerify === true
               ? { rebuiltToVerify: true }
               : {}),
         };
      }
   }
   return {
      v: value.v,
      ...(typeof value.producer === "string"
         ? { producer: value.producer }
         : {}),
      dialect: typeof value.dialect === "string" ? value.dialect : "",
      ...(typeof value.defaultCollation === "string"
         ? { defaultCollation: value.defaultCollation }
         : {}),
      columns,
      ...(Object.keys(keys).length > 0 ? { keys } : {}),
   };
}

/**
 * The contract a bound table may be served under: {@link parseContract}'s,
 * when it lists at least one column. A contract that lists none (its
 * capture read nothing) proves nothing about any column, so its table is
 * `contract_missing` as one with no contract is.
 */
export function servableContract(raw: unknown): ColumnContract | undefined {
   const contract = parseContract(raw);
   return contract && contract.columns.length > 0 ? contract : undefined;
}

/**
 * The column of `contract` named `name`: exactly, else the one column that
 * matches it case-blind. An unlisted (or ambiguous) column is unknown:
 * `compare` `none`, gate not exact, no type.
 */
export function contractColumn(
   contract: ColumnContract,
   name: string,
): ContractColumn | { name: string; compare: "none"; gateExact: false } {
   const found = columnNamed(contract.columns, name);
   return found ?? { name, compare: "none", gateExact: false };
}

/** The column named `name`: exactly, else the one that matches it case-blind. */
function columnNamed<C extends { name: string }>(
   columns: readonly C[],
   name: string,
): C | undefined {
   const exact = columns.find((c) => c.name === name);
   if (exact) return exact;
   const folded = columns.filter(
      (c) => c.name.toLowerCase() === name.toLowerCase(),
   );
   return folded.length === 1 ? folded[0] : undefined;
}

/** A native type spelled one way whatever its case and spacing. */
function spelledType(nativeType: string): string {
   return nativeType
      .replace(/\s+/g, " ")
      .replace(/\s*([(),])\s*/g, "$1")
      .trim()
      .toUpperCase();
}

/**
 * A digest of what a reader of the table depends on: each column's name,
 * native type (spelled) and collation, in table order. Two contracts with
 * the same columns digest alike whatever else they carry, so a downstream's
 * `builtFrom[].contractDigest` moves only when its upstream's table did.
 */
export function contractDigest(contract: ColumnContract): string {
   const canonical = contract.columns.map((c) => [
      c.name,
      spelledType(c.nativeType),
      c.collation,
   ]);
   return createHash("sha256")
      .update(JSON.stringify(canonical))
      .digest("hex")
      .slice(0, 32);
}

/** Whether the key check named `key` ({@link contractKeyName}) is recorded as passed. */
export function keyCheckPassed(
   contract: ColumnContract | undefined,
   key: string,
): boolean {
   const check = contract?.keys?.[key];
   return check !== undefined && passed(check);
}

/** Whether the key check named `key` ran and found the key repeated or NULL. */
function keyCheckFailed(
   contract: Pick<ColumnContract, "keys"> | undefined,
   key: string,
): boolean {
   const check = contract?.keys?.[key];
   return check?.unique === false && check.unverified !== true;
}

/**
 * What a pair table's contract says of the leaf key checks `leafKeys` a build
 * over the table relies on, when any did not pass: the keys found held more
 * than once or NULL, else those it records no result for (or a check that
 * could not run), named as the model
 * names the path (`items.item_id`). Undefined when every one passed.
 */
export function pairLeafCheckFailure(
   contract: ColumnContract | undefined,
   leafKeys: readonly string[],
): string | undefined {
   const failed = leafKeys.filter((k) => keyCheckFailed(contract, k));
   // Not recorded, or recorded as a check that could not run.
   const unrecorded = leafKeys.filter(
      (k) => !keyCheckPassed(contract, k) && !keyCheckFailed(contract, k),
   );
   if (failed.length === 0 && unrecorded.length === 0) return undefined;
   const named = (ks: string[]) =>
      ks.map((k) => `'${contractKeyWithoutLevel(k, "leaf") ?? k}'`).join(", ");
   return failed.length > 0
      ? `found the key ${named(failed)} held more than once or NULL`
      : `recorded no result for the key ${named(unrecorded)}`;
}

/** The stored side of a table a build just wrote, as the bind reads it. */
interface StoredTableFacts {
   /** The holding engine's dialect. */
   dialect: string;
   columns: readonly {
      name: string;
      nativeType: string;
      /** Malloy reads it as a string. */
      isString: boolean;
      /** A DuckLake 128-bit integer its Parquet files hold as a DOUBLE. */
      storedAsDouble?: boolean;
   }[];
   /** Its columns' collations; undefined when the holding engine's were not read. */
   collations?: TableCollations;
   defaultCollation?: string;
}

/** The order of {@link ContractCompare}, weakest first. */
const COMPARE_RANK: Record<ContractCompare, number> = {
   none: 0,
   equality: 1,
   exact: 2,
};

/** The weaker of two compares. */
function weakerCompare(
   a: ContractCompare,
   b: ContractCompare,
): ContractCompare {
   return COMPARE_RANK[a] <= COMPARE_RANK[b] ? a : b;
}

/** A warehouse column's type, with the dialect it is named in. */
type UpstreamType = { dialect: string; nativeType: string };

/** The upstream type of the stored column `name`: matched exactly, else case-blind when one matches. */
function upstreamTypeNamed(
   types: ReadonlyMap<string, UpstreamType> | undefined,
   name: string,
): UpstreamType | undefined {
   if (!types) return undefined;
   const exact = types.get(name);
   if (exact) return exact;
   const folded = [...types].filter(
      ([n]) => n.toLowerCase() === name.toLowerCase(),
   );
   return folded.length === 1 ? folded[0][1] : undefined;
}

/**
 * The contract a build records for a table it wrote, from the table as its
 * holding engine reads it now ({@link StoredTableFacts}, the read a bind
 * makes, so a table straight out of a build always passes its own check)
 * and, for a `storage=` table, how its source's strings compared live.
 *
 * A string column's `compare`:
 * - its stored collation unread: `none`;
 * - a database table: `exact`. Live reads the same engine, and the build's
 *   `CREATE TABLE AS` keeps each column's collation;
 * - a `storage=` table: `none` unless its stored collation is binary; then
 *   the weakest compare of the stored tables it read when it stacked on
 *   them (`stackedOn`), else its source's live judgement (binary: `exact`;
 *   deterministic: `equality`; anything else, or unread: `none`).
 *
 * `gateExact`: on a database table, every column but a DuckLake double; on
 * a `storage=` table, a column of a type a gate compares exactly, or a
 * destination double that copies a warehouse binary64 unchanged
 * ({@link gateColumnTypeExact}, judged by `upstreamTypes`). A column with no
 * upstream type (computed, read by author SQL, or its table unread) is not.
 */
export function deriveContract(args: {
   tier: Tier;
   stored: StoredTableFacts;
   /** A `storage=` table built from its source: each string column's live collation. */
   live?: ReadonlyMap<string, Collation>;
   /** A `storage=` table built over stored tables: their weakest string compare. */
   stackedOn?: ContractCompare;
   keys?: Record<string, ContractKeyCheck>;
   producer?: string;
   /** The Malloy type the build source declares for each column, by name. */
   declared?: readonly { name: string; type: string }[];
   /**
    * A `storage=` table: the warehouse type of each column that is a plain
    * copy of one warehouse column, by its stored name.
    */
   upstreamTypes?: ReadonlyMap<string, UpstreamType>;
}): ColumnContract {
   const { tier, stored } = args;
   const columns = stored.columns.map((c): ContractColumn => {
      const declaredType = args.declared
         ? columnNamed(args.declared, c.name)?.type
         : undefined;
      const collation =
         c.isString && stored.collations
            ? collationOf(stored.collations, c.name)
            : null;
      return {
         name: c.name,
         nativeType: c.nativeType,
         collation,
         compare: columnCompare(args, c.isString, collation, c.name),
         gateExact:
            tier === "database"
               ? c.storedAsDouble !== true
               : gateColumnTypeExact(
                    c.nativeType,
                    upstreamTypeNamed(args.upstreamTypes, c.name),
                 ),
         ...(declaredType ? { declaredType } : {}),
      };
   });
   return {
      v: CONTRACT_VERSION,
      ...(args.producer ? { producer: args.producer } : {}),
      dialect: stored.dialect,
      ...(stored.defaultCollation
         ? { defaultCollation: stored.defaultCollation }
         : {}),
      columns,
      ...(args.keys && Object.keys(args.keys).length > 0
         ? { keys: args.keys }
         : {}),
   };
}

/** One column's `compare` (see {@link deriveContract}). */
function columnCompare(
   args: Parameters<typeof deriveContract>[0],
   isString: boolean,
   collation: string | null,
   name: string,
): ContractCompare {
   if (!isString) return "exact";
   if (collation === null) return "none";
   if (args.tier === "database") return "exact";
   if (collation !== BINARY_COLLATION) return "none";
   // A column with no collation of its own compares under the destination
   // session's default, which must be binary too.
   const sessionDefault = args.stored.defaultCollation;
   if (sessionDefault !== undefined && sessionDefault !== BINARY_COLLATION) {
      return "none";
   }
   if (args.stackedOn !== undefined) return args.stackedOn;
   return liveCompare(args.live?.get(name));
}

/**
 * The `compare` a byte-exact `storage=` table records for a string column
 * built from its source, by how the source compares it live: binary,
 * `exact`; deterministic, `equality`; anything else, or unread, `none`.
 */
export function liveCompare(live: Collation | undefined): ContractCompare {
   return live === "binary"
      ? "exact"
      : live === "deterministic"
        ? "equality"
        : "none";
}

/**
 * The weakest `compare` over the string columns of the stored tables a
 * build read; `none` when any of them has no contract. A table with no
 * string column reads `exact`.
 */
export function weakestStringCompare(
   upstreams: readonly (ColumnContract | undefined)[],
): ContractCompare {
   let out: ContractCompare = "exact";
   for (const contract of upstreams) {
      if (!contract) return "none";
      for (const c of contract.columns) {
         if (c.collation !== null || c.compare !== "exact") {
            out = weakerCompare(out, c.compare);
         }
      }
   }
   return out;
}

/** Why a stored table no longer matches its contract. */
export type IncompatibleCode =
   | "column_gone"
   | "column_retyped"
   | "collation_changed"
   /** A `.sql()` or substituted table's source now declares another type. */
   | "declared_moved";

/** A table as a bind reads it now (`ObservedColumns`, with its collations). */
interface ObservedTable {
   columns: readonly { name: string; nativeType?: string; type?: string }[];
   /**
    * Its collations, and (DuckDB) the reading session's default collation;
    * undefined when its engine's are not read.
    */
   collations?: TableCollations;
}

/** What {@link compatible} found. */
type Compatibility =
   | { ok: true }
   | { ok: false; code: IncompatibleCode; columns: string[] };

/**
 * Whether a stored table, as a bind reads it now, still matches the contract
 * its build recorded: the one comparator every drift trigger asks. A column
 * the contract records that is gone, holds another native type (case and
 * spacing aside), or, on an engine whose collations are read, another
 * collation, is incompatible; a column the table gained is not. So is a
 * destination whose session default collation is not the one the build
 * recorded (`defaultCollation`), which every string column declared with no
 * collation of its own compares under. The first of gone, retyped and
 * collation reports, naming every such column.
 *
 * Never asked of a table whose read failed: that table is unread, on its own
 * path, and never incompatible.
 */
export function compatible(
   contract: ColumnContract,
   observed: ObservedTable,
): Compatibility {
   const gone: string[] = [];
   const retyped: string[] = [];
   const recollated: string[] = [];
   for (const c of contract.columns) {
      const now = columnNamed(observed.columns, c.name);
      if (!now) {
         gone.push(c.name);
         continue;
      }
      const type = now.nativeType ?? now.type;
      if (
         type === undefined ||
         spelledType(type) !== spelledType(c.nativeType)
      ) {
         retyped.push(c.name);
         continue;
      }
      if (
         c.collation !== null &&
         observed.collations !== undefined &&
         collationOf(observed.collations, now.name) !== c.collation
      ) {
         recollated.push(c.name);
      }
   }
   if (gone.length > 0)
      return { ok: false, code: "column_gone", columns: gone };
   if (retyped.length > 0) {
      return { ok: false, code: "column_retyped", columns: retyped };
   }
   if (recollated.length > 0) {
      return { ok: false, code: "collation_changed", columns: recollated };
   }
   const defaultNow = observed.collations?.defaultCollation;
   if (
      contract.defaultCollation !== undefined &&
      defaultNow !== undefined &&
      defaultNow !== contract.defaultCollation
   ) {
      // Only a string column with no collation of its own (recorded binary)
      // compares under the default: a table with none is not changed by it.
      const underDefault = contract.columns
         .filter((c) => c.collation === BINARY_COLLATION)
         .map((c) => c.name);
      if (underDefault.length > 0) {
         return { ok: false, code: "collation_changed", columns: underDefault };
      }
   }
   return { ok: true };
}

/**
 * The columns whose build source now declares another Malloy type than the
 * one the contract recorded at build (`declaredType`): the table holds values
 * of the type it was built for, which a query would read as the type the
 * source declares now. Compares only columns the contract recorded a declared
 * type for, and that the declaration still names; a contract that recorded
 * none (an entry written by a stand-in build) is never judged here.
 */
export function declaredMoves(
   contract: ColumnContract,
   declared: readonly { name: string; type?: string }[],
): string[] {
   return contract.columns.flatMap((c) => {
      if (c.declaredType === undefined) return [];
      const now = columnNamed(declared, c.name)?.type;
      return now !== undefined && now !== "" && now !== c.declaredType
         ? [c.name]
         : [];
   });
}
