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
 *   (`contract_missing`).
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
import { gateTypeInexact } from "./gate_policy";
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

/** One pair-table key check a build ran (`ColumnContract.keys`). */
type ContractKeyCheck = {
   unique: boolean;
   level?: string;
};

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

/** Whether the pair table's key check on `column` is recorded as passed. */
export function keyCheckPassed(
   contract: ColumnContract | undefined,
   column: string,
): boolean {
   return contract?.keys?.[column]?.unique === true;
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
 * a `storage=` table, a column of a type a gate compares exactly
 * ({@link gateTypeInexact}), since the build does not name each column's
 * source.
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
               : !gateTypeInexact(c.nativeType),
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
   const live = args.live?.get(name);
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
