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

/** The contract shape this release writes and reads. */
export const CONTRACT_VERSION = 1;

/**
 * How a stored read of a column may stand in for live: `exact` (equality,
 * grouping, filters and ordering match), `equality` (equality, grouping,
 * equality filters and joins match; ordering and ranges may not), `none`.
 */
export type ContractCompare = "exact" | "equality" | "none";

/** One column a build recorded. */
export interface ContractColumn {
   name: string;
   /** Its type as the holding engine's catalog names it. */
   nativeType: string;
   /** The stored column's normalised collation identity; null: not a string. */
   collation: string | null;
   compare: ContractCompare;
   /** Whether a row-level gate over it reads the same rows as live (R-10). */
   gateExact: boolean;
}

/** One pair-table key check a build ran (`ColumnContract.keys`). */
export interface ContractKeyCheck {
   unique: boolean;
   level?: string;
}

/** A table's contract, as {@link parseContract} reads it. */
export interface ColumnContract {
   v: number;
   producer?: string;
   dialect: string;
   defaultCollation?: string;
   columns: ContractColumn[];
   keys?: Record<string, ContractKeyCheck>;
}

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
export function columnNamed<C extends { name: string }>(
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
export function spelledType(nativeType: string): string {
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
