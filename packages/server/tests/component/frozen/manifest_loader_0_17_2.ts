// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * FROZEN: the manifest projection of the last release, v0.17.2, copied
 * verbatim from its sources so the mixed-version spec can run a manifest
 * this branch writes through the code an older worker runs. Never edit it to
 * follow today's code: it stands for a server already deployed.
 *
 *  - `splitManifestEntries` and `keyUniquenessOnEntry`:
 *    `src/service/materialization/run/manifest_loader.ts`
 *  - `referenceManifestToEntries`:
 *    `src/service/materialization/run/manifest_committer.ts`
 *  - `boundSchemaOf`, `boundNativeSchemaOf`:
 *    `src/service/materialization/stored_column_types.ts`
 *  - `isLegacyFailedEntry`: `src/service/materialization/domain/types.ts`
 *  - `isUsableTableName`, `parseTablePath`, `quotable`:
 *    `src/service/quoting.ts`
 *
 * The only changes: the logger is replaced by `skipped`, which records each
 * entry the projection skips, and the types are written out here so a type
 * this branch changes cannot change the copy.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

/** A wire manifest entry, as v0.17.2 read it (every field it names). */
export type FrozenEntry = Record<string, any>;

/** v0.17.2's `FreshnessManifest` value. */
export type FrozenBound = Record<string, unknown>;

export interface FrozenFetchedManifest {
   tableNameManifest: Record<string, FrozenBound>;
   storageEntries: Record<string, FrozenEntry>;
}

/** The ManifestEntry fields v0.17.2's api-doc declares, in its order. */
export const MANIFEST_ENTRY_FIELDS_0_17_2 = [
   "sourceEntityId",
   "error",
   "sourceName",
   "origin",
   "materializedTableId",
   "physicalTableName",
   "connectionName",
   "storageDestinationName",
   "schema",
   "nativeSchema",
   "upstreamSchemas",
   "realization",
   "rowCount",
   "buildDurationMs",
   "queryCostBytes",
   "upstreamReuse",
   "upstreamRecomputeReason",
   "dataAsOf",
   "freshnessWindowSeconds",
   "freshnessFallback",
   "ledger",
   "refresh",
   "lateRows",
] as const;

/** The ManifestReference fields v0.17.2's api-doc declares. */
export const MANIFEST_REFERENCE_FIELDS_0_17_2 = [
   "sourceEntityId",
   "physicalTableName",
   "connectionName",
   "storageDestinationName",
   "nativeSchema",
] as const;

function isLegacyFailedEntry(entry: { error?: string | null }): boolean {
   return Boolean(entry?.error);
}

function boundSchemaOf(
   schema: readonly { name?: string; type?: string }[] | undefined,
): { schema: { name: string; type: string }[] } | Record<string, never> {
   const columns = (schema ?? []).flatMap((c) =>
      typeof c.name === "string" && typeof c.type === "string"
         ? [{ name: c.name, type: c.type }]
         : [],
   );
   return columns.length > 0 ? { schema: columns } : {};
}

function boundNativeSchemaOf(
   schema: readonly { name?: string; type?: string }[] | undefined,
): { nativeSchema: { name: string; type: string }[] } | Record<string, never> {
   const bound = boundSchemaOf(schema);
   return "schema" in bound ? { nativeSchema: bound.schema } : {};
}

// eslint-disable-next-line no-control-regex
const CONTROL_CHARACTER = /[\u0000-\u001f\u007f]/;

interface TablePathPart {
   value: string;
   quoted: boolean;
}

function parseTablePath(
   path: string,
   dialect: string,
   { dottedParts = false }: { dottedParts?: boolean } = {},
): TablePathPart[] | undefined {
   const bigQuery = dialect === "standardsql";
   const parts: TablePathPart[] = [];
   let i = 0;
   for (;;) {
      const q = path[i];
      if (q === '"' || q === "`") {
         const backslashes = bigQuery && q === "`";
         let value = "";
         let j = i + 1;
         for (;;) {
            if (j >= path.length) return undefined;
            const c = path[j];
            if (backslashes && c === "\\") {
               if (j + 1 >= path.length) return undefined;
               const e = path[j + 1];
               value +=
                  e === "n" ? "\n" : e === "r" ? "\r" : e === "t" ? "\t" : e;
               j += 2;
               continue;
            }
            if (c === q) {
               if (!backslashes && path[j + 1] === q) {
                  value += q;
                  j += 2;
                  continue;
               }
               break;
            }
            value += c;
            j++;
         }
         if (backslashes && value.includes(".")) {
            for (const v of value.split(".")) {
               parts.push({ value: v, quoted: true });
            }
         } else {
            parts.push({ value, quoted: true });
         }
         i = j + 1;
      } else {
         const end = path.indexOf(".", i);
         const value = end < 0 ? path.slice(i) : path.slice(i, end);
         if (value.includes('"') || value.includes("`")) return undefined;
         parts.push({ value, quoted: false });
         i = end < 0 ? path.length : end;
      }
      if (i >= path.length) break;
      if (path[i] !== ".") return undefined;
      i++;
      if (i >= path.length) return undefined;
   }
   return parts.every((p) => quotable(p.value, dialect, dottedParts))
      ? parts
      : undefined;
}

function isUsableTableName(name: string): boolean {
   if (name.includes("\\") || CONTROL_CHARACTER.test(name)) return false;
   return (
      parseTablePath(name, "") !== undefined &&
      parseTablePath(name, "standardsql", { dottedParts: true }) !== undefined
   );
}

function quotable(
   identifier: string,
   dialect: string,
   dottedParts = false,
): boolean {
   if (identifier === "" || identifier.includes("\0")) return false;
   if (dialect !== "standardsql") return true;
   if (CONTROL_CHARACTER.test(identifier)) return false;
   return dottedParts || !identifier.includes(".");
}

/** v0.17.2's `splitManifestEntries`, its skip log recorded in `skipped`. */
export function splitManifestEntries(
   entries: Record<string, FrozenEntry>,
   from: "store" | "host" = "host",
   skipped: string[] = [],
): FrozenFetchedManifest {
   const readKeyUniqueness = from === "store";
   const tableNameManifest: Record<string, FrozenBound> = {};
   const storageEntries: Record<string, FrozenEntry> = {};
   for (const [sourceEntityId, entry] of Object.entries(entries)) {
      if (isLegacyFailedEntry(entry)) {
         skipped.push(`${sourceEntityId}: records a failed source`);
         continue;
      }
      const physicalTableName = entry?.physicalTableName;
      if (!physicalTableName) {
         skipped.push(`${sourceEntityId}: has no physicalTableName`);
         continue;
      }
      if (!isUsableTableName(physicalTableName)) {
         skipped.push(`${sourceEntityId}: names an unusable table`);
         continue;
      }
      if (entry.storageDestinationName) {
         const { keyUniqueness: _unread, ...plain } = entry;
         const keyUniqueness = readKeyUniqueness
            ? keyUniquenessOnEntry(entry)
            : undefined;
         storageEntries[sourceEntityId] = keyUniqueness
            ? { ...plain, keyUniqueness }
            : plain;
         continue;
      }
      tableNameManifest[sourceEntityId] = {
         tableName: physicalTableName,
         connectionName: entry.connectionName,
         dataAsOf: entry.dataAsOf,
         freshnessWindowSeconds: entry.freshnessWindowSeconds,
         freshnessFallback: entry.freshnessFallback,
         ...boundSchemaOf(entry.schema),
         ...boundNativeSchemaOf(entry.nativeSchema),
         ...(entry.lateRows ? { lateRows: entry.lateRows } : {}),
         ...(readKeyUniqueness && keyUniquenessOnEntry(entry)
            ? { keyUniqueness: keyUniquenessOnEntry(entry) }
            : {}),
      };
   }
   return { tableNameManifest, storageEntries };
}

function keyUniquenessOnEntry(entry: FrozenEntry): unknown {
   const k = entry.keyUniqueness;
   if (!k) return undefined;
   const own =
      typeof k.keyColumn === "string" && typeof k.unique === "boolean"
         ? { keyColumn: k.keyColumn, unique: k.unique }
         : undefined;
   const parentKeys =
      k.parentKeys && typeof k.parentKeys === "object"
         ? Object.fromEntries(
              Object.entries(k.parentKeys).filter(
                 ([, unique]) => typeof unique === "boolean",
              ),
           )
         : {};
   const anyParent = Object.keys(parentKeys).length > 0;
   return own || anyParent
      ? { ...(own ?? {}), ...(anyParent ? { parentKeys } : {}) }
      : undefined;
}

/** v0.17.2's `referenceManifestToEntries`. */
export function referenceManifestToEntries(
   referenceManifest: FrozenEntry[] | undefined,
): Record<string, FrozenEntry> {
   const entries: Record<string, FrozenEntry> = {};
   for (const ref of referenceManifest ?? []) {
      entries[ref.sourceEntityId] = {
         sourceEntityId: ref.sourceEntityId,
         physicalTableName: ref.physicalTableName,
         connectionName: ref.connectionName,
         ...(ref.storageDestinationName
            ? { storageDestinationName: ref.storageDestinationName }
            : {}),
         ...(ref.nativeSchema ? { nativeSchema: ref.nativeSchema } : {}),
      };
   }
   return entries;
}
