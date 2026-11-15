// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the materialization goldens record, read off the real code paths:
 *
 *  - every generated Malloy file, captured where it is handed to the compiler
 *    (the overlay of `createModelRuntime`, or a corpus model's runtime
 *    factory), so a build copy, a companion, a served copy and each rollup
 *    or view variant is frozen byte for byte without the code under test
 *    exposing it;
 *  - each planned source's content address, compiler key, address SQL and
 *    build SQL, in build order;
 *  - the internal plans (`MaterializePlan`, the rollup plans) and the plan's
 *    refusals and findings.
 */

import { makeDigest, type PersistSource } from "@malloydata/malloy";
import { spyOn } from "bun:test";
import * as fs from "fs";
import { fileURLToPath } from "url";
import {
   buildSQLOf,
   compilerBuildId,
   computeSourceEntityId,
} from "../../../src/service/materialization/plan/addressing";
import type { CompiledBuildPlan } from "../../../src/service/materialization/plan/build_plan";
import * as modelRuntime from "../../../src/service/model_runtime";
import type { Normalizer } from "./normalize";

/** The digest every golden address is computed with, per connection. */
export function fixedDigest(connectionName: string): string {
   return `golden-digest:${connectionName}`;
}

/**
 * Generated files by URL, each URL's distinct texts in the order first seen.
 * A file identical to the one on disk at its URL is the author's, passed
 * through an overlay, and is not recorded.
 */
export class GeneratedFiles {
   private readonly files = new Map<string, string[]>();

   add(url: string, text: string): void {
      if (onDisk(url) === text) return;
      const texts = this.files.get(url) ?? [];
      if (!texts.includes(text)) texts.push(text);
      this.files.set(url, texts);
   }

   addAll(overlay: ReadonlyMap<string, string> | undefined): void {
      for (const [url, text] of overlay ?? []) this.add(url, text);
   }

   get size(): number {
      return this.files.size;
   }

   /**
    * The files as one text, sorted by normalised URL, each headed by it, or
    * undefined when nothing was generated.
    */
   render(
      normalizer: Normalizer,
      title: string,
      comment = "//",
   ): string | undefined {
      if (this.files.size === 0) return undefined;
      const entries = [...this.files].map(
         ([url, texts]) => [normalizer.text(url), texts] as const,
      );
      entries.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
      const out = [`${comment} ${title}`];
      for (const [url, texts] of entries) {
         texts.forEach((text, i) => {
            out.push(
               "",
               `${comment} ==== ${url}${texts.length > 1 ? ` (text ${i + 1} of ${texts.length})` : ""}`,
               normalizer.text(text).replace(/\n+$/, ""),
            );
         });
      }
      return out.join("\n");
   }
}

function onDisk(url: string): string | undefined {
   try {
      return fs.readFileSync(fileURLToPath(url), "utf8");
   } catch {
      return undefined;
   }
}

/**
 * Record every overlay handed to `createModelRuntime` while `run` runs: the
 * build plan's companions and build copies, and the served copy's and the
 * variants' generated files. Calls run sequentially in these specs, so what
 * `run` triggers is what is recorded.
 */
export async function recordingOverlays<T>(
   into: GeneratedFiles,
   run: () => Promise<T>,
): Promise<T> {
   const original = modelRuntime.createModelRuntime;
   const spy = spyOn(modelRuntime, "createModelRuntime").mockImplementation(
      (...args: Parameters<typeof original>) => {
         into.addAll(args[3]?.overlay);
         return original(...args);
      },
   );
   try {
      return await run();
   } finally {
      spy.mockRestore();
   }
}

/** One planned source's addressing, as the build would compute it. */
export interface SourceAddressing {
   sourceID: string;
   name: string;
   connectionName: string;
   /** The sourceIDs on the tables it reads (the plan's build edges). */
   reads: string[];
   /** Why it cannot be keyed, when it cannot (no address, no SQL). */
   unkeyable?: string;
   /** The real address and compiler key, mapped to the fixed-digest ones. */
   realAddress?: string;
   address?: string;
   realKey?: string;
   key?: string;
   /** The compiler key is not `makeDigest(digest, sql)`: unexpected. */
   keyIsNotSqlDigest?: true;
   addressSQL?: string;
   buildSQL?: string;
   /** The build SQL with every other planned source built (a chained read). */
   chainedBuildSQL?: string;
}

/** Each planned source's addressing, in build order, each source once. */
export function addressesOf(compiled: CompiledBuildPlan): SourceAddressing[] {
   const digests = compiled.connectionDigests;
   const fixed: Record<string, string> = {};
   const seen = new Set<string>();
   const ordered: { source: PersistSource; reads: string[] }[] = [];
   for (const build of compiled.builds) {
      for (const sourceID of build.order) {
         const source = compiled.sources[sourceID];
         if (!source || seen.has(sourceID)) continue;
         seen.add(sourceID);
         fixed[source.connectionName] = fixedDigest(source.connectionName);
         ordered.push({ source, reads: build.dependsOn[sourceID] ?? [] });
      }
   }
   // Keys first, so each source's chained build SQL can read the others.
   const keys = new Map<string, string>();
   for (const { source } of ordered) {
      try {
         keys.set(
            source.sourceID,
            compilerBuildId(
               source,
               digests,
               compiled.targets?.[source.sourceID],
            ),
         );
      } catch {
         // Unkeyable: reported below.
      }
   }
   return ordered.map(({ source, reads }) => {
      const base = {
         sourceID: source.sourceID,
         name: source.name,
         connectionName: source.connectionName,
         reads,
      };
      const target = compiled.targets?.[source.sourceID];
      try {
         const realAddress = computeSourceEntityId(source, digests, target);
         const address = computeSourceEntityId(source, fixed, target);
         const addressSQL = target
            ? target.sql
            : buildSQLOf(source, { entries: {} });
         const realKey = keys.get(source.sourceID);
         const digest = digests[source.connectionName];
         const keyIsSqlDigest =
            realKey !== undefined &&
            digest !== undefined &&
            realKey === makeDigest(digest, addressSQL);
         const buildSQL = buildSQLOf(source, { entries: {} }, target);
         const others: Record<string, { tableName: string }> = {};
         for (const other of ordered) {
            const key = keys.get(other.source.sourceID);
            if (other.source === source || key === undefined) continue;
            others[key] = { tableName: `built.${other.source.name}` };
         }
         const chainedBuildSQL = buildSQLOf(
            source,
            { entries: others },
            target,
         );
         return {
            ...base,
            realAddress,
            address,
            realKey,
            key: keyIsSqlDigest
               ? makeDigest(fixed[source.connectionName], addressSQL)
               : undefined,
            ...(keyIsSqlDigest ? {} : { keyIsNotSqlDigest: true as const }),
            addressSQL,
            buildSQL,
            chainedBuildSQL,
         };
      } catch (error) {
         return {
            ...base,
            unkeyable: error instanceof Error ? error.message : String(error),
         };
      }
   });
}

/**
 * Teach `normalizer` each real address and compiler key, so any output that
 * carries one (the wire plan, a path report, a table id) reads the fixed one.
 */
export function learnAddresses(
   normalizer: Normalizer,
   addresses: readonly SourceAddressing[],
): void {
   for (const a of addresses) {
      if (a.realAddress && a.address) {
         normalizer.addReplacement(a.realAddress, a.address);
      }
      if (a.realKey) {
         normalizer.addReplacement(a.realKey, a.key ?? "<compiler-key>");
      }
   }
}

/** The build-plan text golden: addresses and SQL per source, then the generated files. */
export function renderBuild(
   title: string,
   normalizer: Normalizer,
   addresses: readonly SourceAddressing[],
   generated: GeneratedFiles,
): string {
   const out: string[] = [
      `-- ${title}`,
      `-- Addresses are computed with a fixed connection digest (${fixedDigest("<connection>")}),`,
      "-- so they move exactly when the SQL or the folded layout moves.",
   ];
   if (addresses.length === 0) out.push("", "-- (no planned source)");
   for (const a of addresses) {
      out.push(
         "",
         `-- ==== source ${a.name}`,
         `-- sourceID: ${normalizer.text(a.sourceID)}`,
         `-- connection: ${a.connectionName}`,
      );
      if (a.reads.length > 0) {
         out.push(
            `-- reads: ${a.reads.map((r) => normalizer.text(r)).join(", ")}`,
         );
      }
      if (a.unkeyable !== undefined) {
         out.push(`-- unkeyable: ${normalizer.text(a.unkeyable)}`);
         continue;
      }
      out.push(`-- sourceEntityId: ${a.address}`);
      out.push(
         a.keyIsNotSqlDigest
            ? "-- compiler key: NOT makeDigest(digest, address SQL)"
            : `-- compiler key: ${a.key}${a.key === a.address ? " (the sourceEntityId)" : ""}`,
      );
      const sql = (s: string | undefined) => normalizer.text(s ?? "").trim();
      if (sql(a.addressSQL) !== sql(a.buildSQL)) {
         out.push("-- address SQL:", sql(a.addressSQL));
      }
      out.push("-- build SQL:", sql(a.buildSQL));
      if (sql(a.chainedBuildSQL) !== sql(a.buildSQL)) {
         out.push(
            "-- build SQL with every other planned source built:",
            sql(a.chainedBuildSQL),
         );
      }
   }
   const files = generated.render(normalizer, "generated files", "--");
   if (files) out.push("", files);
   return out.join("\n");
}

/** The internal plan state a golden keeps, beside the wire plan. */
export function internalPlans(compiled: CompiledBuildPlan): unknown {
   return {
      materializePlans: compiled.materializePlans ?? {},
      rollupPlans: compiled.preaggregatePlans ?? {},
      sourceGateOutcomes: compiled.sourceGateOutcomes ?? {},
      droppedPersistSources: compiled.droppedPersistSources ?? [],
      annotationFindings: compiled.annotationFindings ?? [],
      sourceModelPaths: compiled.sourceModelPaths ?? {},
      builds: compiled.builds,
   };
}

/** A SQL text as a short, stable fingerprint, for JSON goldens that point at the text golden. */
export function sqlFingerprint(normalizer: Normalizer, sql: unknown): unknown {
   if (typeof sql !== "string") return sql;
   return `sha256:${new Bun.CryptoHasher("sha256")
      .update(normalizer.text(sql))
      .digest("hex")
      .slice(0, 16)}`;
}
