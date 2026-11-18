// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { components } from "../api";
import { BuildInstructionsInvalidError, PackageVersionError } from "../errors";
import { Materialization } from "../storage/DatabaseInterface";
import {
   BuildInstruction,
   LedgerEntry,
   ManifestReference,
} from "../service/materialization/domain/types";
import { MaterializationService } from "../service/materialization/run/materialization_service";
import type { WarehouseReach } from "../service/materialization/run/materialization_run_support";
import { VersionRef } from "../service/identity";
import { isUsableTableName } from "../service/materialization";
import { queryMetadataViolations } from "../service/query_metadata";

/** The values `buildInstructions.warehouseReach` accepts (ms2data/service#6756). */
const WAREHOUSE_REACH_VALUES: readonly WarehouseReach[] = [
   "recompute",
   "refuse",
];

type RunContext = components["schemas"]["RunContext"];

const RUN_TRIGGERS = ["publish", "on_demand", "scheduler"] as const;

/**
 * A run as the API answers it. The record keeps the version a run built for
 * the server's own use, but the wire Materialization has no `version` field:
 * the version goes out in `metadata.versionId`. So it is left off here, for
 * every package, with versioning on or off.
 */
function toWire(run: Materialization): Omit<Materialization, "version"> {
   const { version: _version, ...wire } = run;
   return wire;
}

export class MaterializationController {
   constructor(private materializationService: MaterializationService) {}

   async createMaterialization(
      environmentName: string,
      packageName: string,
      body: Record<string, unknown>,
   ) {
      return toWire(
         await this.materializationService.createMaterialization(
            environmentName,
            packageName,
            this.validateCreateBody(body),
         ),
      );
   }

   // Whitelist of client body fields. Never accept `trigger` here: only the
   // in-process scheduler (which calls the service directly) sets `scheduler`,
   // and accepting it would let an API caller forge a scheduled run.
   private validateCreateBody(body: Record<string, unknown>): {
      forceRefresh?: boolean;
      reseed?: boolean;
      sourceNames?: string[];
      buildInstructions?: BuildInstruction[];
      referenceManifest?: ManifestReference[];
      strictUpstreams?: boolean;
      warehouseReach?: WarehouseReach;
      ledger?: LedgerEntry[];
      runContext?: RunContext;
      versionId?: VersionRef;
   } {
      const result: {
         forceRefresh?: boolean;
         reseed?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
         warehouseReach?: WarehouseReach;
         ledger?: LedgerEntry[];
         runContext?: RunContext;
         versionId?: VersionRef;
      } = {};
      // The version to build. Its format, existence and archive state are
      // the service's to check, the way every read checks them; this only
      // refuses a value that is not text at all. Empty or null means latest.
      if (body.versionId !== undefined && body.versionId !== null) {
         if (typeof body.versionId !== "string") {
            throw new PackageVersionError(
               "VERSION_ID_INVALID",
               'versionId must be a string, such as "1.2.0".',
            );
         }
         if (body.versionId !== "") {
            result.versionId = VersionRef.parse(body.versionId);
         }
      }
      if (body.runContext !== undefined && body.runContext !== null) {
         result.runContext = this.validateRunContext(body.runContext);
      }
      if (
         body.buildInstructions !== undefined &&
         body.buildInstructions !== null
      ) {
         const parsed = this.validateBuildInstructions(body.buildInstructions);
         result.buildInstructions = parsed.sources;
         if (parsed.referenceManifest !== undefined) {
            result.referenceManifest = parsed.referenceManifest;
         }
         if (parsed.strictUpstreams !== undefined) {
            result.strictUpstreams = parsed.strictUpstreams;
         }
         if (parsed.warehouseReach !== undefined) {
            result.warehouseReach = parsed.warehouseReach;
         }
         if (parsed.ledger !== undefined) {
            result.ledger = parsed.ledger;
         }
      }
      if (body.forceRefresh !== undefined) {
         if (typeof body.forceRefresh !== "boolean") {
            throw new BuildInstructionsInvalidError(
               "forceRefresh must be a boolean",
            );
         }
         result.forceRefresh = body.forceRefresh;
      }
      // Full rebuild of the in-scope incremental sources. Separate from
      // forceRefresh, which only defeats skip-if-unchanged: conflating them
      // would make every scheduled fire (always forced) a full rebuild.
      if (body.reseed !== undefined) {
         if (typeof body.reseed !== "boolean") {
            throw new BuildInstructionsInvalidError("reseed must be a boolean");
         }
         result.reseed = body.reseed;
      }
      if (body.sourceNames !== undefined) {
         if (
            !Array.isArray(body.sourceNames) ||
            body.sourceNames.some((n) => typeof n !== "string")
         ) {
            throw new BuildInstructionsInvalidError(
               "sourceNames must be an array of strings",
            );
         }
         result.sourceNames = body.sourceNames as string[];
      }
      return result;
   }

   /**
    * Validate `runContext`, the caller's observability context for one run.
    * `trigger` is a closed enum so readers can group runs by how they started.
    * It only labels the run's statements (unlike the service-level `trigger`
    * refused above), so accepting `publish` from a caller forges nothing.
    */
   private validateRunContext(raw: unknown): RunContext {
      if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
         throw new BuildInstructionsInvalidError(
            "runContext must be an object",
         );
      }
      const obj = raw as Record<string, unknown>;
      const context: RunContext = {};
      if (obj.trigger !== undefined && obj.trigger !== null) {
         if (
            typeof obj.trigger !== "string" ||
            !RUN_TRIGGERS.includes(obj.trigger as (typeof RUN_TRIGGERS)[number])
         ) {
            throw new BuildInstructionsInvalidError(
               `runContext.trigger must be one of ${RUN_TRIGGERS.join(" | ")}`,
            );
         }
         context.trigger = obj.trigger as RunContext["trigger"];
      }
      if (obj.runId !== undefined && obj.runId !== null) {
         if (typeof obj.runId !== "string") {
            throw new BuildInstructionsInvalidError(
               "runContext.runId must be a string",
            );
         }
         // Held to the metadata contract: it becomes `run_id` on every
         // statement, and a rejected value would otherwise be silently
         // rewritten, leaving the caller an id it cannot join on.
         const violations = queryMetadataViolations({ run_id: obj.runId });
         if (violations.length > 0) {
            throw new BuildInstructionsInvalidError(
               `runContext.runId is attached to every statement as the run_id ` +
                  `property: ${violations.join("; ")}`,
            );
         }
         context.runId = obj.runId;
      }
      return context;
   }

   /**
    * Validate the orchestrated `buildInstructions` payload (BuildInstructions:
    * `{ sources: BuildInstruction[], referenceManifest?, strictUpstreams? }`)
    * into the parts the service consumes: the flattened instruction list, the
    * optional upstream reference manifest, and the strict flag.
    */
   private validateBuildInstructions(raw: unknown): {
      sources: BuildInstruction[];
      referenceManifest?: ManifestReference[];
      strictUpstreams?: boolean;
      warehouseReach?: WarehouseReach;
      ledger?: LedgerEntry[];
   } {
      if (typeof raw !== "object" || raw === null) {
         throw new BuildInstructionsInvalidError(
            "buildInstructions must be an object",
         );
      }
      const obj = raw as Record<string, unknown>;
      const sources = obj.sources;
      if (!Array.isArray(sources) || sources.length === 0) {
         throw new BuildInstructionsInvalidError(
            "buildInstructions requires a non-empty 'sources' array of BuildInstruction",
         );
      }
      const result: {
         sources: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
         warehouseReach?: WarehouseReach;
         ledger?: LedgerEntry[];
      } = {
         sources: sources.map((instruction) =>
            this.validateInstruction(instruction),
         ),
      };
      if (
         obj.referenceManifest !== undefined &&
         obj.referenceManifest !== null
      ) {
         if (!Array.isArray(obj.referenceManifest)) {
            throw new BuildInstructionsInvalidError(
               "buildInstructions.referenceManifest must be an array of ManifestReference",
            );
         }
         result.referenceManifest = obj.referenceManifest.map((ref) =>
            this.validateManifestReference(ref),
         );
      }
      if (obj.strictUpstreams !== undefined) {
         if (typeof obj.strictUpstreams !== "boolean") {
            throw new BuildInstructionsInvalidError(
               "buildInstructions.strictUpstreams must be a boolean",
            );
         }
         result.strictUpstreams = obj.strictUpstreams;
      }
      // Null is read as absent (the default, `recompute`): Credible's clients
      // drop null fields, so null and absent must mean the same.
      if (obj.warehouseReach !== undefined && obj.warehouseReach !== null) {
         if (
            typeof obj.warehouseReach !== "string" ||
            !WAREHOUSE_REACH_VALUES.includes(
               obj.warehouseReach as WarehouseReach,
            )
         ) {
            throw new BuildInstructionsInvalidError(
               `buildInstructions.warehouseReach must be one of ${WAREHOUSE_REACH_VALUES.join(" | ")}`,
            );
         }
         result.warehouseReach = obj.warehouseReach as WarehouseReach;
      }
      // Null is read as absent ("use the local store"), not as an empty ledger:
      // the two mean opposite things, and a caller that means "I own the ledger
      // and it is empty" says so with `[]`.
      if (obj.ledger !== undefined && obj.ledger !== null) {
         if (!Array.isArray(obj.ledger)) {
            throw new BuildInstructionsInvalidError(
               "buildInstructions.ledger must be an array",
            );
         }
         result.ledger = obj.ledger.map((entry) =>
            this.validateLedgerEntry(entry),
         );
      }
      return result;
   }

   private validateManifestReference(raw: unknown): ManifestReference {
      if (typeof raw !== "object" || raw === null) {
         throw new BuildInstructionsInvalidError(
            "Each manifest reference must be an object",
         );
      }
      const ref = raw as Record<string, unknown>;
      for (const field of ["sourceEntityId", "physicalTableName"] as const) {
         if (typeof ref[field] !== "string") {
            throw new BuildInstructionsInvalidError(
               `Manifest reference is missing required string field '${field}'`,
            );
         }
      }
      assertUsableTableName(
         ref.physicalTableName as string,
         "Manifest reference",
      );
      return {
         sourceEntityId: ref.sourceEntityId as string,
         physicalTableName: ref.physicalTableName as string,
         // The upstream table's connection. Must be carried through so the seed
         // loop can dialect-quote the reference; an unquoted seed fails a
         // downstream build on a case-folding engine.
         ...(typeof ref.connectionName === "string"
            ? { connectionName: ref.connectionName }
            : {}),
      };
   }

   private validateInstruction(raw: unknown): BuildInstruction {
      if (typeof raw !== "object" || raw === null) {
         throw new BuildInstructionsInvalidError(
            "Each build instruction must be an object",
         );
      }
      const instruction = raw as Record<string, unknown>;
      const required = [
         "sourceEntityId",
         "materializedTableId",
         "physicalTableName",
      ] as const;
      for (const field of required) {
         if (typeof instruction[field] !== "string") {
            throw new BuildInstructionsInvalidError(
               `Build instruction is missing required string field '${field}'`,
            );
         }
      }
      assertUsableTableName(
         instruction.physicalTableName as string,
         "Build instruction",
      );
      // Absent means COPY, the only realization built (SNAPSHOT is refused
      // when the run reads its instructions).
      if (
         instruction.realization !== undefined &&
         instruction.realization !== "COPY" &&
         instruction.realization !== "SNAPSHOT"
      ) {
         throw new BuildInstructionsInvalidError(
            "Build instruction 'realization' must be COPY or SNAPSHOT",
         );
      }
      if (
         instruction.reseed !== undefined &&
         typeof instruction.reseed !== "boolean"
      ) {
         throw new BuildInstructionsInvalidError(
            "Build instruction 'reseed' must be a boolean",
         );
      }
      return {
         sourceEntityId: instruction.sourceEntityId as string,
         sourceID:
            typeof instruction.sourceID === "string"
               ? instruction.sourceID
               : undefined,
         materializedTableId: instruction.materializedTableId as string,
         physicalTableName: instruction.physicalTableName as string,
         ...(instruction.realization !== undefined
            ? { realization: instruction.realization }
            : {}),
         // The `storage=` destination. Must be carried through, or an
         // orchestrated build silently falls back to a database build.
         ...(typeof instruction.destination === "string"
            ? { destination: instruction.destination }
            : {}),
         // Per-source full rebuild, OR-ed with the request-level `reseed`;
         // without it a host cannot rebuild one source without all of them.
         ...(typeof instruction.reseed === "boolean"
            ? { reseed: instruction.reseed }
            : {}),
      };
   }

   /**
    * Validate one `buildInstructions.ledger` entry's SHAPE only. Each echoes a
    * `ManifestEntry.ledger` the publisher reported; what it claims is checked
    * against the build plan by the service at create time.
    */
   private validateLedgerEntry(raw: unknown): LedgerEntry {
      if (typeof raw !== "object" || raw === null) {
         throw new BuildInstructionsInvalidError(
            "Each buildInstructions.ledger entry must be an object",
         );
      }
      const entry = raw as Record<string, unknown>;
      for (const field of [
         "connectionName",
         "physicalTableName",
         "coveredThrough",
         "coveredThroughType",
         "watermark",
         "sourceEntityId",
      ] as const) {
         if (typeof entry[field] !== "string" || entry[field] === "") {
            throw new BuildInstructionsInvalidError(
               `Ledger entry '${field}' must be a non-empty string`,
            );
         }
      }
      assertUsableTableName(entry.physicalTableName as string, "Ledger entry");
      if (entry.strategy !== "merge" && entry.strategy !== "range_replace") {
         throw new BuildInstructionsInvalidError(
            "Ledger entry 'strategy' must be 'merge' or 'range_replace'",
         );
      }
      if (
         entry.mergeKeys !== undefined &&
         (!Array.isArray(entry.mergeKeys) ||
            entry.mergeKeys.some((k) => typeof k !== "string"))
      ) {
         throw new BuildInstructionsInvalidError(
            "Ledger entry 'mergeKeys' must be an array of strings",
         );
      }
      if (
         entry.storageDestinationName !== undefined &&
         (typeof entry.storageDestinationName !== "string" ||
            entry.storageDestinationName === "")
      ) {
         throw new BuildInstructionsInvalidError(
            "Ledger entry 'storageDestinationName' must be a non-empty string when present",
         );
      }
      return {
         connectionName: entry.connectionName as string,
         physicalTableName: entry.physicalTableName as string,
         coveredThrough: entry.coveredThrough as string,
         coveredThroughType: entry.coveredThroughType as string,
         watermark: entry.watermark as string,
         strategy: entry.strategy,
         sourceEntityId: entry.sourceEntityId as string,
         // Absent and empty both mean a keyless (range-replace) source.
         ...(Array.isArray(entry.mergeKeys)
            ? { mergeKeys: entry.mergeKeys as string[] }
            : {}),
         // Absent means database. Part of the table identity: an entry indexed
         // without its destination is never found by its source, which then
         // rebuilds in full on every refresh instead of advancing.
         ...(typeof entry.storageDestinationName === "string"
            ? { storageDestinationName: entry.storageDestinationName }
            : {}),
      };
   }

   async stopMaterialization(
      environmentName: string,
      packageName: string,
      materializationId: string,
      versionId: VersionRef = VersionRef.LATEST,
   ) {
      return toWire(
         await this.materializationService.stopMaterialization(
            environmentName,
            packageName,
            materializationId,
            versionId,
         ),
      );
   }

   async listMaterializations(
      environmentName: string,
      packageName: string,
      options?: { limit?: number; offset?: number; versionId?: VersionRef },
   ) {
      return (
         await this.materializationService.listMaterializations(
            environmentName,
            packageName,
            options,
         )
      ).map(toWire);
   }

   async getMaterialization(
      environmentName: string,
      packageName: string,
      materializationId: string,
      versionId: VersionRef = VersionRef.LATEST,
   ) {
      return toWire(
         await this.materializationService.getMaterialization(
            environmentName,
            packageName,
            materializationId,
            versionId,
         ),
      );
   }

   /**
    * `drop-materialized-table`: drop one database-tier table a run of the
    * package built on one of its connections, when Publisher can prove it
    * is the package's and unused (MaterializationService.dropMaterializedTable).
    * Answers nothing: 204 whether the table was dropped or was already gone.
    */
   async dropMaterializedTable(
      environmentName: string,
      packageName: string,
      connectionName: string,
      tableName: string,
      options: { materializationId?: string; versionId?: VersionRef } = {},
   ): Promise<void> {
      await this.materializationService.dropMaterializedTable(
         environmentName,
         packageName,
         connectionName,
         tableName,
         options,
      );
   }

   async deleteMaterialization(
      environmentName: string,
      packageName: string,
      materializationId: string,
      options: { versionId?: VersionRef } = {},
   ) {
      return this.materializationService.deleteMaterialization(
         environmentName,
         packageName,
         materializationId,
         options,
      );
   }
}

/**
 * Refuse a host-supplied table name this server would not put in SQL, the
 * same rule a bound manifest's names are held to (isUsableTableName), with
 * a 400 that names the field.
 */
function assertUsableTableName(name: string, what: string): void {
   if (!isUsableTableName(name)) {
      throw new BuildInstructionsInvalidError(
         `${what} 'physicalTableName' must be a table path with no empty part, no backslash or control character, and only whole quoted parts`,
      );
   }
}
