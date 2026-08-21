// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { components } from "../api";
import { BadRequestError } from "../errors";
import {
   BuildInstruction,
   LedgerEntry,
   ManifestReference,
} from "../storage/DatabaseInterface";
import { MaterializationService } from "../service/materialization_service";
import { queryMetadataViolations } from "../service/query_metadata";

type RunContext = components["schemas"]["RunContext"];

const RUN_TRIGGERS = ["publish", "on_demand", "scheduler"] as const;

export class MaterializationController {
   constructor(private materializationService: MaterializationService) {}

   async createMaterialization(
      environmentName: string,
      packageName: string,
      body: Record<string, unknown>,
   ) {
      return this.materializationService.createMaterialization(
         environmentName,
         packageName,
         this.validateCreateBody(body),
      );
   }

   // Whitelist: only these fields are read off the client body. `trigger` is
   // deliberately NOT accepted here — it is service-level-only, defaulted to
   // `ON_DEMAND` and set to `SCHEDULER` solely by the in-process scheduler
   // (which calls the service directly, not this HTTP path). Do not add
   // `trigger` to this parser, or an API caller could forge a scheduled run.
   private validateCreateBody(body: Record<string, unknown>): {
      forceRefresh?: boolean;
      reseed?: boolean;
      sourceNames?: string[];
      buildInstructions?: BuildInstruction[];
      referenceManifest?: ManifestReference[];
      strictUpstreams?: boolean;
      ledger?: LedgerEntry[];
      runContext?: RunContext;
   } {
      const result: {
         forceRefresh?: boolean;
         reseed?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
         ledger?: LedgerEntry[];
         runContext?: RunContext;
      } = {};
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
         if (parsed.ledger !== undefined) {
            result.ledger = parsed.ledger;
         }
      }
      if (body.forceRefresh !== undefined) {
         if (typeof body.forceRefresh !== "boolean") {
            throw new BadRequestError("forceRefresh must be a boolean");
         }
         result.forceRefresh = body.forceRefresh;
      }
      // The run-level ask for a full rebuild of the in-scope incremental sources,
      // and a SEPARATE flag from forceRefresh on purpose: that one only defeats
      // skip-if-unchanged, and conflating the two would make every scheduled fire
      // (which forces on every tick) a full rebuild.
      if (body.reseed !== undefined) {
         if (typeof body.reseed !== "boolean") {
            throw new BadRequestError("reseed must be a boolean");
         }
         result.reseed = body.reseed;
      }
      if (body.sourceNames !== undefined) {
         if (
            !Array.isArray(body.sourceNames) ||
            body.sourceNames.some((n) => typeof n !== "string")
         ) {
            throw new BadRequestError(
               "sourceNames must be an array of strings",
            );
         }
         result.sourceNames = body.sourceNames as string[];
      }
      return result;
   }

   /**
    * Validate `runContext`, the caller's observability context for one run.
    * `trigger` is a closed enum here even though it feeds a metadata property:
    * the whole point is that a reader can group runs by how they started, which a
    * free-form value would quietly break.
    *
    * Note this is NOT the service-level `trigger` the parser above deliberately
    * refuses. That one decides whether the run counts as scheduled; this one only
    * labels the statements the run issues, so accepting `publish` from a caller
    * forges nothing.
    */
   private validateRunContext(raw: unknown): RunContext {
      if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
         throw new BadRequestError("runContext must be an object");
      }
      const obj = raw as Record<string, unknown>;
      const context: RunContext = {};
      if (obj.trigger !== undefined && obj.trigger !== null) {
         if (
            typeof obj.trigger !== "string" ||
            !RUN_TRIGGERS.includes(obj.trigger as (typeof RUN_TRIGGERS)[number])
         ) {
            throw new BadRequestError(
               `runContext.trigger must be one of ${RUN_TRIGGERS.join(" | ")}`,
            );
         }
         context.trigger = obj.trigger as RunContext["trigger"];
      }
      if (obj.runId !== undefined && obj.runId !== null) {
         if (typeof obj.runId !== "string") {
            throw new BadRequestError("runContext.runId must be a string");
         }
         // Held to the metadata contract like any other caller-supplied
         // property: it becomes the `run_id` on every statement of the build,
         // and a value the contract rejects would otherwise be truncated and
         // rewritten in silence — leaving the caller with an id it cannot join
         // on and no way to know why.
         const violations = queryMetadataViolations({ run_id: obj.runId });
         if (violations.length > 0) {
            throw new BadRequestError(
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
      ledger?: LedgerEntry[];
   } {
      if (typeof raw !== "object" || raw === null) {
         throw new BadRequestError("buildInstructions must be an object");
      }
      const obj = raw as Record<string, unknown>;
      const sources = obj.sources;
      if (!Array.isArray(sources) || sources.length === 0) {
         throw new BadRequestError(
            "buildInstructions requires a non-empty 'sources' array of BuildInstruction",
         );
      }
      const result: {
         sources: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
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
            throw new BadRequestError(
               "buildInstructions.referenceManifest must be an array of ManifestReference",
            );
         }
         result.referenceManifest = obj.referenceManifest.map((ref) =>
            this.validateManifestReference(ref),
         );
      }
      if (obj.strictUpstreams !== undefined) {
         if (typeof obj.strictUpstreams !== "boolean") {
            throw new BadRequestError(
               "buildInstructions.strictUpstreams must be a boolean",
            );
         }
         result.strictUpstreams = obj.strictUpstreams;
      }
      // Null is read as absent ("use the local store"), not as an empty ledger:
      // the two mean opposite things, and a caller that means "I own the ledger
      // and it is empty" says so with `[]`.
      if (obj.ledger !== undefined && obj.ledger !== null) {
         if (!Array.isArray(obj.ledger)) {
            throw new BadRequestError(
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
         throw new BadRequestError("Each manifest reference must be an object");
      }
      const ref = raw as Record<string, unknown>;
      for (const field of ["sourceEntityId", "physicalTableName"] as const) {
         if (typeof ref[field] !== "string") {
            throw new BadRequestError(
               `Manifest reference is missing required string field '${field}'`,
            );
         }
      }
      return {
         sourceEntityId: ref.sourceEntityId as string,
         physicalTableName: ref.physicalTableName as string,
         // The connection the upstream table lives on. Optional; carried through
         // so the seed loop can dialect-quote the reference for a case-folding
         // engine (#904). Dropping it silently reverts to an unquoted seed, which
         // fails a downstream build on such an engine — the same manual-copy drift
         // that dropped BuildInstruction.destination.
         ...(typeof ref.connectionName === "string"
            ? { connectionName: ref.connectionName }
            : {}),
      };
   }

   private validateInstruction(raw: unknown): BuildInstruction {
      if (typeof raw !== "object" || raw === null) {
         throw new BadRequestError("Each build instruction must be an object");
      }
      const instruction = raw as Record<string, unknown>;
      const required = [
         "sourceEntityId",
         "materializedTableId",
         "physicalTableName",
         "realization",
      ] as const;
      for (const field of required) {
         if (typeof instruction[field] !== "string") {
            throw new BadRequestError(
               `Build instruction is missing required string field '${field}'`,
            );
         }
      }
      if (
         instruction.realization !== "COPY" &&
         instruction.realization !== "SNAPSHOT"
      ) {
         throw new BadRequestError(
            "Build instruction 'realization' must be COPY or SNAPSHOT",
         );
      }
      if (
         instruction.reseed !== undefined &&
         typeof instruction.reseed !== "boolean"
      ) {
         throw new BadRequestError(
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
         realization: instruction.realization,
         // The `storage=` destination the orchestrator targets. Optional in the
         // schema; must be carried through, or an orchestrated build silently
         // falls back to a colocated build (isStorageBuild=false) and
         // never materializes into the storage destination.
         ...(typeof instruction.destination === "string"
            ? { destination: instruction.destination }
            : {}),
         // The per-source ask for a full rebuild, OR-ed with the request-level
         // `reseed`. Must be carried through for the same reason as `destination`
         // above: dropping it here would leave a host unable to rebuild one source
         // without rebuilding all of them.
         ...(typeof instruction.reseed === "boolean"
            ? { reseed: instruction.reseed }
            : {}),
      };
   }

   /**
    * Validate one `buildInstructions.ledger` entry's SHAPE. Every field is an
    * echo of a `ManifestEntry.ledger` the publisher reported, so nothing here
    * is interpreted — only checked for being present and of the right type.
    * What the entry claims (which table, which source definition) is validated
    * against the package's build plan by the service, at create time.
    */
   private validateLedgerEntry(raw: unknown): LedgerEntry {
      if (typeof raw !== "object" || raw === null) {
         throw new BadRequestError(
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
            throw new BadRequestError(
               `Ledger entry '${field}' must be a non-empty string`,
            );
         }
      }
      if (entry.strategy !== "merge" && entry.strategy !== "range_replace") {
         throw new BadRequestError(
            "Ledger entry 'strategy' must be 'merge' or 'range_replace'",
         );
      }
      if (
         entry.mergeKeys !== undefined &&
         (!Array.isArray(entry.mergeKeys) ||
            entry.mergeKeys.some((k) => typeof k !== "string"))
      ) {
         throw new BadRequestError(
            "Ledger entry 'mergeKeys' must be an array of strings",
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
         // Absent and empty mean the same thing — a keyless (range-replace)
         // source — so an absent list is not normalized into one here; the
         // comparison reads both as no merge keys.
         ...(Array.isArray(entry.mergeKeys)
            ? { mergeKeys: entry.mergeKeys as string[] }
            : {}),
      };
   }

   async stopMaterialization(
      environmentName: string,
      packageName: string,
      materializationId: string,
   ) {
      return this.materializationService.stopMaterialization(
         environmentName,
         packageName,
         materializationId,
      );
   }

   async listMaterializations(
      environmentName: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ) {
      return this.materializationService.listMaterializations(
         environmentName,
         packageName,
         options,
      );
   }

   async listEnvironmentMaterializations(
      environmentName: string,
      options?: { limit?: number; offset?: number },
   ) {
      return this.materializationService.listEnvironmentMaterializations(
         environmentName,
         options,
      );
   }

   async getMaterialization(
      environmentName: string,
      packageName: string,
      materializationId: string,
   ) {
      return this.materializationService.getMaterialization(
         environmentName,
         packageName,
         materializationId,
      );
   }

   async deleteMaterialization(
      environmentName: string,
      packageName: string,
      materializationId: string,
      options: { dropTables?: boolean } = {},
   ) {
      return this.materializationService.deleteMaterialization(
         environmentName,
         packageName,
         materializationId,
         options,
      );
   }
}
