// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../logger";
import type { components } from "../api";
import { BuildInstructionsInvalidError, PackageVersionError } from "../errors";
import { Materialization } from "../storage/DatabaseInterface";
import {
   BuildInstruction,
   LedgerEntry,
   MalformedLedgerEntry,
   ManifestReference,
} from "../service/materialization/domain/types";
import { MaterializationService } from "../service/materialization/run/materialization_service";
import { VersionRef } from "../service/identity";
import { isUsableTableName } from "../service/materialization";
import { queryMetadataViolations } from "../service/query_metadata";

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
            this.validateCreateBody(body, { environmentName, packageName }),
         ),
      );
   }

   // Whitelist of client body fields. Never accept `trigger` here: only the
   // in-process scheduler (which calls the service directly) sets `scheduler`,
   // and accepting it would let an API caller forge a scheduled run.
   private validateCreateBody(
      body: Record<string, unknown>,
      context: { environmentName: string; packageName: string },
   ): {
      forceRefresh?: boolean;
      reseed?: boolean;
      sourceNames?: string[];
      buildInstructions?: BuildInstruction[];
      referenceManifest?: ManifestReference[];
      ledger?: LedgerEntry[];
      malformedLedger?: MalformedLedgerEntry[];
      runContext?: RunContext;
      versionId?: VersionRef;
   } {
      const result: {
         forceRefresh?: boolean;
         reseed?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         ledger?: LedgerEntry[];
         malformedLedger?: MalformedLedgerEntry[];
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
         const parsed = this.validateBuildInstructions(
            body.buildInstructions,
            context,
         );
         result.buildInstructions = parsed.sources;
         if (parsed.referenceManifest !== undefined) {
            result.referenceManifest = parsed.referenceManifest;
         }
         const deprecated = parsed.deprecatedOptions;
         if (deprecated.length > 0) {
            // Once per request that sends it; it does not change the run.
            logger.warn(
               "buildInstructions options are deprecated and ignored: every build is strict",
               { ...context, options: deprecated },
            );
         }
         if (parsed.ledger !== undefined) {
            result.ledger = parsed.ledger;
         }
         if (parsed.malformedLedger?.length) {
            result.malformedLedger = parsed.malformedLedger;
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
   private validateBuildInstructions(
      raw: unknown,
      context: { environmentName: string; packageName: string },
   ): {
      sources: BuildInstruction[];
      referenceManifest?: ManifestReference[];
      /** The deprecated options the payload sent, which are ignored. */
      deprecatedOptions: "strictUpstreams"[];
      ledger?: LedgerEntry[];
      malformedLedger?: MalformedLedgerEntry[];
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
         deprecatedOptions: "strictUpstreams"[];
         ledger?: LedgerEntry[];
         malformedLedger?: MalformedLedgerEntry[];
      } = {
         sources: sources.map((instruction) =>
            this.validateInstruction(instruction),
         ),
         deprecatedOptions: [],
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
            this.validateManifestReference(ref, context),
         );
      }
      if (obj.strictUpstreams !== undefined) {
         if (typeof obj.strictUpstreams !== "boolean") {
            throw new BuildInstructionsInvalidError(
               "buildInstructions.strictUpstreams must be a boolean",
            );
         }
         result.deprecatedOptions.push("strictUpstreams");
      }
      // Removed, and refused by name so a caller learns it is gone. Null is
      // read as absent: Credible's clients drop null fields, so null and
      // absent must mean the same.
      if (obj.warehouseReach !== undefined && obj.warehouseReach !== null) {
         throw new BuildInstructionsInvalidError(
            "buildInstructions.warehouseReach was removed: every build is " +
               "strict, and none recomputes a stored upstream from the " +
               "warehouse, so there is nothing for it to choose",
         );
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
         // A malformed entry is dropped, so its source seeds: hosts courier
         // entries whole, and one corrupted entry must not wedge every build
         // of the package. Errors about the REQUEST (duplicates, a table not
         // instructed, a moved definition) stay 400s in validateInstructions.
         const ledger: LedgerEntry[] = [];
         const malformed: MalformedLedgerEntry[] = [];
         for (const entry of obj.ledger) {
            const checked = this.validateLedgerEntry(entry);
            if ("defect" in checked) {
               logger.warn(
                  "Dropped a malformed buildInstructions.ledger entry; its source seeds",
                  {
                     ...context,
                     table: checked.table,
                     defect: checked.defect,
                     reasonCode: "ledger_entry_malformed",
                  },
               );
               // Its source seeds under its own reason, told apart from a
               // source the caller sent no entry for.
               if (checked.table !== undefined) {
                  malformed.push({
                     physicalTableName: checked.table,
                     defect: checked.defect,
                  });
               }
               continue;
            }
            ledger.push(checked.entry);
         }
         result.ledger = ledger;
         if (malformed.length > 0) result.malformedLedger = malformed;
      }
      return result;
   }

   private validateManifestReference(
      raw: unknown,
      context: { environmentName: string; packageName: string },
   ): ManifestReference {
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
         // Where a stored upstream's table lives. Without it a reference to
         // a stored table reads as a warehouse table, and a chained
         // `storage=` build over it refuses or recomputes it. The service
         // checks it names a configured destination.
         ...(referenceDestination(ref.storageDestinationName) !== undefined
            ? {
                 storageDestinationName: referenceDestination(
                    ref.storageDestinationName,
                 ),
              }
            : {}),
         // An empty list is no types, as an absent one is.
         ...(ref.nativeSchema !== undefined &&
         validReferenceSchema(ref.nativeSchema).length > 0
            ? { nativeSchema: validReferenceSchema(ref.nativeSchema) }
            : {}),
         // A stored upstream's columns, as its build reported them.
         ...(ref.schema !== undefined &&
         validReferenceSchema(ref.schema, "schema").length > 0
            ? { schema: validReferenceSchema(ref.schema, "schema") }
            : {}),
         ...(ref.dataAsOf !== undefined
            ? { dataAsOf: referenceInstant(ref.dataAsOf, "dataAsOf") }
            : {}),
         ...(ref.builtAt !== undefined
            ? { builtAt: referenceInstant(ref.builtAt, "builtAt") }
            : {}),
         // Carried unparsed, as a host stores it: a contract that does not
         // read is none (domain/column_contract.ts).
         ...(ref.contract !== undefined && ref.contract !== null
            ? { contract: ref.contract as ManifestReference["contract"] }
            : {}),
         // The upstream's ledger entry, for an incremental extension over it
         // (incremental_upstream.ts). Null reads as absent.
         ...this.validateReferenceLedger(ref, context),
      };
   }

   /**
    * A reference's `ledger`, checked as an echoed ledger entry is
    * ({@link validateLedgerEntry}) and held to the reference's own table and
    * source: the same `sourceEntityId`, so a host whose physical names are
    * stable across versions cannot carry the ledger of the parent's previous
    * definition. A malformed or mismatched one is dropped with a warning,
    * never a 400, as if absent: the extensions over that upstream read the
    * bound manifest's or this worker's entry instead, and reseed only when
    * neither proves which build they read over (incremental_upstream.ts).
    * Returned whole, keys this release does not know included.
    */
   private validateReferenceLedger(
      ref: Record<string, unknown>,
      context: { environmentName: string; packageName: string },
   ): { ledger?: LedgerEntry } {
      if (ref.ledger === undefined || ref.ledger === null) return {};
      const checked = this.validateLedgerEntry(ref.ledger);
      let defect: string;
      if ("defect" in checked) {
         defect = checked.defect;
      } else if (
         checked.entry.connectionName !== ref.connectionName ||
         checked.entry.physicalTableName !== ref.physicalTableName ||
         checked.entry.storageDestinationName !==
            referenceDestination(ref.storageDestinationName)
      ) {
         defect =
            "it names another table than the reference (connectionName, " +
            "physicalTableName, storageDestinationName)";
      } else if (checked.entry.sourceEntityId !== ref.sourceEntityId) {
         defect =
            "it names another source than the reference (sourceEntityId): " +
            "an entry from another definition of the upstream";
      } else {
         return { ledger: checked.entry };
      }
      logger.warn(
         "Dropped a referenceManifest entry's ledger; extensions over it fall back to the bound or local ledger",
         {
            ...context,
            table: ref.physicalTableName,
            defect,
            reasonCode: "reference_ledger_dropped",
         },
      );
      return {};
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
    * Check one `buildInstructions.ledger` entry's SHAPE only. Each echoes a
    * `ManifestEntry.ledger` the publisher reported; what it claims is checked
    * against the build plan by the service at create time.
    *
    * The entry is opaque to hosts and the publisher may add keys to it, so it
    * is returned whole: keys this release does not know pass through untouched.
    * A malformed entry is returned as its defect (and the table, when it names
    * one) for the caller to drop, not thrown.
    */
   private validateLedgerEntry(
      raw: unknown,
   ): { entry: LedgerEntry } | { defect: string; table?: string } {
      if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
         return { defect: "the entry is not an object" };
      }
      const entry = raw as Record<string, unknown>;
      const table =
         typeof entry.physicalTableName === "string"
            ? entry.physicalTableName
            : undefined;
      const malformed = (defect: string) => ({
         defect,
         ...(table !== undefined ? { table } : {}),
      });
      for (const field of [
         "connectionName",
         "physicalTableName",
         "coveredThrough",
         "coveredThroughType",
         "watermark",
         "sourceEntityId",
      ] as const) {
         if (typeof entry[field] !== "string" || entry[field] === "") {
            return malformed(`'${field}' must be a non-empty string`);
         }
      }
      try {
         assertUsableTableName(
            entry.physicalTableName as string,
            "Ledger entry",
         );
      } catch (err) {
         return malformed(err instanceof Error ? err.message : String(err));
      }
      if (entry.strategy !== "merge" && entry.strategy !== "range_replace") {
         return malformed("'strategy' must be 'merge' or 'range_replace'");
      }
      if (
         entry.mergeKeys !== undefined &&
         (!Array.isArray(entry.mergeKeys) ||
            entry.mergeKeys.some((k) => typeof k !== "string"))
      ) {
         return malformed("'mergeKeys' must be an array of strings");
      }
      if (
         entry.storageDestinationName !== undefined &&
         (typeof entry.storageDestinationName !== "string" ||
            entry.storageDestinationName === "")
      ) {
         return malformed(
            "'storageDestinationName' must be a non-empty string when present",
         );
      }
      // Absent and empty `mergeKeys` both mean a keyless (range-replace)
      // source; absent `storageDestinationName` means database. Both stay as
      // sent, like every other key.
      return { entry: { ...entry } as LedgerEntry };
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

/**
 * A reference's `nativeSchema`: the columns its upstream's build reported,
 * each a name and a type, both non-empty strings. Anything else is refused
 * (400), so a malformed list never reads as "no types recorded".
 */
function validReferenceSchema(
   raw: unknown,
   field: "nativeSchema" | "schema" = "nativeSchema",
): { name: string; type: string }[] {
   if (
      !Array.isArray(raw) ||
      !raw.every(
         (c) =>
            typeof c === "object" &&
            c !== null &&
            typeof (c as { name?: unknown }).name === "string" &&
            (c as { name: string }).name !== "" &&
            typeof (c as { type?: unknown }).type === "string" &&
            (c as { type: string }).type !== "",
      )
   ) {
      throw new BuildInstructionsInvalidError(
         `Manifest reference '${field}' must be an array of { name, type } objects with non-empty string fields`,
      );
   }
   return raw.map((c: { name: string; type: string }) => ({
      name: c.name,
      type: c.type,
   }));
}

/**
 * A reference's instant (`dataAsOf`, `builtAt`): a string `Date` reads as
 * an instant, refused otherwise (400), so a malformed one never reads as
 * "of unknown age".
 */
function referenceInstant(raw: unknown, field: string): string {
   if (typeof raw !== "string" || Number.isNaN(Date.parse(raw))) {
      throw new BuildInstructionsInvalidError(
         `Manifest reference '${field}' must be a date-time string when present`,
      );
   }
   return raw;
}

/**
 * A reference's `storageDestinationName`: absent, or a non-empty string
 * (refused otherwise, 400). Whether it names a configured destination is
 * the service's check, which holds the environment.
 */
function referenceDestination(raw: unknown): string | undefined {
   if (raw === undefined) return undefined;
   if (typeof raw !== "string" || raw === "") {
      throw new BuildInstructionsInvalidError(
         "Manifest reference 'storageDestinationName' must be a non-empty string when present",
      );
   }
   return raw;
}
