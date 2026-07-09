import { BadRequestError } from "../errors";
import {
   BuildInstruction,
   ManifestReference,
} from "../storage/DatabaseInterface";
import { MaterializationService } from "../service/materialization_service";

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

   private validateCreateBody(body: Record<string, unknown>): {
      forceRefresh?: boolean;
      sourceNames?: string[];
      buildInstructions?: BuildInstruction[];
      referenceManifest?: ManifestReference[];
      strictUpstreams?: boolean;
   } {
      const result: {
         forceRefresh?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
      } = {};
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
      }
      if (body.forceRefresh !== undefined) {
         if (typeof body.forceRefresh !== "boolean") {
            throw new BadRequestError("forceRefresh must be a boolean");
         }
         result.forceRefresh = body.forceRefresh;
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
    * Validate the orchestrated `buildInstructions` payload (BuildInstructions:
    * `{ sources: BuildInstruction[], referenceManifest?, strictUpstreams? }`)
    * into the parts the service consumes: the flattened instruction list, the
    * optional upstream reference manifest, and the strict flag.
    */
   private validateBuildInstructions(raw: unknown): {
      sources: BuildInstruction[];
      referenceManifest?: ManifestReference[];
      strictUpstreams?: boolean;
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
      return {
         sourceEntityId: instruction.sourceEntityId as string,
         sourceID:
            typeof instruction.sourceID === "string"
               ? instruction.sourceID
               : undefined,
         materializedTableId: instruction.materializedTableId as string,
         physicalTableName: instruction.physicalTableName as string,
         realization: instruction.realization,
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
