import { BadRequestError } from "../errors";
import { BuildInstruction } from "../storage/DatabaseInterface";
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
   } {
      const result: { forceRefresh?: boolean; sourceNames?: string[] } = {};
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

   async buildMaterialization(
      environmentName: string,
      packageName: string,
      materializationId: string,
      body: Record<string, unknown>,
   ) {
      return this.materializationService.buildMaterialization(
         environmentName,
         packageName,
         materializationId,
         this.validateBuildBody(body),
      );
   }

   private validateBuildBody(
      body: Record<string, unknown>,
   ): BuildInstruction[] {
      const sources = body.sources;
      if (!Array.isArray(sources) || sources.length === 0) {
         throw new BadRequestError(
            "build requires a non-empty 'sources' array of BuildInstruction",
         );
      }
      return sources.map((raw) => this.validateInstruction(raw));
   }

   private validateInstruction(raw: unknown): BuildInstruction {
      if (typeof raw !== "object" || raw === null) {
         throw new BadRequestError("Each build instruction must be an object");
      }
      const instruction = raw as Record<string, unknown>;
      const required = [
         "buildId",
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
         buildId: instruction.buildId as string,
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
   ) {
      return this.materializationService.deleteMaterialization(
         environmentName,
         packageName,
         materializationId,
      );
   }
}
