import { BadRequestError } from "../errors";
import { MaterializationService } from "../service/materialization_service";

export class MaterializationController {
   constructor(private materializationService: MaterializationService) {}

   async createMaterialization(
      projectName: string,
      packageName: string,
      body: Record<string, unknown>,
   ) {
      const options = this.validateCreateBody(body);
      return this.materializationService.createMaterialization(
         projectName,
         packageName,
         options,
      );
   }

   private validateCreateBody(body: Record<string, unknown>): {
      forceRefresh?: boolean;
      autoLoadManifest?: boolean;
   } {
      const result: { forceRefresh?: boolean; autoLoadManifest?: boolean } = {};
      if (body.forceRefresh !== undefined) {
         if (typeof body.forceRefresh !== "boolean") {
            throw new BadRequestError("forceRefresh must be a boolean");
         }
         result.forceRefresh = body.forceRefresh;
      }
      if (body.autoLoadManifest !== undefined) {
         if (typeof body.autoLoadManifest !== "boolean") {
            throw new BadRequestError("autoLoadManifest must be a boolean");
         }
         result.autoLoadManifest = body.autoLoadManifest;
      }
      return result;
   }

   async startMaterialization(
      projectName: string,
      packageName: string,
      materializationId: string,
   ) {
      return this.materializationService.startMaterialization(
         projectName,
         packageName,
         materializationId,
      );
   }

   async stopMaterialization(
      projectName: string,
      packageName: string,
      materializationId: string,
   ) {
      return this.materializationService.stopMaterialization(
         projectName,
         packageName,
         materializationId,
      );
   }

   async listMaterializations(
      projectName: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ) {
      return this.materializationService.listMaterializations(
         projectName,
         packageName,
         options,
      );
   }

   async getMaterialization(
      projectName: string,
      packageName: string,
      materializationId: string,
   ) {
      return this.materializationService.getMaterialization(
         projectName,
         packageName,
         materializationId,
      );
   }

   async deleteMaterialization(
      projectName: string,
      packageName: string,
      materializationId: string,
   ) {
      return this.materializationService.deleteMaterialization(
         projectName,
         packageName,
         materializationId,
      );
   }

   async gcPackage(
      projectName: string,
      packageName: string,
      body: Record<string, unknown>,
   ) {
      const options = this.validateGcBody(body);
      return this.materializationService.gcPackage(
         projectName,
         packageName,
         options,
      );
   }

   private validateGcBody(body: Record<string, unknown>): { dryRun?: boolean } {
      const options: { dryRun?: boolean } = {};
      if (body.dryRun !== undefined) {
         if (typeof body.dryRun !== "boolean") {
            throw new BadRequestError("dryRun must be a boolean");
         }
         options.dryRun = body.dryRun;
      }
      return options;
   }
}
