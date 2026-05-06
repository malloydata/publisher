import { BadRequestError } from "../errors";
import { MaterializationService } from "../service/materialization_service";

export class MaterializationController {
   constructor(private materializationService: MaterializationService) {}

   async createMaterialization(
      environmentName: string,
      packageName: string,
      body: Record<string, unknown>,
   ) {
      const options = this.validateCreateBody(body);
      return this.materializationService.createMaterialization(
         environmentName,
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
      environmentName: string,
      packageName: string,
      materializationId: string,
   ) {
      return this.materializationService.startMaterialization(
         environmentName,
         packageName,
         materializationId,
      );
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

   async teardownPackage(
      environmentName: string,
      packageName: string,
      body: Record<string, unknown>,
   ) {
      const options = this.validateTeardownBody(body);
      return this.materializationService.teardownPackage(
         environmentName,
         packageName,
         options,
      );
   }

   private validateTeardownBody(body: Record<string, unknown>): {
      dryRun?: boolean;
   } {
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
