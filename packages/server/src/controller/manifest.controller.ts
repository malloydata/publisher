import { EnvironmentStore } from "../service/environment_store";
import { ManifestService } from "../service/manifest_service";
import { resolveEnvironmentId } from "../service/resolve_environment";

export class ManifestController {
   constructor(
      private environmentStore: EnvironmentStore,
      private manifestService: ManifestService,
   ) {}

   async getManifest(environmentName: string, packageName: string) {
      const repository = this.environmentStore.storageManager.getRepository();
      const environmentId = await resolveEnvironmentId(
         repository,
         environmentName,
         this.environmentStore,
      );
      // Verify the package exists so we return 404 instead of an empty manifest.
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      await environment.getPackage(packageName, false);
      return this.manifestService.getManifest(environmentId, packageName);
   }

   async reloadManifest(environmentName: string, packageName: string) {
      const repository = this.environmentStore.storageManager.getRepository();
      const environmentId = await resolveEnvironmentId(
         repository,
         environmentName,
         this.environmentStore,
      );
      return this.manifestService.reloadManifest(
         environmentId,
         packageName,
         environmentName,
      );
   }
}
