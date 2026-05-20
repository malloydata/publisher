import { components } from "../api";
import { EnvironmentStore } from "../service/environment_store";

type ApiDatabase = components["schemas"]["Database"];

export class DatabaseController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async listDatabases(
      environmentName: string,
      packageName: string,
   ): Promise<ApiDatabase[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return p.listDatabases();
   }
}
