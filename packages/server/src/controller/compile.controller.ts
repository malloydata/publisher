import type { LogMessage } from "@malloydata/malloy";
import { EnvironmentStore } from "../service/environment_store";

export class CompileController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async compile(
      environmentName: string,
      packageName: string,
      modelName: string,
      source: string,
      includeSql: boolean = false,
   ): Promise<{ status: string; problems: LogMessage[]; sql?: string }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const { problems, sql } = await environment.compileSource(
         packageName,
         modelName,
         source,
         includeSql,
      );

      // Determine overall status based on presence of errors
      const hasErrors = problems.some((p) => p.severity === "error");

      return {
         status: hasErrors ? "error" : "success",
         problems: problems,
         ...(sql !== undefined && { sql }),
      };
   }
}
