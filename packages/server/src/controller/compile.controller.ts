import type { GivenValue } from "@malloydata/malloy";
import { EnvironmentStore } from "../service/environment_store";
import type { CompileScope, TaggedLogMessage } from "../service/environment";

export class CompileController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async compile(
      environmentName: string,
      packageName: string,
      modelName: string,
      source: string | undefined,
      includeSql: boolean = false,
      givens?: Record<string, GivenValue>,
      scope: CompileScope = "append",
   ): Promise<{ status: string; problems: TaggedLogMessage[]; sql?: string }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const { problems, sql } = await environment.compileSource(
         packageName,
         modelName,
         source,
         includeSql,
         givens,
         scope,
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
