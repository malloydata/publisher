import type { LogMessage } from "@malloydata/malloy";
import { ProjectStore } from "../service/project_store";

export class CompileController {
   private projectStore: ProjectStore;

   constructor(projectStore: ProjectStore) {
      this.projectStore = projectStore;
   }

   public async compile(
      projectName: string,
      packageName: string,
      modelName: string,
      source: string,
      includeSql: boolean = false,
   ): Promise<{ status: string; problems: LogMessage[]; sql?: string }> {
      const project = await this.projectStore.getProject(projectName, false);
      const { problems, sql } = await project.compileSource(
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
