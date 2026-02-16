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
    ): Promise<{ status: string; problems: LogMessage[] }> {
        const project = await this.projectStore.getProject(projectName, false);
        const problems = await project.compileSource(packageName, modelName, source);

        // Determine overall status based on presence of errors
        const hasErrors = problems.some((p) => p.severity === "error");

        return {
            status: hasErrors ? "error" : "success",
            problems: problems,
        };
    }
}
