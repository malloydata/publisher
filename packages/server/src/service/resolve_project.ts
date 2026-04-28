import { ProjectNotFoundError } from "../errors";
import { ResourceRepository } from "../storage/DatabaseInterface";

export async function resolveProjectId(
   repository: ResourceRepository,
   projectName: string,
): Promise<string> {
   const dbProject = await repository.getProjectByName(projectName);
   if (!dbProject) {
      throw new ProjectNotFoundError(`Project '${projectName}' not found`);
   }
   return dbProject.id;
}
