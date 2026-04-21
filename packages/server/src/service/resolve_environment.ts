import { EnvironmentNotFoundError } from "../errors";
import { ResourceRepository } from "../storage/DatabaseInterface";

export async function resolveEnvironmentId(
   repository: ResourceRepository,
   environmentName: string,
): Promise<string> {
   const dbEnvironment = await repository.getEnvironmentByName(environmentName);
   if (!dbEnvironment) {
      throw new EnvironmentNotFoundError(
         `Environment '${environmentName}' not found`,
      );
   }
   return dbEnvironment.id;
}
