import { EnvironmentNotFoundError } from "../errors";
import { ResourceRepository } from "../storage/DatabaseInterface";
import type { EnvironmentStore } from "./environment_store";

/**
 * Resolves a DuckDB `environments.id` for API routes that persist state in the
 * repository. When `environmentStore` is provided and the row is missing, we
 * attempt a one-shot sync from the in-memory `Environment` — package routes
 * can still serve from memory while the DB row is absent (e.g. rare races or
 * legacy drift), but materialization/manifest flows require the id.
 */
export async function resolveEnvironmentId(
   repository: ResourceRepository,
   environmentName: string,
   environmentStore?: EnvironmentStore,
): Promise<string> {
   let dbEnvironment = await repository.getEnvironmentByName(environmentName);
   if (!dbEnvironment && environmentStore) {
      try {
         const liveEnvironment = await environmentStore.getEnvironment(
            environmentName,
            false,
         );
         await environmentStore.addEnvironmentToDatabase(liveEnvironment);
         dbEnvironment = await repository.getEnvironmentByName(environmentName);
      } catch (err) {
         if (err instanceof EnvironmentNotFoundError) {
            dbEnvironment = null;
         } else {
            throw err;
         }
      }
   }
   if (!dbEnvironment) {
      throw new EnvironmentNotFoundError(
         `Environment '${environmentName}' not found`,
      );
   }
   return dbEnvironment.id;
}
