import { EnvironmentNotFoundError } from "../errors";
import { ResourceRepository } from "../storage/DatabaseInterface";
import type { EnvironmentStore } from "./environment_store";

/**
 * Resolves a DuckDB `environments.id` for API routes that persist state in
 * the repository.
 *
 * In-memory first: we check `environmentStore` to confirm the environment
 * exists at all. If memory doesn't have it → 404 immediately (no point
 * looking in the DB for something we won't serve). If memory has it, we
 * look up the DB id, back-filling the row if it's missing so downstream
 * FK-joined queries (materializations, manifests, build_manifests) have a
 * valid id to bind against.
 *
 * Rationale: CI runs have hit "memory says yes, DB says no" split states
 * where `bun install`'s DuckDB addon under certain Bun versions drops
 * INSERTs silently. Treating memory as the source of truth for existence
 * and healing the DB on-demand keeps the API serving correctly regardless
 * of the underlying driver behavior.
 */
export async function resolveEnvironmentId(
   repository: ResourceRepository,
   environmentName: string,
   environmentStore?: EnvironmentStore,
): Promise<string> {
   if (environmentStore) {
      // Existence check via in-memory store. Throws EnvironmentNotFoundError
      // if neither memory nor the on-disk config knows about this env.
      await environmentStore.getEnvironment(environmentName, false);
   }

   let dbEnvironment = await repository.getEnvironmentByName(environmentName);
   if (!dbEnvironment && environmentStore) {
      // Memory has it, DB doesn't → heal the DB and re-read.
      const liveEnvironment = await environmentStore.getEnvironment(
         environmentName,
         false,
      );
      await environmentStore.addEnvironmentToDatabase(liveEnvironment);
      dbEnvironment = await repository.getEnvironmentByName(environmentName);
   }

   if (!dbEnvironment) {
      throw new EnvironmentNotFoundError(
         `Environment '${environmentName}' not found`,
      );
   }
   return dbEnvironment.id;
}
