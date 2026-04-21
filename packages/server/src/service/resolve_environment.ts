import { EnvironmentNotFoundError } from "../errors";
import { ResourceRepository } from "../storage/DatabaseInterface";
import type { EnvironmentStore } from "./environment_store";

/** Set `PUBLISHER_RESOLVE_ENV_DIAG=1` to log every resolve to stderr (Ubuntu CI / local). */
function resolveEnvDiagVerbose(): boolean {
   return true;
}

function resolveEnvDiag(phase: string, data: Record<string, unknown>): void {
   try {
      const line = JSON.stringify({
         phase,
         ts: new Date().toISOString(),
         platform: process.platform,
         arch: process.arch,
         cwd: process.cwd(),
         node: process.version,
         ...data,
      });
      process.stderr.write(`[resolveEnvironmentId] ${line}\n`);
   } catch {
      // ignore diagnostics failures
   }
}

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
   const verbose = resolveEnvDiagVerbose();
   if (verbose) {
      resolveEnvDiag("enter", {
         environmentName,
         hasEnvironmentStore: Boolean(environmentStore),
      });
   }

   let dbEnvironment = await repository.getEnvironmentByName(environmentName);
   if (verbose) {
      resolveEnvDiag("after_getEnvironmentByName_1", {
         environmentName,
         found: Boolean(dbEnvironment),
         row: dbEnvironment
            ? {
                 id: dbEnvironment.id,
                 name: dbEnvironment.name,
                 path: dbEnvironment.path,
              }
            : null,
      });
   }

   let healAttempted = false;
   let healError: string | undefined;
   if (!dbEnvironment && environmentStore) {
      healAttempted = true;
      try {
         const liveEnvironment = await environmentStore.getEnvironment(
            environmentName,
            false,
         );
         if (verbose) {
            resolveEnvDiag("heal_got_live_environment", {
               environmentName,
               liveName: liveEnvironment.metadata?.name,
               livePath: liveEnvironment.metadata?.location,
            });
         }
         await environmentStore.addEnvironmentToDatabase(liveEnvironment);
         dbEnvironment = await repository.getEnvironmentByName(environmentName);
         if (verbose) {
            resolveEnvDiag("after_getEnvironmentByName_2", {
               environmentName,
               found: Boolean(dbEnvironment),
               row: dbEnvironment
                  ? {
                       id: dbEnvironment.id,
                       name: dbEnvironment.name,
                       path: dbEnvironment.path,
                    }
                  : null,
            });
         }
      } catch (err) {
         healError =
            err instanceof Error ? `${err.name}: ${err.message}` : String(err);
         if (verbose) {
            resolveEnvDiag("heal_caught", {
               environmentName,
               healError,
               isEnvNotFound: err instanceof EnvironmentNotFoundError,
            });
         }
         if (err instanceof EnvironmentNotFoundError) {
            dbEnvironment = null;
         } else {
            throw err;
         }
      }
   }

   if (!dbEnvironment) {
      let listed: unknown;
      try {
         const rows = await repository.listEnvironments();
         listed = {
            total: rows.length,
            sample: rows.slice(0, 80).map((e) => ({
               id: e.id,
               name: e.name,
               path: e.path,
            })),
         };
      } catch (listErr) {
         listed = {
            listEnvironmentsFailed:
               listErr instanceof Error
                  ? `${listErr.name}: ${listErr.message}`
                  : String(listErr),
         };
      }

      resolveEnvDiag("FAILED_throw_EnvironmentNotFoundError", {
         environmentName,
         healAttempted,
         hadEnvironmentStore: Boolean(environmentStore),
         healError,
         listedEnvironments: listed,
      });
      throw new EnvironmentNotFoundError(
         `Environment '${environmentName}' not found`,
      );
   }

   if (verbose) {
      resolveEnvDiag("ok", {
         environmentName,
         id: dbEnvironment.id,
      });
   }
   return dbEnvironment.id;
}
