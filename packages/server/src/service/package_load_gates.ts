// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError } from "@malloydata/malloy";
import {
   BadRequestError,
   ModelCompilationError,
   PackageManifestError,
   ServiceUnavailableError,
} from "../errors";
import { logger } from "../logger";
import type { LoadPackageOutcome } from "../package_load/package_load_pool";
import { llmConfigured } from "../providers/active";
import type { Package } from "./package";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   assertRequiredStagesAvailable,
} from "./package_retrieval";

/**
 * Await a package-load worker job, rewrapping a pool-infrastructure failure as
 * a 503.
 */
export async function loadOutcomeOrThrow(
   job: Promise<LoadPackageOutcome>,
): Promise<LoadPackageOutcome> {
   try {
      return await job;
   } catch (err: unknown) {
      // A pool rejection is an infra failure (shutdown, spawn, crash, RPC
      // timeout) the client should retry. Compile and manifest errors keep
      // their class across the wire and bubble untouched for their 4xx.
      const realError =
         err instanceof Error
            ? err
            : new Error(`Package-load worker pool failure: ${String(err)}`);
      if (
         realError instanceof MalloyError ||
         realError instanceof ModelCompilationError ||
         realError instanceof PackageManifestError
      ) {
         throw realError;
      }
      throw new ServiceUnavailableError(
         `Package-load worker pool unavailable: ${realError.message}`,
         // The cause may be a refused filesystem access, which the error
         // mapper reads from it; a bare 503 would read as "retry".
         { cause: realError },
      );
   }
}

/**
 * A stage the package requires (`enabled: true`) needs the operator's
 * LLM; without one the package does not load.
 */
export function assertOutcomeStagesAvailable(
   outcome: LoadPackageOutcome,
): void {
   assertRequiredStagesAvailable(
      outcome.packageMetadata.retrieval ?? DEFAULT_PACKAGE_RETRIEVAL,
      llmConfigured(),
   );
}

/**
 * Fail-safe at load: a bad explores entry doesn't fail the package
 * (its models still load and listModels hides the unmatched entry — it
 * never falls back to listing everything). Warn so the misconfig is
 * visible; the publish path rejects it outright (see package.controller).
 */
export function warnInvalidExplores(pkg: Package): void {
   const packageName = pkg.getPackageName();
   const invalidMsg = pkg.formatInvalidExplores();
   if (invalidMsg) {
      logger.warn(`Package ${packageName} has invalid explores`, {
         packageName,
         detail: invalidMsg,
      });
   }
}

/**
 * The manifest and persistence policy gates a freshly loaded package passes
 * before it serves. Most warn; an invalid incremental refresh or
 * pre-aggregation declaration fails the load.
 */
export function applyLoadGates(pkg: Package): void {
   const packageName = pkg.getPackageName();
   warnInvalidExplores(pkg);
   // Same fail-safe split: warn at load, reject at publish.
   const invalidPolicy = pkg.formatInvalidPersistencePolicy();
   if (invalidPolicy) {
      logger.warn(`Package ${packageName} has an invalid persistence policy`, {
         packageName,
         detail: invalidPolicy,
      });
   }
   // An invalid incremental `#@ persist` declaration fails the load, like a
   // compile error. Publish cannot be the only strict point: a package loaded
   // from control-plane storage never passes through POST/PATCH /packages, and
   // a warning would leave the author with a source that silently rebuilds in
   // full. BadRequestError gives a 400 (a control plane defaults to 424); the
   // message carries every rejection so one republish fixes them all.
   const invalidIncremental = pkg.formatInvalidIncrementalPolicy();
   if (invalidIncremental) {
      // Logged too: the throw is the author's copy, the log the operator's.
      logger.error(
         `Package ${packageName} has an invalid incremental refresh policy`,
         {
            packageName,
            detail: invalidIncremental,
         },
      );
      throw new BadRequestError(invalidIncremental);
   }
   // `#@ preaggregate` is strict at load too (see
   // formatInvalidPreaggregatePolicy), except the access-modifier pair, which
   // only warns.
   const accessWarnings = pkg.preaggregateAccessWarnings();
   if (accessWarnings.length > 0) {
      logger.warn(
         `Package ${packageName} pre-aggregates a field the source hides`,
         {
            packageName,
            detail: accessWarnings.map((w) => w.message).join("\n"),
         },
      );
   }
   const invalidPreaggregate = pkg.formatInvalidPreaggregatePolicy();
   if (invalidPreaggregate) {
      logger.error(
         `Package ${packageName} has an invalid pre-aggregation declaration`,
         {
            packageName,
            detail: invalidPreaggregate,
         },
      );
      throw new BadRequestError(invalidPreaggregate);
   }
   // Persist-target collisions only warn at load, whatever
   // PERSIST_COLLISION_ENFORCE says; that flag governs publish rejection.
   const collisions = pkg.persistenceCollisionWarnings();
   if (collisions.length > 0) {
      logger.warn(`Package ${packageName} has persist-target collisions`, {
         packageName,
         detail: collisions.join("\n"),
      });
   }
}
