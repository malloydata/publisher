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
      // Compile errors surface in-band via
      // `LoadPackageOutcome.models[i].compilationError`; if the
      // pool itself rejects, it's an infra-side failure
      // (shutting down, worker spawn failed, worker crashed,
      // RPC timeout) and the client should retry. Real Malloy
      // compile errors deserialised by the pool still carry
      // their MalloyError / ModelCompilationError identity, and an
      // unusable publisher.json its PackageManifestError identity —
      // let those bubble untouched so they keep their 4xx
      // mapping in `errors.ts`.
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
         // The worker's failure may be a refused filesystem access on
         // the package's own files, which the error mapper reads from
         // the cause; a bare 503 would read as "retry".
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
   // Same fail-safe split for the persistence-policy gate: an existing
   // package whose manifest violates the scope/schedule/freshness rules
   // still loads (warn), but a publish of it is rejected (see
   // package.controller).
   const invalidPolicy = pkg.formatInvalidPersistencePolicy();
   if (invalidPolicy) {
      logger.warn(`Package ${packageName} has an invalid persistence policy`, {
         packageName,
         detail: invalidPolicy,
      });
   }
   // The incremental-refresh gate does NOT get that fail-safe split: an
   // invalid `#@ persist` declaration fails the load, the way a model that
   // does not compile fails it.
   //
   // Publish cannot be the only strict point, because a package can arrive
   // without one: uploaded to a control plane's storage and loaded by a
   // worker, it never passes through POST/PATCH /packages. Warning here left
   // the rejection in a worker log while the author — the only person who can
   // fix the declaration — saw a clean publish and a source that quietly
   // rebuilt in full forever.
   //
   // BadRequestError, for the 400: a control plane resolves the status off
   // the error and otherwise defaults to 424, which would describe an
   // authoring error as a dependency failure. The message carries EVERY
   // rejection, so two broken declarations take one republish to fix.
   //
   // The accepted cost: `refresh=` was inert metadata before these rules, so
   // a package published earlier saying refresh="incremental" and nothing
   // else now stops serving until it is fixed. That is the one rule with any
   // legacy exposure (watermark= and merge_key= did not exist as keys), the
   // feature is gated behind `##! experimental.persistence`, and a bare
   // refresh="incremental" is an authoring error worth surfacing rather than
   // carrying — so every rule keeps the same severity, with no special cases.
   const invalidIncremental = pkg.formatInvalidIncrementalPolicy();
   if (invalidIncremental) {
      // Logged as well as thrown: the throw is what the author reads, the log
      // is the operator's copy.
      logger.error(
         `Package ${packageName} has an invalid incremental refresh policy`,
         {
            packageName,
            detail: invalidIncremental,
         },
      );
      throw new BadRequestError(invalidIncremental);
   }
   // `#@ preaggregate` gets the same strict-at-load treatment, for the reason
   // given on formatInvalidPreaggregatePolicy: a declaration that cannot take
   // effect is invisible in the answers, so warning here would leave the
   // author believing they had a rollup.
   //
   // Except the access-modifier pair, which warns at publish and at load —
   // see formatInvalidPreaggregatePolicy for why, and why warning is safe
   // there when it would not be for the others.
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
   // Persist-target collisions are ALWAYS warn-only at load (never fail an
   // already-published package), regardless of PERSIST_COLLISION_ENFORCE —
   // the flag only governs whether they REJECT a publish (see
   // package.controller). Surface them so an operator can remediate.
   const collisions = pkg.persistenceCollisionWarnings();
   if (collisions.length > 0) {
      logger.warn(`Package ${packageName} has persist-target collisions`, {
         packageName,
         detail: collisions.join("\n"),
      });
   }
}
