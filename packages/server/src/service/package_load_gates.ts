// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError } from "@malloydata/malloy";
import {
   BadRequestError,
   DatabaseBusyError,
   ModelCompilationError,
   PackageManifestError,
   ServiceUnavailableError,
} from "../errors";
import { operatorLlmConfigured } from "../config";
import { logger } from "../logger";
import type { LoadPackageOutcome } from "../package_load/package_load_pool";
import { PackageLoadProtocolError } from "../package_load/protocol_decode";
import type { PreparedCeilingQueries } from "./classification_report";
import type { components } from "../api";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   assertRequiredStagesAvailable,
} from "./package_retrieval_settings";

type ApiPackage = components["schemas"]["Package"];
/** The listing fields a load is judged under. */
type Listing = Pick<ApiPackage, "explores" | "queryableSources">;

/** What the load gates read from the package they judge; `Package` implements it. */
export interface LoadGatedPackage {
   getPackageName(): string;
   prepareCeilingQueries(): Promise<PreparedCeilingQueries>;
   underListing<T>(listing: Listing | undefined, judge: () => T): T;
   formatInvalidExplores(exploresOverride?: string[]): string;
   formatInvalidPersistencePolicy(): string;
   formatLoadGateText(): string;
   formatClassificationLeaks(): string;
   formatArrayQueryJoins(): string;
   formatCarriesCeilingLeaks(prepared: PreparedCeilingQueries): string;
   formatAuthorSqlRefusals(): Promise<string>;
}

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
      // their class across the wire and bubble untouched for their 4xx. A
      // protocol error is not retryable (the worker bundle disagrees with
      // the server), so it bubbles untouched as an internal 500.
      const realError =
         err instanceof Error
            ? err
            : new Error(`Package-load worker pool failure: ${String(err)}`);
      if (
         realError instanceof MalloyError ||
         realError instanceof ModelCompilationError ||
         realError instanceof PackageManifestError ||
         realError instanceof PackageLoadProtocolError ||
         // Already a 503, and its message names the connection and the cause.
         realError instanceof DatabaseBusyError
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
      operatorLlmConfigured(),
   );
}

/**
 * Fail-safe at load: a bad explores entry doesn't fail the package
 * (its models still load and listModels hides the unmatched entry — it
 * never falls back to listing everything). Warn so the misconfig is
 * visible; the publish path rejects it outright (see package.controller).
 */
export function warnInvalidExplores(pkg: LoadGatedPackage): void {
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
 * before it serves. An invalid materialization annotation (any route), an
 * invalid incremental refresh or pre-aggregation declaration fails the load;
 * the package manifest's own policy only warns. With a `listing`, the package is
 * judged as that listing would serve it, and keeps its own.
 */
export async function applyLoadGates(
   pkg: LoadGatedPackage,
   listing?: Listing,
): Promise<void> {
   // Author SQL on a DuckDB connection reads as one SELECT, or the package
   // does not load (duckdb_lockdown.ts): independent of any listing.
   const authorSql = await pkg.formatAuthorSqlRefusals();
   if (authorSql) {
      const packageName = pkg.getPackageName();
      logger.error(
         `Package ${packageName} has author SQL that is not one SELECT`,
         {
            packageName,
            detail: authorSql,
         },
      );
      throw new BadRequestError(authorSql);
   }
   // The only await ahead of the listing, so no request is served under one the gates have not judged.
   const prepared = await pkg.prepareCeilingQueries();
   pkg.underListing(listing, () => judgeLoadGates(pkg, prepared));
}

function judgeLoadGates(
   pkg: LoadGatedPackage,
   prepared: PreparedCeilingQueries,
): void {
   const packageName = pkg.getPackageName();
   warnInvalidExplores(pkg);
   // The package manifest's materialization policy (publisher.json schedule
   // rules) keeps the fail-safe split: warn at load, reject at publish.
   const invalidPolicy = pkg.formatInvalidPersistencePolicy();
   if (invalidPolicy) {
      logger.warn(`Package ${packageName} has an invalid persistence policy`, {
         packageName,
         detail: invalidPolicy,
      });
   }
   // An invalid materialization annotation fails the load, like a compile
   // error, on every load path: a package loaded from control-plane storage
   // never passes through POST/PATCH /packages, so a publish-only refusal
   // would leave it serving with the mistake (an incremental declaration
   // would silently rebuild in full, a rollup silently never build).
   // BadRequestError gives a 400 (a control plane defaults to 424); the
   // message carries every one, with its code, so one republish fixes them
   // all. A valid annotation Publisher cannot build only warns.
   const invalid = pkg.formatLoadGateText();
   if (invalid) {
      // Logged too: the throw is the author's copy, the log the operator's.
      logger.error(
         `Package ${packageName} has an invalid materialization annotation`,
         { packageName, detail: invalid },
      );
      throw new BadRequestError(invalid);
   }
   // A join inside a query on an array is read by none of the access checks,
   // so a model that declares one refuses it rather than serve reads nothing
   // accounts for (a `#(carries)` ceiling or a gate it would pass unseen).
   const arrayQueryJoins = pkg.formatArrayQueryJoins();
   if (arrayQueryJoins) {
      logger.error(
         `Package ${packageName} joins a source inside a query on an array`,
         { packageName, detail: arrayQueryJoins },
      );
      throw new BadRequestError(arrayQueryJoins);
   }
   // A `#(carries)` ceiling is strict at load too: a surface that lists what
   // it carries must not serve a field outside the list, and a warning would
   // leave it serving. The message names every leak so one fix clears them.
   const leaks = pkg.formatClassificationLeaks();
   if (leaks) {
      logger.error(
         `Package ${packageName} exposes a field outside a source's #(carries)`,
         { packageName, detail: leaks },
      );
      throw new BadRequestError(leaks);
   }
   const ceiling = pkg.formatCarriesCeilingLeaks(prepared);
   if (ceiling) {
      logger.error(
         `Package ${packageName} has a query that reads past a source's #(carries)`,
         { packageName, detail: ceiling },
      );
      throw new BadRequestError(ceiling);
   }
}
