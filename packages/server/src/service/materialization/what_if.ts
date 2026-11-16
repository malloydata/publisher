// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What-if over a package's v1 materialization annotations (materialization
 * v2, T3): given a proposed edit to the package's files (add or remove a
 * `#@ persist`, change its `storage=`, add a `#@ preaggregate`), which table
 * would answer each of a set of queries, and why every other table would be
 * skipped, once the proposal were built.
 *
 * Nothing is built and nothing runs against a warehouse. The proposal loads as
 * a throwaway package (the dry-run load a publish would run, with the
 * publish's own refusals), which is then bound to the tables its build plan
 * would build: each eligible colocated source to a placeholder table name,
 * each eligible `storage=` source to a table declared from its columns'
 * Malloy types. Every query is then routed against that binding with
 * {@link Model.routeWithoutRunning}, the router a served query takes, compiled
 * only. The answer is each query's path report.
 *
 * The binding goes through the package's own eligibility gates, so a source a
 * real build would refuse is refused here too, and is reported. No routing
 * metric is recorded and no serve log is written; the load itself is a dry
 * run, which records no load metrics.
 *
 * Compiling resolves each source's schema against its connection, as a
 * compile or a dry run does: metadata, never rows.
 */

import { promises as fs } from "fs";
import * as path from "path";
import type { GivenValue } from "@malloydata/malloy";
import { components } from "../../api";
import { getPersistStorageMode } from "../../config";
import {
   BadRequestError,
   NotQueryableError,
   QueryCompileError,
} from "../../errors";
import {
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
} from "../../constants";
import type { OverlayFile } from "../request_overlay";
import type { PackageMaterializationView } from "./package_materialization";
import { formatPublishRejections } from "../publish_checks";
import type { FreshnessManifest, ManifestEntry } from "./domain/types";
import { PREVIEW_STORED_TYPES } from "./serve_verdicts";
import { errMessage } from "../../utils";

type ApiBuildPlan = components["schemas"]["BuildPlan"];
type ApiPersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type ApiWhatIf = components["schemas"]["WhatIf"];
type ApiWhatIfAnswer = components["schemas"]["WhatIfAnswer"];
type ApiWhatIfSource = components["schemas"]["WhatIfSource"];
type ApiWhatIfRefusal = components["schemas"]["WhatIfRefusal"];

type ApiQueryPath = components["schemas"]["QueryPath"];

/**
 * What what-if reads of a package, the one served now and the throwaway load
 * alike: typed here rather than against `Package`, which would close an
 * import cycle through `environment.ts` (import_cycles.spec.ts).
 */
type PublishCheckedPackage = Parameters<typeof formatPublishRejections>[0];

interface WhatIfPackage extends PublishCheckedPackage {
   getBuildPlan(): ApiBuildPlan | null;
   isBuildPlanUnavailable(): boolean;
   getPackagePath(): string;
   getModel(modelPath: string): WhatIfModel | undefined;
   materialization(): Pick<
      PackageMaterializationView,
      "servingEligibility" | "bind"
   >;
}

/** What what-if reads of one model: its compile-only route. */
interface WhatIfModel {
   prepareServedCopy(): Promise<void>;
   routeWithoutRunning(request: {
      query?: string;
      sourceName?: string;
      queryName?: string;
      givens?: Record<string, GivenValue>;
      includeHiddenFilesAndSources?: boolean;
   }): Promise<ApiQueryPath>;
}

/** The environment's dry-run load, which what-if loads the proposal with. */
interface WhatIfEnvironment {
   dryRunInstall<T>(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      options: {
         validate?: (pkg: WhatIfPackage) => string | undefined;
         readVersion: boolean;
         describe: (pkg: WhatIfPackage) => T | Promise<T>;
      },
   ): Promise<T>;
}

/** One query to route: ad-hoc text, or a named view or query. */
interface WhatIfQuery {
   modelPath: string;
   query?: string;
   sourceName?: string;
   queryName?: string;
}

interface WhatIfRequest {
   /** The proposed files, each replacing (or adding) one package file. */
   files: OverlayFile[];
   queries: WhatIfQuery[];
   givens?: Record<string, GivenValue>;
   /**
    * As on a query: off, a query against a source the package does not
    * publish is refused, and each answer names no hidden source. The `plan`
    * and `refused` lists keep the build plan's semantics and name every
    * planned source, as `GET …/build-plan` does.
    */
   includeHiddenFilesAndSources?: boolean;
   /**
    * Told the detail of a query that failed for an internal reason, which
    * its answer reports generically; the shell logs it.
    */
   onInternalError?: (detail: string) => void;
}

/** At most this many queries per request: each is a compile, or several. */
export const MAX_WHAT_IF_QUERIES = 50;

/**
 * The table a `storage=` build would store `columns` (name and Malloy type)
 * as, declared ahead of the build: each column at the DuckDB type for its
 * Malloy type, and a column of any other type left off.
 */
export function storedSchemaOf(
   columns: readonly { name?: string; type?: string }[],
): { name: string; type: string }[] {
   return columns.flatMap((column) => {
      // The types the dry-run preview declares a stored column as.
      const type = column.type ? PREVIEW_STORED_TYPES[column.type] : undefined;
      return column.name && type ? [{ name: column.name, type }] : [];
   });
}

/** A placeholder table name, valid unquoted in every dialect. */
function placeholderTable(source: ApiPersistSourcePlan): string {
   const safe = source.name.replace(/[^A-Za-z0-9_]/g, "_");
   return `what_if__${safe}`;
}

/** The checked request body, or a 400. */
export function parseWhatIfQueries(input: unknown): WhatIfQuery[] {
   if (!Array.isArray(input) || input.length === 0) {
      throw new BadRequestError(
         "`queries` must be a non-empty array of { modelPath, query } or { modelPath, sourceName?, queryName } entries.",
      );
   }
   if (input.length > MAX_WHAT_IF_QUERIES) {
      throw new BadRequestError(
         `\`queries\` holds ${input.length} queries; a request takes at most ${MAX_WHAT_IF_QUERIES}.`,
      );
   }
   return input.map((entry: unknown, index: number) => {
      const where = `\`queries[${index}]\``;
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
         throw new BadRequestError(`${where} must be an object.`);
      }
      const { modelPath, query, sourceName, queryName } = entry as Record<
         string,
         unknown
      >;
      if (typeof modelPath !== "string" || modelPath.length === 0) {
         throw new BadRequestError(`${where}.modelPath must be a string.`);
      }
      for (const [key, value] of Object.entries({
         query,
         sourceName,
         queryName,
      })) {
         if (value !== undefined && typeof value !== "string") {
            throw new BadRequestError(`${where}.${key} must be a string.`);
         }
      }
      return {
         modelPath,
         ...(typeof query === "string" ? { query } : {}),
         ...(typeof sourceName === "string" ? { sourceName } : {}),
         ...(typeof queryName === "string" ? { queryName } : {}),
      };
   });
}

/** Directories a package load never reads, left out of the staged copy. */
const UNSTAGED_DIRECTORIES: ReadonlySet<string> = new Set([".git"]);

/**
 * Copy the package tree at `from` into `to` for the throwaway load: model
 * files, notebooks and the manifest are copied, so the proposal's edits land
 * on a copy; everything else (data files, a JSON dataset among them, which can
 * be large) is linked, never written. A link that resolves nowhere is left
 * out, as a load would not find it either.
 */
export async function stagePackageTree(
   from: string,
   to: string,
): Promise<void> {
   await fs.mkdir(to, { recursive: true });
   for (const entry of await fs.readdir(from, { withFileTypes: true })) {
      const source = path.join(from, entry.name);
      const target = path.join(to, entry.name);
      if (entry.isDirectory()) {
         if (UNSTAGED_DIRECTORIES.has(entry.name)) continue;
         await stagePackageTree(source, target);
      } else if (
         entry.name.endsWith(MODEL_FILE_SUFFIX) ||
         entry.name.endsWith(NOTEBOOK_FILE_SUFFIX) ||
         entry.name === PACKAGE_MANIFEST_NAME
      ) {
         await fs.copyFile(source, target);
      } else if (entry.isFile() || entry.isSymbolicLink()) {
         let resolved: string;
         try {
            resolved = await fs.realpath(source);
         } catch {
            continue;
         }
         await fs.symlink(resolved, target);
      }
   }
}

/** The plan's sources, keyed by sourceID, as the diff reads them. */
function planSummary(source: ApiPersistSourcePlan): ApiWhatIfSource {
   const storage = source.annotationFields?.storage?.trim();
   return {
      sourceID: source.sourceID,
      name: source.name,
      origin: source.origin,
      ...(storage ? { storage } : {}),
   };
}

/** What the proposal adds, removes and changes against the plan served now. */
export function diffPlans(
   current: ApiBuildPlan | null,
   proposed: ApiBuildPlan,
): ApiWhatIf["plan"] {
   const before = current?.sources ?? {};
   const after = proposed.sources;
   const added: ApiWhatIfSource[] = [];
   const changed: ApiWhatIfSource[] = [];
   const unchanged: ApiWhatIfSource[] = [];
   for (const [id, source] of Object.entries(after)) {
      const previous = before[id];
      if (!previous) added.push(planSummary(source));
      else if (
         previous.sourceEntityId !== source.sourceEntityId ||
         previous.annotationFields?.storage !== source.annotationFields?.storage
      ) {
         changed.push(planSummary(source));
      } else unchanged.push(planSummary(source));
   }
   const removed = Object.entries(before)
      .filter(([id]) => !after[id])
      .map(([, source]) => planSummary(source));
   return { added, removed, changed, unchanged };
}

/**
 * Bind `pkg` to the tables its build plan would build, through its own
 * eligibility gates. Returns the sources a build would refuse, with why;
 * they stay unbound and serve live.
 */
function bindProposedTables(
   pkg: WhatIfPackage,
   plan: ApiBuildPlan,
): ApiWhatIfRefusal[] {
   // Pre-filtered against the same gates the bind applies, so each refusal is
   // reported with its reason rather than dropped by the bind.
   const { storage: storageEligibility, colocated: colocatedEligibility } = pkg
      .materialization()
      .servingEligibility();
   const refusals: ApiWhatIfRefusal[] = [];
   for (const refused of Object.values(plan.refusedSources ?? {})) {
      refusals.push({
         name: refused.name,
         sourceID: refused.sourceID,
         reason: refused.reason,
         message: refused.message,
      });
   }
   const colocated: FreshnessManifest = {};
   const stored: Record<string, ManifestEntry> = {};
   for (const source of Object.values(plan.sources)) {
      const destination = source.annotationFields?.storage?.trim();
      if (destination && getPersistStorageMode() === "off") {
         // A build skips it rather than downgrade it to colocated, so it
         // serves live (see deriveSelfInstructions).
         refusals.push({
            name: source.name,
            sourceID: source.sourceID,
            reason: "storage_mode_off",
            message:
               "PERSIST_STORAGE_MODE is off, so a build skips a `storage=` source and it serves live",
         });
         continue;
      }
      if (destination) {
         // Keyed by sourceID, as the bind keys it.
         if (!storageEligibility?.eligible.includes(source.sourceID)) {
            refusals.push({
               name: source.name,
               sourceID: source.sourceID,
               reason: "not_eligible",
               message:
                  storageEligibility?.refused[source.sourceID] ??
                  "the build plan could not examine this source",
            });
            continue;
         }
         const schema = storedSchemaOf(source.columns ?? []);
         stored[source.sourceID] = {
            sourceEntityId: source.sourceEntityId,
            sourceName: source.name,
            origin: source.origin,
            storageDestinationName: destination,
            physicalTableName: placeholderTable(source),
            schema,
         };
         continue;
      }
      if (!colocatedEligibility?.eligibleEntityIds.has(source.sourceEntityId)) {
         refusals.push({
            name: source.name,
            sourceID: source.sourceID,
            reason: "not_eligible",
            message:
               colocatedEligibility?.refused[source.sourceEntityId] ??
               "the build plan could not examine this source",
         });
         continue;
      }
      colocated[source.sourceEntityId] = {
         tableName: placeholderTable(source),
         // Quoted for its dialect at bind, as the dry-run preview's are.
         connectionName: source.connectionName,
      };
   }
   // One bind, as a run's manifest is bound. The bind admits through the same
   // gates, so nothing pre-filtered above is dropped silently.
   pkg.materialization().bind({ colocated, storage: stored });
   return refusals;
}

/**
 * The error text an answer may carry: what the query route would tell the
 * caller (a compile error in their own query, a 400, the boundary's 404).
 * Anything else is an internal failure, whose text can name connections or
 * paths; it is handed to `onInternal` (the shell logs it, at debug inside the
 * hypothetical scope) and answered generically.
 */
export function answerErrorText(
   error: unknown,
   onInternal: (detail: string) => void = () => {},
): string {
   if (
      error instanceof QueryCompileError ||
      error instanceof BadRequestError ||
      error instanceof NotQueryableError
   ) {
      return error.message;
   }
   onInternal(errMessage(error));
   return "This query could not be routed against the proposal.";
}

/** Route each query against the bound proposal, one answer each. */
async function answerQueries(
   pkg: WhatIfPackage,
   queries: readonly WhatIfQuery[],
   givens: Record<string, GivenValue> | undefined,
   includeHiddenFilesAndSources: boolean,
   onInternalError: ((detail: string) => void) | undefined,
): Promise<ApiWhatIfAnswer[]> {
   const answers: ApiWhatIfAnswer[] = [];
   for (const query of queries) {
      const echo = {
         modelPath: query.modelPath,
         ...(query.query !== undefined ? { query: query.query } : {}),
         ...(query.sourceName !== undefined
            ? { sourceName: query.sourceName }
            : {}),
         ...(query.queryName !== undefined
            ? { queryName: query.queryName }
            : {}),
      };
      const model = pkg.getModel(query.modelPath);
      if (!model) {
         answers.push({
            ...echo,
            error: `The proposal has no model at \`${query.modelPath}\`.`,
         });
         continue;
      }
      try {
         // The bind compiles each model's served copy in the background; the
         // route reads it, so it must be compiled over the proposal first.
         await model.prepareServedCopy();
         const path = await model.routeWithoutRunning({
            query: query.query,
            sourceName: query.sourceName,
            queryName: query.queryName,
            givens,
            includeHiddenFilesAndSources,
         });
         answers.push({ ...echo, path });
      } catch (error) {
         answers.push({
            ...echo,
            error: answerErrorText(error, onInternalError),
         });
      }
   }
   return answers;
}

/**
 * Answer a what-if request against `current`, the package served now under
 * `packageName` in `environment`. A proposal a publish would refuse is refused
 * with the publish's own 400.
 */
export async function runWhatIf(
   environment: WhatIfEnvironment,
   current: WhatIfPackage,
   packageName: string,
   request: WhatIfRequest,
): Promise<ApiWhatIf> {
   const currentPlan = current.getBuildPlan();
   return environment.dryRunInstall(
      packageName,
      async (stagingPath) => {
         await stagePackageTree(current.getPackagePath(), stagingPath);
         for (const file of request.files) {
            const target = path.join(stagingPath, file.path);
            await fs.mkdir(path.dirname(target), { recursive: true });
            // A linked file is replaced, never written through.
            await fs.rm(target, { force: true });
            await fs.writeFile(target, file.content);
         }
      },
      {
         readVersion: false,
         validate: (loaded) => formatPublishRejections(loaded),
         describe: async (pkg) => {
            if (pkg.isBuildPlanUnavailable()) {
               throw new BadRequestError(
                  "The proposal's build plan could not be derived, so no table it would build is known.",
               );
            }
            const plan = pkg.getBuildPlan() ?? { graphs: [], sources: {} };
            const refused = bindProposedTables(pkg, plan);
            return {
               plan: diffPlans(currentPlan, plan),
               refused,
               answers: await answerQueries(
                  pkg,
                  request.queries,
                  request.givens,
                  !!request.includeHiddenFilesAndSources,
                  request.onInternalError,
               ),
            };
         },
      },
   );
}
