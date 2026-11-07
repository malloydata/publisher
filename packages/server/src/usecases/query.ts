// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The query use case REST (query.controller) and MCP (execute_query_tool)
 * share: one fixed sequence, so the two transports cannot drift on the steps
 * that decide what runs and what is recorded.
 *
 *   withAccessRecord → precondition → metadata parse → resolve the model →
 *   audit.version → query-metadata input → concurrency slot → runModelQuery
 *
 * What each transport decides for itself is an explicit option
 * ({@link QueryTransportOptions}): how a missing model is found and worded,
 * which result shape is built and byte-capped, whether the caller's
 * `#(authorize)` bypass is honored, and where the slot is taken. Shaping the
 * answer (the REST `QueryResult`, the MCP envelope) stays with the adapter.
 */

import type { GivenValue } from "@malloydata/malloy";
import { BadRequestError, ModelNotFoundError } from "../errors";
import {
   tryAcquireQuerySlot,
   type QuerySlotHandle,
} from "../query_concurrency";
import {
   type AccessIdentity,
   type AccessRecordBase,
   recordRefusal,
   withAccessRecord,
} from "../service/access_record";
import type { Environment } from "../service/environment";
import type { EnvironmentStore } from "../service/environment_store";
import type { FilterParams } from "../service/filter";
import type { Model } from "../service/model";
import type { Package } from "../service/package";
import {
   buildQueryMetadataInput,
   parseQueryClass,
   parseSuppliedQueryMetadata,
   type QueryClass,
   type QueryMetadata,
} from "../service/query_metadata";
import { resolveQueryModel, runModelQuery } from "../service/run_model_query";

/** What a query asks for, and of whom, as either transport received it. */
export interface QueryCommand {
   environmentName: string;
   packageName: string;
   modelPath: string;
   sourceName?: string;
   queryName?: string;
   query?: string;
   filterParams?: FilterParams;
   bypassFilters?: boolean;
   givens?: Record<string, GivenValue>;
   /** Unvalidated: a bad bag becomes a 400 inside the record, not a connector refusal. */
   queryMetadata?: unknown;
   queryClass?: unknown;
   /** The published version to query; absent, empty or `latest` means `latest`. */
   versionId?: unknown;
   /** Honored only when {@link QueryTransportOptions.honorBypassAuthorize} is set. */
   bypassAuthorize?: boolean;
   includeHiddenFilesAndSources?: boolean;
   /** Aborted when the client goes away: cancels the query, and the record says so. */
   clientSignal?: AbortSignal;
   /** The caller from the deployment's header and the trace id, for the access record. */
   identity: AccessIdentity;
}

/**
 * Find the environment, package and model a command names. `model` is
 * undefined when the package serves no such file; a transport that refuses
 * earlier (MCP's notebook refusal) throws instead. A thrown error is what the
 * access record classifies.
 */
export type QueryModelResolver = (
   environmentStore: EnvironmentStore,
   command: QueryCommand,
) => Promise<{ environment: Environment; pkg: Package; model?: Model }>;

/** The named version of the package (`latest` when none is named), unrefused. */
export const resolveCommandModel: QueryModelResolver = (
   environmentStore,
   { environmentName, packageName, modelPath, versionId },
) =>
   resolveQueryModel(
      environmentStore,
      environmentName,
      packageName,
      modelPath,
      versionId,
   );

/** Each transport's documented differences, stated rather than re-coded. */
export interface QueryTransportOptions {
   /** How the model is found. */
   resolveModel: QueryModelResolver;
   /**
    * The refusal for a package that serves no such model, thrown after the
    * answering version is recorded. Its wording is the transport's.
    */
   modelNotFound: (command: QueryCommand) => Error;
   /** The result shape the transport sends, so exactly that one is built and byte-capped. */
   shape: "compact" | "full";
   /**
    * Whether the command's `bypassAuthorize` reaches the model. False sends
    * an explicit `false` whatever the command carries.
    */
   honorBypassAuthorize: boolean;
   /**
    * Take the concurrency slot here, after the model resolves, under this
    * rejection label. Absent: the caller already holds one (REST middleware).
    */
   querySlotLabel?: string;
   /** Ad-hoc text runs on its own: a source or view name sent beside it is not passed on. */
   adHocRunsAlone: boolean;
   /** Debug message logged when a connection's query-metadata layers cannot be read. */
   noLayersLogMessage: string;
   /**
    * Refused inside the record before anything is resolved (a malformed
    * argument the transport already detected).
    */
   precondition?: Error;
}

/** The options REST's query route runs with. */
export const REST_QUERY_OPTIONS: Omit<QueryTransportOptions, "shape"> = {
   resolveModel: resolveCommandModel,
   // Worded exactly as Model.assertQueryBoundaryEarly's generic refusal, so
   // a 404 cannot tell a gated hidden file from a missing one.
   modelNotFound: ({ modelPath }) =>
      new ModelNotFoundError(`No queryable model "${modelPath}".`),
   // From the `x-publisher-bypass-authorize` header the route reads.
   honorBypassAuthorize: true,
   // queryConcurrency() middleware holds the slot for the whole request.
   querySlotLabel: undefined,
   adHocRunsAlone: false,
   noLayersLogMessage: "No query-metadata layers for connection",
};

/** What a query answered, for the adapter to shape. */
export interface QueryOutcome {
   pkg: Package;
   /** The version that answered; undefined for a package with no versions. */
   version: string | undefined;
   result: Awaited<ReturnType<typeof runModelQuery>>;
}

function accessBase({
   environmentName,
   packageName,
   modelPath,
   sourceName,
   queryName,
   query,
   identity,
}: Pick<
   QueryCommand,
   | "environmentName"
   | "packageName"
   | "modelPath"
   | "sourceName"
   | "queryName"
   | "query"
   | "identity"
>): AccessRecordBase {
   return {
      environment: environmentName,
      package: packageName,
      modelPath,
      ...(queryName ? { queryName } : {}),
      ...(sourceName ? { requestedSource: sourceName } : {}),
      adHoc: Boolean(query),
      ...identity,
   };
}

export class QueryUseCase {
   constructor(private readonly environmentStore: EnvironmentStore) {}

   /**
    * Run a query and emit its access record (`access_record.ts`), on success
    * and on refusal alike. `present` shapes the answer for the transport; it
    * runs inside the record and while the slot is held, so a failure to
    * shape is recorded as the request's outcome.
    */
   run<T>(
      command: QueryCommand,
      options: QueryTransportOptions,
      present: (outcome: QueryOutcome) => T | Promise<T>,
   ): Promise<T> {
      return withAccessRecord(
         accessBase(command),
         command.clientSignal,
         async (audit) => {
            if (options.precondition) throw options.precondition;

            let requestMetadata: QueryMetadata | undefined;
            let queryClass: QueryClass | undefined;
            try {
               requestMetadata = parseSuppliedQueryMetadata(
                  command.queryMetadata,
               );
               queryClass = parseQueryClass(command.queryClass);
            } catch (error) {
               throw new BadRequestError((error as Error).message);
            }

            const { environment, pkg, model } = await options.resolveModel(
               this.environmentStore,
               command,
            );
            const version = pkg.getVersionId();
            if (version) audit.version = version;
            if (!model) throw options.modelNotFound(command);

            let querySlot: QuerySlotHandle | null = null;
            try {
               if (options.querySlotLabel !== undefined) {
                  querySlot = tryAcquireQuerySlot(options.querySlotLabel);
               }
               const queryMetadataInput = buildQueryMetadataInput({
                  environmentName: command.environmentName,
                  environment,
                  pkg,
                  request: requestMetadata,
                  queryClass,
                  // The version that answered (latest when none was named),
                  // not the raw request value.
                  version,
                  noLayersLogMessage: options.noLayersLogMessage,
               });
               const target = !options.adHocRunsAlone
                  ? {
                       sourceName: command.sourceName,
                       queryName: command.queryName,
                       query: command.query,
                    }
                  : command.query
                    ? { query: command.query }
                    : {
                         sourceName: command.sourceName,
                         queryName: command.queryName,
                      };
               const result = await runModelQuery(
                  audit,
                  pkg,
                  command.modelPath,
                  model,
                  {
                     ...target,
                     filterParams: command.filterParams,
                     bypassFilters: command.bypassFilters,
                     givens: command.givens,
                     queryMetadataInput,
                     shape: options.shape,
                     bypassAuthorize: options.honorBypassAuthorize
                        ? command.bypassAuthorize
                        : false,
                     includeHiddenFilesAndSources:
                        command.includeHiddenFilesAndSources,
                     clientSignal: command.clientSignal,
                  },
               );
               return await present({ pkg, version, result });
            } finally {
               // Idempotent, so a double-fault cannot double-decrement.
               querySlot?.release();
            }
         },
      );
   }

   /**
    * Record a request a transport refused before {@link run} was called, so
    * it is not the one request without a record.
    */
   recordRefusal(
      command: Pick<
         QueryCommand,
         | "environmentName"
         | "packageName"
         | "modelPath"
         | "sourceName"
         | "queryName"
         | "query"
         | "identity"
      >,
      error: Error,
   ): Promise<void> {
      return recordRefusal(accessBase(command), error);
   }
}
