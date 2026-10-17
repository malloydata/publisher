// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenValue } from "@malloydata/malloy";
import { BadRequestError } from "../errors";
import { EnvironmentStore } from "../service/environment_store";
import {
   lazyCounter,
   lazyHistogram,
   REQUEST_DURATION_BUCKETS_MS,
   withSpan,
} from "../telemetry";
import type { CompiledDocument } from "../service/compile_document";
import type { CompileScope, TaggedLogMessage } from "../service/environment";
import {
   withLegacyDocumentKind,
   type LegacyKind,
} from "../service/legacy_report_views";

const compiles = lazyCounter(
   "publisher_compiles_total",
   "Compile requests (REST and MCP). Labels: scope ('append'|'file'|'package'), outcome ('success' compiled clean, 'error' answered with error problems, 'refused' threw).",
);
const compileDuration = lazyHistogram(
   "publisher_compile_duration_ms",
   "Wall-clock duration of a compile request. Labels: scope, outcome.",
   { buckets: REQUEST_DURATION_BUCKETS_MS },
);

export class CompileController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async compile(
      environmentName: string,
      packageName: string,
      modelName: string,
      source: unknown,
      includeSql: boolean = false,
      givens?: Record<string, GivenValue>,
      scope: CompileScope = "append",
      versionId?: unknown,
   ): Promise<{
      status: string;
      problems: TaggedLogMessage[];
      sql?: string;
      document?: CompiledDocument & { kind: LegacyKind };
   }> {
      const start = performance.now();
      let outcome = "refused";
      try {
         const result = await withSpan(
            "publisher.compile",
            { "malloy.package": packageName, "publisher.compile.scope": scope },
            () =>
               this.compileText(
                  environmentName,
                  packageName,
                  modelName,
                  source,
                  includeSql,
                  givens,
                  scope,
                  versionId,
               ),
         );
         outcome = result.status;
         return result;
      } finally {
         // An unknown scope is the caller's typo, not a label value.
         const label = ["append", "file", "package"].includes(scope)
            ? scope
            : "invalid";
         compiles().add(1, { scope: label, outcome });
         compileDuration().record(performance.now() - start, {
            scope: label,
            outcome,
         });
      }
   }

   private async compileText(
      environmentName: string,
      packageName: string,
      modelName: string,
      source: unknown,
      includeSql: boolean,
      givens: Record<string, GivenValue> | undefined,
      scope: CompileScope,
      versionId: unknown,
   ): Promise<{
      status: string;
      problems: TaggedLogMessage[];
      sql?: string;
      document?: CompiledDocument & { kind: LegacyKind };
   }> {
      // A JSON object with its own `length` would make the text readers loop to it.
      let text: string | undefined;
      if (typeof source === "string") text = source;
      else if (source !== undefined)
         throw new BadRequestError("`source` must be a string of Malloy text.");
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const { problems, sql, document } = await environment.compileSource(
         packageName,
         modelName,
         text,
         includeSql,
         givens,
         scope,
         versionId,
      );

      const hasErrors = problems.some((p) => p.severity === "error");

      return {
         status: hasErrors ? "error" : "success",
         problems: problems,
         ...(sql !== undefined && { sql }),
         // DEPRECATED: `document.kind`, the layout in the old vocabulary, which
         // existing callers of this route read.
         ...(document !== undefined && {
            document: withLegacyDocumentKind(document),
         }),
      };
   }
}
