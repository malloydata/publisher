// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { GivenValue } from "@malloydata/malloy";
import { BadRequestError } from "../errors";
import { EnvironmentStore } from "../service/environment_store";
import type { CompiledDocument } from "../service/compile_document";
import type { CompileScope, TaggedLogMessage } from "../service/environment";

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
      versionId?: string,
   ): Promise<{
      status: string;
      problems: TaggedLogMessage[];
      sql?: string;
      document?: CompiledDocument;
   }> {
      // A JSON body can send an object with its own `length`, which the text readers would loop to.
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

      // Determine overall status based on presence of errors
      const hasErrors = problems.some((p) => p.severity === "error");

      return {
         status: hasErrors ? "error" : "success",
         problems: problems,
         ...(sql !== undefined && { sql }),
         ...(document !== undefined && { document }),
      };
   }
}
