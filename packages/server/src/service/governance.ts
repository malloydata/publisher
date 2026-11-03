// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The governance export: one deterministic document describing what a loaded
 * package serves, source by source, in policy terms. For every source, the
 * gates in force with the givens each reads, its `#(carries)` ceiling, the
 * control identifiers it declares, and every reachable field with its
 * classes, whether they were declared or inferred, what they were inferred
 * from, and the field's own controls. Everything comes from the COMPILED
 * model, not the files, so inherited gates, renamed fields and lineage are
 * what the server actually serves.
 *
 * Its reader is a pipeline, not a person: checked in next to the policy so a
 * pull request's diff shows what a model change does to a surface before it
 * deploys; fed to a review as the model inventory; handed to an auditor as
 * the control-to-implementation map. Ordering is by codepoint everywhere and
 * nothing in it depends on the clock, so two servers on identical bytes write
 * identical documents.
 */

import type { ModelDef } from "@malloydata/malloy";
import type { components } from "../api";
import { referencedGivenNames } from "./authorize";
import {
   findIrSource,
   isDeniedToAll,
   readCarries,
   readSourceClassifications,
   readSourceControls,
} from "./classification";
import type { Package } from "./package";

export type ApiPackageGovernance = components["schemas"]["PackageGovernance"];
type ApiGovernanceSource = components["schemas"]["GovernanceSource"];
type ApiGovernanceField = components["schemas"]["GovernanceField"];
type ApiGovernanceGate = components["schemas"]["GovernanceGate"];

const byCodepoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

function gates(
   expressions: readonly string[] | undefined,
): ApiGovernanceGate[] {
   return (expressions ?? []).map((expression) => ({
      expression,
      givenNames: referencedGivenNames(expression),
   }));
}

/**
 * The governance document for a loaded package. Every model the package
 * compiled is read, hidden ones included, each marked `published`; a model
 * that failed to compile contributes nothing and is named in `unreadModels`.
 */
export async function packageGovernance(
   pkg: Package,
   environmentName: string,
   packageName: string,
): Promise<ApiPackageGovernance> {
   const models = [
      ...(await pkg.listModels({ includeHiddenFilesAndSources: true })),
   ].sort((a, b) => byCodepoint(a.path ?? "", b.path ?? ""));
   const sources: ApiGovernanceSource[] = [];
   const unreadModels: string[] = [];
   let classifiesFields = false;
   for (const apiModel of models) {
      const modelPath = apiModel.path;
      if (!modelPath) continue;
      const model = pkg.getModel(modelPath);
      if (!model) {
         unreadModels.push(modelPath);
         continue;
      }
      let modelDef: ModelDef | undefined;
      let sourceInfos: ReturnType<typeof model.getSourceInfos>;
      let apiSources: ReturnType<typeof model.getSources>;
      try {
         modelDef = model.getModelDef();
         sourceInfos = model.getSourceInfos();
         apiSources = model.getSources();
      } catch {
         unreadModels.push(modelPath);
         continue;
      }
      if (!modelDef) {
         unreadModels.push(modelPath);
         continue;
      }
      const published = !(apiModel as { isHidden?: boolean }).isHidden;
      const names = (sourceInfos ?? []).map((s) => s.name).sort(byCodepoint);
      for (const name of names) {
         const ir = findIrSource(modelDef, name);
         if (!ir) continue;
         const apiSource = apiSources?.find((s) => s.name === name);
         const carries = readCarries(ir.annotations);
         const controls = readSourceControls(ir.annotations);
         const lineage = readSourceClassifications(ir.fields);
         const fields: ApiGovernanceField[] = Array.from(lineage)
            .filter(([, c]) => c.isField && !c.hidden)
            .sort(([a], [b]) => byCodepoint(a, b))
            .map(([path, c]) => {
               if (c.classifications.length > 0) classifiesFields = true;
               return {
                  path,
                  kind: c.kind,
                  classifications: c.classifications,
                  ...(c.inferred.length > 0 ? { inferred: true as const } : {}),
                  ...(c.inputs.length > 0 ? { inputs: c.inputs } : {}),
                  ...(c.controls.length > 0 ? { controls: c.controls } : {}),
                  ...(c.malformed ? { malformed: c.malformed } : {}),
               };
            });
         sources.push({
            modelPath,
            name,
            published,
            deniedToAll: apiSource ? isDeniedToAll(apiSource) : false,
            authorize: gates(apiSource?.authorize),
            accessFilter: gates(apiSource?.accessFilter),
            ...(carries.declared
               ? { carries: Array.from(carries.classes).sort(byCodepoint) }
               : {}),
            controls: controls.values,
            fields,
         });
      }
   }
   return {
      environmentName,
      packageName,
      classifiesFields,
      sources,
      ...(unreadModels.length > 0 ? { unreadModels } : {}),
   };
}
