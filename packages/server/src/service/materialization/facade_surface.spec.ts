// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package facade's public surface, listed: every public method and
 * getter of `PackageMaterialization`. A name added here is a new door into
 * the module, so it is a reviewed change; a name the class no longer has
 * fails until it is deleted from the list, so the surface only shrinks.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import * as ts from "typescript";

/** Every public member name of `className` in `file`, sorted. */
function publicMembersOf(file: string, className: string): string[] {
   const sf = ts.createSourceFile(
      file,
      fs.readFileSync(file, "utf8"),
      ts.ScriptTarget.Latest,
      true,
   );
   const names: string[] = [];
   const visit = (node: ts.Node): void => {
      if (ts.isClassDeclaration(node) && node.name?.text === className) {
         for (const member of node.members) {
            if (
               !(
                  ts.isMethodDeclaration(member) ||
                  ts.isGetAccessorDeclaration(member) ||
                  ts.isPropertyDeclaration(member)
               )
            ) {
               continue;
            }
            const modifiers = ts.getModifiers(member) ?? [];
            const hidden = modifiers.some(
               (m) =>
                  m.kind === ts.SyntaxKind.PrivateKeyword ||
                  m.kind === ts.SyntaxKind.ProtectedKeyword,
            );
            if (hidden || !member.name || ts.isPrivateIdentifier(member.name)) {
               continue;
            }
            names.push(member.name.getText(sf));
         }
      }
      ts.forEachChild(node, visit);
   };
   visit(sf);
   return [...new Set(names)].sort();
}

const FACADE_SURFACE = [
   "annotationFindingWarnings",
   "applyLoadBuildPlan",
   "asOfBuildWarnings",
   "bind",
   "bindCompiled",
   "incompatibleTables",
   "bindingState",
   "boundDatabaseEntries",
   "boundIncrementalEntries",
   "boundLateRows",
   "boundParentReads",
   "boundStorageColumns",
   "boundStorageContract",
   "boundTableContract",
   "chainBuildText",
   "dataInliningWarnings",
   "databaseTableUnreadWarnings",
   "dispose",
   "findingRefusalWarnings",
   "forgetKeyUniqueness",
   "formatInvalidAnnotations",
   "formatInvalidPersistencePolicy",
   "formatInvalidPreaggregatePolicy",
   "getBuildManifestEntries",
   "getBuildPlan",
   "getFreshBuildManifest",
   "getFreshCompilerManifest",
   "getPreaggregateEntityIds",
   "incrementalCheckWarnings",
   "hasBoundTableNameManifest",
   "hasStorageServeBindings",
   "hasUnreadDatabaseTables",
   "incrementalPolicyAdvisories",
   "invalidateCompiledLegs",
   "isBuildPlanStale",
   "isBuildPlanUnavailable",
   "isHostOwned",
   "keptTableWarnings",
   "lastBuildPlanRefreshFailedTransiently",
   "lateRowWarnings",
   "loadGateText",
   "markBuildPlanStale",
   "markHostOwned",
   "markManifestBindFailed",
   "markManifestBound",
   "markServingLive",
   "nativeSchemaWarnings",
   "nearMissLines",
   "noteIncrementalCheck",
   "persistRefusalWarnings",
   "persistenceCollisionRefusals",
   "persistenceCollisionWarnings",
   "persistencePolicyWarnings",
   "plan",
   "planConnectionNames",
   "previewServeVerdicts",
   "pushServeDestinationConfigToModels",
   "pushServedCopyContextToModels",
   "readDatabaseColumns",
   "readStorageColumns",
   "refreshBuildPlan",
   "repeatedJoinOneKeyWarnings",
   "reportIncompatible",
   "serveVerdicts",
   "servedCopyWarnings",
   "setDeploymentFacts",
   "setKeptTables",
   "setServeDestinationConfig",
   "setServeDestinationSchema",
   "setUnprovenGenerations",
   "snapshot",
   "storageWarnings",
   "storedColumnTypeWarnings",
   "unprovenGenerationWarnings",
   "verdictWarnings",
   "verdicts",
   "whenBuildPlanCurrent",
   "wireServingResolvers",
];

describe("the package facade's public surface", () => {
   it("is exactly the listed names", () => {
      expect(
         publicMembersOf(
            path.join(__dirname, "package_materialization.ts"),
            "PackageMaterialization",
         ),
      ).toEqual([...FACADE_SURFACE].sort());
   });
});
