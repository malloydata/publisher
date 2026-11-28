// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import {
   SkillNotFoundError,
   SkillSetNotFoundError,
   WorkflowNotFoundError,
} from "../errors";
import {
   BUNDLED_CATALOG,
   fileChangedMessage,
   resolveSkills,
   skillFileView,
   type ResolvedSkill,
} from "../service/skills/package_skills";
import { SKILL_MD } from "../service/skills/skill_files";
import type { PackageLookup } from "../service/environment_ports";
import { VersionRef } from "../service/identity";
import { loadedPackage } from "./loaded_package";

type ApiWorkflow = components["schemas"]["Workflow"];
type ApiWorkflowSummary = components["schemas"]["WorkflowSummary"];
type ApiSkillSet = components["schemas"]["SkillSet"];
type ApiSkillSetSummary = components["schemas"]["SkillSetSummary"];
type ApiSkillSetFileRead = components["schemas"]["SkillSetFileRead"];
type ApiSkill = components["schemas"]["Skill"];
type ApiSkillSummary = components["schemas"]["SkillSummary"];

/**
 * The REST half of the `get_skill`, `get_skill_set` and `get_workflow` MCP
 * tools: the bundled guides with the package's `default` skill set's added
 * (replacing a bundled one only where that set's `overrides` says so), the
 * package's declared skill sets, whose file route is the one REST read of a
 * skill's other files, and its declared workflows. Serves state read at load;
 * nothing compiles. Every route takes the package's `versionId`, so a version's
 * own skills, sets and workflows are served, never `latest`'s.
 */
export class SkillController {
   private environmentStore: PackageLookup;

   constructor(environmentStore: PackageLookup) {
      this.environmentStore = environmentStore;
   }

   private packageOf(
      environmentName: string,
      packageName: string,
      versionId: VersionRef,
   ) {
      return loadedPackage(
         this.environmentStore,
         environmentName,
         packageName,
         versionId,
      );
   }

   private async resolvedSkills(
      environmentName: string,
      packageName: string,
      versionId: VersionRef,
   ): Promise<ResolvedSkill[]> {
      const p = await this.packageOf(environmentName, packageName, versionId);
      return resolveSkills(BUNDLED_CATALOG, p.defaultSkills());
   }

   /** Name, description and origin for every skill in force for the package. */
   public async listSkills(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiSkillSummary[]> {
      const resolved = await this.resolvedSkills(
         environmentName,
         packageName,
         versionId,
      );
      return resolved.map((skill) => ({
         name: skill.name,
         description: skill.description,
         origin: skill.origin,
      }));
   }

   /** A skill's `SKILL.md`, verbatim as `content`, with its `frontmatter` and every file it holds. */
   public async getSkill(
      environmentName: string,
      packageName: string,
      skillName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiSkill> {
      const resolved = await this.resolvedSkills(
         environmentName,
         packageName,
         versionId,
      );
      const skill = resolved.find((s) => s.name === skillName);
      if (!skill) {
         throw new SkillNotFoundError(
            `Skill '${skillName}' not found in package '${packageName}'.`,
         );
      }
      // A SKILL.md is held inline from the load, so it always reads as text.
      const { text } = (await skill.read(SKILL_MD)) as { text: string };
      return {
         ...skillFileView(skill, SKILL_MD, text),
         description: skill.description,
      };
   }

   /** Name, description and skill set names of every workflow the package serves. */
   public async listWorkflows(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiWorkflowSummary[]> {
      return (
         await this.packageOf(environmentName, packageName, versionId)
      ).listWorkflows();
   }

   /** One workflow resolved to its instructions and every skill set it lists, whole. */
   public async getWorkflow(
      environmentName: string,
      packageName: string,
      workflowName: string,
      skills: "full" | "summary" = "full",
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiWorkflow> {
      const pkg = await this.packageOf(environmentName, packageName, versionId);
      const workflow = pkg.getWorkflow(workflowName, skills);
      if (!workflow) {
         const unserved = pkg.workflowNotServed(workflowName)!;
         throw new WorkflowNotFoundError(unserved.message, unserved.reason);
      }
      return workflow;
   }

   /** Name, description and digest of every skill set the package serves, in name order. */
   public async listSkillSets(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiSkillSetSummary[]> {
      return (
         await this.packageOf(environmentName, packageName, versionId)
      ).listSkillSets();
   }

   /** One skill set, whole. */
   public async getSkillSet(
      environmentName: string,
      packageName: string,
      skillSetName: string,
      skills: "full" | "summary" = "full",
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiSkillSet> {
      const pkg = await this.packageOf(environmentName, packageName, versionId);
      const set = pkg.getSkillSet(skillSetName, skills);
      if (!set) {
         const unserved = pkg.skillSetNotServed(skillSetName)!;
         throw new SkillSetNotFoundError(unserved.message, unserved.reason);
      }
      return set;
   }

   /** One file of a skill set, by a `<skill>/<file>` path from the set's summary or its assets. */
   public async getSkillSetFile(
      environmentName: string,
      packageName: string,
      skillSetName: string,
      relativeFilepath: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiSkillSetFileRead> {
      const pkg = await this.packageOf(environmentName, packageName, versionId);
      const file = await pkg.getSkillSetFile(skillSetName, relativeFilepath);
      if (file && "changed" in file) {
         throw new SkillSetNotFoundError(fileChangedMessage(relativeFilepath));
      }
      if (!file) {
         const unserved = pkg.skillSetNotServed(skillSetName);
         if (unserved) {
            throw new SkillSetNotFoundError(unserved.message, unserved.reason);
         }
         throw new SkillSetNotFoundError(
            `Skill file '${relativeFilepath}' not found in skill set '${skillSetName}' in package '${packageName}'.`,
         );
      }
      return file;
   }
}
