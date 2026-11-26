// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { SkillNotFoundError, WorkflowNotFoundError } from "../errors";
import {
   BUNDLED_CATALOG,
   fileChangedMessage,
   namedSkillFiles,
   resolveSkills,
   skillFilePath,
   readSkillFile,
   skillFileView,
   type ResolvedSkill,
} from "../service/skills/package_skills";
import type { PackageLookup } from "../service/environment_ports";
import { VersionRef } from "../service/identity";
import { loadedPackage } from "./loaded_package";

type ApiWorkflow = components["schemas"]["Workflow"];
type ApiWorkflowSummary = components["schemas"]["WorkflowSummary"];
type ApiWorkflowSkillRead = components["schemas"]["WorkflowSkillRead"];
type ApiSkill = components["schemas"]["Skill"];
type ApiSkillSummary = components["schemas"]["SkillSummary"];

/**
 * The REST half of the `get_skill` and `get_workflow` MCP tools: the bundled
 * guides, shadowed by the skills of the package's `default` workflow of the
 * same name and read file by file by path, and the package's declared
 * workflows. Serves state read at load; nothing compiles.
 * Every route takes the package's `versionId`, so a version's own skills and
 * workflows are served, never `latest`'s.
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
      return resolveSkills(BUNDLED_CATALOG, p.listSkills());
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

   /**
    * One file of a skill, its `SKILL.md` when `path` is omitted. Text comes
    * back verbatim as `content`; a file with none (binary, or one a bundled
    * skill does not carry) comes back as its `size` and a `message`, never as
    * bytes. A `SKILL.md` also carries the skill's `frontmatter` and every file.
    */
   public async getSkill(
      environmentName: string,
      packageName: string,
      skillName: string,
      path?: string,
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
      const relative = skillFilePath(path);
      if (!Object.hasOwn(skill.catalog.files, relative)) {
         throw new SkillNotFoundError(
            `File '${relative}' not found in skill '${skillName}' of package '${packageName}'. Its files: ${namedSkillFiles(skill)}.`,
         );
      }
      const read = readSkillFile(skill, relative);
      if ("changed" in read) {
         throw new SkillNotFoundError(fileChangedMessage(relative));
      }
      return {
         ...skillFileView(
            skill,
            relative,
            "text" in read ? read.text : undefined,
         ),
         description: skill.description,
         ...("text" in read ? {} : { message: read.message }),
      };
   }

   /** Name, description, instructions flag and skill sets of every workflow the package serves. */
   public async listWorkflows(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiWorkflowSummary[]> {
      return (
         await this.packageOf(environmentName, packageName, versionId)
      ).listWorkflows();
   }

   /** One workflow resolved to its instructions and skill sets, or to one set with `skillSet`. */
   public async getWorkflow(
      environmentName: string,
      packageName: string,
      workflowName: string,
      skills: "full" | "summary" = "full",
      skillSet?: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiWorkflow> {
      const workflow = (
         await this.packageOf(environmentName, packageName, versionId)
      ).getWorkflow(workflowName, skills, skillSet);
      if (!workflow) {
         throw new WorkflowNotFoundError(
            skillSet === undefined
               ? `Workflow '${workflowName}' not found in package '${packageName}'.`
               : `Workflow '${workflowName}' with a skill set '${skillSet}' not found in package '${packageName}'.`,
         );
      }
      return workflow;
   }

   /** One file of a workflow's skills, by a `<skill>/<file>` path from the summary, read inside one set with `skillSet`. */
   public async getWorkflowSkillFile(
      environmentName: string,
      packageName: string,
      workflowName: string,
      relativeFilepath: string,
      skillSet?: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiWorkflowSkillRead> {
      const file = (
         await this.packageOf(environmentName, packageName, versionId)
      ).getWorkflowSkillFile(workflowName, relativeFilepath, skillSet);
      if (!file) {
         throw new WorkflowNotFoundError(
            skillSet === undefined
               ? `Skill file '${relativeFilepath}' not found for workflow '${workflowName}' in package '${packageName}'.`
               : `Skill file '${relativeFilepath}' not found in skill set '${skillSet}' of workflow '${workflowName}' in package '${packageName}'.`,
         );
      }
      return file;
   }
}
