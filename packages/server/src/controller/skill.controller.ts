// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { AgentNotFoundError, SkillNotFoundError } from "../errors";
import {
   BUNDLED_CATALOG,
   fileChangedMessage,
   namedSkillFiles,
   resolveSkills,
   skillFilePath,
   skillFileView,
   type ResolvedSkill,
} from "../service/skills/package_skills";
import type { PackageLookup } from "../service/environment_ports";
import { VersionRef } from "../service/identity";
import { loadedPackage } from "./loaded_package";

type ApiAgent = components["schemas"]["Agent"];
type ApiAgentSummary = components["schemas"]["AgentSummary"];
type ApiAgentSkillRead = components["schemas"]["AgentSkillRead"];
type ApiSkill = components["schemas"]["Skill"];
type ApiSkillSummary = components["schemas"]["SkillSummary"];

/**
 * The REST half of the `get_skill` and `get_agent` MCP tools: the bundled
 * guides, shadowed by the package's own `skills/` of the same name and read
 * file by file by path, and the package's declared agents. Serves state read at load; nothing compiles.
 * Every route takes the package's `versionId`, so a version's own skills and
 * agents are served, never `latest`'s.
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
      const read = skill.read(relative);
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
         ...("text" in read
            ? {}
            : {
                 message:
                    "message" in read
                       ? read.message
                       : `'${relative}' is a binary file (${skill.catalog.files[relative]!.size} bytes), and only text files are served.`,
              }),
      };
   }

   /** Name, description, model, schedules and budget of every agent the package serves. */
   public async listAgents(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiAgentSummary[]> {
      return (
         await this.packageOf(environmentName, packageName, versionId)
      ).listAgents();
   }

   /** One agent resolved to its instructions, skill files and schedules. */
   public async getAgent(
      environmentName: string,
      packageName: string,
      agentName: string,
      skills: "full" | "summary" = "full",
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiAgent> {
      const agent = (
         await this.packageOf(environmentName, packageName, versionId)
      ).getAgent(agentName, skills);
      if (!agent) {
         throw new AgentNotFoundError(
            `Agent '${agentName}' not found in package '${packageName}'.`,
         );
      }
      return agent;
   }

   /** One file of an agent's skills, by a `relative_filepath` from the summary. */
   public async getAgentSkillFile(
      environmentName: string,
      packageName: string,
      agentName: string,
      relativeFilepath: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiAgentSkillRead> {
      const file = (
         await this.packageOf(environmentName, packageName, versionId)
      ).getAgentSkillFile(agentName, relativeFilepath);
      if (!file) {
         throw new AgentNotFoundError(
            `Skill file '${relativeFilepath}' not found for agent '${agentName}' in package '${packageName}'.`,
         );
      }
      return file;
   }
}
