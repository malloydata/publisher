// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { AgentNotFoundError, SkillNotFoundError } from "../errors";
import { type SkillEntry } from "../mcp/skills/build_skills_bundle";
import { BUNDLED_SKILLS, resolveSkills } from "../mcp/skills/package_skills";
import { EnvironmentStore } from "../service/environment_store";
import { versionOptions } from "./report.controller";

type ApiAgent = components["schemas"]["Agent"];
type ApiAgentSummary = components["schemas"]["AgentSummary"];
type ApiAgentSkillRead = components["schemas"]["AgentSkillRead"];
type ApiSkill = components["schemas"]["Skill"];
type ApiSkillSummary = components["schemas"]["SkillSummary"];

/**
 * The REST half of the `get_skill` and `get_agent` MCP tools: the bundled
 * guides, shadowed by the package's own `skills/` of the same name, and the
 * package's declared agents. Serves state read at load; nothing compiles.
 * Every route takes the package's `versionId`, so a version's own skills and
 * agents are served, never `latest`'s.
 */
export class SkillController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   private async packageOf(
      environmentName: string,
      packageName: string,
      versionId: unknown,
   ) {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return environment.getPackage(
         packageName,
         false,
         ...versionOptions(versionId),
      );
   }

   private async resolvedSkills(
      environmentName: string,
      packageName: string,
      versionId: unknown,
   ): Promise<Array<SkillEntry & { origin: "bundled" | "package" }>> {
      const p = await this.packageOf(environmentName, packageName, versionId);
      return resolveSkills(BUNDLED_SKILLS, p.listSkills());
   }

   /** Name, description and origin for every skill in force for the package. */
   public async listSkills(
      environmentName: string,
      packageName: string,
      versionId?: unknown,
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
    * One skill with its body.
    *
    * `skillName` may name a reference file as `<skill>/<stem>`, so a caller
    * that received that pointer in a body can fetch what it points at.
    */
   public async getSkill(
      environmentName: string,
      packageName: string,
      skillName: string,
      versionId?: unknown,
   ): Promise<ApiSkill> {
      const resolved = await this.resolvedSkills(
         environmentName,
         packageName,
         versionId,
      );
      const match = resolved.find((skill) => skill.name === skillName);
      if (!match) {
         throw new SkillNotFoundError(
            `Skill '${skillName}' not found in package '${packageName}'.`,
         );
      }
      return {
         name: match.name,
         description: match.description,
         content: match.body,
         origin: match.origin,
      };
   }

   /** Name, description, model and schedules of every agent the package serves. */
   public async listAgents(
      environmentName: string,
      packageName: string,
      versionId?: unknown,
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
      versionId?: unknown,
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
      versionId?: unknown,
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
