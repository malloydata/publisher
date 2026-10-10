// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { AgentNotFoundError, SkillNotFoundError } from "../errors";
import skillsBundle from "../mcp/skills/skills_bundle.json";
import { type SkillEntry } from "../mcp/skills/build_skills_bundle";
import { resolveSkills } from "../mcp/skills/package_skills";
import { EnvironmentStore } from "../service/environment_store";

type ApiAgent = components["schemas"]["Agent"];
type ApiAgentSummary = components["schemas"]["AgentSummary"];
type ApiSkill = components["schemas"]["Skill"];
type ApiSkillSummary = components["schemas"]["SkillSummary"];

const BUNDLED_SKILLS = (skillsBundle as { skills: SkillEntry[] }).skills;

/**
 * Read-only access to the agent skills in force for a package: the server's
 * bundled guides, with the package's own `skills/` shadowing any of the same
 * name. The REST half of the `get_skill` MCP tool, for callers running
 * unattended with no MCP client (see docs/ai-agents.md).
 *
 * Also serves the package's declared agents, the REST half of `get_agent`.
 *
 * Serves state the package read at load, so no route compiles or queries
 * anything.
 */
export class SkillController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   private async resolvedSkills(
      environmentName: string,
      packageName: string,
   ): Promise<Array<SkillEntry & { origin: "bundled" | "package" }>> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const p = await environment.getPackage(packageName, false);
      return resolveSkills(BUNDLED_SKILLS, p.listSkills());
   }

   /** Name, description and origin for every skill in force for the package. */
   public async listSkills(
      environmentName: string,
      packageName: string,
   ): Promise<ApiSkillSummary[]> {
      const resolved = await this.resolvedSkills(environmentName, packageName);
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
   ): Promise<ApiSkill> {
      const resolved = await this.resolvedSkills(environmentName, packageName);
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
   ): Promise<ApiAgentSummary[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return (await environment.getPackage(packageName, false)).listAgents();
   }

   /** One agent resolved to its instructions, skill files and schedules. */
   public async getAgent(
      environmentName: string,
      packageName: string,
      agentName: string,
   ): Promise<ApiAgent> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const agent = (await environment.getPackage(packageName, false)).getAgent(
         agentName,
      );
      if (!agent) {
         throw new AgentNotFoundError(
            `Agent '${agentName}' not found in package '${packageName}'.`,
         );
      }
      return agent;
   }
}
