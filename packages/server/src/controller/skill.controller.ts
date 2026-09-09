// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { getPackageSkillsMode } from "../config";
import { SkillNotFoundError } from "../errors";
import skillsBundle from "../mcp/skills/skills_bundle.json";
import { type SkillEntry } from "../mcp/skills/build_skills_bundle";
import { resolveSkills } from "../mcp/skills/package_skills";
import { EnvironmentStore } from "../service/environment_store";

type ApiSkill = components["schemas"]["Skill"];
type ApiSkillSummary = components["schemas"]["SkillSummary"];

const BUNDLED_SKILLS = (skillsBundle as { skills: SkillEntry[] }).skills;

/**
 * Read-only access to the agent skills in force for a package: the server's
 * bundled guides, with the package's own `skills/` shadowing any of the same
 * name. The REST half of the `get_skill` MCP tool, for callers running
 * unattended with no MCP client (see docs/ai-agents.md).
 *
 * Serves state the package read at load, so neither route compiles or queries
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
      // `off` means the deployment's own harness installs skills and does not
      // want the server offering a second copy, so the package's set is
      // withheld here exactly as it is on the MCP tool.
      const packageSkills =
         getPackageSkillsMode() === "on" ? p.listSkills() : [];
      return resolveSkills(BUNDLED_SKILLS, packageSkills);
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
}
