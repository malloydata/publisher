// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { getPackageSkillsMode } from "../../config";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { type SkillEntry } from "../skills/build_skills_bundle";
import { resolveSkills, type ResolvedSkill } from "../skills/package_skills";
import { jsonResource, jsonToolError } from "../tool_response";

const GET_SKILL_DESCRIPTION = `Fetch a task guide, or list the guides available. Guides are concise procedural instructions for working with Malloy and this server: query patterns, chart and rendering tags, analysis pitfalls, how to build or review a model.

A package can ship guides of its OWN, describing how to use that package specifically: which source answers which question, what the business terms mean here, which conventions apply. Pass \`scopes\` to see them. A package guide whose name matches a built-in one REPLACES it for that package, so the package's version is what you get back; \`origin\` on every entry says which you are reading.

## Call modes
- No arguments: list the built-in guides (name plus a one-line description).
- \`scopes\`: list the guides in force for that package, built-in and package alike.
- \`skill_name\`: fetch that guide's full Markdown. Combine with \`scopes\` to get the package's version when it has one.

## Parameters
- skill_name (optional): a name from \`availableSkills\`. Omit to list.
- scopes (optional): exactly one {environment, package}, from list_packages. Omit for the built-in set only.

## Response
- skill: the requested guide ({name, content, origin}), or null when listing or when skill_name did not match.
- availableSkills: always present; every guide in scope with its description and origin.
- message: only when skill_name did not match; says so and points at availableSkills.

\`origin\` is "package" for a guide the package ships (including one replacing a built-in) and "bundled" for a built-in. Reading a guide is not the same as having it available: fetch the one that covers what you are about to do before you do it.`;

const getSkillShape = {
   skill_name: z
      .string()
      .min(1)
      .max(200)
      .nullish()
      .describe(
         "Name of the guide to fetch, as returned in availableSkills. Omit to list.",
      ),
   scopes: z
      .array(
         z.object({
            environment: z.string().describe("Environment name."),
            package: z.string().describe("Package name."),
         }),
      )
      .max(1)
      .nullish()
      .describe(
         "Exactly one {environment, package}. Scopes the listing to the guides in force for that package. Omit for the built-in set.",
      ),
};

type GetSkillParams = z.infer<z.ZodObject<typeof getSkillShape>>;

/** Name and description only: what a caller reads before choosing a body. */
function summarize(skills: ResolvedSkill[]) {
   return skills.map((skill) => ({
      name: skill.name,
      description: skill.description,
      origin: skill.origin,
   }));
}

/**
 * Registers the get_skill MCP tool: serves the bundled guides, and the guides a
 * package ships under `skills/`, with the package's copy shadowing a bundled
 * one of the same name.
 *
 * Why a tool and not more MCP prompts. The bundled guides are already
 * registered as prompts, once, at server start. A package's guides cannot be:
 * packages load, reload, and are added at runtime, so a per-package prompt
 * would need prompt-list-changed notifications for a set that every client
 * would then have to disambiguate by name across packages. A tool takes the
 * package as an argument, which is what the question actually has in it.
 *
 * SECURITY: returns author-written guidance from the package's own tree, the
 * same trust level as the `#(doc)` annotations already returned by get_context,
 * and no row data. `origin` is on every entry so a caller can see when a
 * package has replaced a built-in guide rather than inferring it.
 */
export function registerGetSkillTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
   bundledSkills: SkillEntry[],
): void {
   mcpServer.tool(
      "get_skill",
      GET_SKILL_DESCRIPTION,
      getSkillShape,
      async (params: GetSkillParams) => {
         const scope = params.scopes?.[0];
         const uri = buildMalloyUri(
            scope
               ? { environment: scope.environment, package: scope.package }
               : {},
            "getSkill",
         );

         try {
            let resolved = resolveSkills(bundledSkills, []);
            if (scope && getPackageSkillsMode() === "on") {
               const environment = await environmentStore.getEnvironment(
                  scope.environment,
                  false,
               );
               const pkg = await environment.getPackage(scope.package, false);
               resolved = resolveSkills(bundledSkills, pkg.listSkills());
            }

            const availableSkills = summarize(resolved);
            if (params.skill_name === undefined || params.skill_name === null) {
               return jsonResource(uri, { skill: null, availableSkills });
            }

            const match = resolved.find((s) => s.name === params.skill_name);
            if (!match) {
               return jsonResource(uri, {
                  skill: null,
                  availableSkills,
                  message: `No guide named ${JSON.stringify(params.skill_name)}${
                     scope
                        ? ` for package ${scope.environment}/${scope.package}`
                        : ""
                  }. Fix: use a name from availableSkills exactly as spelled there${
                     scope
                        ? "."
                        : ", or pass scopes to include a package's own guides."
                  }`,
               });
            }

            return jsonResource(uri, {
               skill: {
                  name: match.name,
                  content: match.body,
                  origin: match.origin,
               },
               availableSkills,
            });
         } catch (error) {
            logger.warn("[MCP Tool getSkill] lookup failed", {
               skillName: params.skill_name,
               environmentName: scope?.environment,
               packageName: scope?.package,
               error: error instanceof Error ? error.message : String(error),
            });
            const errorDetails: ErrorDetails = classifyToolError(
               "getSkill",
               scope ? `${scope.environment}/${scope.package}` : "skills",
               error,
            );
            return jsonToolError(uri, errorDetails);
         }
      },
   );
}
