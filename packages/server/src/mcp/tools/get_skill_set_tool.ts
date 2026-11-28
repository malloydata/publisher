// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
import { API_PREFIX } from "../../constants";
import type { PackageLookup } from "../../service/environment_ports";
import { SkillSetsUnavailableError } from "../../errors";
import { fileChangedMessage } from "../../service/skills/package_skills";
import { buildMalloyUri } from "../handler_utils";
import { registerGuardedTool } from "../register_tool";
import type { RequestExtra } from "../request_extra";
import { jsonResource } from "../tool_response";
import { parseEnvironmentName, parsePackageName } from "../../service/identity";
import {
   fileTextChars,
   INSTALL_RULE,
   restQuery,
   sendLadder,
   tooLarge,
   withAvailable,
   type ListShape,
   type Payload,
} from "./skill_reply";

const GET_SKILL_SET_DESCRIPTION = `List the skill sets a package serves, fetch one whole, or read one of its files. A skill set is a folder of skills the package author wrote, each a SKILL.md and the files it names; a package's \`default\` set is its own skills, which get_skill already advertises. Other sets are for a workflow that lists them, or a chat that picks one.

## Call modes
- \`scopes\` only: list the sets (name, description, digest).
- plus \`skill_set\`: that set whole, every skill it holds.
- plus \`skills: "summary"\`: each skill file reduced to {relative_filepath, name, description, bytes}.
- plus \`skill_path\`: one file, {relative_filepath, file_contents, digest, source}, by a \`<skill>/<file>\` path from the summary or the set's \`assets\`.

## Response
- skillSet: the set; null when listing or when nothing matched; absent when skill_path returns a file.
- skillFile: only with skill_path; skills is ignored. A missing, binary or oversize file gets skillFile.message.
- availableSkillSets: always present; the sets served, as many as fit.
- message: when skill_set did not match, or the reply was cut to fit the tool-result limit (REST serves all). A cut keeps the set's {name, digest} and its source before it keeps less.
- reason: with skillSet null for an unserved set: SKILL_SET_NOT_SERVED (refused; message says why) or SKILL_SET_NOT_DECLARED.
- digest: pins exactly what the set serves; two sets with the same content have the same digest.
- source: sourceContentSha says which bytes this came from; report it when you use it.

${INSTALL_RULE}`;

const getSkillSetShape = {
   skill_set: z
      .string()
      .min(1)
      .max(200)
      .nullish()
      .describe(
         "Name of the skill set to fetch, as returned in availableSkillSets. Omit to list.",
      ),
   skills: z
      .enum(["full", "summary"])
      .nullish()
      .describe(
         'With skill_set: "full" (default) returns every skill file in full; "summary" returns each file\'s relative_filepath, name, description and bytes without its text.',
      ),
   skill_path: z
      .string()
      .min(1)
      .max(500)
      .nullish()
      .describe(
         "With skill_set: return only this skill file or asset, a <skill>/<file> relative_filepath from the summary or the set's assets in the full view.",
      ),
   scopes: z
      .array(
         z.object({
            environment: z.string().describe("Environment name."),
            package: z.string().describe("Package name."),
         }),
      )
      .length(1)
      .describe("Exactly one {environment, package}, from list_packages."),
};

type GetSkillSetParams = z.infer<z.ZodObject<typeof getSkillSetShape>>;

const SKILL_SETS: ListShape = {
   key: "availableSkillSets",
   noun: "skill sets",
   fetch: "`skill_set` name",
};

/**
 * Registers the get_skill_set MCP tool: serves the skill sets a package
 * declares in `publisher.json`, each whole, and the one file of a set a caller
 * asks for.
 *
 * Skill sets are an explicit choice, so nothing advertises them beyond
 * get_skill's listing of the `default` set's skills.
 *
 * SECURITY: returns author-written text from the package's tree, no row data.
 * A SKILL.md arrives as written, so its frontmatter may name tools or hooks
 * that nothing here removes, and publishing is unauthenticated on a shared
 * Publisher: an installer writes only `name` and `description` from the entry's
 * fields and names any other key to the user (the install rule in the tool's
 * description).
 */
export function registerGetSkillSetTool(
   mcpServer: McpServer,
   environmentStore: PackageLookup,
): void {
   registerGuardedTool(mcpServer, {
      name: "get_skill_set",
      description: GET_SKILL_SET_DESCRIPTION,
      inputSchema: getSkillSetShape,
      op: "getSkillSet",
      failed: "lookup failed",
      uri: ({ scopes: [scope] }) =>
         buildMalloyUri(
            { environment: scope!.environment, package: scope!.package },
            "getSkillSet",
         ),
      identifier: ({ scopes: [scope] }) =>
         `${scope!.environment}/${scope!.package}`,
      logFields: (params) => ({
         skillSet: params.skill_set,
         skillPath: params.skill_path,
         environmentName: params.scopes[0]!.environment,
         packageName: params.scopes[0]!.package,
      }),
      run: async (params: GetSkillSetParams, uri, extra: RequestExtra) => {
         const scope = params.scopes[0]!;
         const environment = await environmentStore.getEnvironment(
            parseEnvironmentName(scope.environment),
            false,
         );
         const pkg = await environment.getPackage(
            parsePackageName(scope.package),
            false,
         );
         const name = params.skill_set ?? undefined;
         let available: ReturnType<typeof pkg.listSkillSets>;
         try {
            available = pkg.listSkillSets();
         } catch (error) {
            if (
               !(error instanceof SkillSetsUnavailableError) ||
               name === undefined
            ) {
               throw error;
            }
            // Throws for a name the manifest declares and did not refuse; an undeclared or refused one is still not found.
            pkg.getSkillSet(name);
            // No availableSkillSets: an empty list would read as a package with no skill sets. An undeclared name has no Fix clause: its advice is to look in the list.
            const unserved = pkg.skillSetNotServed(name, false)!;
            return jsonResource(uri, {
               skillSet: null,
               reason: unserved.reason,
               message: `${unserved.message} No skill set can be listed: ${error.message}`,
            });
         }
         const id = extra?.requestId;
         // An answer short by nature: sent with the list cut to what is left, whether or not that is enough.
         const send = (payload: Payload) =>
            jsonResource(
               uri,
               withAvailable(uri, id, SKILL_SETS, available, payload).payload,
            );
         if (name === undefined) {
            return send({
               skillSet: null,
               ...(params.skill_path && {
                  message:
                     "skill_path needs skill_set. Fix: name the set from availableSkillSets whose file you want.",
               }),
            });
         }
         const unserved = pkg.skillSetNotServed(name);
         if (unserved) return send({ skillSet: null, ...unserved });

         const rest = `${API_PREFIX}/environments/${scope.environment}/packages/${scope.package}/skill-sets/${name}`;
         if (params.skill_path) {
            const skillFile = await pkg.getSkillSetFile(
               name,
               params.skill_path,
            );
            // No `skillSet` key in any answer to a file read: it must not look like "no set matched".
            if (!skillFile) {
               return send({
                  skillFile: {
                     relative_filepath: params.skill_path,
                     message: `Skill set ${JSON.stringify(name)} has no skill file ${JSON.stringify(params.skill_path)}. Fix: use a relative_filepath exactly as the set lists it (a skill file in the summary, an asset in the full view).`,
                  },
               });
            }
            if ("changed" in skillFile) {
               return send({
                  skillFile: {
                     relative_filepath: params.skill_path,
                     message: fileChangedMessage(params.skill_path),
                  },
               });
            }
            const path = params.skill_path.split("/").map(encodeURIComponent);
            return sendLadder(uri, id, SKILL_SETS, available, [
               {
                  view: "file",
                  floor: skillFile.file_contents?.length,
                  build: () => ({ skillFile }),
               },
               {
                  view: "refusal",
                  build: (prior) => ({
                     skillFile: {
                        relative_filepath: skillFile.relative_filepath,
                        message: `'${params.skill_path}' would be ${prior!.atLeast ? "at least " : ""}${prior!.chars} characters as a tool result, over the limit, so get_skill_set cannot return it. REST has no such limit: GET ${rest}/skills/${path.join("/")}${restQuery(skillFile.source)}`,
                        digest: skillFile.digest,
                        source: skillFile.source,
                     },
                  }),
               },
            ]);
         }

         const whole = pkg.getSkillSet(name, params.skills ?? "full")!;
         const summary = pkg.getSkillSet(name, "summary")!;
         const read = `REST has no such limit: GET ${rest}${restQuery(whole.source, "skills=summary")}, and one file is read by skill_path`;
         return sendLadder(uri, id, SKILL_SETS, available, [
            ...(params.skills === "summary"
               ? []
               : [
                    {
                       view: "full",
                       floor: fileTextChars([whole]),
                       build: () => ({ skillSet: whole }),
                    },
                 ]),
            {
               view: "summary",
               build: (prior) => ({
                  skillSet: summary,
                  ...(prior && {
                     message: `${tooLarge(prior)}, so this is the summary view. It leaves out the file text and the listed files (scripts, templates and other assets). Read a file by skill_path; ${read}.`,
                  }),
               }),
            },
            {
               view: "pin",
               // The set matched: its name and pin stay, so this does not read as "no set matched".
               build: (prior) => ({
                  skillSet: {
                     name: whole.name,
                     digest: whole.digest,
                     source: whole.source,
                  },
                  message: `${tooLarge(prior!)}, so this keeps the set's name, digest and source, and leaves out its skill entries and warnings. ${read}.`,
               }),
            },
         ]);
      },
   });
}
