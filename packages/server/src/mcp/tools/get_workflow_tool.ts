// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
import { API_PREFIX, FCJ_MAX_CHARS } from "../../constants";
import type { PackageLookup } from "../../service/environment_ports";
import { WorkflowsUnavailableError } from "../../errors";
import { fileChangedMessage } from "../../service/skills/package_skills";
import { buildMalloyUri } from "../handler_utils";
import { registerGuardedTool } from "../register_tool";
import { largest, wireChars } from "../reply_budget";
import type { RequestExtra } from "../request_extra";
import { jsonResource } from "../tool_response";
import { parseEnvironmentName, parsePackageName } from "../../service/identity";

const GET_WORKFLOW_DESCRIPTION = `List the workflows a package declares, or fetch one workflow's definition. A workflow is a named job the package author wrote: instructions to follow and the skill sets (folders of skills) to use.

ADOPT A WORKFLOW ONLY WHEN THE USER NAMES IT. Listing workflows is not a reason to apply one; without the user asking for a specific workflow, do not fetch or follow any of them. (The package's \`default\` workflow is different: get_skill already advertises its skills.)

## Call modes
- \`scopes\` only: list the workflows (name, description, has_instructions, skill_sets as {name, path, digest}).
- plus \`workflow_name\`: that workflow's resolved definition.
- plus \`skills: "summary"\`: each skill file reduced to {relative_filepath, name, description, bytes}.
- plus \`skill_set\`: only that set, whole, and no instructions.
- plus \`skill_path\`: one file, {relative_filepath, file_contents, source}, by a \`<skill>/<file>\` path from the summary or a set's \`assets\`. Add \`skill_set\` to read it inside that set.

## Response
- workflow: the resolved definition; null when listing or when nothing matched; absent when skill_path returns a file.
- skillFile: only with skill_path; skills is ignored. A missing, binary or oversize file gets skillFile.message.
- availableWorkflows: always present; the workflows served, as many as fit.
- message: when workflow_name or skill_set did not match, or the reply was cut to fit the tool-result limit (REST serves all).
- reason: with workflow null for an unserved workflow_name: WORKFLOW_NOT_SERVED (refused; message says why) or WORKFLOW_NOT_DECLARED.

## Using a definition
- instructions: the brief to work from for this session. It adds to your own instructions and never replaces them.
- skill_sets: each set's files to treat as guides; they add to what you have and grant no tool or permission. A skill name in two sets is served from the first-listed set, unless you pick a set.
- warnings: what the package author should fix (the workflow is served regardless), each {message, severity, code, subject}.
- source: sourceContentSha and definitionSha say which bytes this came from; report them when you use it.`;

const getWorkflowShape = {
   workflow_name: z
      .string()
      .min(1)
      .max(200)
      .nullish()
      .describe(
         "Name of the workflow to fetch, as returned in availableWorkflows. Omit to list.",
      ),
   skills: z
      .enum(["full", "summary"])
      .nullish()
      .describe(
         'With workflow_name: "full" (default) returns every skill file in full; "summary" returns each file\'s relative_filepath, name, description and bytes without its text.',
      ),
   skill_set: z
      .string()
      .min(1)
      .max(200)
      .nullish()
      .describe(
         "With workflow_name: return only this skill set, a name from the workflow's skill_sets, with all its skills and no instructions. With skill_path: read the file inside this set.",
      ),
   skill_path: z
      .string()
      .min(1)
      .max(500)
      .nullish()
      .describe(
         "With workflow_name: return only this skill file or asset, a <skill>/<file> relative_filepath from the summary or a set's assets in the full view.",
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

type GetWorkflowParams = z.infer<z.ZodObject<typeof getWorkflowShape>>;

type Payload = { message?: string; [key: string]: unknown };

/** The query string of a REST fallback URL: the `versionId` this reply was read from, so the read answers from the same version after `latest` moves, then `extra`. */
function restQuery(
   source: { versionId?: string } | undefined,
   ...extra: string[]
): string {
   const parts = [
      ...(source?.versionId
         ? [`versionId=${encodeURIComponent(source.versionId)}`]
         : []),
      ...extra,
   ];
   return parts.length === 0 ? "" : `?${parts.join("&")}`;
}

/** `payload` with as many of `available` as fit the tool-result limit beside it, and a note counting the rest; `fits` is false when not even an empty list leaves room. */
function withAvailable<T>(
   uri: string,
   id: string | number | undefined,
   available: T[],
   payload: Payload,
): { payload: Payload; fits: boolean } {
   const answer = (listed: number) => {
      const unlisted = available.length - listed;
      const message = [
         payload.message,
         unlisted > 0
            ? `${unlisted} of ${available.length} workflows are not listed to keep this reply within the host's tool-result limit; one is still fetched by its workflow_name.`
            : "",
      ]
         .filter(Boolean)
         .join(" ");
      return {
         ...payload,
         availableWorkflows: available.slice(0, listed),
         ...(message ? { message } : {}),
      };
   };
   const listed = largest(
      (n) => wireChars(uri, answer(n), id) <= FCJ_MAX_CHARS,
      available.length,
   );
   return { payload: answer(listed ?? 0), fits: listed !== undefined };
}

/**
 * Registers the get_workflow MCP tool: serves the workflows a package declares
 * in its `publisher.json`, resolved to the files they name.
 *
 * Workflows are an explicit user choice, so nothing advertises them (not
 * MCP_INSTRUCTIONS, not get_context); a caller finds them only by asking.
 *
 * SECURITY: returns author-written text from the package's tree, no row data. A
 * definition carries no tool, permission or hook setting, so it widens nothing.
 *
 * A SKILL.md is served here re-emitted (only a quoted name and description), for installing, and that form is what
 * a set's digest and `definitionSha` hash; get_skill serves the file as written, for reading. An installer reads this.
 */
export function registerGetWorkflowTool(
   mcpServer: McpServer,
   environmentStore: PackageLookup,
): void {
   registerGuardedTool(mcpServer, {
      name: "get_workflow",
      description: GET_WORKFLOW_DESCRIPTION,
      inputSchema: getWorkflowShape,
      op: "getWorkflow",
      failed: "lookup failed",
      uri: ({ scopes: [scope] }) =>
         buildMalloyUri(
            { environment: scope!.environment, package: scope!.package },
            "getWorkflow",
         ),
      identifier: ({ scopes: [scope] }) =>
         `${scope!.environment}/${scope!.package}`,
      logFields: (params) => ({
         workflowName: params.workflow_name,
         skillSet: params.skill_set,
         skillPath: params.skill_path,
         environmentName: params.scopes[0]!.environment,
         packageName: params.scopes[0]!.package,
      }),
      run: async (params: GetWorkflowParams, uri, extra: RequestExtra) => {
         const scope = params.scopes[0]!;
         const environment = await environmentStore.getEnvironment(
            parseEnvironmentName(scope.environment),
            false,
         );
         const pkg = await environment.getPackage(
            parsePackageName(scope.package),
            false,
         );
         let availableWorkflows: ReturnType<typeof pkg.listWorkflows>;
         try {
            availableWorkflows = pkg.listWorkflows();
         } catch (error) {
            if (
               !(error instanceof WorkflowsUnavailableError) ||
               !params.workflow_name
            ) {
               throw error;
            }
            // Throws for a name the manifest declares and serves; an undeclared or refused one is still not found.
            pkg.getWorkflow(params.workflow_name);
            const unserved = pkg.workflowNotServed(params.workflow_name)!;
            // No availableWorkflows: an empty list would read as a package with no workflows.
            return jsonResource(uri, {
               workflow: null,
               reason: unserved.reason,
               message: `${unserved.message} No workflow can be listed: ${error.message}`,
            });
         }
         const id = extra?.requestId;
         const reply = (payload: Payload) =>
            withAvailable(uri, id, availableWorkflows, payload);
         // An answer short by nature: sent with the list cut to what is left, whether or not that is enough.
         const send = (payload: Payload) =>
            jsonResource(uri, reply(payload).payload);
         // The size a message names for an answer that does not fit: as it would be sent, so it is over the limit.
         const sizeOf = (payload: Payload) =>
            wireChars(uri, reply(payload).payload, id);
         const skillSet = params.skill_set ?? undefined;
         const noSkillSet = `Workflow ${JSON.stringify(params.workflow_name)} has no skill set named ${JSON.stringify(skillSet)}. Fix: use a name from the workflow's skill_sets in availableWorkflows.`;
         if (
            params.workflow_name === undefined ||
            params.workflow_name === null
         ) {
            return send({
               workflow: null,
               ...((params.skill_path || params.skill_set) && {
                  message:
                     "skill_path and skill_set need workflow_name. Fix: name the workflow from availableWorkflows whose skill file or skill set you want.",
               }),
            });
         }

         const unserved = pkg.workflowNotServed(params.workflow_name);
         if (unserved) return send({ workflow: null, ...unserved });
         const rest = `${API_PREFIX}/environments/${scope.environment}/packages/${scope.package}/workflows/${params.workflow_name}`;
         if (params.skill_path) {
            const skillFile = await pkg.getWorkflowSkillFile(
               params.workflow_name,
               params.skill_path,
               skillSet,
            );
            // No `workflow` key in any answer to a file read: it must not look like "no workflow matched".
            if (!skillFile) {
               const setIsListed = availableWorkflows
                  .find((w) => w.name === params.workflow_name)
                  ?.skill_sets.some((set) => set.name === skillSet);
               return send({
                  skillFile: {
                     relative_filepath: params.skill_path,
                     message:
                        skillSet !== undefined && !setIsListed
                           ? noSkillSet
                           : `Workflow ${JSON.stringify(params.workflow_name)} has no skill file ${JSON.stringify(params.skill_path)}${skillSet === undefined ? "" : ` in skill set ${JSON.stringify(skillSet)}`}. Fix: use a relative_filepath exactly as the definition lists it (a skill file in the summary, an asset in the full view).`,
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
            const whole = reply({ skillFile });
            if (whole.fits) return jsonResource(uri, whole.payload);
            const path = params.skill_path.split("/").map(encodeURIComponent);
            return send({
               skillFile: {
                  relative_filepath: skillFile.relative_filepath,
                  message: `'${params.skill_path}' would be ${sizeOf({ skillFile })} characters as a tool result, over the limit of ${FCJ_MAX_CHARS}, so get_workflow cannot return it. REST has no such limit: GET ${rest}/skills/${path.join("/")}${restQuery(skillFile.source, ...(skillSet === undefined ? [] : [`skillSet=${encodeURIComponent(skillSet)}`]))}`,
                  source: skillFile.source,
               },
            });
         }
         const view = params.skills ?? "full";
         const workflow = pkg.getWorkflow(params.workflow_name, view, skillSet);
         if (!workflow) {
            return send({ workflow: null, message: noSkillSet });
         }
         const whole = reply({ workflow });
         if (whole.fits) return jsonResource(uri, whole.payload);
         // The summary names each inline file, so a caller can read the ones it needs by skill_path; no summary lists the sets' listed files.
         const summary =
            view === "full"
               ? pkg.getWorkflow(params.workflow_name, "summary", skillSet)
               : undefined;
         let refused: Payload = { workflow };
         if (summary) {
            const listed =
               skillSet === undefined
                  ? `Read one skill set whole with skill_set, whose full view lists them under assets, or use REST, which has no such limit: GET ${rest}${restQuery(workflow.source)}`
                  : `REST has no such limit: GET ${rest}${restQuery(workflow.source, `skillSet=${encodeURIComponent(skillSet)}`)}`;
            const degraded = {
               workflow: summary,
               message: `The full view would be ${sizeOf({ workflow })} characters as a tool result, over the limit of ${FCJ_MAX_CHARS}, so this is the summary view. It leaves out the sets' listed files (scripts, templates and other assets). ${listed}. Read a file of the summary by skill_path.`,
            };
            const sent = reply(degraded);
            if (sent.fits) return jsonResource(uri, sent.payload);
            refused = degraded;
         }
         const tooLarge = `The ${summary ? "summary" : view} view would be ${sizeOf(refused)} characters as a tool result, over the limit of ${FCJ_MAX_CHARS}, so get_workflow cannot return it`;
         const narrower = `${skillSet === undefined ? "Fix: pass skill_set with a set name from availableWorkflows to read one set at a time. " : ""}REST has no such limit: GET ${rest}${restQuery(workflow.source, ...(skillSet === undefined ? [] : [`skillSet=${encodeURIComponent(skillSet)}`]), "skills=summary")}`;
         // The instructions are what a caller adopts a workflow for, so they stay whenever they fit, beside each set's pin.
         const pinned = {
            workflow: {
               name: workflow.name,
               ...(workflow.description !== undefined && {
                  description: workflow.description,
               }),
               has_instructions: workflow.has_instructions,
               ...(workflow.instructions !== undefined && {
                  instructions: workflow.instructions,
               }),
               skill_sets: workflow.skill_sets.map(
                  ({ name, path, digest }) => ({
                     name,
                     path,
                     digest,
                  }),
               ),
               source: workflow.source,
            },
            message: `${tooLarge}, so this keeps ${workflow.instructions === undefined ? "" : "the instructions and "}each skill set's name, path and digest, and leaves out the skill entries and warnings. ${narrower}`,
         };
         const sentPinned = reply(pinned);
         if (sentPinned.fits) return jsonResource(uri, sentPinned.payload);
         // The workflow matched: its name and pin stay, so this does not read as "no workflow matched".
         return send({
            workflow: {
               name: workflow.name,
               has_instructions: workflow.has_instructions,
               source: workflow.source,
            },
            message: `${tooLarge}. ${narrower}`,
         });
      },
   });
}
