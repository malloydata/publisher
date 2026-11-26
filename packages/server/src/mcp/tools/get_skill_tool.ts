// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
import type { PackageLookup } from "../../service/environment_ports";
import { buildMalloyUri } from "../handler_utils";
import { registerGuardedTool } from "../register_tool";
import type { RequestExtra } from "../request_extra";
import { FCJ_MAX_CHARS } from "../../constants";
import { type BundledSkill } from "../../service/skills/build_skills_bundle";
import {
   fileChangedMessage,
   namedSkillFiles,
   readSkillFile,
   resolveSkills,
   SKILL_MD,
   skillFileList,
   skillFilePath,
   skillFileView,
   type ResolvedSkill,
} from "../../service/skills/package_skills";
import { jsonResource } from "../tool_response";
import { parseEnvironmentName, parsePackageName } from "../../service/identity";

const GET_SKILL_DESCRIPTION = `Fetch a task guide, or list the guides available. Guides are concise procedural instructions for working with Malloy and this server: query patterns, chart and rendering tags, analysis pitfalls, how to build or review a model.

A guide is a directory: a SKILL.md plus files it names by path relative to its root (\`references/joins.md\`, \`scripts/check.py\`). A package can ship guides of its OWN, about that package (which source answers which question, its terms, its conventions). Pass \`scopes\` to see them. A package guide named like a built-in one REPLACES it for that package; \`origin\` ("package" or "bundled") says which you are reading.

## Call modes
- No arguments: list the built-in guides.
- \`scopes\`: list the guides in force for that package.
- \`skill_name\`: that guide's SKILL.md verbatim, with \`frontmatter\` and \`files\`. Add \`scopes\` for a package's version.
- \`skill_name\` and \`path\`: one other file of the guide, verbatim.

## Parameters
- skill_name (optional): a name from \`availableSkills\`. Omit to list.
- path (optional): a path from \`files\`. Defaults to SKILL.md.
- scopes (optional): exactly one {environment, package}, from list_packages. Omit for the built-in set only.

## Response
- skill: {name, path, content, origin}. SKILL.md also has \`frontmatter\` (its YAML as an object, parsed as YAML 1.2) and \`files\` ([{path, size}]; for a package guide, exactly what this server serves).
- A binary file, or one too large for a tool result, comes back as {name, path, size, origin} and a message. A SKILL.md reply always fits and keeps the body over the file list: if the list does not fit, \`files\` is cut short and the message counts the files not listed (read any by \`path\`).
- A built-in guide serves its Markdown only; for any other file the message says to install \`@malloy-publisher/skills\`.
- availableSkills: on a listing, and with a message when skill_name did not match.
- message: when skill_name or path did not match (an unknown path names the guide's files), or why a file has no content.

Reading a guide is not the same as having it available: fetch the one that covers what you are about to do before you do it.`;

const getSkillShape = {
   skill_name: z
      .string()
      .min(1)
      .max(200)
      .nullish()
      .describe(
         "Name of the guide to fetch, as returned in availableSkills. Omit to list.",
      ),
   path: z
      .string()
      .max(512)
      .nullish()
      .describe(
         "File to read, relative to the guide's root (for example `references/joins.md` or `scripts/check.py`). Defaults to `SKILL.md`, which also returns the guide's `frontmatter` and `files`. Ignored when skill_name is omitted.",
      ),
   scopes: z
      .array(
         z.object({
            environment: z.string().describe("Environment name."),
            package: z.string().describe("Package name."),
         }),
      )
      .length(1)
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

/** The id a host is assumed to send when the request carries none. */
const ASSUMED_ID = "i".repeat(62);

/** Characters of the JSON-RPC message the host receives for this answer, which echoes the request's own id. */
function wireChars(
   uri: string,
   payload: unknown,
   id: string | number = ASSUMED_ID,
): number {
   return JSON.stringify({
      jsonrpc: "2.0",
      id,
      result: jsonResource(uri, payload),
   }).length;
}

function oversizeMessage(relative: string, wire: number): string {
   const next =
      relative === SKILL_MD
         ? "Read one of the other files listed in `files`."
         : "Call get_skill with this skill_name and no path for its file list, then read a smaller file by `path`.";
   return `'${relative}' would be ${wire} characters as a tool result, over the limit of ${FCJ_MAX_CHARS}, so get_skill cannot return it. ${next}`;
}

/** The most files that fit: all `hi` if that does, else the largest n below it (a cut list carries a longer message, so the full count is checked on its own), or undefined when not even 0 fit. */
function largest(fits: (n: number) => boolean, hi: number): number | undefined {
   if (fits(hi)) return hi;
   if (hi === 0 || !fits(0)) return undefined;
   let lo = 0;
   let top = hi - 1;
   while (lo < top) {
      const mid = Math.ceil((lo + top) / 2);
      if (fits(mid)) lo = mid;
      else top = mid - 1;
   }
   return lo;
}

/**
 * The fullest SKILL.md answer within the tool-result limit: the whole read, else
 * the body with as many file entries as fit, else the body without the parsed
 * frontmatter (its text is in the body), else the entries alone with the body
 * withheld, else without the frontmatter too. The body outranks the list
 * because the list can be read from the skill's own files one by one, and the
 * body cannot.
 */
function guideAnswer(
   skill: ResolvedSkill,
   text: string,
   uri: string,
   id?: string | number,
) {
   const entries = skillFileList(skill);
   const answer = (
      content: boolean,
      frontmatter: boolean,
      listed: number,
      wire: number,
   ) => {
      const unlisted = entries.length - listed;
      const message = [
         content ? "" : oversizeMessage(SKILL_MD, wire),
         frontmatter
            ? ""
            : content
              ? "Its parsed frontmatter was too large to return; the text is in `content`."
              : "Its frontmatter was too large to return.",
         unlisted > 0
            ? `${unlisted} of ${entries.length} files are not listed to keep this reply within the host's tool-result limit. Read any file of the skill by calling get_skill again with its \`path\` (and the \`scopes\` you used).`
            : "",
      ]
         .filter(Boolean)
         .join(" ");
      return {
         skill: skillFileView(
            skill,
            SKILL_MD,
            content ? text : undefined,
            entries.slice(0, listed),
            frontmatter,
         ),
         ...(message ? { message } : {}),
      };
   };
   const full = answer(true, true, entries.length, 0);
   const wire = wireChars(uri, full, id);
   if (wire <= FCJ_MAX_CHARS) return full;
   for (const [content, frontmatter] of [
      [true, true],
      [true, false],
      [false, true],
      [false, false],
   ] as const) {
      const listed = largest(
         (n) =>
            wireChars(uri, answer(content, frontmatter, n, wire), id) <=
            FCJ_MAX_CHARS,
         entries.length,
      );
      if (listed !== undefined) {
         return answer(content, frontmatter, listed, wire);
      }
   }
   return answer(false, false, 0, wire);
}

/** The guides' names and descriptions that fit the tool-result limit, with `message` (if any) and a count of the guides left out. */
function availableAnswer(
   skills: ResolvedSkill[],
   scoped: boolean,
   uri: string,
   id: string | number | undefined,
   message?: string,
) {
   const entries = summarize(skills);
   // What a cut keeps first: a scoped caller asked for the package's guides, and a call with no scopes lists the built-in ones. The answer keeps the resolved order.
   const priority = entries
      .map((_, i) => i)
      .sort(
         (a, b) =>
            Number(scoped && entries[b]!.origin === "package") -
               Number(scoped && entries[a]!.origin === "package") || a - b,
      );
   const own = entries.filter((e) => e.origin === "package").length;
   const answer = (listed: number) => {
      const ownCut = Math.max(0, own - listed);
      const builtInCut = Math.min(
         entries.length - own,
         entries.length - listed,
      );
      const notes = scoped
         ? [
              ownCut > 0
                 ? `${ownCut} of the package's ${own} own guides are not listed to keep this reply within the host's tool-result limit; one is still read by its \`skill_name\`.`
                 : "",
              builtInCut > 0
                 ? `${builtInCut} built-in guides are not listed either; a call with no \`scopes\` lists them.`
                 : "",
           ]
         : [
              entries.length > listed
                 ? `${entries.length - listed} of ${entries.length} guides are not listed to keep this reply within the host's tool-result limit; one is still read by its \`skill_name\`.`
                 : "",
           ];
      const text = [message, ...notes].filter(Boolean).join(" ");
      const kept = new Set(priority.slice(0, listed));
      return {
         availableSkills: entries.filter((_, i) => kept.has(i)),
         ...(text ? { message: text } : {}),
      };
   };
   const listed = largest(
      (n) => wireChars(uri, answer(n), id) <= FCJ_MAX_CHARS,
      entries.length,
   );
   return answer(listed ?? 0);
}

/** One file of a skill as the tool answers it; `relative` is a path the skill lists. */
function fileAnswer(
   skill: ResolvedSkill,
   relative: string,
   uri: string,
   id?: string | number,
) {
   const read = readSkillFile(skill, relative);
   if ("changed" in read) return { message: fileChangedMessage(relative) };
   if (!("text" in read)) {
      return {
         skill: skillFileView(skill, relative, undefined),
         message: read.message,
      };
   }
   if (relative === SKILL_MD) return guideAnswer(skill, read.text, uri, id);
   const full = { skill: skillFileView(skill, relative, read.text) };
   const wire = wireChars(uri, full, id);
   return wire <= FCJ_MAX_CHARS
      ? full
      : {
           skill: skillFileView(skill, relative, undefined),
           message: oversizeMessage(relative, wire),
        };
}

/**
 * Registers the get_skill MCP tool: serves the bundled guides, and the guides a
 * package ships under `skills/`, with the package's copy shadowing a bundled
 * one of the same name. A guide is read file by file, by path.
 *
 * A tool rather than more prompts: prompts are registered once at start, while
 * packages load and reload at runtime, and a tool takes the package as an
 * argument instead of disambiguating prompt names across packages.
 *
 * SECURITY: returns author-written guidance from the package's own tree (the
 * trust level of `#(doc)` in get_context), never row data.
 */
export function registerGetSkillTool(
   mcpServer: McpServer,
   environmentStore: PackageLookup,
   bundledSkills: BundledSkill[],
): void {
   registerGuardedTool(mcpServer, {
      name: "get_skill",
      description: GET_SKILL_DESCRIPTION,
      inputSchema: getSkillShape,
      op: "getSkill",
      failed: "lookup failed",
      uri: (params) => {
         const scope = params.scopes?.[0];
         return buildMalloyUri(
            scope
               ? { environment: scope.environment, package: scope.package }
               : {},
            "getSkill",
         );
      },
      identifier: (params) => {
         const scope = params.scopes?.[0];
         return scope ? `${scope.environment}/${scope.package}` : "skills";
      },
      logFields: (params) => ({
         skillName: params.skill_name,
         path: params.path,
         environmentName: params.scopes?.[0]?.environment,
         packageName: params.scopes?.[0]?.package,
      }),
      run: async (params: GetSkillParams, uri, extra: RequestExtra) => {
         const scope = params.scopes?.[0];
         let resolved = resolveSkills(bundledSkills, []);
         if (scope) {
            const environment = await environmentStore.getEnvironment(
               parseEnvironmentName(scope.environment),
               false,
            );
            const pkg = await environment.getPackage(
               parsePackageName(scope.package),
               false,
            );
            resolved = resolveSkills(bundledSkills, pkg.listSkills());
         }

         if (params.skill_name === undefined || params.skill_name === null) {
            return jsonResource(
               uri,
               availableAnswer(
                  resolved,
                  scope !== undefined,
                  uri,
                  extra?.requestId,
               ),
            );
         }

         const skill = resolved.find((s) => s.name === params.skill_name);
         if (!skill) {
            const fix = `No guide named ${JSON.stringify(params.skill_name)}${
               scope ? ` for package ${scope.environment}/${scope.package}` : ""
            }. Fix: use a name from availableSkills exactly as spelled there${
               scope
                  ? "."
                  : ", or pass scopes to include a package's own guides."
            }${
               params.skill_name.includes("/")
                  ? " A file inside a guide is read with `skill_name` plus `path`."
                  : ""
            }`;
            return jsonResource(
               uri,
               availableAnswer(
                  resolved,
                  scope !== undefined,
                  uri,
                  extra?.requestId,
                  fix,
               ),
            );
         }

         const relative = skillFilePath(params.path);
         if (!Object.hasOwn(skill.catalog.files, relative)) {
            return jsonResource(uri, {
               message: `'${relative}' is not a file of skill '${skill.name}'. Its files: ${namedSkillFiles(skill)}. Call get_skill again with one of these as \`path\`.`,
            });
         }
         return jsonResource(
            uri,
            fileAnswer(skill, relative, uri, extra?.requestId),
         );
      },
   });
}
