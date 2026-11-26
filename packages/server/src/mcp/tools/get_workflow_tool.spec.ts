// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { FCJ_MAX_CHARS } from "../../constants";
import { PackageNotFoundError } from "../../errors";
import { getNotFoundError } from "../error_messages";
import { registerGetWorkflowTool } from "./get_workflow_tool";
import { fakeOf } from "../../test_helpers/store_fakes";
import type { PackageLookup } from "../../service/environment_ports";

type Result = {
   isError?: boolean;
   content: Array<{
      type: string;
      text?: string;
      resource?: { uri: string; text: string };
   }>;
};
type Handler = (
   params: Record<string, unknown>,
   extra?: { requestId?: string | number },
) => Promise<Result>;

const scopes = [{ environment: "env", package: "pkg" }];
const WORKFLOWS = [
   {
      name: "analyst",
      description: "answers questions",
      has_instructions: true,
      skill_sets: [{ name: "main", path: "sets/main", digest: "d" }],
   },
];

/** get_workflow over a package that serves one workflow with one skill set and one skill file; `over` replaces what the package answers. */
function getWorkflow(
   getPackage?: () => Promise<unknown>,
   over: Record<string, unknown> = {},
): Handler {
   const pkg = {
      listWorkflows: () => WORKFLOWS,
      getWorkflow: (name: string, skills: string, skillSet?: string) =>
         skillSet !== undefined && skillSet !== "main"
            ? undefined
            : { name, skills, skillSet },
      getWorkflowSkillFile: (
         _workflow: string,
         path: string,
         skillSet?: string,
      ) =>
         path === "guide/SKILL.md" &&
         (skillSet === undefined || skillSet === "main")
            ? {
                 relative_filepath: path,
                 file_contents:
                    skillSet === undefined ? "# guide" : "# in main",
              }
            : undefined,
      ...over,
   };
   const store = {
      getEnvironment: async () => ({
         getPackage: getPackage ?? (async () => pkg),
      }),
   };
   let handler: Handler | undefined;
   registerGetWorkflowTool(
      {
         registerTool: (_n: string, _config: unknown, h: Handler) => {
            handler = h;
         },
      } as never,
      fakeOf<PackageLookup>(store),
   );
   return handler!;
}

const parse = (r: Result) => JSON.parse(r.content[0]!.resource!.text);

describe("get_workflow", () => {
   it("lists the package's workflows when no workflow is named", async () => {
      const result = await getWorkflow()({ scopes });
      expect(result.isError).toBe(false);
      expect(result.content[0]!.resource!.uri).toBe(
         "malloy://environment/env/package/pkg#getWorkflow",
      );
      expect(parse(result)).toEqual({
         workflow: null,
         availableWorkflows: WORKFLOWS,
      });
   });

   it.each([
      ["skill_path", { skill_path: "guide/SKILL.md" }],
      ["skill_set", { skill_set: "main" }],
   ])("says %s needs workflow_name", async (_label, extra) => {
      const payload = parse(await getWorkflow()({ scopes, ...extra }));
      expect(payload.workflow).toBeNull();
      expect(payload.message).toContain(
         "skill_path and skill_set need workflow_name",
      );
   });

   it("names an unknown workflow and how to fix it", async () => {
      const payload = parse(
         await getWorkflow()({ scopes, workflow_name: "nope" }),
      );
      expect(payload.workflow).toBeNull();
      expect(payload.message).toContain('No workflow named "nope"');
      expect(payload.message).toContain("Fix:");
   });

   it("resolves a workflow, defaulting to full skills and every set", async () => {
      expect(
         parse(await getWorkflow()({ scopes, workflow_name: "analyst" }))
            .workflow,
      ).toEqual({ name: "analyst", skills: "full" });
   });

   it("passes the summary view and the skill set through", async () => {
      expect(
         parse(
            await getWorkflow()({
               scopes,
               workflow_name: "analyst",
               skills: "summary",
               skill_set: "main",
            }),
         ).workflow,
      ).toEqual({ name: "analyst", skills: "summary", skillSet: "main" });
   });

   it("says when the workflow has no skill set of that name", async () => {
      const payload = parse(
         await getWorkflow()({
            scopes,
            workflow_name: "analyst",
            skill_set: "nope",
         }),
      );
      expect(payload.workflow).toBeNull();
      expect(payload.message).toContain('no skill set named "nope"');
      expect(payload.availableWorkflows).toEqual(WORKFLOWS);
   });

   it("returns one skill file without a workflow key, or says it is missing", async () => {
      const handler = getWorkflow();
      expect(
         parse(
            await handler({
               scopes,
               workflow_name: "analyst",
               skill_path: "guide/SKILL.md",
            }),
         ),
      ).toEqual({
         skillFile: {
            relative_filepath: "guide/SKILL.md",
            file_contents: "# guide",
         },
         availableWorkflows: WORKFLOWS,
      });
      const missing = parse(
         await handler({
            scopes,
            workflow_name: "analyst",
            skill_path: "x/SKILL.md",
         }),
      );
      // Not `workflow: null`, which the run-workflow recipe reads as "the name did not match".
      expect("workflow" in missing).toBe(false);
      expect(missing.message).toBeUndefined();
      expect(missing.skillFile.relative_filepath).toBe("x/SKILL.md");
      expect(missing.skillFile.file_contents).toBeUndefined();
      expect(missing.skillFile.message).toContain(
         'has no skill file "x/SKILL.md"',
      );
   });

   it("scopes a skill file to the skill set it names, or says it is missing there", async () => {
      const handler = getWorkflow();
      const read = (skill_set: string) =>
         handler({
            scopes,
            workflow_name: "analyst",
            skill_path: "guide/SKILL.md",
            skill_set,
         });
      expect(parse(await read("main")).skillFile.file_contents).toBe(
         "# in main",
      );
      const missing = parse(await read("other"));
      expect("workflow" in missing).toBe(false);
      expect(missing.skillFile.file_contents).toBeUndefined();
      expect(missing.skillFile.message).toContain(
         'has no skill file "guide/SKILL.md"',
      );
      expect(missing.skillFile.message).toContain('in skill set "other"');
   });

   it("answers a missing package with a not-found error naming it", async () => {
      const result = await getWorkflow(async () => {
         throw new PackageNotFoundError("Package pkg does not exist");
      })({ scopes, workflow_name: "analyst" });
      expect(result.isError).toBe(true);
      const details = getNotFoundError("env/pkg");
      expect(parse(result)).toEqual({
         error: details.message,
         suggestions: details.suggestions,
      });
   });
});

describe("get_workflow: the reply stays within the tool-result limit", () => {
   const wire = (r: Result, id: string | number = "i".repeat(62)) =>
      JSON.stringify({ jsonrpc: "2.0", id, result: r }).length;
   const text = "x".repeat(FCJ_MAX_CHARS);
   const source = { sourceContentSha: "s", definitionSha: "d" };
   const workflowOf = (skills: string) =>
      skills === "full"
         ? { name: "analyst", view: "full", text }
         : { name: "analyst", view: "summary" };
   const call = { scopes, workflow_name: "analyst" };

   it("answers a full view that does not fit with the summary view, and says so", async () => {
      const result = await getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) => workflowOf(skills),
      })(call);
      const payload = parse(result);
      expect(payload.workflow).toEqual({ name: "analyst", view: "summary" });
      expect(payload.message).toContain("The full view would be");
      expect(payload.message).toContain(
         `over the limit of ${FCJ_MAX_CHARS}, so this is the summary view`,
      );
      expect(payload.availableWorkflows).toEqual(WORKFLOWS);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("says the summary leaves out the listed files, and where to get them", async () => {
      const handler = getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) => workflowOf(skills),
      });
      const whole = parse(await handler(call));
      expect(whole.message).toContain("leaves out the sets' listed files");
      expect(whole.message).toContain("skill_set");
      expect(whole.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/workflows/analyst",
      );
      // A set already picked has no narrower full view: REST is the way to its listed files.
      const picked = parse(await handler({ ...call, skill_set: "main" }));
      expect(picked.message).toContain("leaves out the sets' listed files");
      expect(picked.message).toContain("workflows/analyst?skillSet=main");
      expect(picked.message).not.toContain("with skill_set");
   });

   it("leaves a full view that fits, and a summary that was asked for, as they are", async () => {
      const handler = getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) =>
            skills === "full"
               ? { name: "analyst", view: "full" }
               : workflowOf(skills),
      });
      const full = parse(await handler(call));
      expect(full.workflow.view).toBe("full");
      expect(full.message).toBeUndefined();
      const summary = parse(await handler({ ...call, skills: "summary" }));
      expect(summary.workflow.view).toBe("summary");
      expect(summary.message).toBeUndefined();
   });

   it("refuses a view that does not fit even as a summary, naming the narrower reads and REST", async () => {
      const result = await getWorkflow(undefined, {
         getWorkflow: () => ({ name: "analyst", text }),
      })({ ...call, skills: "summary" });
      const payload = parse(result);
      expect(payload.workflow).toBeNull();
      expect(payload.message).toContain(
         `over the limit of ${FCJ_MAX_CHARS}, so get_workflow cannot return it`,
      );
      expect(payload.message).toContain("skill_set");
      expect(payload.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/workflows/analyst",
      );
      expect(payload.availableWorkflows).toEqual(WORKFLOWS);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("answers a skill file that does not fit with a message in place of its text, keeping its pin", async () => {
      const result = await getWorkflow(undefined, {
         getWorkflowSkillFile: (_workflow: string, path: string) => ({
            relative_filepath: path,
            file_contents: text,
            source,
         }),
      })({ ...call, skill_path: "guide/assets/big.txt" });
      const { skillFile, workflow } = parse(result);
      expect(workflow).toBeUndefined();
      expect(skillFile).toEqual({
         relative_filepath: "guide/assets/big.txt",
         message: expect.stringContaining(
            `over the limit of ${FCJ_MAX_CHARS}, so get_workflow cannot return it`,
         ),
         source,
      });
      expect(skillFile.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/workflows/analyst/skills/guide/assets/big.txt",
      );
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("hints the REST path with each segment URL-encoded", async () => {
      const result = await getWorkflow(undefined, {
         getWorkflowSkillFile: (_workflow: string, path: string) => ({
            relative_filepath: path,
            file_contents: text,
            source,
         }),
      })({ ...call, skill_path: "guide/reference/a b#c%d?.md" });
      expect(parse(result).skillFile.message).toContain(
         "/skills/guide/reference/a%20b%23c%25d%3F.md",
      );
   });

   /** The text length at which `handlerFor`'s answer, with the workflow list beside it, just fills the limit. */
   const edgeOf = async (
      handlerFor: (n: number) => Handler,
      params: Record<string, unknown>,
   ) => 1000 + FCJ_MAX_CHARS - wire(await handlerFor(1000)(params));

   /** Every answer fits while one part of it grows through the limit; returns the shapes the answers took. */
   async function sweep(
      handlerFor: (n: number) => Handler,
      params: Record<string, unknown>,
      shape: (payload: ReturnType<typeof parse>) => string,
      past: number,
   ) {
      const edge = await edgeOf(handlerFor, params);
      const shapes = new Set<string>();
      for (let n = edge - 20; n <= edge + past; n++) {
         const result = await handlerFor(n)(params);
         expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
         // A message that names a size names one over the limit, never one at or under it.
         const text = JSON.stringify(parse(result));
         for (const [, size] of text.matchAll(/would be (\d+) characters/g)) {
            expect(Number(size)).toBeGreaterThan(FCJ_MAX_CHARS);
         }
         shapes.add(shape(parse(result)));
      }
      return shapes;
   }

   it("keeps every answer within the limit as a full view grows through it", async () => {
      const shapes = await sweep(
         (n) =>
            getWorkflow(undefined, {
               getWorkflow: (_name: string, skills: string) =>
                  skills === "full"
                     ? { name: "analyst", text: "y".repeat(n) }
                     : { name: "analyst" },
            }),
         call,
         (payload) =>
            payload.workflow.text === undefined ? "summary" : "full",
         500,
      );
      expect([...shapes].sort()).toEqual(["full", "summary"]);
   });

   it("keeps every answer within the limit as a summary grows through it, message included", async () => {
      const shapes = await sweep(
         (n) =>
            getWorkflow(undefined, {
               getWorkflow: (_name: string, skills: string) => ({
                  name: "analyst",
                  text: "y".repeat(skills === "full" ? FCJ_MAX_CHARS : n),
               }),
            }),
         call,
         (payload) => (payload.workflow === null ? "refused" : "summary"),
         900,
      );
      expect([...shapes].sort()).toEqual(["refused", "summary"]);
   });

   it("keeps every answer within the limit as a skill file grows through it", async () => {
      const shapes = await sweep(
         (n) =>
            getWorkflow(undefined, {
               getWorkflowSkillFile: (_workflow: string, path: string) => ({
                  relative_filepath: path,
                  file_contents: "y".repeat(n),
                  source,
               }),
            }),
         { ...call, skill_path: "guide/SKILL.md" },
         (payload) =>
            payload.skillFile.file_contents === undefined ? "message" : "file",
         500,
      );
      expect([...shapes].sort()).toEqual(["file", "message"]);
   });

   it("pins every REST fallback URL to the version the reply was read from, after the skill set when one is picked", async () => {
      const versioned = { ...source, versionId: "1.2.3" };
      const fileHandler = getWorkflow(undefined, {
         getWorkflowSkillFile: (_workflow: string, path: string) => ({
            relative_filepath: path,
            file_contents: text,
            source: versioned,
         }),
      });
      const fileCall = { ...call, skill_path: "guide/SKILL.md" };
      expect(parse(await fileHandler(fileCall)).skillFile.message).toContain(
         "/skills/guide/SKILL.md?versionId=1.2.3",
      );
      expect(
         parse(await fileHandler({ ...fileCall, skill_set: "main" })).skillFile
            .message,
      ).toContain("/skills/guide/SKILL.md?versionId=1.2.3&skillSet=main");
      const view = getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) => ({
            ...workflowOf(skills),
            source: versioned,
         }),
      });
      expect(parse(await view(call)).message).toContain(
         "workflows/analyst?versionId=1.2.3.",
      );
      expect(
         parse(await view({ ...call, skill_set: "main" })).message,
      ).toContain("workflows/analyst?versionId=1.2.3&skillSet=main.");
      const refusing = getWorkflow(undefined, {
         getWorkflow: () => ({ name: "analyst", text, source: versioned }),
      });
      expect(
         parse(await refusing({ ...call, skills: "summary" })).message,
      ).toContain("workflows/analyst?versionId=1.2.3&skills=summary");
      expect(
         parse(
            await refusing({ ...call, skills: "summary", skill_set: "main" }),
         ).message,
      ).toContain(
         "workflows/analyst?versionId=1.2.3&skillSet=main&skills=summary",
      );
   });

   it("lists as many workflows as fit beside the answer, and counts the rest", async () => {
      const many = Array.from({ length: 1000 }, (_, i) => ({
         ...WORKFLOWS[0]!,
         name: i === 0 ? "analyst" : `analyst-${i}`,
      }));
      const handler = getWorkflow(undefined, { listWorkflows: () => many });
      for (const params of [
         { scopes },
         call,
         { ...call, workflow_name: "nope" },
      ]) {
         const result = await handler(params);
         const payload = parse(result);
         expect(payload.availableWorkflows.length).toBeGreaterThan(0);
         expect(payload.availableWorkflows.length).toBeLessThan(many.length);
         expect(payload.availableWorkflows[0].name).toBe("analyst");
         expect(payload.message).toContain(
            `${many.length - payload.availableWorkflows.length} of ${many.length} workflows are not listed`,
         );
         expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      }
      // A name beyond the cut is still resolved.
      const beyond = parse(
         await handler({ ...call, workflow_name: "analyst-999" }),
      );
      expect(beyond.workflow).toMatchObject({ name: "analyst-999" });
   });

   it("measures the reply with the request's own id", async () => {
      const handler = getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) => ({
            name: "analyst",
            skills,
            ...(skills === "full" && {
               text: "y".repeat(FCJ_MAX_CHARS - 1500),
            }),
         }),
      });
      const plain = await handler(call);
      expect(parse(plain).message).toBeUndefined();
      // A long id of escaping characters leaves the reply no room for the full view.
      const requestId = '"'.repeat(1000);
      const escaped = await handler(call, { requestId });
      expect(parse(escaped).message).toContain("so this is the summary view");
      expect(wire(escaped, requestId)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });
});
