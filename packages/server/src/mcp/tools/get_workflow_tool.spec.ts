// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
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
type Handler = (params: Record<string, unknown>) => Promise<Result>;

const scopes = [{ environment: "env", package: "pkg" }];
const WORKFLOWS = [
   {
      name: "analyst",
      description: "answers questions",
      has_instructions: true,
      skill_sets: [{ name: "main", path: "sets/main", digest: "d" }],
   },
];

/** get_workflow over a package that serves one workflow with one skill set and one skill file. */
function getWorkflow(getPackage?: () => Promise<unknown>): Handler {
   const pkg = {
      listWorkflows: () => WORKFLOWS,
      getWorkflow: (name: string, skills: string, skillSet?: string) =>
         skillSet !== undefined && skillSet !== "main"
            ? undefined
            : { name, skills, skillSet },
      getWorkflowSkillFile: (_workflow: string, path: string) =>
         path === "guide/SKILL.md"
            ? { relative_filepath: path, file_contents: "# guide" }
            : undefined,
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
      expect(missing.workflow).toBeNull();
      expect(missing.message).toContain('has no skill file "x/SKILL.md"');
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
