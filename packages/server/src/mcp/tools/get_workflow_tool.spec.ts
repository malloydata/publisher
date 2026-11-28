// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import { FCJ_MAX_CHARS } from "../../constants";
import { PackageNotFoundError } from "../../errors";
import { logger } from "../../logger";
import {
   handlerOf,
   parse,
   scopes,
   sweep,
   wire,
   type Handler,
} from "../../test_helpers/skill_tool_harness";
import { getNotFoundError } from "../error_messages";
import { registerGetWorkflowTool } from "./get_workflow_tool";

const WORKFLOWS = [
   {
      name: "analyst",
      description: "answers questions",
      skill_set_names: ["main"],
   },
];

/** The refusal text a package gives for a workflow its manifest declares and does not serve. */
const REFUSAL =
   "Workflow 'broken' is not served: its instructions file is missing. Fix: correct the 'workflows.broken' entry in publisher.json and the files it names.";

/** What the package answers for a name it is asked to serve among `served`. */
const notServed = (served: Array<{ name: string }>) => (name: string) =>
   served.some((w) => w.name === name)
      ? undefined
      : name === "broken"
        ? { reason: "WORKFLOW_NOT_SERVED", message: REFUSAL }
        : {
             reason: "WORKFLOW_NOT_DECLARED",
             message: `No workflow named '${name}' is declared in package 'pkg'. Fix: use a name the package lists.`,
          };

/** get_workflow over a package that serves one workflow; `over` replaces what the package answers. */
function getWorkflow(
   getPackage?: () => Promise<unknown>,
   over: Record<string, unknown> = {},
): Handler {
   const pkg = {
      listWorkflows: () => WORKFLOWS,
      workflowNotServed: notServed(WORKFLOWS),
      getWorkflow: (name: string, skills: string) => ({
         name,
         view: skills,
         skill_sets: [],
      }),
      ...over,
   };
   return handlerOf(registerGetWorkflowTool, getPackage ?? (async () => pkg));
}

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

   it("names an undeclared workflow, with its reason and how to fix it", async () => {
      const payload = parse(await getWorkflow()({ scopes, workflow: "nope" }));
      expect(payload.workflow).toBeNull();
      expect(payload.reason).toBe("WORKFLOW_NOT_DECLARED");
      expect(payload.message).toContain("No workflow named 'nope' is declared");
      expect(payload.message).toContain("Fix:");
      expect(payload.availableWorkflows).toEqual(WORKFLOWS);
   });

   it("gives a declared workflow the package refuses its refusal, with its reason", async () => {
      const payload = parse(
         await getWorkflow()({ scopes, workflow: "broken" }),
      );
      expect(payload.workflow).toBeNull();
      expect(payload.reason).toBe("WORKFLOW_NOT_SERVED");
      expect(payload.message).toBe(REFUSAL);
   });

   it("resolves a workflow, defaulting to full skills", async () => {
      expect(
         parse(await getWorkflow()({ scopes, workflow: "analyst" })).workflow,
      ).toEqual({ name: "analyst", view: "full", skill_sets: [] });
   });

   it("passes the summary view through", async () => {
      expect(
         parse(
            await getWorkflow()({
               scopes,
               workflow: "analyst",
               skills: "summary",
            }),
         ).workflow,
      ).toEqual({ name: "analyst", view: "summary", skill_sets: [] });
   });

   it("logs the workflow a failed read asked for", async () => {
      const warn = spyOn(logger, "warn");
      try {
         await getWorkflow(async () => {
            throw new PackageNotFoundError("Package pkg does not exist");
         })({ scopes, workflow: "analyst" });
         expect(warn.mock.calls as unknown[][]).toContainEqual([
            "[MCP Tool getWorkflow] lookup failed",
            expect.objectContaining({
               workflow: "analyst",
               packageName: "pkg",
            }),
         ]);
      } finally {
         warn.mockRestore();
      }
   });

   it("answers a missing package with a not-found error naming it", async () => {
      const result = await getWorkflow(async () => {
         throw new PackageNotFoundError("Package pkg does not exist");
      })({ scopes, workflow: "analyst" });
      expect(result.isError).toBe(true);
      const details = getNotFoundError("env/pkg");
      expect(parse(result)).toEqual({
         error: details.message,
         suggestions: details.suggestions,
      });
   });
});

describe("get_workflow: the reply stays within the tool-result limit", () => {
   const text = "x".repeat(FCJ_MAX_CHARS);
   const source = { sourceContentSha: "s", definitionSha: "d" };
   const workflowOf = (skills: string) =>
      skills === "full"
         ? { name: "analyst", view: "full", text, skill_sets: [] }
         : { name: "analyst", view: "summary", skill_sets: [] };
   const call = { scopes, workflow: "analyst" };

   it("answers a full view that does not fit with the summary view, and says so", async () => {
      const result = await getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) => workflowOf(skills),
      })(call);
      const payload = parse(result);
      expect(payload.workflow).toEqual({
         name: "analyst",
         view: "summary",
         skill_sets: [],
      });
      expect(payload.message).toContain("The full view would be");
      expect(payload.message).toContain(
         `over the limit of ${FCJ_MAX_CHARS}, so this is the summary view`,
      );
      expect(payload.availableWorkflows).toEqual(WORKFLOWS);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("says the summary leaves out the listed files, and names get_skill_set and REST for them", async () => {
      const whole = parse(
         await getWorkflow(undefined, {
            getWorkflow: (_name: string, skills: string) => workflowOf(skills),
         })(call),
      );
      expect(whole.message).toContain("listed files");
      expect(whole.message).toContain("get_skill_set");
      expect(whole.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/workflows/analyst?skills=summary",
      );
   });

   it("leaves a full view that fits, and a summary that was asked for, as they are", async () => {
      const handler = getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) =>
            skills === "full"
               ? { name: "analyst", view: "full", skill_sets: [] }
               : workflowOf(skills),
      });
      const full = parse(await handler(call));
      expect(full.workflow.view).toBe("full");
      expect(full.message).toBeUndefined();
      const summary = parse(await handler({ ...call, skills: "summary" }));
      expect(summary.workflow.view).toBe("summary");
      expect(summary.message).toBeUndefined();
   });

   /** A workflow whose every view is `instructions` plus one set whose skill entries are `skillText` long. */
   const withInstructions = (instructions: string, skillText: string) => ({
      name: "analyst",
      description: "answers questions",
      instructions,
      skill_sets: [
         {
            name: "main",
            path: "sets/main",
            digest: "d",
            skills: [{ relative_filepath: "guide/SKILL.md", text: skillText }],
            warnings: [],
         },
      ],
      warnings: [
         {
            severity: "warn",
            code: "skill-shadowed",
            subject: "analyst",
            message: "a finding",
         },
      ],
      source,
   });

   it("keeps the instructions and each set's name and digest when the summary does not fit", async () => {
      const result = await getWorkflow(undefined, {
         getWorkflow: () => withInstructions("Do the job.", text),
      })(call);
      const payload = parse(result);
      // The brief is what a caller adopts a workflow for: it stays whenever it fits, with the pins that name the sets.
      expect(payload.workflow).toEqual({
         name: "analyst",
         description: "answers questions",
         instructions: "Do the job.",
         skill_sets: [{ name: "main", digest: "d" }],
         source,
      });
      expect(payload.message).toContain(
         `over the limit of ${FCJ_MAX_CHARS}, so this keeps the instructions and each skill set's name and digest`,
      );
      expect(payload.message).toContain("skill entries and warnings");
      expect(payload.message).toContain("get_skill_set");
      expect(payload.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/workflows/analyst?skills=summary",
      );
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("refuses a view that does not fit even without its skill entries, keeping its name and pin", async () => {
      const result = await getWorkflow(undefined, {
         getWorkflow: () => withInstructions(text, text),
      })({ ...call, skills: "summary" });
      const payload = parse(result);
      // Matched, not "no workflow matched": its name and pin stay, and nothing of its body.
      expect(payload.workflow).toEqual({ name: "analyst", source });
      expect(payload.message).toContain(
         `over the limit of ${FCJ_MAX_CHARS}, so get_workflow cannot return it`,
      );
      expect(payload.message).toContain("get_skill_set");
      expect(payload.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/workflows/analyst",
      );
      expect(payload.availableWorkflows).toEqual(WORKFLOWS);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("keeps every answer within the limit as a full view grows through it", async () => {
      const shapes = await sweep(
         (n) =>
            getWorkflow(undefined, {
               getWorkflow: (_name: string, skills: string) =>
                  skills === "full"
                     ? {
                          name: "analyst",
                          text: "y".repeat(n),
                          skill_sets: [],
                       }
                     : { name: "analyst", skill_sets: [] },
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
               getWorkflow: (_name: string, skills: string) =>
                  withInstructions(
                     "Do the job.",
                     "y".repeat(skills === "full" ? FCJ_MAX_CHARS : n),
                  ),
            }),
         call,
         (payload) =>
            payload.workflow.skill_sets[0].skills === undefined
               ? "sets only"
               : "summary",
         900,
      );
      expect([...shapes].sort()).toEqual(["sets only", "summary"]);
   });

   it("keeps every answer within the limit as the instructions grow through it, message included", async () => {
      const shapes = await sweep(
         (n) =>
            getWorkflow(undefined, {
               getWorkflow: () =>
                  withInstructions("y".repeat(n), "z".repeat(2000)),
            }),
         call,
         (payload) =>
            payload.workflow.skill_sets === undefined
               ? "refused"
               : payload.workflow.skill_sets[0].skills === undefined
                 ? "sets only"
                 : "whole",
         2500,
      );
      expect([...shapes].sort()).toEqual(["refused", "sets only", "whole"]);
   });

   it("pins every REST fallback URL to the version the reply was read from", async () => {
      const versioned = { ...source, versionId: "1.2.3" };
      const view = getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) => ({
            ...workflowOf(skills),
            source: versioned,
         }),
      });
      expect(parse(await view(call)).message).toContain(
         "workflows/analyst?versionId=1.2.3&skills=summary.",
      );
      const refusing = getWorkflow(undefined, {
         getWorkflow: () => ({
            ...withInstructions(text, text),
            source: versioned,
         }),
      });
      expect(
         parse(await refusing({ ...call, skills: "summary" })).message,
      ).toContain("workflows/analyst?versionId=1.2.3&skills=summary");
   });

   it("lists as many workflows as fit beside the answer, and counts the rest", async () => {
      const many = Array.from({ length: 1000 }, (_, i) => ({
         ...WORKFLOWS[0]!,
         name: i === 0 ? "analyst" : `analyst-${i}`,
      }));
      const handler = getWorkflow(undefined, {
         listWorkflows: () => many,
         workflowNotServed: notServed(many),
      });
      for (const params of [{ scopes }, call, { ...call, workflow: "nope" }]) {
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
      const beyond = parse(await handler({ ...call, workflow: "analyst-999" }));
      expect(beyond.workflow).toMatchObject({ name: "analyst-999" });
   });

   it("measures the reply with the request's own id", async () => {
      const handler = getWorkflow(undefined, {
         getWorkflow: (_name: string, skills: string) => ({
            name: "analyst",
            skill_sets: [],
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
