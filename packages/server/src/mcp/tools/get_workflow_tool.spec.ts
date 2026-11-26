// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { PackageNotFoundError } from "../../errors";
import { getNotFoundError } from "../error_messages";
import { registerGetAgentTool } from "./get_agent_tool";
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
const AGENTS = [{ name: "analyst", description: "answers questions" }];

/** get_agent over a package that serves one agent with one skill file. */
function getAgent(getPackage?: () => Promise<unknown>): Handler {
   const pkg = {
      listAgents: () => AGENTS,
      getAgent: (name: string, skills: string) => ({ name, skills }),
      getAgentSkillFile: (_agent: string, path: string) =>
         path === "guide.md"
            ? { relative_filepath: path, file_contents: "# guide" }
            : undefined,
   };
   const store = {
      getEnvironment: async () => ({
         getPackage: getPackage ?? (async () => pkg),
      }),
   };
   let handler: Handler | undefined;
   registerGetAgentTool(
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

describe("get_agent", () => {
   it("lists the package's agents when no agent is named", async () => {
      const result = await getAgent()({ scopes });
      expect(result.isError).toBe(false);
      expect(result.content[0]!.resource!.uri).toBe(
         "malloy://environment/env/package/pkg#getAgent",
      );
      expect(parse(result)).toEqual({ agent: null, availableAgents: AGENTS });
   });

   it("says skill_path needs agent_name", async () => {
      const payload = parse(
         await getAgent()({ scopes, skill_path: "guide.md" }),
      );
      expect(payload.agent).toBeNull();
      expect(payload.message).toContain("skill_path needs agent_name");
   });

   it("names an unknown agent and how to fix it", async () => {
      const payload = parse(await getAgent()({ scopes, agent_name: "nope" }));
      expect(payload.agent).toBeNull();
      expect(payload.message).toContain('No agent named "nope"');
   });

   it("resolves an agent, defaulting to full skills", async () => {
      expect(
         parse(await getAgent()({ scopes, agent_name: "analyst" })).agent,
      ).toEqual({ name: "analyst", skills: "full" });
   });

   it("returns one skill file without an agent key, or says it is missing", async () => {
      const handler = getAgent();
      expect(
         parse(
            await handler({
               scopes,
               agent_name: "analyst",
               skill_path: "guide.md",
            }),
         ),
      ).toEqual({
         skillFile: { relative_filepath: "guide.md", file_contents: "# guide" },
         availableAgents: AGENTS,
      });
      const missing = parse(
         await handler({ scopes, agent_name: "analyst", skill_path: "x.md" }),
      );
      expect(missing.agent).toBeNull();
      expect(missing.message).toContain('has no skill file "x.md"');
   });

   it("answers a missing package with a not-found error naming it", async () => {
      const result = await getAgent(async () => {
         throw new PackageNotFoundError("Package pkg does not exist");
      })({ scopes, agent_name: "analyst" });
      expect(result.isError).toBe(true);
      const details = getNotFoundError("env/pkg");
      expect(parse(result)).toEqual({
         error: details.message,
         suggestions: details.suggestions,
      });
   });
});
