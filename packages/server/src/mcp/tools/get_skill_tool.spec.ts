// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { z } from "zod/v3";
import type { EnvironmentStore } from "../../service/environment_store";
import { type SkillEntry } from "../skills/build_skills_bundle";
import { registerGetSkillTool } from "./get_skill_tool";

type Handler = (params: unknown) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

const bundled: SkillEntry[] = [
   { name: "malloy-analysis", description: "bundled", body: "BUNDLED" },
   { name: "malloy-review", description: "bundled", body: "BUNDLED REVIEW" },
];

const storeWith = (skills: SkillEntry[]): EnvironmentStore =>
   ({
      getEnvironment: async (name: string) => {
         if (name !== "env") throw new Error("unknown environment");
         return { getPackage: async () => ({ listSkills: () => skills }) };
      },
   }) as unknown as EnvironmentStore;

function register(skills: SkillEntry[]) {
   let handler: Handler | undefined;
   let shape: z.ZodRawShape | undefined;
   registerGetSkillTool(
      {
         tool: (_n: string, _d: string, s: z.ZodRawShape, h: Handler) => {
            shape = s;
            handler = h;
         },
      } as never,
      storeWith(skills),
      bundled,
   );
   return { handler: handler!, shape: shape! };
}

const body = (r: { content: Array<{ resource?: { text: string } }> }) =>
   JSON.parse(r.content[0]!.resource!.text);
const scopes = [{ environment: "env", package: "pkg" }];

describe("get_skill tool", () => {
   it("lists only the bundled set without scopes", async () => {
      const { handler } = register([
         { name: "house", description: "ours", body: "HOUSE" },
      ]);
      const out = body(await handler({}));
      expect(out.skill).toBeNull();
      expect(out.availableSkills).toEqual([
         { name: "malloy-analysis", description: "bundled", origin: "bundled" },
         { name: "malloy-review", description: "bundled", origin: "bundled" },
      ]);
   });

   it("lets the package's copy replace a bundled guide and reports origin", async () => {
      const { handler } = register([
         { name: "malloy-review", description: "ours", body: "OURS" },
         { name: "house", description: "new", body: "HOUSE" },
      ]);
      const out = body(await handler({ scopes, skill_name: "malloy-review" }));
      expect(out.skill).toEqual({
         name: "malloy-review",
         content: "OURS",
         origin: "package",
      });
      expect(
         out.availableSkills.map((s: { name: string; origin: string }) => [
            s.name,
            s.origin,
         ]),
      ).toEqual([
         ["malloy-analysis", "bundled"],
         ["malloy-review", "package"],
         ["house", "package"],
      ]);
   });

   it("fetches a bundled guide with its bundled origin when the package does not shadow it", async () => {
      const { handler } = register([]);
      const out = body(
         await handler({ scopes, skill_name: "malloy-analysis" }),
      );
      expect(out.skill).toEqual({
         name: "malloy-analysis",
         content: "BUNDLED",
         origin: "bundled",
      });
   });

   it("answers a name that matches nothing with the list and a Fix, not an error", async () => {
      const { handler } = register([]);
      const result = await handler({ scopes, skill_name: "nope" });
      expect(result.isError).toBeFalsy();
      const out = body(result);
      expect(out.skill).toBeNull();
      expect(out.message).toContain("Fix:");
      expect(out.availableSkills).toHaveLength(2);
   });

   it("returns a tool error for an unknown environment", async () => {
      const { handler } = register([]);
      const result = await handler({
         scopes: [{ environment: "missing", package: "pkg" }],
      });
      expect(result.isError).toBe(true);
   });

   it("requires exactly one scope when scopes is given", () => {
      const { shape } = register([]);
      const schema = z.object(shape);
      expect(schema.safeParse({ scopes: [] }).success).toBe(false);
      expect(schema.safeParse({ scopes: [...scopes, ...scopes] }).success).toBe(
         false,
      );
      expect(schema.safeParse({ scopes }).success).toBe(true);
      expect(schema.safeParse({}).success).toBe(true);
   });
});
