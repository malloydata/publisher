// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { z } from "zod/v3";
import { type BundledSkill } from "../../service/skills/build_skills_bundle";
import {
   type ServedSkill,
   type SkillFileRead,
} from "../../service/skills/package_skills";
import { FCJ_MAX_CHARS } from "../../retrieval/search/pages";
import { registerGetSkillTool } from "./get_skill_tool";
import { fakeOf } from "../../test_helpers/store_fakes";
import type { PackageLookup } from "../../service/environment_ports";

type Handler = (
   params: unknown,
   extra?: { requestId?: string | number },
) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

const DIGEST = "sha256:0";

const bundledSkill = (
   name: string,
   description: string,
   bodies: Record<string, string>,
   manifestOnly: Record<string, number> = {},
): BundledSkill => ({
   name,
   frontmatter: { name, description },
   files: {
      ...Object.fromEntries(
         Object.entries(bodies).map(([p, t]) => [
            p,
            { digest: DIGEST, size: t.length },
         ]),
      ),
      ...Object.fromEntries(
         Object.entries(manifestOnly).map(([p, size]) => [
            p,
            { digest: DIGEST, size },
         ]),
      ),
   },
   bodies,
});

/** A package skill whose files are text, or a read with a size (a file with no text). */
const packageSkill = (
   name: string,
   description: string,
   files: Record<string, string | { read: SkillFileRead; size: number }>,
): ServedSkill => ({
   name,
   description,
   catalog: {
      name,
      frontmatter: { name, description, license: "MIT" },
      files: Object.fromEntries(
         Object.entries(files).map(([p, f]) => [
            p,
            { digest: DIGEST, size: typeof f === "string" ? f.length : f.size },
         ]),
      ),
   },
   read: (p) => {
      const file = files[p]!;
      return typeof file === "string" ? { text: file } : file.read;
   },
});

const bundled: BundledSkill[] = [
   bundledSkill(
      "malloy-analysis",
      "bundled",
      { "SKILL.md": "BUNDLED", "references/joins.md": "JOINS" },
      { "scripts/check.py": 120 },
   ),
   bundledSkill("malloy-review", "bundled", { "SKILL.md": "BUNDLED REVIEW" }),
];

const storeWith = (skills: ServedSkill[]): PackageLookup =>
   fakeOf<PackageLookup>({
      getEnvironment: async (name: string) => {
         if (name !== "env") throw new Error("unknown environment");
         return { getPackage: async () => ({ listSkills: () => skills }) };
      },
   });

function register(skills: ServedSkill[]) {
   let handler: Handler | undefined;
   let shape: z.ZodRawShape | undefined;
   registerGetSkillTool(
      {
         registerTool: (
            _n: string,
            { inputSchema: s }: { inputSchema: z.ZodRawShape },
            h: Handler,
         ) => {
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

/** Characters of the JSON-RPC message a host receives for this answer. */
const wire = (
   r: { content: unknown[] },
   id: string | number = "i".repeat(62),
) => JSON.stringify({ jsonrpc: "2.0", id, result: r }).length;

const house = packageSkill("house", "ours", {
   "SKILL.md": "HOUSE",
   "scripts/run.py": "print('run')\n",
   "references/a.md": "# A\n",
   "assets/logo.png": { read: { binary: true }, size: 68 },
   "scripts/moved.py": { read: { changed: true }, size: 9 },
});

describe("get_skill tool", () => {
   it("lists only the bundled set without scopes, and returns no skill", async () => {
      const { handler } = register([house]);
      const out = body(await handler({}));
      expect(out.skill).toBeUndefined();
      expect(out.message).toBeUndefined();
      expect(out.availableSkills).toEqual([
         { name: "malloy-analysis", description: "bundled", origin: "bundled" },
         { name: "malloy-review", description: "bundled", origin: "bundled" },
      ]);
   });

   it("ignores path when no skill is named", async () => {
      const { handler } = register([]);
      const out = body(await handler({ path: "scripts/check.py" }));
      expect(out.skill).toBeUndefined();
      expect(out.availableSkills).toHaveLength(2);
   });

   it("lets the package's copy replace a bundled guide and reports origin", async () => {
      const { handler } = register([
         packageSkill("malloy-review", "ours", { "SKILL.md": "OURS" }),
         house,
      ]);
      const out = body(await handler({ scopes, skill_name: "malloy-review" }));
      expect(out.skill).toEqual({
         name: "malloy-review",
         path: "SKILL.md",
         content: "OURS",
         origin: "package",
         frontmatter: {
            name: "malloy-review",
            description: "ours",
            license: "MIT",
         },
         files: [{ path: "SKILL.md", size: 4 }],
      });
      // A listing is not returned with a read.
      expect(out.availableSkills).toBeUndefined();
      const listed = body(await handler({ scopes }));
      expect(
         listed.availableSkills.map((s: { name: string; origin: string }) => [
            s.name,
            s.origin,
         ]),
      ).toEqual([
         ["malloy-analysis", "bundled"],
         ["malloy-review", "package"],
         ["house", "package"],
      ]);
   });

   it("reads a bundled SKILL.md verbatim with its frontmatter and every file of the skill", async () => {
      const { handler } = register([]);
      const out = body(
         await handler({ scopes, skill_name: "malloy-analysis" }),
      );
      expect(out.skill).toEqual({
         name: "malloy-analysis",
         path: "SKILL.md",
         content: "BUNDLED",
         origin: "bundled",
         frontmatter: { name: "malloy-analysis", description: "bundled" },
         files: [
            { path: "SKILL.md", size: 7 },
            { path: "references/joins.md", size: 5 },
            { path: "scripts/check.py", size: 120 },
         ],
      });
      expect(out.message).toBeUndefined();
   });

   it("reads another file by its path, with no frontmatter or files", async () => {
      const { handler } = register([house]);
      const script = body(
         await handler({
            scopes,
            skill_name: "house",
            path: "scripts/run.py",
         }),
      );
      expect(script).toEqual({
         skill: {
            name: "house",
            path: "scripts/run.py",
            content: "print('run')\n",
            origin: "package",
         },
      });
      const bundledRef = body(
         await handler({
            skill_name: "malloy-analysis",
            path: "references/joins.md",
         }),
      );
      expect(bundledRef.skill).toEqual({
         name: "malloy-analysis",
         path: "references/joins.md",
         content: "JOINS",
         origin: "bundled",
      });
   });

   it("reads a path with a leading ./ as the path without it", async () => {
      const { handler } = register([house]);
      const out = body(
         await handler({
            scopes,
            skill_name: "house",
            path: "./references/a.md",
         }),
      );
      expect(out.skill.path).toBe("references/a.md");
      expect(out.skill.content).toBeDefined();
   });

   it("reads ./SKILL.md and an empty path as SKILL.md", async () => {
      const { handler } = register([]);
      for (const path of ["./SKILL.md", "", " "]) {
         const out = body(await handler({ skill_name: "malloy-review", path }));
         expect(out.skill.path).toBe("SKILL.md");
         expect(out.skill.content).toBe("BUNDLED REVIEW");
      }
   });

   it("names the skill's files when the path is not one of them", async () => {
      const { handler } = register([house]);
      for (const path of ["nope.md", "../house/SKILL.md", "__proto__"]) {
         const out = body(await handler({ scopes, skill_name: "house", path }));
         expect(out.skill).toBeUndefined();
         expect(out.availableSkills).toBeUndefined();
         expect(out.message).toContain(
            `'${path}' is not a file of skill 'house'`,
         );
         expect(out.message).toContain(
            "Its files: SKILL.md, assets/logo.png, references/a.md, scripts/moved.py, scripts/run.py.",
         );
      }
   });

   it("answers a binary file as its size and a message, with no content", async () => {
      const { handler } = register([house]);
      const out = body(
         await handler({
            scopes,
            skill_name: "house",
            path: "assets/logo.png",
         }),
      );
      expect(out).toEqual({
         skill: {
            name: "house",
            path: "assets/logo.png",
            size: 68,
            origin: "package",
         },
         message:
            "'assets/logo.png' is a binary file (68 bytes), and only text files are served.",
      });
   });

   it("answers a listed file that changed since the load as a message alone, with no size", async () => {
      const { handler } = register([house]);
      const out = body(
         await handler({
            scopes,
            skill_name: "house",
            path: "scripts/moved.py",
         }),
      );
      expect(out).toEqual({
         message:
            "'scripts/moved.py' changed on disk after the package loaded, so it is not served until the package reloads.",
      });
   });

   it("answers a bundled skill's non-Markdown file as its manifest entry and the package to install", async () => {
      const { handler } = register([]);
      const out = body(
         await handler({
            skill_name: "malloy-analysis",
            path: "scripts/check.py",
         }),
      );
      expect(out.skill).toEqual({
         name: "malloy-analysis",
         path: "scripts/check.py",
         size: 120,
         origin: "bundled",
      });
      expect(out.message).toContain("`@malloy-publisher/skills`");
   });

   it("no longer reads <skill>/<stem>: it is an unknown skill, and says where a file goes", async () => {
      const { handler } = register([]);
      const result = await handler({
         skill_name: "malloy-analysis/joins",
      });
      expect(result.isError).toBeFalsy();
      const out = body(result);
      expect(out.skill).toBeUndefined();
      expect(out.availableSkills).toHaveLength(2);
      expect(out.message).toContain("Fix:");
      expect(out.message).toContain("`skill_name` plus `path`");
   });

   it("answers a name that matches nothing with the list and a Fix, not an error", async () => {
      const { handler } = register([]);
      const result = await handler({ scopes, skill_name: "nope" });
      expect(result.isError).toBeFalsy();
      const out = body(result);
      expect(out.skill).toBeUndefined();
      expect(out.message).toContain("Fix:");
      expect(out.message).not.toContain("`skill_name` plus `path`");
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
      expect(
         schema.safeParse({ skill_name: "a", path: "references/a.md" }).success,
      ).toBe(true);
   });
});

describe("get_skill tool: a reply always fits the tool-result limit", () => {
   const entries = (n: number) =>
      Object.fromEntries(
         Array.from({ length: n }, (_, i) => [
            `references/file-${String(i).padStart(4, "0")}.md`,
            "x",
         ]),
      );

   it("withholds a file over the limit, with its size and a message", async () => {
      const big = "y".repeat(FCJ_MAX_CHARS + 1);
      const { handler } = register([
         packageSkill("big", "d", {
            "SKILL.md": "S",
            "references/big.md": big,
         }),
      ]);
      const result = await handler({
         scopes,
         skill_name: "big",
         path: "references/big.md",
      });
      const out = body(result);
      expect(out.skill).toEqual({
         name: "big",
         path: "references/big.md",
         size: big.length,
         origin: "package",
      });
      expect(out.message).toContain("over the limit of");
      expect(out.message).toContain("a smaller file");
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("measures the message the host receives, not the text alone", async () => {
      // Quotes escape twice (in the text, then in the message), so text under the cap can still be over it on the wire.
      const quotes = '"'.repeat(Math.floor(FCJ_MAX_CHARS / 3));
      const { handler } = register([
         packageSkill("q", "d", { "SKILL.md": "S", "references/q.md": quotes }),
      ]);
      const result = await handler({
         scopes,
         skill_name: "q",
         path: "references/q.md",
      });
      const out = body(result);
      expect(out.skill.content).toBeUndefined();
      expect(out.message).toContain("over the limit of");
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("keeps a SKILL.md that fits, with every file listed and no message", async () => {
      const { handler } = register([
         packageSkill("many", "d", { "SKILL.md": "S", ...entries(20) }),
      ]);
      const result = await handler({ scopes, skill_name: "many" });
      const out = body(result);
      expect(out.skill.content).toBe("S");
      expect(out.skill.files).toHaveLength(21);
      expect(out.message).toBeUndefined();
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("keeps the body and trims the file list, saying how many files are not listed", async () => {
      const text = "b".repeat(FCJ_MAX_CHARS - 20_000);
      const { handler } = register([
         packageSkill("many", "d", { "SKILL.md": text, ...entries(1500) }),
      ]);
      const result = await handler({ scopes, skill_name: "many" });
      const out = body(result);
      expect(out.skill.content).toBe(text);
      expect(out.skill.size).toBeUndefined();
      const listed = out.skill.files.length;
      expect(listed).toBeGreaterThan(0);
      expect(listed).toBeLessThan(1501);
      expect(out.skill.files[0]).toEqual({
         path: "SKILL.md",
         size: text.length,
      });
      expect(out.message).toContain(
         `${1501 - listed} of 1501 files are not listed`,
      );
      expect(out.message).toContain("`path`");
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      // The most that fits: the slack is less than one more entry.
      expect(wire(result)).toBeGreaterThan(FCJ_MAX_CHARS - 150);
   });

   it("drops the body only when SKILL.md is over the limit alone, and still lists what fits", async () => {
      const text = "b".repeat(FCJ_MAX_CHARS + 1);
      const { handler } = register([
         packageSkill("huge", "d", { "SKILL.md": text, ...entries(3) }),
      ]);
      const result = await handler({ scopes, skill_name: "huge" });
      const out = body(result);
      expect(out.skill.content).toBeUndefined();
      expect(out.skill.size).toBe(text.length);
      expect(out.skill.frontmatter).toMatchObject({ name: "huge" });
      expect(out.skill.files).toHaveLength(4);
      expect(out.message).toContain("over the limit of");
      expect(out.message).toContain("one of the other files");
      expect(out.message).not.toContain("not listed");
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("trims the list of a withheld SKILL.md too, when even that reply is over", async () => {
      const text = "b".repeat(FCJ_MAX_CHARS + 1);
      const { handler } = register([
         packageSkill("huge", "d", { "SKILL.md": text, ...entries(3000) }),
      ]);
      const result = await handler({ scopes, skill_name: "huge" });
      const out = body(result);
      expect(out.skill.content).toBeUndefined();
      expect(out.skill.files.length).toBeLessThan(3001);
      expect(out.message).toContain("over the limit of");
      expect(out.message).toContain("files are not listed");
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("keeps a short body and says its text is in `content` when only the frontmatter is over the limit", async () => {
      const { handler } = register([
         packageSkill("fat", "d".repeat(FCJ_MAX_CHARS + 1), {
            "SKILL.md": "S",
            ...entries(3),
         }),
      ]);
      const result = await handler({ scopes, skill_name: "fat" });
      const out = body(result);
      expect(out.skill.content).toBe("S");
      expect(out.skill.frontmatter).toBeUndefined();
      expect(out.skill.files).toHaveLength(4);
      expect(out.message).toBe(
         "Its parsed frontmatter was too large to return; the text is in `content`.",
      );
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("drops the frontmatter too when the body and the frontmatter cannot both fit", async () => {
      const { handler } = register([
         packageSkill("fat", "d".repeat(FCJ_MAX_CHARS - 10_000), {
            "SKILL.md": "b".repeat(FCJ_MAX_CHARS - 10_000),
            ...entries(3),
         }),
      ]);
      const result = await handler({ scopes, skill_name: "fat" });
      const out = body(result);
      expect(out.skill.content).toHaveLength(FCJ_MAX_CHARS - 10_000);
      expect(out.skill.frontmatter).toBeUndefined();
      expect(out.message).toContain("the text is in `content`");
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("drops the body, then the frontmatter, as a last resort", async () => {
      const text = "b".repeat(FCJ_MAX_CHARS + 1);
      const { handler } = register([
         packageSkill("fat", "d".repeat(FCJ_MAX_CHARS + 1), {
            "SKILL.md": text,
            ...entries(3),
         }),
      ]);
      const result = await handler({ scopes, skill_name: "fat" });
      const out = body(result);
      expect(out.skill.content).toBeUndefined();
      expect(out.skill.frontmatter).toBeUndefined();
      expect(out.skill.files).toHaveLength(4);
      expect(out.message).toContain("Its frontmatter was too large to return.");
      expect(out.message).toContain("one of the other files");
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("lists every file when the full count fits, though a cut list would carry a longer message", async () => {
      // The body is withheld (over the limit alone); grow the file count until one more would not fit.
      const text = "b".repeat(FCJ_MAX_CHARS + 1);
      const probe = async (n: number) => {
         const { handler } = register([
            packageSkill("edge", "d", { "SKILL.md": text, ...entries(n) }),
         ]);
         return handler({ scopes, skill_name: "edge" });
      };
      const notListed = (r: Awaited<ReturnType<typeof probe>>) =>
         String(body(r).message).includes("not listed");
      let n = 1;
      while (!notListed(await probe(n + 1))) n += 1;
      const atEdge = await probe(n);
      expect(notListed(atEdge)).toBe(false);
      expect(body(atEdge).skill.files).toHaveLength(n + 1);
      // Less slack than the longer message needs: a search that tried n - 1 first would have missed it.
      expect(wire(atEdge)).toBeGreaterThan(FCJ_MAX_CHARS - 150);
      expect(wire(atEdge)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });
});

describe("get_skill tool: a listing and an unknown-name reply fit the limit", () => {
   const many = Array.from({ length: 300 }, (_, i) =>
      packageSkill(`skill-${String(i).padStart(3, "0")}`, "d".repeat(500), {
         "SKILL.md": "S",
      }),
   );

   it("lists the guides that fit and says how many are not listed", async () => {
      const { handler } = register(many);
      const result = await handler({ scopes });
      const out = body(result);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      expect(out.availableSkills.length).toBeGreaterThan(10);
      expect(out.availableSkills.length).toBeLessThan(302);
      expect(out.message).toContain(
         `${302 - out.availableSkills.length} of 302 guides are not listed`,
      );
      expect(out.message).toContain("skill_name");
   });

   it("keeps an unknown-name reply within the limit, with its fix and the count", async () => {
      const { handler } = register(many);
      const result = await handler({ scopes, skill_name: "nope" });
      const out = body(result);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      expect(out.message).toContain('No guide named "nope"');
      expect(out.message).toContain("guides are not listed");
   });

   it("lists every guide, with no message, when they fit", async () => {
      const { handler } = register([house]);
      const out = body(await handler({ scopes }));
      expect(out.availableSkills).toHaveLength(3);
      expect(out.message).toBeUndefined();
   });
});

describe("get_skill tool: the reply is measured with the request's own id", () => {
   it("trims a SKILL.md that fits with a short id but not with a long id whose characters escape", async () => {
      const files = (n: number) => ({ "SKILL.md": "b".repeat(n) });
      const reply = async (n: number, requestId?: string) => {
         const { handler } = register([packageSkill("edge", "d", files(n))]);
         return handler({ scopes, skill_name: "edge" }, { requestId });
      };
      // The reply grows one character per character of text, so the probe finds the size that just fits a plain id.
      const base = wire(await reply(1000));
      const n = 1000 + FCJ_MAX_CHARS - base - 5;
      const plain = await reply(n);
      expect(body(plain).message).toBeUndefined();
      expect(wire(plain)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      const requestId = '"'.repeat(60);
      const escaped = await reply(n, requestId);
      expect(body(escaped).skill.content).toBeUndefined();
      expect(wire(escaped, requestId)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });
});

describe("get_skill tool: an oversize file that is not SKILL.md", () => {
   it("does not point at a `files` list the reply does not carry", async () => {
      const big = "y".repeat(FCJ_MAX_CHARS + 1);
      const { handler } = register([
         packageSkill("big", "d", {
            "SKILL.md": "S",
            "references/big.md": big,
         }),
      ]);
      const out = body(
         await handler({
            scopes,
            skill_name: "big",
            path: "references/big.md",
         }),
      );
      expect(out.message).not.toContain("`files`");
      expect(out.message).toContain("a smaller file");
   });
});
