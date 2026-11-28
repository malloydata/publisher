// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import { FCJ_MAX_CHARS } from "../../constants";
import { PackageNotFoundError } from "../../errors";
import { logger } from "../../logger";
import {
   handlerOf,
   measuredWhile,
   parse,
   scopes,
   sweep,
   wire,
   type Handler,
} from "../../test_helpers/skill_tool_harness";
import { getNotFoundError } from "../error_messages";
import { registerGetSkillSetTool } from "./get_skill_set_tool";

const SETS = [{ name: "main", description: "house rules", digest: "d" }];
const source = { sourceContentSha: "s", servedRevision: "r" };

/** The refusal text a package gives for a skill set its manifest declares and does not serve. */
const REFUSAL =
   "Skill set 'broken' is not served: its folder is missing. Fix: correct the 'skillSets.broken' entry in publisher.json and the folder it names.";

/** What the package answers for a name it is asked to serve among `served`. */
const notServed = (served: Array<{ name: string }>) => (name: string) =>
   served.some((s) => s.name === name)
      ? undefined
      : name === "broken"
        ? { reason: "SKILL_SET_NOT_SERVED", message: REFUSAL }
        : {
             reason: "SKILL_SET_NOT_DECLARED",
             message: `No skill set named '${name}' is declared in package 'pkg'. Fix: use a name the package lists.`,
          };

/** get_skill_set over a package that serves one set holding one skill file; `over` replaces what the package answers. */
function getSkillSet(
   getPackage?: () => Promise<unknown>,
   over: Record<string, unknown> = {},
): Handler {
   const pkg = {
      listSkillSets: () => SETS,
      skillSetNotServed: notServed(SETS),
      getSkillSet: (name: string, skills: string) => ({
         name,
         view: skills,
         skills: [],
         source,
      }),
      getSkillSetFile: async (name: string, path: string) =>
         name === "main" && path === "guide/SKILL.md"
            ? {
                 relative_filepath: path,
                 file_contents: "# guide",
                 digest: "d",
                 source,
              }
            : undefined,
      ...over,
   };
   return handlerOf(registerGetSkillSetTool, getPackage ?? (async () => pkg));
}

describe("get_skill_set", () => {
   it("lists the package's skill sets when no set is named", async () => {
      const result = await getSkillSet()({ scopes });
      expect(result.isError).toBe(false);
      expect(result.content[0]!.resource!.uri).toBe(
         "malloy://environment/env/package/pkg#getSkillSet",
      );
      expect(parse(result)).toEqual({
         skillSet: null,
         availableSkillSets: SETS,
      });
   });

   it("says skill_path needs skill_set", async () => {
      const payload = parse(
         await getSkillSet()({ scopes, skill_path: "guide/SKILL.md" }),
      );
      expect(payload.skillSet).toBeNull();
      expect(payload.message).toContain("skill_path needs skill_set");
      expect(payload).not.toHaveProperty("skillFile");
   });

   it("names an undeclared set, with its reason and how to fix it", async () => {
      const payload = parse(await getSkillSet()({ scopes, skill_set: "nope" }));
      expect(payload.skillSet).toBeNull();
      expect(payload.reason).toBe("SKILL_SET_NOT_DECLARED");
      expect(payload.message).toContain("No skill set named 'nope'");
      expect(payload.message).toContain("Fix:");
      expect(payload.availableSkillSets).toEqual(SETS);
   });

   it("gives a declared set the package refuses its refusal, with its reason, for the set and for a file of it", async () => {
      for (const extra of [{}, { skill_path: "guide/SKILL.md" }]) {
         const payload = parse(
            await getSkillSet()({ scopes, skill_set: "broken", ...extra }),
         );
         expect(payload.skillSet).toBeNull();
         expect(payload.reason).toBe("SKILL_SET_NOT_SERVED");
         expect(payload.message).toBe(REFUSAL);
         expect(payload).not.toHaveProperty("skillFile");
      }
   });

   it("resolves a set, defaulting to full skills", async () => {
      expect(
         parse(await getSkillSet()({ scopes, skill_set: "main" })).skillSet,
      ).toEqual({ name: "main", view: "full", skills: [], source });
   });

   it("passes the summary view through", async () => {
      expect(
         parse(
            await getSkillSet()({
               scopes,
               skill_set: "main",
               skills: "summary",
            }),
         ).skillSet,
      ).toEqual({ name: "main", view: "summary", skills: [], source });
   });

   it("returns one skill file without a skillSet key, or says it is missing", async () => {
      const handler = getSkillSet();
      expect(
         parse(
            await handler({
               scopes,
               skill_set: "main",
               skill_path: "guide/SKILL.md",
            }),
         ),
      ).toEqual({
         skillFile: {
            relative_filepath: "guide/SKILL.md",
            file_contents: "# guide",
            digest: "d",
            source,
         },
         availableSkillSets: SETS,
      });
      const missing = parse(
         await handler({
            scopes,
            skill_set: "main",
            skill_path: "x/SKILL.md",
         }),
      );
      // Not `skillSet: null`, which reads as "the name did not match".
      expect("skillSet" in missing).toBe(false);
      expect(missing.message).toBeUndefined();
      expect(missing.skillFile.relative_filepath).toBe("x/SKILL.md");
      expect(missing.skillFile.file_contents).toBeUndefined();
      expect(missing.skillFile.message).toContain(
         'has no skill file "x/SKILL.md"',
      );
   });

   it("answers a file that changed since the load with a message and no Fix clause", async () => {
      const payload = parse(
         await getSkillSet(undefined, {
            getSkillSetFile: async () => ({ changed: true }),
         })({ scopes, skill_set: "main", skill_path: "guide/SKILL.md" }),
      );
      expect("skillSet" in payload).toBe(false);
      expect(payload.skillFile.relative_filepath).toBe("guide/SKILL.md");
      expect(payload.skillFile.message).toContain(
         "changed on disk after the package",
      );
      expect(payload.skillFile.message).not.toContain("Fix:");
   });

   it("logs the set and the file a failed read asked for", async () => {
      const warn = spyOn(logger, "warn");
      try {
         await getSkillSet(async () => {
            throw new PackageNotFoundError("Package pkg does not exist");
         })({ scopes, skill_set: "main", skill_path: "guide/SKILL.md" });
         expect(warn.mock.calls as unknown[][]).toContainEqual([
            "[MCP Tool getSkillSet] lookup failed",
            expect.objectContaining({
               skillSet: "main",
               skillPath: "guide/SKILL.md",
            }),
         ]);
      } finally {
         warn.mockRestore();
      }
   });

   it("answers a missing package with a not-found error naming it", async () => {
      const result = await getSkillSet(async () => {
         throw new PackageNotFoundError("Package pkg does not exist");
      })({ scopes, skill_set: "main" });
      expect(result.isError).toBe(true);
      const details = getNotFoundError("env/pkg");
      expect(parse(result)).toEqual({
         error: details.message,
         suggestions: details.suggestions,
      });
   });
});

describe("get_skill_set: the reply stays within the tool-result limit", () => {
   const text = "x".repeat(FCJ_MAX_CHARS);
   const call = { scopes, skill_set: "main" };
   const setOf = (skills: string) =>
      skills === "full"
         ? { name: "main", digest: "d", view: "full", text, skills: [], source }
         : { name: "main", digest: "d", view: "summary", skills: [], source };

   it("answers a full view that does not fit with the summary view, and says so", async () => {
      const result = await getSkillSet(undefined, {
         getSkillSet: (_name: string, skills: string) => setOf(skills),
      })(call);
      const payload = parse(result);
      expect(payload.skillSet.view).toBe("summary");
      expect(payload.message).toContain("The full view would be");
      expect(payload.message).toContain(
         `over the limit of ${FCJ_MAX_CHARS}, so this is the summary view`,
      );
      expect(payload.message).toContain("skill_path");
      expect(payload.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/skill-sets/main?skills=summary",
      );
      expect(payload.availableSkillSets).toEqual(SETS);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("leaves a full view that fits, and a summary that was asked for, as they are", async () => {
      const handler = getSkillSet(undefined, {
         getSkillSet: (_name: string, skills: string) =>
            skills === "full"
               ? { name: "main", view: "full", skills: [] }
               : setOf(skills),
      });
      const full = parse(await handler(call));
      expect(full.skillSet.view).toBe("full");
      expect(full.message).toBeUndefined();
      const summary = parse(await handler({ ...call, skills: "summary" }));
      expect(summary.skillSet.view).toBe("summary");
      expect(summary.message).toBeUndefined();
   });

   /** The views the package is asked to build while the tool answers `params`. */
   const viewsBuilt = async (
      read: (skills: string) => unknown,
      params: Record<string, unknown> = {},
   ) => {
      const views: string[] = [];
      await getSkillSet(undefined, {
         getSkillSet: (_name: string, skills: string) => {
            views.push(skills);
            return read(skills);
         },
      })({ ...call, ...params });
      return views;
   };

   it("builds the summary view only when the reply falls back to it", async () => {
      const fits = (skills: string) => ({
         name: "main",
         view: skills,
         skills: [],
      });
      expect(await viewsBuilt(fits)).toEqual(["full"]);
      expect(await viewsBuilt(setOf)).toEqual(["full", "summary"]);
      expect(await viewsBuilt(fits, { skills: "summary" })).toEqual([
         "summary",
      ]);
   });

   it("keeps the set's name, digest and source when its summary does not fit", async () => {
      const result = await getSkillSet(undefined, {
         getSkillSet: () => ({
            name: "main",
            description: "house rules",
            digest: "d",
            path: "sets/main",
            skills: [{ relative_filepath: "a/SKILL.md", text }],
            warnings: [],
            source,
         }),
      })({ ...call, skills: "summary" });
      const payload = parse(result);
      // Matched, not "no set matched": its name and pin stay, and nothing of its body.
      expect(payload.skillSet).toEqual({
         name: "main",
         digest: "d",
         source,
      });
      expect(payload.message).toContain(
         `over the limit of ${FCJ_MAX_CHARS}, so this keeps the set's name, digest and source`,
      );
      expect(payload.message).toContain("skill_path");
      expect(payload.availableSkillSets).toEqual(SETS);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   /** A set whose full view holds `chars` characters of real file text, and whose summary holds none. */
   const withFileText = (chars: number) => (name: string, skills: string) => ({
      name,
      digest: "d",
      skills: [
         skills === "full"
            ? {
                 relative_filepath: "guide/SKILL.md",
                 file_contents: "x".repeat(chars),
              }
            : { relative_filepath: "guide/SKILL.md", name: "guide" },
      ],
      warnings: [],
      source,
   });

   it("measures a reply that fits once", async () => {
      const handler = getSkillSet(undefined, {
         getSkillSet: withFileText(1000),
      });
      expect(await measuredWhile(() => handler(call))).toHaveLength(1);
   });

   it("measures no reply twice on its way down the ladder", async () => {
      // File text just under the limit: the full view cannot be ruled out without measuring it.
      const handler = getSkillSet(undefined, {
         getSkillSet: withFileText(FCJ_MAX_CHARS - 50),
      });
      const measured = await measuredWhile(() => handler(call));
      expect(measured.length).toBeGreaterThan(1);
      expect(new Set(measured.map((m) => JSON.stringify(m))).size).toBe(
         measured.length,
      );
   });

   it("never measures a view or a file whose text alone is over the limit", async () => {
      const handler = getSkillSet(undefined, {
         getSkillSet: withFileText(FCJ_MAX_CHARS + 1),
         getSkillSetFile: async (_name: string, path: string) => ({
            relative_filepath: path,
            file_contents: "x".repeat(FCJ_MAX_CHARS + 1),
            digest: "d",
            source,
         }),
      });
      let result: Awaited<ReturnType<Handler>> | undefined;
      const measured = await measuredWhile(async () => {
         result = await handler(call);
      });
      // Only the summary, which fits, is measured.
      expect(measured).toHaveLength(1);
      expect(JSON.stringify(measured)).not.toContain("file_contents");
      expect(parse(result!).message).toContain(
         "The full view would be at least",
      );
      expect(wire(result!)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      const file = await measuredWhile(async () => {
         result = await handler({ ...call, skill_path: "guide/SKILL.md" });
      });
      // Only the refusal, which fits, is measured.
      expect(file).toHaveLength(1);
      expect(JSON.stringify(file)).not.toContain("file_contents");
      expect(parse(result!).skillFile.message).toContain("at least");
   });

   it("answers a skill file that does not fit with a message in place of its text, keeping its pin", async () => {
      const result = await getSkillSet(undefined, {
         getSkillSetFile: async (_name: string, path: string) => ({
            relative_filepath: path,
            file_contents: text,
            digest: "d",
            source,
         }),
      })({ ...call, skill_path: "guide/assets/big.txt" });
      const { skillFile, skillSet } = parse(result);
      expect(skillSet).toBeUndefined();
      expect(skillFile).toEqual({
         relative_filepath: "guide/assets/big.txt",
         message: expect.stringContaining(
            `over the limit, so get_skill_set cannot return it`,
         ),
         digest: "d",
         source,
      });
      expect(skillFile.message).toContain(
         "GET /api/v0/environments/env/packages/pkg/skill-sets/main/skills/guide/assets/big.txt",
      );
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
   });

   it("hints the REST path with each segment URL-encoded", async () => {
      const result = await getSkillSet(undefined, {
         getSkillSetFile: async (_name: string, path: string) => ({
            relative_filepath: path,
            file_contents: text,
            digest: "d",
            source,
         }),
      })({ ...call, skill_path: "guide/reference/a b#c%d?.md" });
      expect(parse(result).skillFile.message).toContain(
         "/skills/guide/reference/a%20b%23c%25d%3F.md",
      );
   });

   it("keeps every answer within the limit as a full view grows through it", async () => {
      const shapes = await sweep(
         (n) =>
            getSkillSet(undefined, {
               getSkillSet: (_name: string, skills: string) =>
                  skills === "full"
                     ? { name: "main", text: "y".repeat(n), skills: [], source }
                     : { name: "main", skills: [], source },
            }),
         call,
         (payload) =>
            payload.skillSet.text === undefined ? "summary" : "full",
         500,
      );
      expect([...shapes].sort()).toEqual(["full", "summary"]);
   });

   it("keeps every answer within the limit as a summary grows through it, message included", async () => {
      const shapes = await sweep(
         (n) =>
            getSkillSet(undefined, {
               getSkillSet: (_name: string, skills: string) => ({
                  name: "main",
                  digest: "d",
                  skills: [
                     {
                        relative_filepath: "a/SKILL.md",
                        text: "y".repeat(skills === "full" ? FCJ_MAX_CHARS : n),
                     },
                  ],
                  source,
               }),
            }),
         call,
         (payload) =>
            payload.skillSet.skills === undefined ? "pin" : "summary",
         900,
      );
      expect([...shapes].sort()).toEqual(["pin", "summary"]);
   });

   it("keeps every answer within the limit as a skill file grows through it", async () => {
      const shapes = await sweep(
         (n) =>
            getSkillSet(undefined, {
               getSkillSetFile: async (_name: string, path: string) => ({
                  relative_filepath: path,
                  file_contents: "y".repeat(n),
                  digest: "d",
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

   it("pins every REST fallback URL to the version the reply was read from", async () => {
      const versioned = { ...source, versionId: "1.2.3" };
      const fileHandler = getSkillSet(undefined, {
         getSkillSetFile: async (_name: string, path: string) => ({
            relative_filepath: path,
            file_contents: text,
            digest: "d",
            source: versioned,
         }),
      });
      expect(
         parse(await fileHandler({ ...call, skill_path: "guide/SKILL.md" }))
            .skillFile.message,
      ).toContain("/skills/guide/SKILL.md?versionId=1.2.3");
      const view = getSkillSet(undefined, {
         getSkillSet: (_name: string, skills: string) => ({
            ...setOf(skills),
            source: versioned,
         }),
      });
      expect(parse(await view(call)).message).toContain(
         "skill-sets/main?versionId=1.2.3&skills=summary",
      );
   });

   it("lists as many sets as fit beside the answer, and counts the rest", async () => {
      const many = Array.from({ length: 1000 }, (_, i) => ({
         ...SETS[0]!,
         name: i === 0 ? "main" : `set-${i}`,
      }));
      const handler = getSkillSet(undefined, {
         listSkillSets: () => many,
         skillSetNotServed: notServed(many),
      });
      for (const params of [{ scopes }, call, { ...call, skill_set: "nope" }]) {
         const result = await handler(params);
         const payload = parse(result);
         expect(payload.availableSkillSets.length).toBeGreaterThan(0);
         expect(payload.availableSkillSets.length).toBeLessThan(many.length);
         expect(payload.availableSkillSets[0].name).toBe("main");
         expect(payload.message).toContain(
            `${many.length - payload.availableSkillSets.length} of ${many.length} skill sets are not listed`,
         );
         expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      }
      // A name beyond the cut is still resolved.
      const beyond = parse(await handler({ ...call, skill_set: "set-999" }));
      expect(beyond.skillSet).toMatchObject({ name: "set-999" });
   });
});
