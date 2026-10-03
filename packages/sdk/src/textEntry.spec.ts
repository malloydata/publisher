// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import ts from "typescript";
import { artifactTag, splitSourceLines } from "./text-entry";

const ENTRY = path.join(import.meta.dir, "text-entry.ts");
const MODULE = path.join(
   import.meta.dir,
   "components/DashboardBuilder/malloyText.ts",
);

describe("@malloy-publisher/sdk/text", () => {
   // Hosts import this in vitest's plain node environment, where they also mock the main entry.
   it("re-exports one module, which imports nothing", () => {
      const entry = fs.readFileSync(ENTRY, "utf8");
      expect([...entry.matchAll(/from\s+"([^"]+)"/g)].map((m) => m[1])).toEqual(
         ["./components/DashboardBuilder/malloyText"],
      );
      expect(fs.readFileSync(MODULE, "utf8")).not.toMatch(
         /^\s*(import|export\s.*\sfrom)\b|\brequire\(|\bimport\(/m,
      );
   });

   it("loads in plain node with nothing installed and no DOM", () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sdk-text-"));
      try {
         // Transpiled rather than type-stripped: CI's node may predate stripping.
         const js = ts.transpileModule(fs.readFileSync(MODULE, "utf8"), {
            compilerOptions: {
               module: ts.ModuleKind.ESNext,
               target: ts.ScriptTarget.ES2022,
            },
         }).outputText;
         fs.writeFileSync(path.join(dir, "text.mjs"), js);
         fs.writeFileSync(
            path.join(dir, "check.mjs"),
            `import { artifactTag, splitSourceLines } from "./text.mjs";
const tag = artifactTag(splitSourceLines("x\\r\\n## artifact { kind=notebook }\\r\\nrun: a"));
console.log(JSON.stringify({ tag, document: typeof document }));`,
         );
         const run = Bun.spawnSync(["node", "check.mjs"], { cwd: dir });
         expect(run.stderr.toString()).toBe("");
         expect(JSON.parse(run.stdout.toString())).toEqual({
            tag: {
               from: 1,
               to: 1,
               block: false,
               text: "## artifact { kind=notebook }",
            },
            document: "undefined",
         });
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("exports only the tag reader and the line splitter", async () => {
      expect(Object.keys(await import("./text-entry")).sort()).toEqual([
         "artifactTag",
         "splitSourceLines",
      ]);
   });
});

describe("splitSourceLines", () => {
   it("splits on LF, CRLF and a bare CR, keeping no line ending", () => {
      expect(splitSourceLines("a\nb\r\nc\rd")).toEqual(["a", "b", "c", "d"]);
      expect(splitSourceLines("a\r\n\r\nb")).toEqual(["a", "", "b"]);
      expect(splitSourceLines("")).toEqual([""]);
      expect(splitSourceLines("a\n")).toEqual(["a", ""]);
   });
});

describe("artifactTag", () => {
   const tagOf = (source: string) => artifactTag(splitSourceLines(source));

   it("finds a one-line tag and a block tag, by line", () => {
      expect(tagOf("##! x\n## artifact { kind=dashboard }\nrun: a")).toEqual({
         from: 1,
         to: 1,
         block: false,
         text: "## artifact { kind=dashboard }",
      });
      expect(tagOf("##| artifact { kind=notebook\n}\n|##\nrun: a")).toEqual({
         from: 0,
         to: 2,
         block: true,
         text: "##| artifact { kind=notebook\n}",
      });
   });

   it("reads the same tag from CRLF text", () => {
      expect(
         tagOf("##| artifact { kind=notebook\r\n}\r\n|##\r\nrun: a"),
      ).toEqual(tagOf("##| artifact { kind=notebook\n}\n|##\nrun: a"));
   });

   it("finds none in an untagged file or inside another block", () => {
      expect(tagOf("source: a is b")).toBeUndefined();
      expect(tagOf('##|"\n## artifact { kind=notebook }\n|##')).toBeUndefined();
   });
});
