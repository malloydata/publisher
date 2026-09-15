// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { INTRO_SKILL, TARGETS, detectTargets, main } from "./cli.js";
import { listSkills } from "./index.js";
import { skillsDir } from "./payload.js";

let tmp: string;
let lines: string[];
let errors: string[];
let stdout: string;
let exitCode: number | undefined;

const realLog = console.log;
const realError = console.error;
const realExit = process.exit;
const realWrite = process.stdout.write.bind(process.stdout);

beforeEach(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "skills-cli-"));
   lines = [];
   errors = [];
   stdout = "";
   exitCode = undefined;
   // print() writes a whole SKILL.md straight to stdout; captured so the suite
   // can assert the bytes rather than infer them, and so a passing run does not
   // bury itself in skill text.
   process.stdout.write = ((chunk: string) => {
      stdout += chunk;
      return true;
   }) as typeof process.stdout.write;
   console.log = (...args: unknown[]) => void lines.push(args.join(" "));
   console.error = (...args: unknown[]) => void errors.push(args.join(" "));
   // fail() is typed `never` and really does exit, so the command surface can
   // only be driven if the exit is trapped. Throwing keeps that contract: the
   // code after a fail() does not run here either.
   process.exit = ((code?: number) => {
      exitCode = code;
      throw new Error(`exit ${code}`);
   }) as never;
});

afterEach(() => {
   console.log = realLog;
   console.error = realError;
   process.exit = realExit;
   process.stdout.write = realWrite;
   fs.rmSync(tmp, { recursive: true, force: true });
});

/** Run the command, letting a trapped exit end it. */
function run(...argv: string[]): void {
   try {
      main(argv);
   } catch (error) {
      if (!(error instanceof Error) || !error.message.startsWith("exit ")) {
         throw error;
      }
   }
}

describe("intro", () => {
   /**
    * The one failure this alias can have: naming a skill that is not shipped.
    * It is silent until someone pastes the line, and the paste is the whole
    * reason the command exists. It has already happened once, when the alias
    * named a skill that existed only on a branch.
    */
   test("names a skill that actually ships", () => {
      expect(listSkills().map((skill) => skill.name)).toContain(INTRO_SKILL);
   });

   test("prints that skill's file, byte for byte", () => {
      const intro = listSkills().find((skill) => skill.name === INTRO_SKILL);
      expect(intro).toBeDefined();

      run("intro");

      expect(errors).toEqual([]);
      expect(exitCode).toBeUndefined();
      expect(stdout).toBe(
         fs.readFileSync(path.join(intro!.dir, "SKILL.md"), "utf8"),
      );
   });

   test("is the same output as naming the skill outright", () => {
      run("intro");
      const viaAlias = stdout;
      stdout = "";
      run(INTRO_SKILL);

      expect(stdout).toBe(viaAlias);
      expect(stdout.length).toBeGreaterThan(0);
   });
});

describe("listing", () => {
   test("names every shipped skill", () => {
      run();

      const names = listSkills().map((skill) => skill.name);
      expect(names.length).toBeGreaterThan(0);
      for (const name of names) {
         expect(lines).toContain(name);
      }
      expect(exitCode).toBeUndefined();
   });

   test("gives each skill its description, for routing", () => {
      run();

      for (const skill of listSkills()) {
         expect(lines).toContain(`  ${skill.description}`);
      }
   });
});

describe("a name that is not a skill", () => {
   test("exits non-zero and names the near miss", () => {
      run("dashboard");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("malloy-dashboards");
   });

   test("with nothing near, says how to see them all", () => {
      run("zzzznope");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("Run with no arguments");
   });
});

describe("unknown input is refused, never ignored", () => {
   test("an unknown option", () => {
      run("--frobnicate");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("--frobnicate");
   });

   test("an unknown install target", () => {
      run("install", "emacs");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("claude, agents");
   });

   test("more than one skill name", () => {
      run("malloy", "malloy-charts");

      expect(exitCode).toBe(1);
   });

   test("--global outside install", () => {
      run("--global", "malloy");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("only applies to install");
   });
});

describe("detectTargets", () => {
   test("a CLAUDE.md means claude", () => {
      fs.writeFileSync(path.join(tmp, "CLAUDE.md"), "");
      expect(detectTargets(tmp)).toEqual(["claude"]);
   });

   test("an AGENTS.md means agents", () => {
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "");
      expect(detectTargets(tmp)).toEqual(["agents"]);
   });

   test("a .cursor directory means agents", () => {
      fs.mkdirSync(path.join(tmp, ".cursor"));
      expect(detectTargets(tmp)).toEqual(["agents"]);
   });

   test("both markers means both, so neither host is silently skipped", () => {
      fs.writeFileSync(path.join(tmp, "CLAUDE.md"), "");
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "");
      expect(detectTargets(tmp)).toEqual(["claude", "agents"]);
   });

   test("nothing detected is empty, so install asks rather than guessing", () => {
      expect(detectTargets(tmp)).toEqual([]);
   });
});

describe("install", () => {
   test("writes where the named host looks, and says so", () => {
      const cwd = process.cwd();
      process.chdir(tmp);
      try {
         run("install", "claude");
      } finally {
         process.chdir(cwd);
      }

      const target = path.join(tmp, TARGETS.claude);
      expect(fs.existsSync(path.join(target, "malloy", "SKILL.md"))).toBe(true);
      expect(lines.join("\n")).toContain(`Installed`);
      expect(exitCode).toBeUndefined();
   });

   test("installs every shipped skill", () => {
      const cwd = process.cwd();
      process.chdir(tmp);
      try {
         run("install", "claude");
      } finally {
         process.chdir(cwd);
      }

      const installed = fs
         .readdirSync(path.join(tmp, TARGETS.claude), { withFileTypes: true })
         .filter((entry) => entry.isDirectory())
         .map((entry) => entry.name)
         .sort();
      const shipped = fs
         .readdirSync(skillsDir, { withFileTypes: true })
         .filter(
            (entry) =>
               entry.isDirectory() &&
               fs.existsSync(path.join(skillsDir, entry.name, "SKILL.md")),
         )
         .map((entry) => entry.name)
         .sort();
      expect(installed).toEqual(shipped);
   });

   test("with no host and no markers, refuses and names the options", () => {
      const cwd = process.cwd();
      process.chdir(tmp);
      try {
         run("install");
      } finally {
         process.chdir(cwd);
      }

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("install claude");
      expect(fs.existsSync(path.join(tmp, ".claude"))).toBe(false);
   });
});
