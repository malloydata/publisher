// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { TARGETS, detectTargets, main, skillUrl } from "./cli.js";
import { listSkills } from "./index.js";
import { skillsDir } from "./payload.js";

let tmp: string;
let lines: string[];
let errors: string[];
let exitCode: number | undefined;

const realLog = console.log;
const realError = console.error;
const realExit = process.exit;

beforeEach(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "skills-cli-"));
   lines = [];
   errors = [];
   exitCode = undefined;
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

   test("'list' is the same as no arguments", () => {
      run();
      const bare = [...lines];
      lines = [];
      run("list");

      expect(lines).toEqual(bare);
      expect(exitCode).toBeUndefined();
   });

   test("ends by saying how to read one and how to install", () => {
      run();

      expect(lines).toContain(
         "Read one: https://unpkg.com/@malloy-publisher/skills@latest/skills/<name>/SKILL.md",
      );
      expect(lines).toContain(
         "Install them all: npx -y @malloy-publisher/skills@latest install",
      );
   });
});

describe("a skill name as the command", () => {
   /**
    * There is no print command; reading a skill is a URL. An agent that guesses
    * otherwise gets that URL, rather than usage text it has to work back from.
    */
   test("exits non-zero and gives that skill's URL", () => {
      run("malloy-getting-started");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain(
         "https://unpkg.com/@malloy-publisher/skills@latest/skills/malloy-getting-started/SKILL.md",
      );
   });

   test("the URL is where the package ships that skill's file", () => {
      // unpkg serves the tarball's own layout, which is skillsDir's layout.
      for (const skill of listSkills()) {
         const tail = skillUrl(skill.name).split("@latest/")[1];
         expect(path.join(skillsDir, "..", tail)).toBe(
            path.join(skill.dir, "SKILL.md"),
         );
      }
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

   test("an unknown command", () => {
      run("zzzznope");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("zzzznope");
   });

   test("an argument to list", () => {
      run("list", "malloy");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("malloy");
   });

   test("--global outside install", () => {
      run("--global", "list");

      expect(exitCode).toBe(1);
      expect(errors.join("\n")).toContain("only applies to install");
   });
});

describe("detectTargets in a project", () => {
   test("a CLAUDE.md means claude", () => {
      fs.writeFileSync(path.join(tmp, "CLAUDE.md"), "");
      expect(detectTargets(tmp, "project")).toEqual(["claude"]);
   });

   test("an AGENTS.md means agents", () => {
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "");
      expect(detectTargets(tmp, "project")).toEqual(["agents"]);
   });

   test("a .cursor directory means agents", () => {
      fs.mkdirSync(path.join(tmp, ".cursor"));
      expect(detectTargets(tmp, "project")).toEqual(["agents"]);
   });

   test("both markers means both, so neither host is silently skipped", () => {
      fs.writeFileSync(path.join(tmp, "CLAUDE.md"), "");
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "");
      expect(detectTargets(tmp, "project")).toEqual(["claude", "agents"]);
   });

   test("nothing detected is empty, so install asks rather than guessing", () => {
      expect(detectTargets(tmp, "project")).toEqual([]);
   });
});

describe("detectTargets in the home directory", () => {
   /**
    * Hosts keep user-level config in a directory, not a top-level file: Claude
    * Code reads ~/.claude/CLAUDE.md and never ~/CLAUDE.md. Checking the project
    * markers here refused for nearly everyone who has Claude set up.
    */
   test("a ~/.claude directory means claude", () => {
      fs.mkdirSync(path.join(tmp, ".claude"));
      expect(detectTargets(tmp, "global")).toEqual(["claude"]);
   });

   test("a ~/.agents directory means agents", () => {
      fs.mkdirSync(path.join(tmp, ".agents"));
      expect(detectTargets(tmp, "global")).toEqual(["agents"]);
   });

   test("a ~/.cursor directory means agents", () => {
      fs.mkdirSync(path.join(tmp, ".cursor"));
      expect(detectTargets(tmp, "global")).toEqual(["agents"]);
   });

   test("a ~/CLAUDE.md alone is not a Claude setup", () => {
      fs.writeFileSync(path.join(tmp, "CLAUDE.md"), "");
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "");
      expect(detectTargets(tmp, "global")).toEqual([]);
   });
});

/**
 * Run the real command in a child process with HOME pointed at the temp dir.
 * A child because Bun reads HOME once at startup: setting process.env.HOME in
 * this process does not move os.homedir().
 */
function runWithHome(...argv: string[]) {
   return spawnSync(
      process.execPath,
      [path.join(import.meta.dir, "cli.ts"), ...argv],
      {
         env: { ...process.env, HOME: tmp, USERPROFILE: tmp },
         encoding: "utf8",
      },
   );
}

describe("install --global", () => {
   test("with ~/.claude present, installs into ~/.claude/skills", () => {
      fs.mkdirSync(path.join(tmp, ".claude"));

      const result = runWithHome("install", "--global");

      expect(result.stderr).toBe("");
      expect(result.status).toBe(0);
      expect(
         fs.existsSync(
            path.join(
               tmp,
               TARGETS.claude,
               "malloy-getting-started",
               "SKILL.md",
            ),
         ),
      ).toBe(true);
      expect(fs.existsSync(path.join(tmp, TARGETS.agents))).toBe(false);
   });

   test("with nothing in HOME, refuses and names what it looked for", () => {
      const result = runWithHome("install", "--global");

      expect(result.status).toBe(1);
      expect(result.stderr).toContain(".claude, .agents, .cursor");
      expect(result.stderr).toContain("install claude --global");
      // Not "HOME is empty": Bun itself may create a cache dir there.
      expect(fs.existsSync(path.join(tmp, ".claude"))).toBe(false);
      expect(fs.existsSync(path.join(tmp, ".agents"))).toBe(false);
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
      expect(
         fs.existsSync(path.join(target, "malloy-getting-started", "SKILL.md")),
      ).toBe(true);
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
