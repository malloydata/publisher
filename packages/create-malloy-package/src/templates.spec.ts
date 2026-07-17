import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { scaffold, type ScaffoldOptions } from "./scaffold";
import { countSkills } from "./skills";
import { renderTemplate } from "./templates";
import { skillsDir } from "@malloy-publisher/skills";

const SKILL_COUNT = countSkills(skillsDir);

let tmp: string;

beforeEach(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-templates-"));
});

afterEach(() => {
   fs.rmSync(tmp, { recursive: true, force: true });
});

function run(overrides: Partial<ScaffoldOptions> = {}) {
   return scaffold({
      name: "sales",
      cwd: tmp,
      host: "claude-code",
      force: false,
      ...overrides,
   });
}

function agentsFile(): string {
   return fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
}

function workspaceScripts(): Record<string, unknown> {
   const pkgPath = path.join(tmp, "package.json");
   if (!fs.existsSync(pkgPath)) {
      return {};
   }
   const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8")) as {
      scripts?: Record<string, unknown>;
   };
   return pkg.scripts ?? {};
}

function writePackageJson(scripts: Record<string, string>): void {
   fs.writeFileSync(
      path.join(tmp, "package.json"),
      JSON.stringify({ name: "someone-elses-app", scripts }, null, 2),
   );
}

/**
 * Every npm script the rendered file tells the reader to run. Only the two forms
 * that are an instruction, so prose about what npm does (`npm needs its own --`)
 * is not mistaken for one.
 */
function npmScriptsToldToRun(agents: string): string[] {
   return [...agents.matchAll(/npm (start|run [A-Za-z0-9:_-]+)/g)].map(
      (match) =>
         match[1] === "start" ? "start" : match[1].slice("run ".length),
   );
}

/**
 * The assertion that would have caught the round where AGENTS.md hardcoded
 * `npm start` and `npm run reset` next to a computed command that said otherwise:
 * every npm script the file names has to exist in this workspace and has to boot
 * Publisher, and every substitution has to have happened.
 */
function expectAgentsFileToBeRunnable(): void {
   const agents = agentsFile();
   expect(agents).not.toContain("{{");
   const scripts = workspaceScripts();
   const named = npmScriptsToldToRun(agents);
   for (const name of named) {
      const script = scripts[name];
      if (typeof script !== "string") {
         throw new Error(
            `AGENTS.md tells the reader to run "npm ${name}", which this ` +
               `workspace's package.json does not define`,
         );
      }
      expect(script).toContain("@malloy-publisher/server");
   }
   // However it is spelled, the boot the file hands over is a real one: either an
   // npm script checked above, or the explicit command.
   const startBlock = agents.split("```bash")[1].trim();
   expect(
      startBlock.startsWith("npm ") ||
         startBlock.includes("@malloy-publisher/server"),
   ).toBe(true);
   // And the reset boot is always named, since that is what makes a package added
   // after the first boot actually get served.
   expect(agents).toContain("--init");
}

describe("AGENTS.md: a workspace whose scripts are ours", () => {
   test("delegates to npm, and both scripts exist and boot Publisher", () => {
      const result = run();
      expect(result.hasStartScript).toBe(true);
      expect(result.hasResetScript).toBe(true);
      expectAgentsFileToBeRunnable();
      expect([...new Set(npmScriptsToldToRun(agentsFile()))].sort()).toEqual([
         "reset",
         "start",
      ]);
   });
});

describe("AGENTS.md: a workspace whose package.json is someone else's", () => {
   test("names no npm script at all when neither script is ours", () => {
      writePackageJson({ start: "vite", reset: "./wipe-db.sh" });
      const result = run();
      expect(result.hasStartScript).toBe(false);
      expect(result.hasResetScript).toBe(false);
      const agents = agentsFile();
      // The failure this replaces: `npm start` ran vite with Publisher's flags,
      // and `npm run reset` was a script that does not exist.
      expect(npmScriptsToldToRun(agents)).toEqual([]);
      expect(agents).toContain(result.startCommand);
      expect(agents).toContain(result.resetCommand);
      expectAgentsFileToBeRunnable();
   });

   test("mixes: an npm start that is ours, a reset that is not", () => {
      writePackageJson({
         start:
            `npx -y @malloy-publisher/server --server_root . ` +
            `--config ./publisher.config.json --host 127.0.0.1 ` +
            `--watch-env default`,
         reset: "./wipe-db.sh",
      });
      const result = run();
      expect(result.hasResetScript).toBe(false);
      const agents = agentsFile();
      expect(npmScriptsToldToRun(agents)).not.toContain("reset");
      expect(agents).toContain(result.resetCommand);
      expectAgentsFileToBeRunnable();
   });

   test("--force takes both scripts back, and the file follows", () => {
      writePackageJson({ start: "vite", reset: "./wipe-db.sh" });
      const result = run({ force: true });
      expect(result.hasStartScript).toBe(true);
      expect(result.hasResetScript).toBe(true);
      expectAgentsFileToBeRunnable();
   });
});

describe("AGENTS.md: other states", () => {
   test("a setup-only run has no package section", () => {
      const result = run({ name: undefined });
      expect(result.packageCreated).toBe(false);
      const agents = agentsFile();
      expect(agents).not.toContain("## The package");
      expectAgentsFileToBeRunnable();
   });

   test("cursor gets cursor's MCP config and reconnect instructions", () => {
      run({ host: "cursor" });
      const agents = agentsFile();
      expect(agents).toContain(".cursor/mcp.json");
      expect(agents).toContain("Cursor");
      expectAgentsFileToBeRunnable();
   });

   test("the skills heading carries the count that was really installed", () => {
      const result = run();
      expect(result.skillsInstalled).toBe(SKILL_COUNT);
      expect(agentsFile()).toContain(`## Skills (${SKILL_COUNT} installed)`);
   });
});

describe("AGENTS.md: the bind address", () => {
   /**
    * The file used to state the server binds to loopback. It cannot know that: an
    * existing start script it delegates to may bind anything, and one that binds
    * 0.0.0.0 serves the whole LAN with no auth. So it tells the reader where to
    * look instead of asserting an answer.
    */
   test("tells the reader to read --host rather than asserting loopback", () => {
      writePackageJson({
         start:
            `npx -y @malloy-publisher/server --server_root . ` +
            `--config ./publisher.config.json --host 0.0.0.0 ` +
            `--watch-env default`,
      });
      run();
      const agents = agentsFile();
      expect(agents).toContain("read the `--host` in the command you");
      expect(agents).not.toContain("so it is reachable from this machine only");
   });
});

/**
 * The template swaps its three command blocks for the direct `npx` form when it
 * finds a package.json that is not ours. Prose about those commands has to swap
 * with them: a parenthetical describing where the port flags sit "run through
 * npm, behind its `--` separator" points at a separator that appears nowhere in
 * the command it is describing, in a directory where nothing goes through npm.
 */
describe("AGENTS.md: prose about the commands swaps with the commands", () => {
   test("no npm separator is described where no command runs through npm", () => {
      writePackageJson({ start: "vite", reset: "./wipe-db.sh" });
      run();
      const agents = agentsFile();

      expect(npmScriptsToldToRun(agents)).toEqual([]);
      expect(agents).not.toContain("behind its `--` separator");
      expect(agents).not.toContain("if it goes through npm");
      // Whatever it says about the port flags, it does not describe them as
      // sitting behind a separator this workspace's commands do not carry.
      for (const line of agents.split("\n")) {
         if (line.includes("separator")) {
            expect(line).toMatch(/where|if/i);
         }
      }
   });

   test("the npm workspace still explains npm's own separator", () => {
      run();
      const agents = agentsFile();
      expect(npmScriptsToldToRun(agents)).toContain("start");
      expect(agents).toContain("separator");
   });
});

describe("renderTemplate", () => {
   test("substitutes what the template references", () => {
      expect(
         renderTemplate("model.default.malloy", { sourceName: "hats" }),
      ).toContain("hats");
   });

   test("throws on a {{var}} the caller did not pass", () => {
      expect(() => renderTemplate("model.default.malloy", {})).toThrow(
         /unknown variable \{\{sourceName\}\}/,
      );
   });

   /**
    * The silent half. A computed `resetCommand` was passed to AGENTS.md and never
    * referenced, while the file hardcoded a different command, and nothing failed.
    */
   test("throws on a var the template never references", () => {
      expect(() =>
         renderTemplate("model.default.malloy", {
            sourceName: "hats",
            resetCommand: "npm run reset",
         }),
      ).toThrow(/never references \{\{resetCommand\}\}/);
   });

   test("names every unreferenced var, not just the first", () => {
      expect(() =>
         renderTemplate("model.default.malloy", {
            sourceName: "hats",
            one: "a",
            two: "b",
         }),
      ).toThrow(/\{\{one\}\}, \{\{two\}\}/);
   });
});
