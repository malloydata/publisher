#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `malloy-skills` -- read the agent skills, and install them, with nothing
 * running.
 *
 * This exists so an agent meeting Publisher for the first time can get oriented
 * before it installs or starts anything. Every other route to these skills is
 * gated: the MCP prompt channel needs the server up, and the server is a large
 * install that clones its example packages over the network. A person can paste
 * one line into any agent, anywhere, and the agent can read what to do next.
 *
 * Which is why this lives in @malloy-publisher/skills and not in the server or a
 * new package: this one already ships the skill files, has no runtime
 * dependencies and no native code, so `npx` is fast and cannot fail on a build
 * step or a platform binary. Adding a CLI framework here would cost that, for
 * four commands, so the argument parsing below is by hand.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { listSkills } from "./index.js";
import { installSkills, type SkillInstall } from "./install.js";
import { skillsDir } from "./payload.js";

/**
 * What `intro` prints. An alias rather than a skill named `intro`, so the
 * pasteable line stays short while the skill keeps the name every other channel
 * routes to -- the MCP prompt pointer and the index both name it. If it is ever
 * renamed or replaced, change it here and the pasteable line keeps working.
 */
const INTRO_SKILL = "malloy-getting-started";

/** Where each host looks for skills, relative to a project or to the home dir. */
const TARGETS: Record<string, string> = {
   claude: path.join(".claude", "skills"),
   agents: path.join(".agents", "skills"),
};

const USAGE = `malloy-skills - read and install the Malloy Publisher agent skills

Usage:
  npx -y @malloy-publisher/skills                 List every skill.
  npx -y @malloy-publisher/skills intro           Print the orientation skill.
  npx -y @malloy-publisher/skills <name>          Print one skill in full.
  npx -y @malloy-publisher/skills install [host]  Copy the skills onto disk.

Install targets:
  claude    .claude/skills/   (detected from a CLAUDE.md)
  agents    .agents/skills/   (detected from an AGENTS.md or .cursor/)
  --global  install into your home directory instead of this project

Nothing here needs a server, an account, or a network connection.`;

function fail(message: string): never {
   console.error(message);
   process.exit(1);
}

/** Print the name and description of every shipped skill. */
function list(): void {
   const skills = listSkills();
   if (skills.length === 0) {
      fail(
         `No skills found in ${skillsDir}. This install of ` +
            `@malloy-publisher/skills is incomplete; reinstall and try again.`,
      );
   }
   for (const skill of skills) {
      console.log(skill.name);
      console.log(`  ${skill.description}`);
   }
}

/**
 * Print one skill's SKILL.md.
 *
 * A miss names the nearest candidates rather than only the bad name, because the
 * reader is usually an agent that guessed, and a bare "not found" makes it guess
 * again. Substring both ways so `dashboard` finds `malloy-dashboards` and
 * `malloy-analysis-report` finds `analysis`.
 */
function print(name: string): void {
   const skills = listSkills();
   const match = skills.find((skill) => skill.name === name);
   if (!match) {
      const near = skills
         .filter(
            (skill) => skill.name.includes(name) || name.includes(skill.name),
         )
         .map((skill) => skill.name);
      const suggestion =
         near.length > 0
            ? `Did you mean: ${near.join(", ")}?`
            : `Run with no arguments to list all ${skills.length}.`;
      fail(`No skill named '${name}'. ${suggestion}`);
   }
   process.stdout.write(
      fs.readFileSync(path.join(match.dir, "SKILL.md"), "utf8"),
   );
}

/**
 * Which hosts this directory is set up for.
 *
 * Detection only ever chooses between hosts the user already has; it never
 * decides *whether* to write. When it finds nothing, install asks rather than
 * picking one, because a skills directory in the wrong place is invisible: the
 * agent simply never loads them and nothing says why.
 */
function detectTargets(root: string): string[] {
   const found: string[] = [];
   if (fs.existsSync(path.join(root, "CLAUDE.md"))) found.push("claude");
   if (
      fs.existsSync(path.join(root, "AGENTS.md")) ||
      fs.existsSync(path.join(root, ".cursor"))
   ) {
      found.push("agents");
   }
   return found;
}

/** Say what an install did, including what it cost. */
function report(target: string, dir: string, result: SkillInstall): void {
   if (result.refused) {
      fail(`Did not install into ${dir}: ${result.refused}.`);
   }
   console.log(`Installed ${result.installed} skills into ${dir}`);
   if (result.skipped.length > 0) {
      console.log(`  left alone (symlinked): ${result.skipped.join(", ")}`);
   }
   if (result.refreshed.length > 0) {
      console.log(`  replaced: ${result.refreshed.join(", ")}`);
   }
   // Not a footnote: these are files the user put there that no copy puts back.
   for (const gone of result.removed) {
      console.log(`  removed (not in the shipped skill): ${target}/${gone}`);
   }
}

function install(hosts: string[], global: boolean): void {
   const root = global ? os.homedir() : process.cwd();
   let chosen = hosts;
   if (chosen.length === 0) {
      chosen = detectTargets(root);
      if (chosen.length === 0) {
         fail(
            `Could not tell which agent to install for: no CLAUDE.md, ` +
               `AGENTS.md, or .cursor/ in ${root}.\n` +
               `Name one: malloy-skills install claude   (or: install agents)`,
         );
      }
   }
   for (const host of chosen) {
      const dir = path.join(root, TARGETS[host]);
      report(host, dir, installSkills(dir, root));
   }
}

function main(argv: string[]): void {
   const global = argv.includes("--global");
   const args = argv.filter((arg) => arg !== "--global");

   if (args.length === 0) {
      if (global) fail("--global only applies to install.");
      return list();
   }

   const [command, ...rest] = args;
   if (command === "--help" || command === "-h" || command === "help") {
      console.log(USAGE);
      return;
   }
   if (command === "install") {
      for (const host of rest) {
         if (!(host in TARGETS)) {
            fail(
               `Unknown install target '${host}'. ` +
                  `Expected one of: ${Object.keys(TARGETS).join(", ")}.`,
            );
         }
      }
      return install(rest, global);
   }
   // Every remaining token has to be a skill name. An unrecognized flag is an
   // error rather than something skipped: a silently ignored argument is how a
   // typo turns into a command that looks like it worked.
   if (command.startsWith("-")) {
      fail(`Unknown option '${command}'.\n\n${USAGE}`);
   }
   if (rest.length > 0) {
      fail(`Expected one skill name, got: ${args.join(" ")}.\n\n${USAGE}`);
   }
   if (global) fail("--global only applies to install.");
   return print(command === "intro" ? INTRO_SKILL : command);
}

// Only when run as the command, so importing this module for a test does not
// parse the test runner's own argv and exit the process.
if (
   process.argv[1] &&
   realpath(process.argv[1]) === realpath(fileURLToPath(import.meta.url))
) {
   main(process.argv.slice(2));
}

function realpath(target: string): string {
   try {
      return fs.realpathSync(target);
   } catch {
      return target;
   }
}

export { INTRO_SKILL, TARGETS, detectTargets, main };
