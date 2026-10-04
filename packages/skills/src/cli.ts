#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `malloy-skills` -- list the agent skills, and install them, with nothing
 * running.
 *
 * This exists so an agent meeting Publisher for the first time can get the
 * skills before it installs or starts anything. Every other route to them is
 * gated: the MCP prompt channel needs the server up, and the server is a large
 * install that clones its example packages over the network.
 *
 * Reading one skill needs no command at all: the package ships the files, so
 * unpkg serves each SKILL.md at a stable URL (`skillUrl`). That works in a chat
 * with no shell, which `npx` cannot. This CLI covers what a URL cannot: listing
 * what ships, and copying the whole tree, `reference/` files included, onto
 * disk through the hardened copy in install.ts.
 *
 * Which is why this lives in @malloy-publisher/skills and not in the server or a
 * new package: this one already ships the skill files, has no runtime
 * dependencies and no native code, so `npx` is fast and cannot fail on a build
 * step or a platform binary. Adding a CLI framework here would cost that, for
 * two commands, so the argument parsing below is by hand.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { listSkills } from "./index.js";
import { installSkills, type SkillInstall } from "./install.js";
import { skillsDir } from "./payload.js";

/** Where each host looks for skills, relative to a project or to the home dir. */
const TARGETS: Record<string, string> = {
   claude: path.join(".claude", "skills"),
   agents: path.join(".agents", "skills"),
};

/**
 * What says a host is set up, per scope. The two differ because the hosts keep
 * their user-level config in a directory rather than a top-level file: Claude
 * Code reads `~/.claude/CLAUDE.md`, never `~/CLAUDE.md`, so the project markers
 * would miss almost everyone who has Claude set up.
 */
const MARKERS: Record<"project" | "global", Record<string, string[]>> = {
   project: { claude: ["CLAUDE.md"], agents: ["AGENTS.md", ".cursor"] },
   global: { claude: [".claude"], agents: [".agents", ".cursor"] },
};

/**
 * Where a skill can be read with nothing installed. `@latest` rather than this
 * package's own version, so a link copied from an old cached copy of this CLI
 * still serves the current skill.
 */
function skillUrl(name: string): string {
   return `https://unpkg.com/@malloy-publisher/skills@latest/skills/${name}/SKILL.md`;
}

const USAGE = `malloy-skills - list and install the Malloy Publisher agent skills

Usage:
  npx -y @malloy-publisher/skills [list]          List every skill.
  npx -y @malloy-publisher/skills install [host]  Copy the skills onto disk.

Install targets:
  claude    .claude/skills/   (detected from a CLAUDE.md)
  agents    .agents/skills/   (detected from an AGENTS.md or .cursor/)
  --global  install into your home directory instead of this project
            (detected from ~/.claude/, ~/.agents/, or ~/.cursor/)

Read one skill without installing anything:
  ${skillUrl("<name>")}

Nothing here needs a running server or an account.`;

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
   console.log("");
   console.log(`Read one: ${skillUrl("<name>")}`);
   console.log("Install them all: npx -y @malloy-publisher/skills install");
}

/**
 * Which hosts this directory is set up for.
 *
 * Detection only ever chooses between hosts the user already has; it never
 * decides *whether* to write. When it finds nothing, install asks rather than
 * picking one, because a skills directory in the wrong place is invisible: the
 * agent simply never loads them and nothing says why.
 */
function detectTargets(root: string, scope: keyof typeof MARKERS): string[] {
   return Object.entries(MARKERS[scope])
      .filter(([, markers]) =>
         markers.some((marker) => fs.existsSync(path.join(root, marker))),
      )
      .map(([host]) => host);
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
   const scope = global ? "global" : "project";
   let chosen = hosts;
   if (chosen.length === 0) {
      chosen = detectTargets(root, scope);
      if (chosen.length === 0) {
         const looked = Object.values(MARKERS[scope]).flat().join(", ");
         const flag = global ? " --global" : "";
         fail(
            `Could not tell which agent to install for: no ${looked} in ${root}.\n` +
               `Name one: npx -y @malloy-publisher/skills install claude${flag}   (or: install agents${flag})`,
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
   const [command = "list", ...rest] = args;

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
   // Anything else is refused rather than skipped: a silently ignored argument
   // is how a typo turns into a command that looks like it worked.
   if (command !== "list") {
      // A skill name is the likeliest mistake, from an agent that expects this
      // to print one, so it gets the URL that does.
      const skill = listSkills().find((s) => s.name === command);
      fail(
         skill
            ? `There is no command to print a skill. Read it at ${skillUrl(command)}`
            : `Unknown command or option '${command}'.\n\n${USAGE}`,
      );
   }
   if (rest.length > 0) {
      fail(`'list' takes no arguments, got: ${rest.join(" ")}.`);
   }
   if (global) fail("--global only applies to install.");
   return list();
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

export { TARGETS, detectTargets, main, skillUrl };
