/**
 * The CLI's own output, rendered from real scaffold results rather than from
 * hand-written fixtures, plus the real bin run as a subprocess.
 *
 * This suite exists because every round of review so far has found the same
 * defect in a different place: printed prose the code does not honor. Prose is
 * only checkable if something renders it, so formatSuccess and formatFailure are
 * pure and every state combination they branch on is rendered here.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import {
   changesSince,
   formatFailure,
   formatSuccess,
   snapshotArtifacts,
} from "./index";
import {
   scaffold,
   type ScaffoldOptions,
   type ScaffoldResult,
} from "./scaffold";

const here = path.dirname(fileURLToPath(import.meta.url));
const cliEntry = path.join(here, "index.ts");

let tmp: string;

beforeEach(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-cli-"));
});

afterEach(() => {
   fs.rmSync(tmp, { recursive: true, force: true });
});

/**
 * A real result from a real scaffold, so the rendered output is checked against
 * the fields scaffold() actually sets. Overrides drive the branches a plain run
 * cannot reach on this machine.
 */
function resultFor(overrides: Partial<ScaffoldResult> = {}): ScaffoldResult {
   return { ...runHere(), ...overrides };
}

/** A real run in the workspace this test has built, with nothing overridden. */
function runHere(options: Partial<ScaffoldOptions> = {}): ScaffoldResult {
   return scaffold({
      name: "sales",
      cwd: tmp,
      host: "claude-code",
      force: false,
      ...options,
   });
}

/** A package.json this tool did not write, so none of its scripts are ours. */
function writeForeignPackageJson(scripts: Record<string, string>): void {
   fs.writeFileSync(
      path.join(tmp, "package.json"),
      JSON.stringify({ name: "someone-elses-app", scripts }, null, 2) + "\n",
   );
}

/** The commands the output offers, without the note that trails each one. */
function offeredCommands(output: string): string[] {
   return output
      .split("\n")
      .map((line) => line.trim().split("   ")[0].trim())
      .filter(
         (line) =>
            line.startsWith("npx -y @malloy-publisher/server") ||
            line.startsWith("npm start") ||
            line.startsWith("npm run reset"),
      );
}

describe("formatSuccess: the readiness check", () => {
   test("offers the package list, not just the status", () => {
      const output = formatSuccess(resultFor());
      expect(output).toContain("curl -s http://localhost:4000/api/v0/status");
      expect(output).toContain(
         "curl -s http://localhost:4000/api/v0/environments/default/packages",
      );
      // The status endpoint answers "serving" over a server that mounted
      // nothing, which is the whole reason the second curl is printed.
      expect(output).toContain("an empty []");
   });

   test("names the environment the package actually landed in", () => {
      const output = formatSuccess(resultFor({ envName: "analytics" }));
      expect(output).toContain(
         "http://localhost:4000/api/v0/environments/analytics/packages",
      );
   });

   test("a setup-only run says the package list 404s instead of offering it", () => {
      const result = scaffold({
         name: undefined,
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const output = formatSuccess(result);
      expect(output).toContain("curl -s http://localhost:4000/api/v0/status");
      expect(output).not.toContain(
         "curl -s http://localhost:4000/api/v0/environments",
      );
      expect(output).toContain("answers 404");
   });
});

/**
 * Which boot the output offers is decided by `needsReset` and by whether this
 * workspace's npm scripts are ours, and both are read off the directory the run
 * happened in. So each case here builds that directory and scaffolds into it.
 * Setting the fields instead is what let three of these tests keep asserting on
 * reset advice after the reset block moved behind `needsReset`: the state they
 * described (a config extended by a run that needs no reset) cannot occur,
 * because scaffold() only sets configExtended over a config that already
 * existed, which is one of the things that makes needsReset true.
 */
describe("formatSuccess: which boot command is offered", () => {
   test("a package added to an existing config offers reset, then a plain boot", () => {
      // Someone else's project, already booted once: their package.json keeps
      // its own scripts, so neither npm script is ours.
      writeConfigWithPackages("default", ["faa"]);
      markAsAlreadyBooted();
      writeForeignPackageJson({ start: "vite" });

      const result = runHere();
      const output = formatSuccess(result);
      const commands = offeredCommands(output);

      expect(result.configExtended).toBe(true);
      expect(result.needsReset).toBe(true);
      expect(result.hasStartScript).toBe(false);
      expect(result.hasResetScript).toBe(false);
      expect(commands).toContain(result.resetCommand);
      // The bug this covers: with a package.json that is not ours, the reset
      // boot was the only command offered, so every later boot carried --init
      // and dropped materialized tables and saved themes with it.
      expect(commands).toContain(result.startCommand);
      expect(commands.some((command) => !command.includes("--init"))).toBe(
         true,
      );
   });

   test("with our own scripts it offers npm run reset then npm start", () => {
      // The first run wrote both scripts and the config; a server has since
      // been pointed at this directory, and now a second package arrives.
      runHere({ name: "sales" });
      markAsAlreadyBooted();
      const result = runHere({ name: "orders" });

      expect(result.configExtended).toBe(true);
      expect(result.hasStartScript).toBe(true);
      expect(result.hasResetScript).toBe(true);
      expect(offeredCommands(formatSuccess(result))).toEqual([
         "npm run reset",
         "npm start",
      ]);
   });

   test("a declined script is told which of the two things is wrong with it", () => {
      // These two cases used to share one reason, so a single clean invocation
      // whose --host the code had just parsed and accepted as loopback was told
      // it sat "inside a longer command line". That is this package's recurring
      // defect, appearing in the message whose whole job is to explain a decline.
      writeForeignPackageJson({
         start:
            "npx -y @malloy-publisher/server --server_root . " +
            "--config ./publisher.config.json --host 127.0.0.1 " +
            "--watch-env default --port 9999",
      });
      const differing = formatSuccess(runHere({ name: "sales" }));
      expect(differing).toContain("different arguments");
      expect(differing).not.toContain("longer command line");
   });

   test("a script genuinely inside a longer command line is told that", () => {
      writeForeignPackageJson({
         start: "echo hi && npx -y @malloy-publisher/server --host 127.0.0.1",
      });
      const embedded = formatSuccess(runHere({ name: "sales" }));
      expect(embedded).toContain("longer command line");
      expect(embedded).not.toContain("different arguments");
   });

   test("the reset step says to stop a server that is already running", () => {
      runHere({ name: "sales" });
      markAsAlreadyBooted();
      const output = formatSuccess(runHere({ name: "orders" }));
      expect(output).toContain("Stop any Publisher already serving");
      expect(output).toContain("publisher.db");
   });

   test("a --force re-run registers nothing new and still offers the reset", () => {
      // needsReset true with configExtended false, which is the state the two
      // flags exist to keep apart: the name is already in the config, so this
      // run adds no entry to it, and the package still needs the --init boot
      // before a server that has already run here will mount it.
      runHere({ name: "sales" });
      markAsAlreadyBooted();
      const result = runHere({ name: "sales", force: true });

      expect(result.configExtended).toBe(false);
      expect(result.needsReset).toBe(true);
      expect(offeredCommands(formatSuccess(result))).toContain("npm run reset");
   });

   test("a first package with no start script of ours offers the explicit boot", () => {
      writeForeignPackageJson({ start: "vite" });
      const result = runHere();
      const output = formatSuccess(result);
      expect(result.hasStartScript).toBe(false);
      expect(offeredCommands(output)).toEqual([result.startCommand]);
   });
});

/**
 * The reset advice is three paragraphs about a persisted package list that wins
 * over publisher.config.json. Nothing in a workspace no server has ever booted
 * in is persisted, so over that workspace every one of those sentences is false,
 * including the warning that the boot it offers drops materialized tables and
 * saved themes. `publisher.config.json exists` is this tool's own footprint, not
 * a server's.
 */
describe("formatSuccess: a workspace no server has booted in", () => {
   test("the two-step flow, set up then add a package, needs no reset", () => {
      runHere({ name: undefined });
      const result = runHere({ name: "sales" });
      const output = formatSuccess(result);

      expect(fs.existsSync(path.join(tmp, "publisher.config.json"))).toBe(true);
      expect(fs.existsSync(path.join(tmp, "publisher_data"))).toBe(false);
      expect(fs.existsSync(path.join(tmp, "publisher.db"))).toBe(false);
      if (result.needsReset) {
         throw new Error(
            `Nothing has ever read this workspace: no publisher_data/, no ` +
               `publisher.db. The only evidence of a boot the run found is the ` +
               `publisher.config.json it wrote itself one call earlier, so the ` +
               `configExists disjunct in needsReset (scaffold.ts) has to go. ` +
               `The output it produced:\n${output}`,
         );
      }
      expect(output).not.toContain("Restarting is not enough");
      expect(output).not.toContain("materialized");
      expect(offeredCommands(output)).toEqual(["npm start"]);
   });

   test("the same workspace after a boot does get the reset advice", () => {
      // The other direction, so the fix above cannot be a blanket removal.
      runHere({ name: undefined });
      markAsAlreadyBooted();
      const result = runHere({ name: "sales" });
      const output = formatSuccess(result);
      expect(result.needsReset).toBe(true);
      expect(output).toContain("Restarting is not enough");
   });
});

describe("formatSuccess: what landed on disk", () => {
   test("prints every path scaffold recorded as written", () => {
      const result = resultFor();
      const output = formatSuccess(result);
      expect(result.written.length).toBeGreaterThan(0);
      for (const item of result.written) {
         expect(output).toContain(item);
      }
   });

   test("prints the skipped and replaced lists", () => {
      // Both lists filled by the run itself: an AGENTS.md of the user's is
      // skipped, and the malloy entry in their .mcp.json is repointed.
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "# my notes\n");
      fs.writeFileSync(
         path.join(tmp, ".mcp.json"),
         JSON.stringify({
            mcpServers: { malloy: { type: "http", url: "http://elsewhere" } },
         }) + "\n",
      );
      const result = runHere();
      const output = formatSuccess(result);

      expect(result.skipped).toContain("AGENTS.md");
      expect(result.replaced).toContain(".mcp.json malloy server");
      expect(output).toContain("Left these existing files alone:");
      expect(output).toContain("AGENTS.md");
      expect(output).toContain("Overwrote these:");
      expect(output).toContain(".mcp.json malloy server");
   });
});

describe("formatSuccess: the MCP endpoint", () => {
   test("an unwritable config prints the block it tells you to paste", () => {
      const output = formatSuccess(
         resultFor({
            mcpWired: false,
            mcpConfigProblem: "not valid JSON",
            mcpPasteBlock: '{\n  "mcpServers": {\n    "malloy": {}\n  }\n}\n',
         }),
      );
      expect(output).toContain("Add this server to it by hand:");
      expect(output).toContain('"mcpServers"');
   });

   test("without a block it does not print an empty paste instruction", () => {
      const output = formatSuccess(
         resultFor({ mcpWired: false, mcpConfigProblem: "not valid JSON" }),
      );
      expect(output).not.toContain("Add this server to it by hand:");
      expect(output).toContain("AGENTS.md has the block");
   });

   test("a wired config is not reported as wired when it is not", () => {
      const output = formatSuccess(resultFor({ mcpWired: false }));
      expect(output).toContain("the MCP endpoint is not wired");
   });
});

describe("changesSince", () => {
   test("separates what was created from what was rewritten", () => {
      fs.writeFileSync(path.join(tmp, "package.json"), '{"name":"mine"}\n');
      fs.writeFileSync(path.join(tmp, "CLAUDE.md"), "mine\n");
      const before = snapshotArtifacts(tmp, "sales");

      fs.writeFileSync(path.join(tmp, "package.json"), '{"name":"ours"}\n');
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "new\n");

      const changes = changesSince(tmp, "sales", before);
      expect(changes.created).toEqual(["AGENTS.md"]);
      expect(changes.changed).toEqual(["package.json"]);
   });

   test("a failed --force run names the files it had already rewritten", () => {
      // The reviewer's reproduction: CLAUDE.md cannot be written, and the run
      // dies there, after --force has already replaced the user's start script
      // and their hand-written AGENTS.md. A directory in the way fails for any
      // user, including root, which chmod does not.
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ scripts: { start: "vite" } }) + "\n",
      );
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "hand written\n");
      fs.mkdirSync(path.join(tmp, "CLAUDE.md"));

      const before = snapshotArtifacts(tmp, "sales");
      let thrown: unknown;
      try {
         scaffold({
            name: "sales",
            cwd: tmp,
            host: "claude-code",
            force: true,
         });
      } catch (err) {
         thrown = err;
      }
      expect(thrown).toBeDefined();

      const changes = changesSince(tmp, "sales", before);
      expect(changes.created).toContain("sales");
      expect(changes.changed).toContain("package.json");
      expect(changes.changed).toContain("AGENTS.md");

      const output = formatFailure(thrown, changes, true);
      expect(output).toContain(
         "It also rewrote these, which were already here",
      );
      expect(output).toContain("package.json");
      // The sentence this whole change exists to remove: it was printed over a
      // run that had just replaced two of the user's files.
      expect(output).not.toContain(
         "Nothing else in this directory was touched",
      );
   });
});

describe("formatFailure", () => {
   const changes = { created: ["sales"], changed: ["package.json"] };

   test("does not tell a user who passed --force to pass --force", () => {
      const output = formatFailure(new Error("boom"), changes, true);
      expect(output).toContain("--force is already set");
      expect(output).not.toContain("re-run with");
   });

   test("offers --force only when it was not given", () => {
      const output = formatFailure(new Error("boom"), changes, false);
      expect(output).toContain("--force once the cause is fixed");
   });

   test("says nothing about the workspace when it wrote nothing", () => {
      const output = formatFailure(
         new Error("boom"),
         { created: [], changed: [] },
         false,
      );
      expect(output).not.toContain("created these");
      expect(output).not.toContain("rewrote these");
   });
});

describe("the bin itself", () => {
   test("running the entry point scaffolds and prints the next steps", () => {
      const proc = spawnSync(process.execPath, [cliEntry, "sales"], {
         cwd: tmp,
         encoding: "utf8",
      });
      // The parse guard decides whether the CLI does anything at all, so it is
      // checked by running the CLI, not by reading it.
      expect(proc.status).toBe(0);
      expect(proc.stdout).toContain("Created package sales");
      expect(proc.stdout).toContain(
         "curl -s http://localhost:4000/api/v0/environments/default/packages",
      );
      expect(fs.existsSync(path.join(tmp, "publisher.config.json"))).toBe(true);
   });

   test("--version prints the version in the manifest", () => {
      const proc = spawnSync(process.execPath, [cliEntry, "--version"], {
         cwd: tmp,
         encoding: "utf8",
      });
      expect(proc.status).toBe(0);
      const manifest = JSON.parse(
         fs.readFileSync(path.join(here, "..", "package.json"), "utf8"),
      ) as { version: string };
      expect(proc.stdout.trim()).toBe(manifest.version);
   });

   test("a failing run exits non-zero and names what it created", () => {
      // --force, because without it an existing CLAUDE.md is skipped rather
      // than written, and the run succeeds.
      fs.mkdirSync(path.join(tmp, "CLAUDE.md"));
      const proc = spawnSync(process.execPath, [cliEntry, "sales", "--force"], {
         cwd: tmp,
         encoding: "utf8",
      });
      expect(proc.status).toBe(1);
      expect(proc.stderr).toContain("Error:");
      expect(proc.stderr).toContain(
         "This run created these before it stopped:",
      );
      expect(proc.stderr).toContain("sales");
   });
});

/**
 * The state a workspace is in once a Publisher has booted in it: the config it
 * was booted from, and the database that config was baked into. From the second
 * boot on, that database is what decides which packages are served, so a package
 * created now is registered, reported, and never mounted until a boot carrying
 * --init rebuilds the list.
 */
function markAsAlreadyBooted(): void {
   fs.mkdirSync(path.join(tmp, "publisher_data"), { recursive: true });
   fs.writeFileSync(path.join(tmp, "publisher.db"), "");
}

function writeConfigWithPackages(envName: string, names: string[]): void {
   fs.writeFileSync(
      path.join(tmp, "publisher.config.json"),
      JSON.stringify(
         {
            frozenConfig: false,
            environments: [
               {
                  name: envName,
                  packages: names.map((name) => ({
                     name,
                     location: `./${name}`,
                  })),
                  connections: [],
               },
            ],
         },
         null,
         2,
      ) + "\n",
   );
}

/** The reset boot as this workspace would run it. */
function resetOffered(output: string, result: ScaffoldResult): boolean {
   return offeredCommands(output).includes(
      result.hasResetScript ? "npm run reset" : result.resetCommand,
   );
}

/**
 * "Did this run write a new entry into the config" and "does the package this run
 * created need --init before a server will mount it" are different questions, and
 * the second is the one the reset guidance answers. Both cases below create a
 * package in a workspace that has already been booted, and in neither does the
 * config gain an entry.
 */
describe("formatSuccess: a package created in a workspace that has already booted", () => {
   test("a name already registered in the config still gets the reset command", () => {
      // No flags, no unusual state: the config already lists the package (a
      // colleague committed it, or an earlier run added it), and the directory
      // for it is what is missing.
      writeConfigWithPackages("default", ["sales", "orders"]);
      markAsAlreadyBooted();
      const result = scaffold({
         name: "orders",
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const output = formatSuccess(result);

      expect(result.packageCreated).toBe(true);
      if (!resetOffered(output, result)) {
         throw new Error(
            `The run created a package in a workspace holding a config and a ` +
               `publisher.db, so a plain restart serves the persisted package ` +
               `list and never mounts it. The output offers only ` +
               `${JSON.stringify(offeredCommands(output))}:\n${output}`,
         );
      }
      // The promise that made the omission silent.
      expect(output).not.toContain("with the package in watch mode");
   });

   test("a --force re-run of an already-created package keeps the reset command", () => {
      // The documented way to refresh the AGENTS.md this tool itself reports as
      // stale, so it is a path the tool steers users into.
      scaffold({ name: "sales", cwd: tmp, host: "claude-code", force: false });
      scaffold({ name: "orders", cwd: tmp, host: "claude-code", force: false });
      markAsAlreadyBooted();
      const result = scaffold({
         name: "orders",
         cwd: tmp,
         host: "claude-code",
         force: true,
      });
      const output = formatSuccess(result);

      if (!resetOffered(output, result)) {
         throw new Error(
            `--force re-ran an already-registered package and dropped the reset ` +
               `command, so the output offers only ` +
               `${JSON.stringify(offeredCommands(output))}:\n${output}`,
         );
      }
   });
});

/**
 * A setup-only run is the documented way to wire an agent into a workspace that
 * already has packages, so the populated config is the main case for that mode,
 * not the edge. Everything the output says about an empty server is a claim about
 * the environment it just resolved out of that config.
 */
describe("formatSuccess: a setup-only run over a config that already has packages", () => {
   test("offers the package list instead of saying the endpoint 404s", () => {
      writeConfigWithPackages("examples", ["faa"]);
      const result = scaffold({
         name: undefined,
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const output = formatSuccess(result);

      // The same run resolved this environment and wrote --watch-env for it.
      expect(result.envName).toBe("examples");
      expect(result.startCommand).toContain("--watch-env examples");
      expect(output).toContain(
         "curl -s http://localhost:4000/api/v0/environments/examples/packages",
      );
      expect(output).not.toContain("answers 404");
      expect(output).not.toContain("nothing to serve");
      expect(output).not.toContain("no environment to browse");
   });

   test("an empty config still says so, so the fix is not a blanket removal", () => {
      writeConfigWithPackages("examples", []);
      const output = formatSuccess(
         scaffold({
            name: undefined,
            cwd: tmp,
            host: "claude-code",
            force: false,
         }),
      );
      expect(output).toContain("answers 404");
   });
});

/**
 * The one moment this tool detects an unauthenticated REST and MCP endpoint about
 * to be served to the network. The script is correctly not adopted; the sentence
 * printed over it said the manifest had no Publisher start script at all, which
 * is both false and silent about the only thing worth saying here.
 */
describe("formatSuccess: a start script that boots Publisher onto the network", () => {
   test("says why it was not adopted, and names the address", () => {
      const script =
         "npx -y @malloy-publisher/server --server_root . " +
         "--config ./publisher.config.json --host 0.0.0.0 --watch-env default";
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ name: "app", scripts: { start: script } }, null, 2) +
            "\n",
      );
      const result = scaffold({
         name: "sales",
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const output = formatSuccess(result);

      expect(result.hasStartScript).toBe(false);
      expect(output).not.toContain("has no Publisher start script");
      expect(output).toContain("0.0.0.0");
      // The fallback is still offered, and it is still the private one.
      expect(offeredCommands(output)).toContain(result.startCommand);
   });

   test.each([
      [
         "a trailing shell comment",
         "--host 0.0.0.0 --watch-env default # --host 127.0.0.1",
      ],
      ["a backslash-escaped quote", "--host 127.0.0.1 \\' --host 0.0.0.0 \\'"],
   ])("%s does not hide the binding", (_label, tail) => {
      // Both bind 0.0.0.0 under sh: `#` opens a comment, and `\'` emits a
      // literal quote where a quote-only tokenizer sees a region opening and
      // swallows the rest. shellTokens models quoting and nothing else, so
      // these have to be declined up front rather than parsed, and a declined
      // script is one whose binding is unknown, which warns.
      const script = `npx -y @malloy-publisher/server@0.0.231 --server_root . --config ./publisher.config.json ${tail}`;
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ name: "app", scripts: { start: script } }, null, 2) +
            "\n",
      );
      const result = scaffold({
         name: "sales",
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const output = formatSuccess(result);
      // Pin the reason, not just the warning: this must not drift back into
      // unrecognised-shape, whose sentence claims a longer command line that
      // neither of these has.
      expect(result.declinedStartScript?.reason).toBe(
         "unmodelled-shell-syntax",
      );
      expect(output).toContain("Do not use");
      expect(output).not.toContain("longer command line");
   });

   test("a script sh cannot even parse is never offered as npm start", () => {
      // An unterminated quote is not a script with an unknown binding, it is a
      // script that starts nothing: sh exits 2 with "unexpected EOF". Adopting
      // it printed `npm start` as the way to boot this workspace, and wrote that
      // into AGENTS.md, over a command that cannot run.
      const script =
         "npx -y @malloy-publisher/server@0.0.231 --server_root . " +
         '--config ./publisher.config.json --host 127.0.0.1 --watch-env "default';
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ name: "app", scripts: { start: script } }, null, 2) +
            "\n",
      );
      const result = scaffold({
         name: "sales",
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const output = formatSuccess(result);

      expect(result.hasStartScript).toBe(false);
      expect(result.declinedStartScript?.reason).toBe("unparseable");
      expect(offeredCommands(output)).not.toContain("npm start");
      expect(offeredCommands(output)).toContain(result.startCommand);
      // It binds nothing, so warning that it serves an unauthenticated API
      // names a risk that cannot occur and buries the real problem.
      expect(output).not.toContain("Do not use");
      expect(output).toContain("leaves a quote open");
   });

   test("a --host swallowed as another flag's value still warns", () => {
      // The server walks argv positionally, so `--server_root` with its own
      // value missing consumes `--host` as that value and never sets a host at
      // all: the bind falls back to 0.0.0.0. Reading the script as text instead
      // reports 127.0.0.1 and calls it private, which is the one mistake this
      // warning cannot afford to make.
      const script =
         "npx -y @malloy-publisher/server@0.0.231 " +
         "--config ./publisher.config.json --watch-env default " +
         "--server_root --host 127.0.0.1";
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ name: "app", scripts: { start: script } }, null, 2) +
            "\n",
      );
      const output = formatSuccess(
         scaffold({
            name: "sales",
            cwd: tmp,
            host: "claude-code",
            force: false,
         }),
      );
      expect(output).toContain("Do not use");
      expect(output).toContain("0.0.0.0");
   });

   test("a loopback boot this tool did not write is not called unknown", () => {
      // `--port 4100` makes this a boot this tool could not have written, so it
      // is declined and the explicit command is offered instead. But its
      // `--host` was read and is loopback: saying the binding could not be
      // established would contradict the flag this code just parsed, in the one
      // paragraph that exists to be trusted about binding.
      const script =
         "npx -y @malloy-publisher/server@0.0.231 --server_root . " +
         "--config ./publisher.config.json --host 127.0.0.1 " +
         "--watch-env default --port 4100";
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ name: "app", scripts: { start: script } }, null, 2) +
            "\n",
      );
      const result = scaffold({
         name: "sales",
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const output = formatSuccess(result);

      // Pin the branch, not just the absence of a warning: without this the
      // test passes for a script that never reaches different-invocation at
      // all, so any future change to what counts as a Publisher boot would
      // stop exercising this case while staying green.
      expect(result.declinedStartScript?.reason).toBe("different-invocation");
      expect(result.hasStartScript).toBe(false);
      expect(output).not.toContain("could not establish");
      expect(output).not.toContain("0.0.0.0");
      expect(offeredCommands(output)).toContain(result.startCommand);
   });

   test("a script with nothing to do with Publisher is not described as a risk", () => {
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ name: "app", scripts: { start: "vite" } }, null, 2) +
            "\n",
      );
      const output = formatSuccess(
         scaffold({
            name: "sales",
            cwd: tmp,
            host: "claude-code",
            force: false,
         }),
      );
      expect(output).not.toContain("0.0.0.0");
   });
});

/**
 * Which file holds the Publisher briefing after this run, checked by running the
 * flows the README documents rather than by handing formatSuccess a result that
 * says an AGENTS.md was skipped. Two of the three cases below could not be
 * reached at all by setting fields: an AGENTS.md this tool wrote is skipped only
 * on a path that also moves the briefing to AGENTS.malloy.md, so the state the
 * old test here described (skipped AGENTS.md, briefing still in AGENTS.md) never
 * occurred, and the message it asserted on was never printed to anyone.
 *
 * CLAUDE.md is the durable half. It is loaded automatically, it names one file,
 * and it outlives the terminal this output scrolls past, so the check that
 * matters is that the file it names describes the package this run created.
 */
describe("formatSuccess: where the Publisher briefing ended up", () => {
   /** The briefing CLAUDE.md sends the reader to, resolved off CLAUDE.md. */
   function briefingClaudeMdPointsAt(): { name: string; text: string } {
      const claude = fs.readFileSync(path.join(tmp, "CLAUDE.md"), "utf8");
      const referenced = [...claude.matchAll(/@([A-Za-z0-9._/-]+\.md)/g)].map(
         (match) => match[1],
      );
      for (const name of referenced) {
         const target = path.join(tmp, name);
         if (fs.existsSync(target) && fs.lstatSync(target).isFile()) {
            return { name, text: fs.readFileSync(target, "utf8") };
         }
      }
      throw new Error(
         `CLAUDE.md names no file that exists here. It holds:\n${claude}`,
      );
   }

   function expectBriefingToDescribe(name: string): void {
      const briefing = briefingClaudeMdPointsAt();
      if (briefing.text.includes(name)) {
         return;
      }
      throw new Error(
         `CLAUDE.md points at ${briefing.name}, and that file says nothing ` +
            `about ${name}, the package this run created. Every file in this ` +
            `workspace was written by this tool, so there is nothing of the ` +
            `user's to protect by skipping AGENTS.md: writeAgentsFile in ` +
            `scaffold.ts has to read the file before deciding it is someone ` +
            `else's, and regenerate its own.`,
      );
   }

   test("a second package: the briefing is regenerated, not orphaned", () => {
      // The flow the README documents under "Running it again".
      runHere({ name: "sales" });
      const result = runHere({ name: "orders" });
      const output = formatSuccess(result);

      expectBriefingToDescribe("orders");
      expect(result.agentsFile).toBe("AGENTS.md");
      expect(fs.existsSync(path.join(tmp, "AGENTS.malloy.md"))).toBe(false);
      expect(
         result.skipped.filter((item) => item.startsWith("AGENTS")),
      ).toEqual([]);
      // Both clauses of this were printed over a file this tool wrote itself.
      expect(output).not.toContain("is the project's own");
   });

   test("a setup-only run, then a package: the briefing gains the package", () => {
      // The other documented two-step flow, and the worse of the two: the
      // briefing written first has no package section at all.
      runHere({ name: undefined });
      const result = runHere({ name: "sales" });

      expectBriefingToDescribe("sales");
      expect(briefingClaudeMdPointsAt().text).toContain("## The package");
      expect(result.agentsFile).toBe("AGENTS.md");
      expect(fs.existsSync(path.join(tmp, "AGENTS.malloy.md"))).toBe(false);
   });

   test("an AGENTS.md that is really the project's own is still left alone", () => {
      const mine =
         "# my-existing-app\n\nRun `npm start` to boot the API on port 3000.\n";
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), mine);
      const result = runHere({ name: "metrics" });
      const output = formatSuccess(result);

      expect(fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8")).toBe(mine);
      expect(result.agentsFile).toBe("AGENTS.malloy.md");
      expect(result.skipped).toContain("AGENTS.md");
      expect(briefingClaudeMdPointsAt().name).toBe("AGENTS.malloy.md");
      expect(output).toContain("is the project's own");
      expectBriefingToDescribe("metrics");
   });

   test("the closing line names the briefing this run actually wrote", () => {
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "# my notes\n");
      const result = runHere();
      const output = formatSuccess(result);
      expect(result.agentsFile).toBe("AGENTS.malloy.md");
      // "See AGENTS.md" was hardcoded into the last line, 28 lines below the
      // line where the same run reports writing the briefing somewhere else.
      expect(output).toContain(`See ${result.agentsFile}`);
      expect(output).not.toContain("See AGENTS.md");
   });

   test("and still names AGENTS.md when that is where the briefing is", () => {
      const result = runHere();
      expect(result.agentsFile).toBe("AGENTS.md");
      expect(formatSuccess(result)).toContain("See AGENTS.md");
   });
});

/**
 * The MCP line reports a merge that happened, so what it says about the file it
 * merged into has to be read off that file rather than off the fact that a merge
 * took place.
 */
describe("formatSuccess: what the MCP merge line claims", () => {
   function writeMcpConfig(servers: Record<string, unknown>): void {
      fs.writeFileSync(
         path.join(tmp, ".mcp.json"),
         JSON.stringify({ mcpServers: servers }, null, 2) + "\n",
      );
   }

   function scaffoldHere(): ScaffoldResult {
      return scaffold({
         name: "sales",
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
   }

   test("an .mcp.json holding no other servers is not 'alongside' any", () => {
      writeMcpConfig({});
      const result = scaffoldHere();
      expect(result.mcpMerged).toBe(true);
      expect(formatSuccess(result)).not.toContain(
         "alongside the servers already in it",
      );
   });

   test("replacing an existing malloy entry is not reported as adding one", () => {
      // The state AGENTS.md itself creates: it tells the reader to edit this url
      // after moving the ports.
      writeMcpConfig({
         malloy: { type: "http", url: "http://localhost:4140/mcp" },
      });
      const result = scaffoldHere();
      const output = formatSuccess(result);
      expect(result.replaced).toContain(".mcp.json malloy server");
      expect(output).not.toMatch(/malloy server was added/);
      expect(output).not.toContain("alongside the servers already in it");
   });

   test("a file that really does hold other servers still says alongside", () => {
      writeMcpConfig({ linear: { type: "http", url: "https://linear" } });
      const output = formatSuccess(scaffoldHere());
      expect(output).toContain("alongside");
   });
});

/**
 * Strings this tool reads out of the workspace (a start script, an environment
 * name out of a config that arrived with a cloned repository) are printed back
 * into its own output. A terminal acts on the control characters in them, so an
 * ESC and a CR in a start script are enough for a cloned repository to erase the
 * line this tool just printed and paint its own over it. Nothing here checks the
 * tool's internal state, which was always right; it checks the bytes that reach
 * the terminal.
 */
describe("formatSuccess: workspace text is shown, not sent", () => {
   /**
    * SGR color, the only escape sequence this tool emits on its own. Built from
    * a char code rather than written into the pattern, because an ESC in a
    * regular expression literal is exactly what no-control-regex exists to
    * flag, and here it is the thing under test.
    */
   const SGR = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "g");

   function controlCharactersIn(text: string): string[] {
      return [...text.replace(SGR, "")].filter((character) => {
         if (character === "\n") {
            return false;
         }
         const code = character.codePointAt(0) as number;
         return code < 0x20 || code === 0x7f || (code >= 0x80 && code <= 0x9f);
      });
   }

   test("an escape sequence in a start script cannot repaint the output", () => {
      // ESC [2K erases the line and CR returns to its start, so together they
      // overwrite whatever was printed just before.
      writeForeignPackageJson({
         start: "vite \u001b[2K\rcreate-malloy-package: all good",
      });
      const result = runHere({ force: true });
      const output = formatSuccess(result);

      // The script really did reach the output, so this does not pass by
      // printing nothing at all.
      expect(
         result.replaced.some((item) => item.includes("start script")),
      ).toBe(true);
      expect(output).toContain("create-malloy-package: all good");
      expect(controlCharactersIn(output)).toEqual([]);
      // Shown instead, the way describeUnsafeCharacter has always shown one.
      expect(output.replace(SGR, "")).toContain("\\r");
   });

   test("an environment name out of a cloned config is shown in the refusal", () => {
      fs.writeFileSync(
         path.join(tmp, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "prod\u001b[2K\rdefault",
                  packages: [],
                  connections: [],
               },
            ],
         }) + "\n",
      );
      let message = "";
      try {
         runHere();
      } catch (err) {
         message = (err as Error).message;
      }
      expect(message).not.toBe("");
      expect(controlCharactersIn(message)).toEqual([]);
   });

   /**
    * The same repaint, through the other file this tool reads out of a directory
    * it did not create. A package already registered under the requested name is
    * refused, and the refusal quotes the entry: ESC [2K erases the line the
    * refusal is being printed on, CR returns to its start, and SGR green writes
    * a success over it. The error is the only thing on screen at that point, and
    * nothing was written, so the repaint is the whole message.
    */
   const REPAINT = "\u001b[2K\r\u001b[32m* Created package sales\u001b[0m";

   test("a package location out of a cloned config cannot repaint the refusal", () => {
      fs.writeFileSync(
         path.join(tmp, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "default",
                  packages: [{ name: "sales", location: `./x${REPAINT}` }],
                  connections: [],
               },
            ],
         }) + "\n",
      );
      let message = "";
      try {
         runHere();
      } catch (err) {
         message = (err as Error).message;
      }
      // The refusal quotes the location, which is the whole point: this must
      // not pass by the tool having stopped saying where the clash is.
      expect(message).toContain("Created package sales");
      expect(controlCharactersIn(message)).toEqual([]);
   });

   /**
    * Through the bin rather than through the thrown message, because the escape
    * only matters once something writes it to a terminal, and the CLI is what
    * writes it. Checked on stdout and stderr together for the same reason.
    */
   test("the bin does not emit a control character out of a cloned config", () => {
      fs.writeFileSync(
         path.join(tmp, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: "default",
                  packages: [{ name: "sales", location: `./x${REPAINT}` }],
                  connections: [],
               },
            ],
         }) + "\n",
      );
      const proc = spawnSync(process.execPath, [cliEntry, "sales"], {
         cwd: tmp,
         encoding: "utf8",
         env: { ...process.env, NO_COLOR: "1" },
      });
      expect(proc.status).not.toBe(0);
      expect(controlCharactersIn(`${proc.stdout}${proc.stderr}`)).toEqual([]);
   });
});

/**
 * A publisher.config.json shaped in a way this tool cannot use is the user's
 * file, and every other bad shape in it gets a sentence naming the problem. Two
 * shapes reached a property read on null instead, and the CLI diagnosed that as
 * a bug in itself, which sends the reader to this repository over a file only
 * they can fix.
 */
describe("the bin over a config holding entries that are not objects", () => {
   const shapes: { label: string; text: string }[] = [
      { label: "an environment that is null", text: '{"environments":[null]}' },
      {
         label: "a package entry that is null",
         text: '{"environments":[{"name":"default","packages":[null]}]}',
      },
   ];

   for (const { label, text } of shapes) {
      test(`${label} is diagnosed as the config, not as a bug in this tool`, () => {
         fs.writeFileSync(path.join(tmp, "publisher.config.json"), text + "\n");
         const proc = spawnSync(process.execPath, [cliEntry, "sales"], {
            cwd: tmp,
            encoding: "utf8",
         });
         const printed = `${proc.stdout}${proc.stderr}`;
         expect(printed).not.toContain("Cannot read properties");
         expect(printed).not.toContain(
            "This looks like a bug in create-malloy-package",
         );
      });
   }
});

/**
 * Blocker 3. Without --force, a package.json that could not be opened or did not
 * parse produced exit 0, three green checks, `package.json` in the left-alone
 * list with no reason at all, and then "this directory's package.json has no
 * start script": a positive statement about the contents of a file nothing had
 * successfully read. The same run reports an unparseable .mcp.json as ".mcp.json
 * (not valid JSON)" and an unreadable .gitignore with its errno, so the machinery
 * was already two files away.
 *
 * Each case builds the real directory and renders the real result, and asserts
 * on the two halves that were wrong: the file is listed with a reason, and
 * nothing in the output claims to know what is in it.
 */
describe("formatSuccess: a package.json this run could not read", () => {
   /** chmod is a no-op for root, and Windows has no mode bits to clear. */
   const permissionTest =
      process.platform === "win32" || process.getuid?.() === 0
         ? test.skip
         : test;

   function outputOver(contents: string): {
      output: string;
      skipped: string[];
   } {
      fs.writeFileSync(path.join(tmp, "package.json"), contents);
      const result = runHere();
      return { output: formatSuccess(result), skipped: result.skipped };
   }

   /** Every claim about what this directory's package.json holds. */
   function contentClaims(output: string): string[] {
      return output
         .split("\n")
         .filter((line) =>
            /package\.json (has|holds|contains) no|has no (Publisher )?start script/i.test(
               line,
            ),
         );
   }

   /** The left-alone entry for package.json, with whatever reason it carries. */
   function listedReason(skipped: string[]): string | undefined {
      return skipped.find((item) => item.startsWith("package.json"));
   }

   test("a manifest that is not valid JSON is listed with that as the reason", () => {
      const { output, skipped } = outputOver("{ not json\n");
      expect(listedReason(skipped)).toMatch(/not valid JSON/i);
      expect(output).toMatch(/not valid JSON/i);
      expect(contentClaims(output)).toEqual([]);
      // The mitigation stays: the boot that is offered instead really works.
      expect(offeredCommands(output).some((c) => c.startsWith("npx -y"))).toBe(
         true,
      );
   });

   test("a manifest that is a JSON array is listed with that as the reason", () => {
      const { output, skipped } = outputOver("[1,2,3]\n");
      expect(listedReason(skipped)).toMatch(/array|not a JSON object/i);
      expect(contentClaims(output)).toEqual([]);
   });

   permissionTest(
      "a manifest that cannot be opened says so, not 'no script'",
      () => {
         fs.writeFileSync(path.join(tmp, "package.json"), '{"name":"x"}\n');
         fs.chmodSync(path.join(tmp, "package.json"), 0o000);
         const result = runHere();
         const output = formatSuccess(result);

         const listed = listedReason(result.skipped);
         expect(listed).toBeDefined();
         // Whatever the wording, it cannot be the bare filename: that is the entry
         // a manifest this tool read fine and chose to leave alone gets.
         expect(listed).not.toBe("package.json");
         expect(listed).toMatch(/could not be read|EACCES|permission/i);
         expect(contentClaims(output)).toEqual([]);
      },
   );

   test("a manifest that reads fine is still listed without a reason", () => {
      // The other side of the same fix: a readable manifest with no Publisher
      // scripts is not a problem, and inventing a reason for it would be the
      // same defect pointing the other way.
      writeForeignPackageJson({ start: "vite" });
      const result = runHere();
      expect(listedReason(result.skipped)).toBe("package.json");
   });
});

/**
 * The joined `--host=<addr>` form, from the CLI's side. Publisher reads `--host`
 * and its address as two arguments, so the joined form matches nothing, is
 * dropped in silence, and the bind falls back to 0.0.0.0. Adopted as loopback,
 * it was the one shape that suppressed the exposure warning while serving the
 * whole network, and the shape most easily written by accident.
 */
describe("formatSuccess: a start script writing --host=<address>", () => {
   const script =
      "npx -y @malloy-publisher/server --server_root . " +
      "--config ./publisher.config.json --host=127.0.0.1 --watch-env default";

   test("npm start is not offered, and the exposure is named", () => {
      writeForeignPackageJson({ start: script });
      const result = runHere();
      const output = formatSuccess(result);

      expect(result.hasStartScript).toBe(false);
      expect(offeredCommands(output)).not.toContain("npm start");
      expect(offeredCommands(output)).toContain(result.startCommand);
      // The warning has to say what would happen, not merely that something is
      // wrong: this is the one moment the tool sees an unauthenticated REST and
      // MCP endpoint about to be served to the network.
      expect(output).toMatch(/0\.0\.0\.0|every interface/);
   });

   test("the reader is not told there is no --host in a script that writes one", () => {
      writeForeignPackageJson({ start: script });
      const output = formatSuccess(runHere());
      const bareNoHost = output
         .split("\n")
         .filter((line) => /no --host/.test(line) && !/--host=/.test(line));
      // Told "no --host" over a line whose own text holds --host=127.0.0.1, a
      // reader concludes the tool cannot read and ignores the warning under it.
      expect(bareNoHost).toEqual([]);
   });
});

/**
 * SHOULD-FIX 1. `no-init-flag` was computed, carried on the public result type,
 * and printed nowhere. The README tells a user in this state, in bold, to
 * "Restart with `npm run reset`, not `npm start`"; a reset script that boots
 * Publisher without --init does not rebuild the package list, so the package
 * that was just created stays unserved with nothing on screen saying why.
 */
describe("formatSuccess: a reset script that cannot reset", () => {
   const LOOPBACK_BOOT =
      "npx -y @malloy-publisher/server --server_root . " +
      "--config ./publisher.config.json --host 127.0.0.1 --watch-env default";

   test("the declined reset script is named, with the reason", () => {
      writeConfigWithPackages("default", ["faa"]);
      markAsAlreadyBooted();
      // A Publisher boot on loopback, so nothing else in the output says a word
      // about it. The only thing missing is the flag reset exists for.
      writeForeignPackageJson({ start: LOOPBACK_BOOT, reset: LOOPBACK_BOOT });

      const result = runHere();
      const output = formatSuccess(result);

      expect(result.needsReset).toBe(true);
      expect(result.hasResetScript).toBe(false);
      expect(result.declinedResetScript?.reason).toBe("no-init-flag");

      expect(offeredCommands(output)).not.toContain("npm run reset");
      expect(offeredCommands(output)).toContain(result.resetCommand);
      const explained = output
         .split("\n")
         .filter((line) => /reset script/i.test(line) && /--init/.test(line));
      if (explained.length === 0) {
         throw new Error(
            `This workspace has a reset script that boots Publisher without ` +
               `--init, so "npm run reset" would not serve the new package. ` +
               `The output never mentions it:\n${output}`,
         );
      }
   });
});

/**
 * The ports the output prints, against the boot the output itself offers.
 *
 * Every URL in the success message is rendered off result.publisherPort and
 * result.mcpPort, which were the two constants in scaffold.ts. Where this run
 * offers `npm start` over a script it did not write, those two numbers are a
 * guess about someone else's command line: the "Check it is ready" curls, the
 * browser URL and the MCP endpoint were all printed as 4000/4040 over a script
 * booting 4100/4140. Both curls then answer connection-refused, or answer from
 * another Publisher on this machine holding the defaults, which is worse: the
 * status looks right and the data is not this workspace's.
 *
 * Neither fix is assumed. Declining the script and printing the explicit npx
 * command passes this, and so does following the script's flags.
 */
describe("formatSuccess: the ports it prints are the ports its boot binds", () => {
   const DEFAULT_PUBLISHER_PORT = 4000;
   const DEFAULT_MCP_PORT = 4040;

   /** Whole-token `--<flag> <value>`, the only form Publisher's argv walk reads. */
   function lastFlagValue(command: string, flag: string): string | undefined {
      let value: string | undefined;
      const pattern = new RegExp(
         `(?:^|\\s)${flag}\\s+("[^"]*"|'[^']*'|\\S+)`,
         "g",
      );
      for (const match of command.matchAll(pattern)) {
         value = match[1].replace(/^["']|["']$/g, "");
      }
      return value;
   }

   function portsOf(command: string): { publisher: number; mcp: number } {
      const read = (flag: string, fallback: number): number => {
         const raw = lastFlagValue(command, flag);
         return raw === undefined ? fallback : Number(raw);
      };
      return {
         publisher: read("--port", DEFAULT_PUBLISHER_PORT),
         mcp: read("--mcp_port", DEFAULT_MCP_PORT),
      };
   }

   /** The distinct ports one shape of URL names in the output. */
   function portsMatching(text: string, pattern: RegExp): number | number[] {
      const found = [
         ...new Set([...text.matchAll(pattern)].map((m) => Number(m[1]))),
      ];
      return found.length === 1 ? found[0] : found;
   }

   /**
    * The boot the output offers, as the process it would really start. `npm
    * start` is an indirection through the workspace manifest, which is the whole
    * of the gap: the output names a command whose flags nothing had read.
    */
   function offeredBoot(output: string): string {
      const command = offeredCommands(output).find(
         (line) => !line.startsWith("npm run reset"),
      );
      if (command === undefined) {
         throw new Error(
            `The output offers no boot command at all:\n${output}`,
         );
      }
      if (!command.startsWith("npm start")) {
         return command;
      }
      const pkg = JSON.parse(
         fs.readFileSync(path.join(tmp, "package.json"), "utf8"),
      ) as { scripts?: Record<string, string> };
      const script = pkg.scripts?.start;
      if (script === undefined) {
         throw new Error(
            `The output offers "npm start", and this workspace's package.json ` +
               `has no start script`,
         );
      }
      return script;
   }

   function expectPrintedPortsMatchTheBoot(output: string): void {
      const boot = offeredBoot(output);
      const ports = portsOf(boot);
      expect({
         boot,
         "the REST URLs": portsMatching(
            output,
            /http:\/\/localhost:(\d+)\/api\/v0\//g,
         ),
         "the MCP endpoint": portsMatching(
            output,
            /http:\/\/localhost:(\d+)\/mcp/g,
         ),
         "the browser URL": portsMatching(
            output,
            /http:\/\/localhost:(\d+)(?=\s|$)/gm,
         ),
      }).toEqual({
         boot,
         "the REST URLs": ports.publisher,
         "the MCP endpoint": ports.mcp,
         "the browser URL": ports.publisher,
      });
   }

   const OUR_SHAPE =
      `npx -y @malloy-publisher/server --server_root . ` +
      `--config ./publisher.config.json --host 127.0.0.1 --watch-env default`;

   const cases: { label: string; start: string }[] = [
      {
         label: "the port pair the briefing hands the reader",
         start: `${OUR_SHAPE} --port 4100 --mcp_port 4140`,
      },
      {
         label: "a port pair nothing here would guess",
         start: `${OUR_SHAPE} --port 5311 --mcp_port 5312`,
      },
      { label: "only the REST port moved", start: `${OUR_SHAPE} --port 4100` },
      {
         label: "only the MCP port moved",
         start: `${OUR_SHAPE} --mcp_port 4140`,
      },
   ];

   for (const { label, start } of cases) {
      test(`${label}: every printed URL is on that boot's ports`, () => {
         writeForeignPackageJson({ start });
         const output = formatSuccess(runHere());
         expectPrintedPortsMatchTheBoot(output);
      });
   }

   /** Controls, so a helper that threw on everything is not six passes. */
   test("a plain run into an empty directory satisfies it", () => {
      expectPrintedPortsMatchTheBoot(formatSuccess(runHere()));
   });

   test("a run over the start script this tool writes satisfies it", () => {
      writeForeignPackageJson({ start: OUR_SHAPE });
      const result = runHere();
      expect(result.hasStartScript).toBe(true);
      expectPrintedPortsMatchTheBoot(formatSuccess(result));
   });

   /*
    * The check, executed against the state it exists to catch: the script moves
    * after the run rendered its output, so the printed curls are on 4000 while
    * `npm start` binds 4100. Every row above passing proves nothing on its own,
    * which is how this defect survived two rounds of checks that compared one
    * generated string against another.
    */
   test("it fails over a boot that moved after the output was rendered", () => {
      writeForeignPackageJson({ start: OUR_SHAPE });
      const result = runHere();
      expect(result.hasStartScript).toBe(true);
      const output = formatSuccess(result);
      writeForeignPackageJson({
         start: `${OUR_SHAPE} --port 4100 --mcp_port 4140`,
      });
      expect(() => expectPrintedPortsMatchTheBoot(output)).toThrow();
   });
});

/**
 * The README's account of a declined start script, against the run.
 *
 * README.md promises the run "names the script it declined and why". The
 * commonest real shape of all, a package.json whose `start` runs something that
 * is not Publisher, was the one case that printed neither: a bare `package.json`
 * in the left-alone list and then a parenthesis saying only that no Publisher
 * script was added. Every other decline reason named the script.
 *
 * Written against the README rather than against a fixed sentence, so rewording
 * the promise is as good a fix as keeping it: the test asks only that the two
 * files agree.
 */
describe("formatSuccess: a start script that runs something else", () => {
   const readme = fs
      .readFileSync(path.join(here, "..", "README.md"), "utf8")
      .replace(/\s+/g, " ");
   const promisesToName = /names the script it declined and why/i.test(readme);

   const scripts = [
      "vite preview --port 3000",
      "node server.js",
      "react-scripts start",
   ];

   for (const script of scripts) {
      test(`\`${script}\` is named, as the README says it is`, () => {
         writeForeignPackageJson({ start: script });
         const result = runHere();
         const output = formatSuccess(result);

         expect(result.hasStartScript).toBe(false);
         // The fallback command is offered either way; that half was never the
         // gap and is asserted here so a fix cannot trade one for the other.
         expect(output).toContain(result.startCommand);
         if (!promisesToName) {
            // The README was reworded to promise only what the run does. Nothing
            // left to check: the two agree.
            return;
         }
         expect(output).toContain(script);
      });
   }
});
