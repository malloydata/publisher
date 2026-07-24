import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
   scaffold,
   type ScaffoldOptions,
   type ScaffoldResult,
} from "./scaffold";
import { countSkills } from "./skills";
import { skillsDir } from "@malloy-publisher/skills";

/**
 * Derived from the payload, never hardcoded: a count in here would fail the
 * cross-platform job on three OSes the day someone adds a skill, and the number
 * would say nothing about why.
 */
const SKILL_COUNT = countSkills(skillsDir);

let tmp: string;

beforeEach(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-scaffold-"));
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

function readJson(relative: string): Record<string, unknown> {
   return JSON.parse(fs.readFileSync(path.join(tmp, relative), "utf8"));
}

function exists(relative: string): boolean {
   return fs.existsSync(path.join(tmp, relative));
}

function agentsFile(): string {
   return fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
}

/** The scripts block of the workspace manifest, empty when there is none. */
function workspaceScripts(): Record<string, string> {
   if (!exists("package.json")) {
      return {};
   }
   const pkg = readJson("package.json") as { scripts?: Record<string, string> };
   return pkg.scripts ?? {};
}

/** A Publisher start script of the shape this tool writes, for a fixture. */
const OUR_START =
   `npx -y @malloy-publisher/server --server_root . ` +
   `--config ./publisher.config.json --host 127.0.0.1 --watch-env default`;

function writePackageJson(scripts: Record<string, string>): void {
   fs.writeFileSync(
      path.join(tmp, "package.json"),
      JSON.stringify({ name: "someone-elses-app", scripts }, null, 2) + "\n",
   );
}

/**
 * Every line inside a fenced block of AGENTS.md that boots the server: the start
 * command, the same boot moved onto free ports, and the reset boot. curl lines
 * and the bare REST base URL are not commands that start anything, so they are
 * not resolved.
 */
function bootCommandsIn(markdown: string): string[] {
   // \r?\n because AGENTS.md is rendered from a template read off disk, and a
   // Windows checkout (git's default core.autocrlf) hands it over with CRLF. An
   // \n-only fence match then finds nothing at all, which reads as "every boot
   // command is fine" rather than as a broken extractor.
   return [...markdown.matchAll(/```[a-z]*\r?\n([\s\S]*?)```/g)]
      .flatMap((match) => match[1].split("\n"))
      .map((line) => line.trim())
      .filter(
         (line) =>
            line.startsWith("npm ") ||
            line.includes("@malloy-publisher/server"),
      );
}

/**
 * What a command in AGENTS.md really runs. `npm start` and `npm run <name>` are
 * indirections through this workspace's own package.json, and the whole of
 * blocker 1 lived in that gap: the file said `npm start` over a script that ran
 * vite, and `npm run reset` over a workspace with no reset script at all. So
 * resolve the indirection and judge the process that would actually start.
 */
function resolveCommand(command: string, scripts: Record<string, string>) {
   const match = /^npm (?:(start)|run\s+([A-Za-z0-9:_-]+))(.*)$/.exec(command);
   if (!match) {
      return { script: undefined, resolved: command };
   }
   const name = match[1] ?? match[2];
   const extra = match[3].replace(/^\s*--(\s|$)/, " ");
   const script = scripts[name];
   return {
      script: name,
      resolved: script === undefined ? undefined : `${script}${extra}`,
   };
}

/**
 * The value of the last --host Publisher would actually read off a command line,
 * quotes stripped, and undefined when it would read none.
 *
 * Whitespace only, never `--host=<addr>`. This is the second opinion on
 * scaffold.ts's own parser, so it is written from the server's source rather
 * than from what the line looks like it means: packages/server/src/server.ts
 * matches `arg === "--host" && args[i + 1]`, a whole argument plus the next one.
 * The joined form matches no flag it knows, is dropped in silence, and the bind
 * falls back to `PUBLISHER_HOST || "0.0.0.0"`. A helper that accepted `=` here
 * agreed with the bug it was supposed to catch, and the whole matrix below went
 * green over a workspace serving the LAN.
 */
function lastHostFlag(command: string): string | undefined {
   let value: string | undefined;
   for (const match of command.matchAll(/--host\s+("[^"]*"|'[^']*'|\S+)/g)) {
      value = match[1].replace(/^["']|["']$/g, "");
   }
   return value;
}

/** Deliberately a second, stricter opinion than scaffold.ts's own. */
function isLoopback(host: string | undefined): boolean {
   return (
      host !== undefined &&
      /^(localhost|::1|\[::1\]|127(\.\d{1,3}){3})$/i.test(host)
   );
}

/**
 * The invariant AGENTS.md has to hold in every state: every boot it hands the
 * reader really boots Publisher, and really binds a loopback address. Both
 * halves are checked on the resolved command, so an `npm` script that is not
 * ours fails here rather than reading as a Publisher boot because the file said
 * so. Nothing in here matches on prose.
 */
function expectEveryBootIsPrivatePublisher(): void {
   const agents = agentsFile();
   expect(agents).not.toContain("{{");
   const scripts = workspaceScripts();
   const commands = bootCommandsIn(agents);
   // The start boot, the same boot on free ports, and the reset boot. A drop
   // below three means the extraction above stopped matching and the loop is
   // passing over nothing.
   expect(commands.length).toBeGreaterThanOrEqual(3);
   for (const command of commands) {
      const { script, resolved } = resolveCommand(command, scripts);
      if (resolved === undefined) {
         throw new Error(
            `AGENTS.md says to run "${command}", but this workspace's ` +
               `package.json defines no "${script}" script`,
         );
      }
      if (!resolved.includes("@malloy-publisher/server")) {
         throw new Error(
            `AGENTS.md says to run "${command}", which in this workspace runs ` +
               `"${resolved}" and does not boot Publisher`,
         );
      }
      if (!isLoopback(lastHostFlag(resolved))) {
         throw new Error(
            `AGENTS.md says to run "${command}", which in this workspace runs ` +
               `"${resolved}" and binds ${lastHostFlag(resolved) ?? "no --host"} ` +
               `rather than a loopback address, so the unauthenticated REST and ` +
               `MCP endpoints are on the network`,
         );
      }
   }
}

describe("scaffold: default package", () => {
   test("creates the expected file tree", () => {
      const result = run();
      expect(result.packageCreated).toBe(true);
      for (const file of [
         "sales/publisher.json",
         "sales/sales.malloy",
         "sales/data/sales.csv",
         "publisher.config.json",
         "package.json",
         ".gitignore",
         ".mcp.json",
         "CLAUDE.md",
         "AGENTS.md",
      ]) {
         expect(exists(file)).toBe(true);
      }
   });

   test("publisher.json is just the name", () => {
      run();
      expect(readJson("sales/publisher.json")).toEqual({ name: "sales" });
   });

   test("registers the package in the config", () => {
      run();
      const config = readJson("publisher.config.json") as {
         environments: { name: string; packages: unknown[] }[];
      };
      expect(config.environments[0].name).toBe("default");
      expect(config.environments[0].packages).toEqual([
         { name: "sales", location: "./sales" },
      ]);
   });

   test("start script boots the server in watch mode", () => {
      run();
      const pkg = readJson("package.json") as { scripts: { start: string } };
      expect(pkg.scripts.start).toContain("@malloy-publisher/server");
      expect(pkg.scripts.start).toContain("--config ./publisher.config.json");
      expect(pkg.scripts.start).toContain("--watch-env default");
      expect(pkg.scripts.start).not.toContain("--init");
   });

   test(".mcp.json points at the local MCP endpoint", () => {
      run();
      const mcp = readJson(".mcp.json") as {
         mcpServers: { malloy: { url: string } };
      };
      expect(mcp.mcpServers.malloy.url).toBe("http://localhost:4040/mcp");
   });

   test("AGENTS.md has every template variable substituted", () => {
      run();
      const agents = fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
      expect(agents).not.toContain("{{");
      expect(agents).toContain("sales");
   });

   test("the model references the sample CSV", () => {
      run();
      const model = fs.readFileSync(
         path.join(tmp, "sales/sales.malloy"),
         "utf8",
      );
      expect(model).toContain("duckdb.table('data/sales.csv')");
      expect(model).toContain("source: sales is");
   });
});

describe("scaffold: skills", () => {
   test("installs every shipped skill as real files, not symlinks", () => {
      expect(SKILL_COUNT).toBeGreaterThan(0);
      const result = run();
      expect(result.skillsInstalled).toBe(SKILL_COUNT);
      expect(countSkills(path.join(tmp, ".claude/skills"))).toBe(SKILL_COUNT);
      const oneSkill = path.join(tmp, ".claude/skills/malloy-getting-started");
      expect(fs.existsSync(path.join(oneSkill, "SKILL.md"))).toBe(true);
      expect(fs.lstatSync(oneSkill).isSymbolicLink()).toBe(false);
   });

   test("copies reference/ subfiles, not just SKILL.md", () => {
      run();
      const ref = path.join(tmp, ".claude/skills/malloy-review/reference");
      expect(fs.existsSync(ref)).toBe(true);
      expect(fs.readdirSync(ref).length).toBeGreaterThan(0);
   });

   test("leaves a symlinked skill directory alone instead of writing through it", () => {
      // The layout this repo's own contributors use: .claude/skills/<name> is a
      // link to the skill's source, so a copy through it edits the source.
      const source = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-link-"));
      fs.writeFileSync(path.join(source, "SKILL.md"), "# mine\n");
      const linkPath = path.join(tmp, ".claude", "skills", "malloy-analysis");
      fs.mkdirSync(path.dirname(linkPath), { recursive: true });
      fs.symlinkSync(source, linkPath);
      try {
         const result = run();
         expect(fs.readFileSync(path.join(source, "SKILL.md"), "utf8")).toBe(
            "# mine\n",
         );
         expect(fs.lstatSync(linkPath).isSymbolicLink()).toBe(true);
         expect(result.skillsInstalled).toBe(SKILL_COUNT - 1);
         expect(
            result.skipped.some((entry) => entry.includes("malloy-analysis")),
         ).toBe(true);
      } finally {
         fs.rmSync(source, { recursive: true, force: true });
      }
   });
});

describe("scaffold: --data", () => {
   test("copies the file and points the model at it", () => {
      const src = path.join(tmp, "orders.csv");
      fs.writeFileSync(src, "id,amount\n1,10\n2,20\n");
      const result = run({ name: "shop", dataFile: src });
      expect(result.packageCreated).toBe(true);
      expect(exists("shop/data/orders.csv")).toBe(true);
      const model = fs.readFileSync(path.join(tmp, "shop/shop.malloy"), "utf8");
      expect(model).toContain("duckdb.table('data/orders.csv')");
   });

   test("copies an xlsx file and points the model at it", () => {
      // Scaffolding only validates the extension and copies the file; DuckDB
      // reads the spreadsheet at serve time (see tests/e2e for that half).
      const src = path.join(tmp, "budget.xlsx");
      fs.writeFileSync(src, "PK not a real spreadsheet");
      const result = run({ name: "shop", dataFile: src });
      expect(result.packageCreated).toBe(true);
      expect(exists("shop/data/budget.xlsx")).toBe(true);
      const model = fs.readFileSync(path.join(tmp, "shop/shop.malloy"), "utf8");
      expect(model).toContain("duckdb.table('data/budget.xlsx')");
   });

   test("rejects an unsupported file type with nothing created", () => {
      const src = path.join(tmp, "notes.txt");
      fs.writeFileSync(src, "hello");
      expect(() => run({ name: "shop", dataFile: src })).toThrow(
         /\.csv, \.parquet, or \.xlsx/i,
      );
      expect(exists("shop")).toBe(false);
   });

   test("rejects a missing --data file", () => {
      expect(() =>
         run({ name: "shop", dataFile: path.join(tmp, "nope.csv") }),
      ).toThrow(/not found/i);
   });
});

describe("scaffold: existing workspace", () => {
   test("a second package extends the config instead of clobbering it", () => {
      run({ name: "sales" });
      const result = run({ name: "marketing" });
      expect(result.configExtended).toBe(true);
      const config = readJson("publisher.config.json") as {
         environments: { packages: { name: string }[] }[];
      };
      expect(config.environments[0].packages.map((p) => p.name)).toEqual([
         "sales",
         "marketing",
      ]);
   });

   test("leaves existing workspace files in place", () => {
      run({ name: "sales" });
      const marker = "# hand-edited\n";
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), marker);
      const result = run({ name: "marketing" });
      expect(fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8")).toBe(marker);
      expect(result.skipped).toContain("AGENTS.md");
   });

   test("re-running the same package without --force refuses to clobber it", () => {
      run({ name: "sales" });
      expect(() => run({ name: "sales" })).toThrow(/already exists/i);
   });
});

describe("scaffold: bad names", () => {
   test("refuses an invalid package name before creating anything", () => {
      expect(() => run({ name: ".git" })).toThrow(/invalid package name/i);
      expect(exists(".git")).toBe(false);
   });
});

describe("scaffold: setup-only (no name)", () => {
   test("wires the workspace but creates no package", () => {
      const result = scaffold({
         name: undefined,
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      expect(result.packageCreated).toBe(false);
      expect(exists(".mcp.json")).toBe(true);
      expect(exists("AGENTS.md")).toBe(true);
      expect(result.skillsInstalled).toBe(SKILL_COUNT);
      const config = readJson("publisher.config.json") as {
         environments: { packages: unknown[] }[];
      };
      expect(config.environments[0].packages).toEqual([]);
   });

   test("AGENTS.md claims no package and no package file", () => {
      scaffold({
         name: undefined,
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      const agents = fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
      expect(agents).not.toContain("your package");
      expect(agents).not.toContain("model.malloy");
      expect(agents).not.toContain("## The package");
      expect(agents).not.toContain("{{");
   });
});

describe("scaffold: cursor host", () => {
   test("writes .cursor/mcp.json instead of .mcp.json and CLAUDE.md", () => {
      run({ host: "cursor" });
      expect(exists(".cursor/mcp.json")).toBe(true);
      expect(exists(".mcp.json")).toBe(false);
      expect(exists("CLAUDE.md")).toBe(false);
      expect(exists("AGENTS.md")).toBe(true);
   });

   test("AGENTS.md describes the host that was actually wired", () => {
      run({ host: "cursor" });
      const agents = fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
      expect(agents).toContain(".cursor/mcp.json");
      expect(agents).not.toContain("Claude Code offers to connect");
   });
});

describe("scaffold: reserved-word names", () => {
   test("a package named a Malloy keyword gets a safe source name", () => {
      const result = run({ name: "count" });
      expect(result.sourceName).toBe("count_source");
      expect(result.modelFile).toBe("count_source.malloy");
      const model = fs.readFileSync(
         path.join(tmp, "count/count_source.malloy"),
         "utf8",
      );
      expect(model).toContain("source: count_source is");
   });
});

describe("scaffold: hasStartScript / start command", () => {
   test("a fresh scaffold owns a Publisher start script", () => {
      const result = run();
      expect(result.hasStartScript).toBe(true);
   });

   test("an existing non-Publisher package.json is left alone and flagged", () => {
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ scripts: { start: "vite" } }),
      );
      const result = scaffold({
         name: undefined,
         cwd: tmp,
         host: "claude-code",
         force: false,
      });
      expect(result.hasStartScript).toBe(false);
      // We did not clobber their start script.
      const pkg = readJson("package.json") as { scripts: { start: string } };
      expect(pkg.scripts.start).toBe("vite");
   });
});

describe("scaffold: malformed existing config", () => {
   test("aborts before creating the package directory", () => {
      fs.writeFileSync(path.join(tmp, "publisher.config.json"), "{ not json");
      expect(() => run({ name: "sales" })).toThrow();
      expect(exists("sales")).toBe(false);
   });
});

describe("scaffold: unservable workspace path", () => {
   test("refuses a path DuckDB cannot read data files from", () => {
      const spaced = path.join(tmp, "my data dir");
      fs.mkdirSync(spaced);
      expect(() => run({ cwd: spaced })).toThrow(/a space/);
      // Nothing was written, so there is no half-scaffolded package to explain.
      expect(fs.readdirSync(spaced)).toEqual([]);
   });

   test("names a non-ASCII character by codepoint", () => {
      const accented = path.join(tmp, "josé");
      fs.mkdirSync(accented);
      expect(() => run({ cwd: accented })).toThrow(/U\+00E9/);
   });

   test("accepts the characters DuckDB does accept", () => {
      const fine = path.join(tmp, "my-data_dir.v2");
      fs.mkdirSync(fine);
      expect(run({ cwd: fine }).packageCreated).toBe(true);
   });
});

describe("scaffold: existing .mcp.json", () => {
   test("merges the malloy server in beside the ones already there", () => {
      fs.writeFileSync(
         path.join(tmp, ".mcp.json"),
         JSON.stringify({
            mcpServers: { linear: { type: "http", url: "https://linear" } },
         }),
      );
      const result = run();
      expect(result.mcpWired).toBe(true);
      const mcp = readJson(".mcp.json") as {
         mcpServers: Record<string, { url: string }>;
      };
      expect(mcp.mcpServers.linear.url).toBe("https://linear");
      expect(mcp.mcpServers.malloy.url).toBe("http://localhost:4040/mcp");
   });

   test("reports an unreadable one instead of claiming it is wired", () => {
      fs.writeFileSync(path.join(tmp, ".mcp.json"), "{ not json");
      const result = run();
      expect(result.mcpWired).toBe(false);
      expect(result.mcpPasteBlock).toContain("http://localhost:4040/mcp");
      expect(fs.readFileSync(path.join(tmp, ".mcp.json"), "utf8")).toBe(
         "{ not json",
      );
      const agents = fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
      expect(agents).toContain("by hand");
   });

   test("an already-wired one is reported as skipped, not rewritten", () => {
      run({ name: "sales" });
      const result = run({ name: "marketing" });
      expect(result.mcpWired).toBe(true);
      expect(
         result.skipped.some((entry) => entry.startsWith(".mcp.json")),
      ).toBe(true);
   });

   test("--force still merges rather than dropping the other servers", () => {
      fs.writeFileSync(
         path.join(tmp, ".mcp.json"),
         JSON.stringify({
            mcpServers: { linear: { type: "http", url: "https://linear" } },
         }),
      );
      run({ force: true });
      const mcp = readJson(".mcp.json") as {
         mcpServers: Record<string, { url: string }>;
      };
      expect(mcp.mcpServers.linear.url).toBe("https://linear");
      expect(mcp.mcpServers.malloy.url).toBe("http://localhost:4040/mcp");
   });
});

describe("scaffold: --force", () => {
   test("keeps an existing manifest and only sets the start script", () => {
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({
            name: "my-real-app",
            version: "3.2.1",
            dependencies: { express: "^4.19.0" },
            scripts: { start: "vite" },
         }),
      );
      const result = run({ force: true });
      const pkg = readJson("package.json") as {
         name: string;
         dependencies: Record<string, string>;
         scripts: { start: string };
      };
      expect(pkg.name).toBe("my-real-app");
      expect(pkg.dependencies.express).toBe("^4.19.0");
      expect(pkg.scripts.start).toContain("@malloy-publisher/server");
      expect(result.hasStartScript).toBe(true);
      // The overwritten script is named, not silently swapped.
      expect(result.replaced.some((entry) => entry.includes("vite"))).toBe(
         true,
      );
   });

   test("reports what it overwrote", () => {
      run({ name: "sales" });
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "# mine\n");
      const result = run({ name: "sales", force: true });
      expect(result.replaced).toContain("AGENTS.md");
      expect(result.replaced).toContain("sales/");
   });
});

describe("scaffold: environment name", () => {
   test("follows the environment the package actually lands in", () => {
      fs.writeFileSync(
         path.join(tmp, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [{ name: "prod", packages: [], connections: [] }],
         }),
      );
      const result = run({ name: "sales" });
      expect(result.envName).toBe("prod");

      const config = readJson("publisher.config.json") as {
         environments: { name: string; packages: { name: string }[] }[];
      };
      expect(config.environments[0].name).toBe("prod");
      expect(config.environments[0].packages[0].name).toBe("sales");

      expect(result.startCommand).toContain("--watch-env prod");
      const agents = fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
      expect(agents).toContain("/environments/prod/packages/sales");
      expect(agents).not.toContain("/environments/default/");
   });
});

describe("scaffold: AGENTS.md start command", () => {
   // The command in the fenced block under "Start the server first", which is the
   // one an agent runs. Asserting on the whole file would catch the prose
   // elsewhere that talks about `npm start -- --port ...` as an aside.
   const startBlock = () =>
      fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8").split("```bash")[1];

   test("names the explicit command when npm start is not ours", () => {
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ scripts: { start: "vite" } }),
      );
      const result = run({ name: "sales" });
      expect(result.hasStartScript).toBe(false);
      expect(startBlock()).toContain("@malloy-publisher/server");
      expect(startBlock().trim()).not.toBe("npm start");
   });

   test("names npm start when we own it", () => {
      run({ name: "sales" });
      expect(startBlock()).toContain("npm start");
   });
});

describe("scaffold: binding host", () => {
   // Publisher's own default is 0.0.0.0, which on a laptop hands the whole LAN an
   // unauthenticated read of the user's data over both REST and MCP.
   test("every generated boot command binds to loopback", () => {
      const result = run();
      expect(result.startCommand).toContain("--host 127.0.0.1");
      expect(result.resetCommand).toContain("--host 127.0.0.1");
      const pkg = readJson("package.json") as {
         scripts: { start: string; reset: string };
      };
      expect(pkg.scripts.start).toContain("--host 127.0.0.1");
      expect(pkg.scripts.reset).toContain("--host 127.0.0.1");
   });
});

describe("scaffold: reset script", () => {
   test("reset carries --init and start does not", () => {
      const result = run();
      expect(result.hasResetScript).toBe(true);
      const pkg = readJson("package.json") as {
         scripts: { start: string; reset: string };
      };
      expect(pkg.scripts.start).not.toContain("--init");
      expect(pkg.scripts.reset).toContain("@malloy-publisher/server");
      expect(pkg.scripts.reset).toContain("--init");
      expect(result.resetCommand).toBe(`${result.startCommand} --init`);
   });

   test("--force adds reset to an existing manifest alongside start", () => {
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ name: "app", scripts: { build: "tsc" } }),
      );
      const result = run({ force: true });
      expect(result.hasResetScript).toBe(true);
      const pkg = readJson("package.json") as {
         scripts: Record<string, string>;
      };
      expect(pkg.scripts.build).toBe("tsc");
      expect(pkg.scripts.reset).toContain("--init");
   });

   test("re-running does not report our own reset script as overwritten", () => {
      // The reset we generate is the start boot plus --init, so recognising it
      // needs requireInit. Without that it reads as someone else's script and
      // the second run tells the user it overwrote something they wrote, naming
      // a command this tool had written itself moments earlier.
      run();
      const first = readJson("package.json") as {
         scripts: { start: string; reset: string };
      };
      const second = run({ force: true });
      const reported = second.replaced.filter((entry) =>
         entry.includes("script"),
      );
      expect(reported).toEqual([]);
      const after = readJson("package.json") as {
         scripts: { start: string; reset: string };
      };
      expect(after.scripts.start).toBe(first.scripts.start);
      expect(after.scripts.reset).toBe(first.scripts.reset);
   });

   test("a foreign package.json we leave alone reports no reset script", () => {
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({ scripts: { start: "vite", reset: "rm -rf dist" } }),
      );
      const result = run();
      expect(result.hasResetScript).toBe(false);
      expect(result.resetCommand).toContain("@malloy-publisher/server");
      const pkg = readJson("package.json") as {
         scripts: Record<string, string>;
      };
      expect(pkg.scripts.reset).toBe("rm -rf dist");
   });
});

describe("scaffold: BOM-prefixed JSON", () => {
   const BOM = "\uFEFF";

   test("--force keeps a package.json carrying a BOM", () => {
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         BOM +
            JSON.stringify({
               name: "my-real-app",
               version: "9.9.9",
               dependencies: { express: "^4.19.0" },
            }),
      );
      const result = run({ force: true });
      const pkg = readJson("package.json") as {
         name: string;
         version: string;
         dependencies: Record<string, string>;
         scripts: { start: string };
      };
      expect(pkg.name).toBe("my-real-app");
      expect(pkg.version).toBe("9.9.9");
      expect(pkg.dependencies.express).toBe("^4.19.0");
      expect(result.hasStartScript).toBe(true);
   });

   test("an .mcp.json carrying a BOM keeps its other servers", () => {
      fs.writeFileSync(
         path.join(tmp, ".mcp.json"),
         BOM +
            JSON.stringify({
               mcpServers: {
                  github: {
                     command: "gh-mcp",
                     env: { GITHUB_TOKEN: "ghp_secret" },
                  },
               },
            }),
      );
      const result = run({ force: true });
      expect(result.mcpWired).toBe(true);
      const mcp = readJson(".mcp.json") as {
         mcpServers: Record<string, { env?: Record<string, string> }>;
      };
      expect(mcp.mcpServers.github.env?.GITHUB_TOKEN).toBe("ghp_secret");
      expect(mcp.mcpServers.malloy).toBeDefined();
   });

   test("a publisher.config.json carrying a BOM is read, not rejected", () => {
      fs.writeFileSync(
         path.join(tmp, "publisher.config.json"),
         BOM +
            JSON.stringify({
               frozenConfig: false,
               environments: [{ name: "prod", packages: [], connections: [] }],
            }),
      );
      const result = run({ name: "sales" });
      expect(result.envName).toBe("prod");
   });
});

describe("scaffold: --force on files it cannot merge", () => {
   test("refuses an unparseable package.json and writes nothing", () => {
      fs.writeFileSync(path.join(tmp, "package.json"), '{"name": "app",}');
      expect(() => run({ force: true })).toThrow(/not valid JSON/i);
      expect(exists("sales")).toBe(false);
      expect(exists("publisher.config.json")).toBe(false);
      expect(fs.readFileSync(path.join(tmp, "package.json"), "utf8")).toBe(
         '{"name": "app",}',
      );
   });

   test("refuses a package.json that is not a JSON object", () => {
      fs.writeFileSync(path.join(tmp, "package.json"), "[1, 2, 3]");
      expect(() => run({ force: true })).toThrow(/not a JSON object/i);
      expect(exists("sales")).toBe(false);
   });

   test("leaves an unparseable .mcp.json alone rather than replacing it", () => {
      const held = '{"mcpServers": {"github": {"env": {"T": "s"}},}}';
      fs.writeFileSync(path.join(tmp, ".mcp.json"), held);
      const result = run({ force: true });
      expect(result.mcpWired).toBe(false);
      expect(result.mcpPasteBlock).toContain("http://localhost:4040/mcp");
      expect(fs.readFileSync(path.join(tmp, ".mcp.json"), "utf8")).toBe(held);
   });
});

describe("scaffold: skills are refreshed on every run", () => {
   test("an overwritten skill is reported, not silently replaced", () => {
      run({ name: "sales" });
      const skill = path.join(tmp, ".claude/skills/malloy-modeling/SKILL.md");
      fs.appendFileSync(skill, "\nmy own note\n");
      const result = run({ name: "marketing" });
      expect(fs.readFileSync(skill, "utf8")).not.toContain("my own note");
      expect(
         result.replaced.some(
            (entry) =>
               entry.includes(path.join(".claude", "skills")) &&
               entry.includes("refreshed"),
         ),
      ).toBe(true);
   });

   test("a first run reports nothing refreshed", () => {
      const result = run();
      expect(result.replaced.some((entry) => entry.includes("refreshed"))).toBe(
         false,
      );
   });
});

describe("scaffold: symlinks that leave the workspace", () => {
   test("refuses a .claude that points outside instead of writing through it", () => {
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-outside-"));
      fs.symlinkSync(outside, path.join(tmp, ".claude"));
      try {
         const result = run();
         expect(result.skillsInstalled).toBe(0);
         expect(fs.readdirSync(outside)).toEqual([]);
         expect(
            result.skipped.some(
               (entry) =>
                  entry.includes(path.join(".claude", "skills")) &&
                  entry.includes("outside the workspace"),
            ),
         ).toBe(true);
         // And the file that claims the skills are there says they are not.
         const agents = fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8");
         expect(agents).toContain("installed no skills");
         expect(agents).not.toContain("auto-discovers them");
      } finally {
         fs.rmSync(outside, { recursive: true, force: true });
      }
   });

   test("refuses a dangling workspace-file symlink and writes nothing", () => {
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-outside-"));
      const escape = path.join(outside, "stolen.md");
      fs.symlinkSync(escape, path.join(tmp, "AGENTS.md"));
      try {
         expect(() => run()).toThrow(/outside the workspace/i);
         expect(fs.existsSync(escape)).toBe(false);
         expect(exists("sales")).toBe(false);
         expect(exists("publisher.config.json")).toBe(false);
      } finally {
         fs.rmSync(outside, { recursive: true, force: true });
      }
   });

   test("a symlink that stays inside the workspace is fine", () => {
      const real = path.join(tmp, "real-claude");
      fs.mkdirSync(real);
      fs.symlinkSync(real, path.join(tmp, ".claude"));
      const result = run();
      expect(result.skillsInstalled).toBe(SKILL_COUNT);
      expect(countSkills(path.join(real, "skills"))).toBe(SKILL_COUNT);
   });
});

describe("scaffold: rejected inputs", () => {
   test("refuses a package name the workspace itself uses", () => {
      for (const name of [
         "package.json",
         "publisher.config.json",
         "AGENTS.md",
      ]) {
         expect(() => run({ name })).toThrow(/reserved/i);
         expect(exists(name)).toBe(false);
      }
   });

   /**
    * The briefing's own diverted filename. A workspace that already has an
    * AGENTS.md of its own has the Publisher briefing written to
    * AGENTS.malloy.md instead, so that name is a path the workspace uses,
    * exactly as AGENTS.md and CLAUDE.md are. It was missing from the reserved
    * set, and a package on it did not fail cleanly: the package directory, the
    * config, the manifest, the MCP config and the skills were all written
    * before the briefing hit EISDIR on its own name, leaving a half-built
    * workspace behind an exit 1. Judged by listing the directory rather than by
    * reading the message, because "nothing was written" is the part that failed.
    */
   test("refuses the diverted briefing's name, before writing anything", () => {
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "# someone else's notes\n");
      const before = fs.readdirSync(tmp).sort();

      expect(() => run({ name: "AGENTS.malloy.md" })).toThrow(/reserved/i);
      expect(fs.readdirSync(tmp).sort()).toEqual(before);
   });

   test("refuses --data with no package to seed", () => {
      const src = path.join(tmp, "orders.csv");
      fs.writeFileSync(src, "id\n1\n");
      expect(() => run({ name: undefined, dataFile: src })).toThrow(
         /needs a package name/i,
      );
      expect(exists("publisher.config.json")).toBe(false);
   });
});

describe("scaffold: .gitignore", () => {
   test("ignores the DuckDB write-ahead log as well as the database", () => {
      run();
      const ignored = fs.readFileSync(path.join(tmp, ".gitignore"), "utf8");
      expect(ignored).toContain("publisher.db*");
   });
});

/**
 * The matrix that catches blocker 1's shape. AGENTS.md delegates to `npm start`
 * and `npm run reset` only when this workspace's package.json really defines
 * them as Publisher boots, and hardcoding either one put an instruction in the
 * file that ran something else (vite, with Publisher's flags appended) or ran
 * nothing at all. Both flags are read off the result rather than assumed, so a
 * case that stops reaching the state it is named for fails here rather than
 * quietly testing the state next door.
 */
describe("scaffold: AGENTS.md across every start/reset script state", () => {
   const cases: {
      label: string;
      scripts?: Record<string, string>;
      hasStart: boolean;
      hasReset: boolean;
   }[] = [
      {
         label: "no package.json yet, both scripts ours",
         hasStart: true,
         hasReset: true,
      },
      {
         label: "someone else's start and reset",
         scripts: { start: "vite", reset: "./wipe-db.sh" },
         hasStart: false,
         hasReset: false,
      },
      {
         label: "our start, their reset",
         scripts: { start: OUR_START, reset: "./wipe-db.sh" },
         hasStart: true,
         hasReset: false,
      },
      {
         label: "their start, our reset",
         scripts: { start: "vite", reset: `${OUR_START} --init` },
         hasStart: false,
         hasReset: true,
      },
   ];

   for (const testCase of cases) {
      test(testCase.label, () => {
         if (testCase.scripts) {
            writePackageJson(testCase.scripts);
         }
         const result = run();
         expect(result.hasStartScript).toBe(testCase.hasStart);
         expect(result.hasResetScript).toBe(testCase.hasReset);

         const agents = agentsFile();
         if (!testCase.hasStart) {
            expect(agents).not.toContain("npm start");
            expect(agents).toContain(result.startCommand);
         }
         if (!testCase.hasReset) {
            expect(agents).not.toContain("npm run reset");
            expect(agents).toContain(result.resetCommand);
         }
         expectEveryBootIsPrivatePublisher();
      });
   }

   test("the port override is the same boot, not a second guess at it", () => {
      writePackageJson({ start: "vite" });
      run();
      const override = bootCommandsIn(agentsFile()).find((command) =>
         command.includes("--port"),
      );
      expect(override).toBeDefined();
      // The failure this replaces: a hardcoded `npm start -- --port 4100
      // --mcp_port 4140` in the template, which here ran vite.
      expect(override).toContain("@malloy-publisher/server");
      expect(override).toContain("--mcp_port");
      // npm's argument separator belongs only on the `npm start` form.
      expect(override).not.toContain(" -- ");
   });
});

/**
 * The security half of blocker 1, one scaffold per row. A pre-existing script is
 * adopted as `npm start` only if it boots Publisher on a loopback address:
 * Publisher's own default is 0.0.0.0, so adopting a script on the strength of
 * the package name alone put the unauthenticated REST and MCP endpoints on the
 * LAN while AGENTS.md said the opposite in the same file.
 *
 * The `--host=<addr>` rows are the second round of the same defect, and they are
 * why this table is judged against Publisher's parser rather than against what a
 * command line looks like it means. Publisher matches `--host` as a whole
 * argument and reads the address out of the next one
 * (packages/server/src/server.ts: `arg === "--host" && args[i + 1]`), so the
 * joined form matches nothing it knows, is dropped without a word, and the bind
 * falls back to 0.0.0.0. Reading it as loopback was exactly backwards: it
 * suppressed the exposure warning on the one shape that looks safest and serves
 * the whole network. tests/e2e/scaffold.e2e.spec.ts boots a real server with the
 * joined form and asks the network what it bound, which is the only thing that
 * keeps this table honest if that parser ever changes.
 */
describe("scaffold: which existing start script counts as ours", () => {
   const cases: { host: string; adopted: boolean }[] = [
      { host: "--host 127.0.0.1", adopted: true },
      { host: "--host ::1", adopted: true },
      { host: "--host [::1]", adopted: true },
      { host: "--host 127.5.5.5", adopted: true },
      { host: '--host "127.0.0.1"', adopted: true },
      // The last flag wins, as it does on a real command line.
      { host: "--host 0.0.0.0 --host 127.0.0.1", adopted: true },
      { host: "--host 127.0.0.1 --host 0.0.0.0", adopted: false },
      { host: "--host 0.0.0.0", adopted: false },
      { host: "--host 192.168.1.9", adopted: false },
      // A different flag that merely starts with the same letters.
      { host: "--hostname 127.0.0.1", adopted: false },
      // The joined form. Publisher drops it and binds 0.0.0.0, so this is the
      // same exposure as carrying no --host at all.
      { host: "--host=127.0.0.1", adopted: false },
      { host: "--host=localhost", adopted: false },
      { host: "--host=0.0.0.0", adopted: false },
      // And a joined form cannot rescue a real one, in either order.
      { host: "--host=127.0.0.1 --host 0.0.0.0", adopted: false },
      { host: "--host 0.0.0.0 --host=127.0.0.1", adopted: false },
      { host: "", adopted: false },
   ];

   for (const { host, adopted } of cases) {
      const label = host === "" ? "no --host at all" : host;
      test(`${label} is ${adopted ? "adopted" : "not adopted"}`, () => {
         const script =
            `npx -y @malloy-publisher/server --server_root . ` +
            `--config ./publisher.config.json ${host} --watch-env default`;
         writePackageJson({ start: script });
         const result = run();

         expect(result.hasStartScript).toBe(adopted);
         // Adopted or not, the script is the user's and stays as it is.
         expect(workspaceScripts().start).toBe(script);
         if (!adopted) {
            expect(agentsFile()).not.toContain("npm start");
         }
         // The invariant either way: nothing this run wrote tells anyone to
         // boot a server that is not private.
         expectEveryBootIsPrivatePublisher();
      });
   }

   /**
    * The other half of the same question, on the other side of the server spec.
    *
    * Adoption used to accept any token in front of the package that began with a
    * dash, on the reasoning that it was "a runner or one of its flags". Every
    * runner here takes flags that choose what actually runs, and joined into one
    * token each of those begins with a dash: `--package=`/`-p=` pick a different
    * package, `--call=` replaces the command, and `--node-options=--require=`
    * preloads a file into the real server. The separated forms were already
    * refused, because the value lands as a bare token; joining them was the
    * whole bypass, and it carries no space, so nothing splits it and `=` is not
    * a shell control character.
    *
    * The last row is the one that matters most: it runs the genuine Publisher
    * afterwards, so the ports, the bind and the watch are all exactly what this
    * tool goes on to claim. Nothing downstream looks wrong.
    */
   const runnerPrefixCases: { label: string; prefix: string; ok: boolean }[] = [
      { label: "-y", prefix: "-y", ok: true },
      { label: "--yes", prefix: "--yes", ok: true },
      { label: "no runner flag at all", prefix: "", ok: true },
      // A value as its own token is a bare word: neither flag nor runner.
      { label: "--package evil", prefix: "--package evil", ok: false },
      // ...and joined, so the whole thing is one token starting with "-".
      { label: "--package=evil", prefix: "--package=evil", ok: false },
      { label: "-p=evil", prefix: "-p=evil", ok: false },
      { label: "--call=evil", prefix: "--call=evil", ok: false },
      {
         label: "--node-options=--require=/tmp/evil.js",
         prefix: "--node-options=--require=/tmp/evil.js",
         ok: false,
      },
      // An unknown boolean flag declines too: this is an allowlist, and a flag
      // nothing here has read is a flag that might redirect what runs.
      { label: "--unknown-flag", prefix: "--unknown-flag", ok: false },
   ];

   for (const { label, prefix, ok } of runnerPrefixCases) {
      test(`runner prefix ${label} is ${ok ? "adopted" : "not adopted"}`, () => {
         const script =
            `npx ${prefix} @malloy-publisher/server --server_root . ` +
            `--config ./publisher.config.json --host 127.0.0.1 ` +
            `--watch-env default`.replace(/\s+/g, " ");
         writePackageJson({ start: script });
         const result = run();

         expect(result.hasStartScript).toBe(ok);
         // Adopted or not, the script is the user's and stays as it is.
         expect(workspaceScripts().start).toBe(script);
         if (!ok) {
            // The briefing must not hand an agent a command this tool could not
            // read. That is the whole harm: not new execution, but this tool
            // vouching for a script it never parsed.
            expect(agentsFile()).not.toContain("npm start");
         }
         expectEveryBootIsPrivatePublisher();
      });
   }

   /**
    * The declined script has to reach the caller with a reason, because the
    * caller is what prints the warning. A `--host=` script that is merely not
    * adopted, with nothing recorded, is silently identical to a script that has
    * nothing to do with Publisher, and that is the case the CLI prints nothing
    * for. index.spec.ts checks the printed half.
    */
   test("a joined --host is reported as a declined Publisher script", () => {
      writePackageJson({
         start:
            `npx -y @malloy-publisher/server --server_root . ` +
            `--config ./publisher.config.json --host=127.0.0.1 ` +
            `--watch-env default`,
      });
      const result = run();
      expect(result.hasStartScript).toBe(false);
      expect(result.declinedStartScript).toBeDefined();
      // Not "nothing to do with Publisher": it is Publisher, on 0.0.0.0.
      expect(result.declinedStartScript?.reason).not.toBe("not-publisher");
   });

   /**
    * The workspace's own briefing must not assert a bind address for a script
    * this run did not write. The claim is only checkable where it is made, so
    * this pins the sentence pattern rather than any one wording: whatever
    * AGENTS.md says about this workspace's `--host`, it cannot promise 127.0.0.1
    * over a package.json whose start script Publisher would bind 0.0.0.0 for.
    */
   test("AGENTS.md claims no loopback bind over a foreign start script", () => {
      writePackageJson({
         start:
            `npx -y @malloy-publisher/server --server_root . ` +
            `--config ./publisher.config.json --host=127.0.0.1 ` +
            `--watch-env default`,
      });
      const result = run();
      expect(result.hasStartScript).toBe(false);

      const agents = agentsFile();
      // "this workspace is scaffolded with --host 127.0.0.1", in any wording:
      // a sentence that says this workspace, plus a loopback promise, with no
      // command in between. The generic explanation of what the flag means is
      // allowed; a claim about what this directory's scripts do is not.
      for (const sentence of agents.split(/(?<=[.!?])\s+/)) {
         const claimsThisWorkspace = /this workspace|this directory/i.test(
            sentence,
         );
         const promisesLoopback =
            /scaffolded with|is scaffolded|binds? loopback|reachable from this machine only/i.test(
               sentence,
            ) && /127\.0\.0\.1|localhost/.test(sentence);
         if (claimsThisWorkspace && promisesLoopback) {
            throw new Error(
               `AGENTS.md tells this workspace it is bound to loopback: ` +
                  `"${sentence.trim()}". Its package.json start script is a ` +
                  `Publisher boot carrying --host=127.0.0.1, which Publisher ` +
                  `drops, so npm start here serves 0.0.0.0.`,
            );
         }
      }
      expectEveryBootIsPrivatePublisher();
   });
});

/**
 * --force, file by file, against the paragraph the tool prints to describe it
 * (forceDescription in scaffold.ts). Every clause of that paragraph is a claim
 * about the filesystem, and the three files it calls merges each got their merge
 * because replacing them lost something the user could not get back.
 */
describe("scaffold: what --force does to each workspace file", () => {
   function seedWorkspace(): void {
      run({ name: "sales" });
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "# my notes\n");
      fs.writeFileSync(path.join(tmp, "CLAUDE.md"), "# my notes\n");
      fs.writeFileSync(path.join(tmp, ".gitignore"), ".env\n*.pem\n");
      fs.writeFileSync(
         path.join(tmp, ".mcp.json"),
         JSON.stringify({
            mcpServers: { linear: { type: "http", url: "https://linear" } },
         }) + "\n",
      );
      const pkg = readJson("package.json") as Record<string, unknown>;
      pkg.dependencies = { express: "^4.19.0" };
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify(pkg, null, 2) + "\n",
      );
   }

   test("replaces the two agent files and merges the other three", () => {
      seedWorkspace();
      const result = run({ name: "marketing", force: true });

      // Replaced outright, which is why the description says to move anything
      // of yours out of them first.
      expect(
         fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8"),
      ).not.toContain("my notes");
      expect(
         fs.readFileSync(path.join(tmp, "CLAUDE.md"), "utf8"),
      ).not.toContain("my notes");
      expect(result.replaced).toContain("AGENTS.md");
      expect(result.replaced).toContain("CLAUDE.md");

      // Merged: the user's half of each file is still there afterwards.
      const pkg = readJson("package.json") as {
         dependencies: Record<string, string>;
         scripts: Record<string, string>;
      };
      expect(pkg.dependencies.express).toBe("^4.19.0");
      expect(pkg.scripts.start).toContain("@malloy-publisher/server");

      const mcp = readJson(".mcp.json") as {
         mcpServers: Record<string, { url: string }>;
      };
      expect(mcp.mcpServers.linear.url).toBe("https://linear");
      expect(mcp.mcpServers.malloy).toBeDefined();

      const ignored = fs.readFileSync(path.join(tmp, ".gitignore"), "utf8");
      expect(ignored).toContain(".env");
      expect(ignored).toContain("*.pem");
      expect(ignored).toContain("publisher.db*");
      // Appended to, never replaced, so it cannot be reported as overwritten.
      expect(result.replaced).not.toContain(".gitignore");

      // Extended, never rewritten: the package that was already registered is
      // still registered.
      const config = readJson("publisher.config.json") as {
         environments: { packages: { name: string }[] }[];
      };
      expect(config.environments[0].packages.map((p) => p.name)).toEqual([
         "sales",
         "marketing",
      ]);
   });
});

/**
 * The count in the CLI's green line, in AGENTS.md's heading, and on disk have to
 * be the same number, in the states where they can differ. A symlink anywhere
 * under .claude/skills is an expected state (this repo's own contributors wire
 * skills that way), and copying through one both edits a file outside the
 * workspace and inflates the count that gets printed.
 */
describe("scaffold: the skill count is the one on disk", () => {
   /** Skills the run really wrote: a real directory holding a SKILL.md. */
   function realSkillsOnDisk(): number {
      const dir = path.join(tmp, ".claude", "skills");
      if (!fs.existsSync(dir)) {
         return 0;
      }
      return fs
         .readdirSync(dir, { withFileTypes: true })
         .filter(
            (entry) =>
               entry.isDirectory() &&
               !fs.lstatSync(path.join(dir, entry.name)).isSymbolicLink() &&
               fs.existsSync(path.join(dir, entry.name, "SKILL.md")),
         ).length;
   }

   function expectCountsAgree(installed: number): void {
      expect(realSkillsOnDisk()).toBe(installed);
      expect(agentsFile()).toContain(`## Skills (${installed} installed)`);
   }

   test("a clean run", () => {
      expectCountsAgree(run().skillsInstalled);
      expect(realSkillsOnDisk()).toBe(SKILL_COUNT);
   });

   test("a symlinked skill directory is left alone and left out of the count", () => {
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-outside-"));
      fs.writeFileSync(path.join(outside, "SKILL.md"), "# mine\n");
      const link = path.join(tmp, ".claude", "skills", "malloy-analysis");
      fs.mkdirSync(path.dirname(link), { recursive: true });
      fs.symlinkSync(outside, link);
      try {
         const result = run();
         expect(result.skillsInstalled).toBe(SKILL_COUNT - 1);
         expectCountsAgree(result.skillsInstalled);
         expect(fs.readFileSync(path.join(outside, "SKILL.md"), "utf8")).toBe(
            "# mine\n",
         );
      } finally {
         fs.rmSync(outside, { recursive: true, force: true });
      }
   });

   test("a symlink inside a skill writes into the workspace, not through it", () => {
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-outside-"));
      const stolenFile = path.join(outside, "target.md");
      const stolenDir = path.join(outside, "refdir");
      fs.writeFileSync(stolenFile, "MINE\n");
      fs.mkdirSync(stolenDir);
      fs.writeFileSync(path.join(stolenDir, "keep.txt"), "MINE\n");
      // malloy-review ships both a SKILL.md and a reference/ directory, so both
      // links sit on a path the copy really writes.
      const skill = path.join(tmp, ".claude", "skills", "malloy-review");
      fs.mkdirSync(skill, { recursive: true });
      fs.symlinkSync(stolenFile, path.join(skill, "SKILL.md"));
      fs.symlinkSync(stolenDir, path.join(skill, "reference"));
      try {
         const result = run();

         // Nothing outside the workspace changed.
         expect(fs.readFileSync(stolenFile, "utf8")).toBe("MINE\n");
         expect(fs.readdirSync(stolenDir)).toEqual(["keep.txt"]);
         // The links were replaced by the real files, so the count is honest.
         expect(
            fs.lstatSync(path.join(skill, "SKILL.md")).isSymbolicLink(),
         ).toBe(false);
         expect(
            fs.lstatSync(path.join(skill, "reference")).isSymbolicLink(),
         ).toBe(false);
         expect(result.skillsInstalled).toBe(SKILL_COUNT);
         expectCountsAgree(result.skillsInstalled);
      } finally {
         fs.rmSync(outside, { recursive: true, force: true });
      }
   });
});

/**
 * The environment name is the one piece of this workspace that can come from a
 * file this tool did not write. It is read out of an existing
 * publisher.config.json and interpolated into `--watch-env` in package.json's
 * start and reset scripts, into the bash fences in AGENTS.md, and into the REST
 * URLs the CLI prints. Publisher configs ship inside repositories (malloy-samples
 * ships one, and its first environment is called "examples"), so adopting a name
 * somebody else chose is the ordinary path through this code, not an edge case.
 *
 * These specs execute the interpolation rather than comparing strings: the
 * generated script is run through a real shell with a stub `npx` first on PATH,
 * and what that shell actually did is the assertion.
 */
describe("scaffold: an environment name out of a foreign config", () => {
   // /bin/sh is the point of the execution specs, so they run where there is one
   // rather than being asserted about on Windows in some weaker form.
   const shellTest = process.platform === "win32" ? test.skip : test;

   function writeConfigWithEnvironment(name: unknown): void {
      fs.writeFileSync(
         path.join(tmp, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [{ name, packages: [], connections: [] }],
         }) + "\n",
      );
   }

   interface ShellRun {
      /** One entry per stub `npx` invocation, holding the argv it was handed. */
      invocations: string[][];
      /** Paths that appeared in the workspace while the command ran. */
      extraFiles: string[];
      status: number | null;
   }

   /**
    * Run a command line through /bin/sh with a stub `npx` ahead of the real one,
    * and report every process that started and every file that appeared. A second
    * invocation, or any new file, means the command line held more than the one
    * command it looks like it holds.
    */
   function runThroughShell(command: string): ShellRun {
      // The stub and its log live outside the workspace, so the file diff below
      // sees only what the command itself created.
      const rig = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-shell-"));
      const record = path.join(rig, "argv");
      const stub = path.join(rig, "npx");
      fs.writeFileSync(
         stub,
         `#!/bin/sh\n` +
            `for arg in "$@"; do printf '%s\\n' "$arg" >> "${record}"; done\n` +
            `printf -- '--END--\\n' >> "${record}"\n`,
      );
      fs.chmodSync(stub, 0o755);

      const before = new Set(fs.readdirSync(tmp));
      const proc = spawnSync("/bin/sh", ["-c", command], {
         cwd: tmp,
         env: {
            ...process.env,
            PATH: `${rig}${path.delimiter}${process.env.PATH ?? ""}`,
         },
         encoding: "utf8",
      });
      const extraFiles = fs
         .readdirSync(tmp)
         .filter((entry) => !before.has(entry));
      const invocations = (
         fs.existsSync(record) ? fs.readFileSync(record, "utf8") : ""
      )
         .split("--END--\n")
         .filter((chunk) => chunk.trim() !== "")
         .map((chunk) => chunk.split("\n").filter((line) => line !== ""));
      fs.rmSync(rig, { recursive: true, force: true });
      return { invocations, extraFiles, status: proc.status };
   }

   /**
    * The control. Without it the specs below would pass over a tool that had
    * simply stopped writing a start script at all, and there would be nothing to
    * show that the payload is live rather than ugly.
    */
   shellTest(
      "a name carrying `;` runs a second command when a shell sees it",
      () => {
         const run = runThroughShell(
            "npx -y @malloy-publisher/server --watch-env prod; touch PWNED #",
         );
         expect(run.status).toBe(0);
         expect(run.invocations).toHaveLength(1);
         // `npm start` over a script like this one is arbitrary code execution as
         // the user, at the command the tool tells them to run next.
         expect(run.extraFiles).toContain("PWNED");
      },
   );

   const refused: { label: string; envName: unknown }[] = [
      { label: "a shell metacharacter", envName: "prod; touch PWNED #" },
      { label: "a command substitution", envName: "prod$(touch PWNED)" },
      { label: "a pipe into a shell", envName: "prod | sh" },
      // Not malicious and just as broken: the server watches "my", finds no such
      // environment, and serves nothing, with no error anywhere.
      { label: "a space", envName: "my env" },
      // Interpolates as "[object Object]", which is nobody's environment.
      { label: "a non-string name", envName: { name: "prod" } },
      { label: "a path traversal", envName: "../escape" },
   ];

   for (const { label, envName } of refused) {
      test(`${label} is refused before anything is written`, () => {
         writeConfigWithEnvironment(envName);
         expect(() => run({ name: "sales" })).toThrow(/environment/i);
         // The check belongs where the name is resolved, which is before the
         // package directory, the manifest, the skills and the agent files.
         expect(exists("sales")).toBe(false);
         expect(exists("package.json")).toBe(false);
         expect(exists("AGENTS.md")).toBe(false);
         expect(exists(".mcp.json")).toBe(false);
         expect(exists(".claude")).toBe(false);
         // And the config it read is still the config it read.
         const config = readJson("publisher.config.json") as {
            environments: { packages: unknown[] }[];
         };
         expect(config.environments[0].packages).toEqual([]);
      });
   }

   test("a name Publisher itself accepts is accepted here", () => {
      // The same character rule as the server's path_safety.ts, so nothing this
      // accepts can be refused by the server it is generated for.
      writeConfigWithEnvironment("prod.v2_x-1");
      const result = run({ name: "sales" });
      expect(result.envName).toBe("prod.v2_x-1");
      expect(result.startCommand).toContain("--watch-env prod.v2_x-1");
   });

   shellTest(
      "the scripts it writes run one command, with the name as one argument",
      () => {
         writeConfigWithEnvironment("prod.v2_x-1");
         run({ name: "sales" });

         for (const scriptName of ["start", "reset"] as const) {
            const script = workspaceScripts()[scriptName];
            const shellRun = runThroughShell(script);
            expect(shellRun.status).toBe(0);
            if (shellRun.invocations.length !== 1) {
               throw new Error(
                  `the "${scriptName}" script runs ${shellRun.invocations.length} ` +
                     `commands, not one: ${script}`,
               );
            }
            const argv = shellRun.invocations[0];
            const watched = argv[argv.indexOf("--watch-env") + 1];
            expect(watched).toBe("prod.v2_x-1");
            expect(shellRun.extraFiles).toEqual([]);
         }
      },
   );
});

/**
 * CLAUDE.md is loaded automatically by Claude Code, which is this tool's default
 * client, and it is a pointer: "See @AGENTS.md for how to start Publisher...".
 * A pointer is a claim about another file, and the one case where this tool knows
 * that file is not ours is the case where it writes the pointer anyway. Unlike
 * the terminal output, this one persists on disk.
 */
describe("scaffold: CLAUDE.md points at instructions that are really there", () => {
   /** True when this text hands the reader a boot that really boots Publisher. */
   function carriesTheBoot(text: string, startCommand: string): boolean {
      if (text.includes(startCommand)) {
         return true;
      }
      // `npm start` counts only where this workspace's own start script is a
      // Publisher boot on a loopback address; in someone else's project it runs
      // their app.
      const start = workspaceScripts().start;
      return (
         /\bnpm start\b/.test(text) &&
         typeof start === "string" &&
         start.includes("@malloy-publisher/server") &&
         isLoopback(lastHostFlag(start))
      );
   }

   function expectClaudeReachesTheBoot(startCommand: string): void {
      const claude = fs.readFileSync(path.join(tmp, "CLAUDE.md"), "utf8");
      if (carriesTheBoot(claude, startCommand)) {
         return;
      }
      const referenced = [...claude.matchAll(/@?([A-Za-z0-9._/-]+\.md)/g)].map(
         (match) => match[1],
      );
      for (const relative of referenced) {
         const target = path.join(tmp, relative);
         if (
            fs.existsSync(target) &&
            fs.lstatSync(target).isFile() &&
            carriesTheBoot(fs.readFileSync(target, "utf8"), startCommand)
         ) {
            return;
         }
      }
      throw new Error(
         `CLAUDE.md tells the agent how to start Publisher is in ` +
            `${referenced.join(", ") || "no file it names"}, and neither it nor ` +
            `anything it points at carries a command that starts Publisher. ` +
            `CLAUDE.md holds:\n${claude}`,
      );
   }

   test("a fresh workspace: the pointer resolves to the AGENTS.md this run wrote", () => {
      const result = run();
      expect(result.skipped).not.toContain("AGENTS.md");
      expectClaudeReachesTheBoot(result.startCommand);
   });

   test("a project with its own AGENTS.md: the pointer cannot send the agent there", () => {
      // Reproduced from the audit: a Node app with its own briefing, which this
      // tool correctly leaves alone and then writes a pointer into.
      fs.writeFileSync(
         path.join(tmp, "AGENTS.md"),
         "# my-existing-app\n\nRun `npm start` to boot the API on port 3000.\n",
      );
      fs.writeFileSync(
         path.join(tmp, "package.json"),
         JSON.stringify({
            name: "my-existing-app",
            scripts: { start: "node src/server.js" },
         }) + "\n",
      );
      const result = run({ name: "metrics" });
      expect(result.skipped).toContain("AGENTS.md");
      expect(exists("CLAUDE.md")).toBe(true);
      expectClaudeReachesTheBoot(result.startCommand);
   });

   test("--force regenerates AGENTS.md, so the pointer resolves again", () => {
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "# my notes\n");
      const result = run({ name: "metrics", force: true });
      expect(result.replaced).toContain("AGENTS.md");
      expectClaudeReachesTheBoot(result.startCommand);
   });
});

/**
 * AGENTS.md is the one generated file that resolves `npm start` against this
 * workspace's real package.json. Every other generated file is written once and
 * read forever, so a hardcoded `npm start` in any of them is an instruction that
 * runs whatever that script happens to be, which in an adopted project is the
 * user's own app.
 */
describe("scaffold: what the generated files tell the reader to run", () => {
   /** Every file this run wrote, minus AGENTS.md and the skills payload. */
   function generatedFiles(): string[] {
      const found: string[] = [];
      const stack = [tmp];
      while (stack.length > 0) {
         const current = stack.pop() as string;
         for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
            const full = path.join(current, entry.name);
            const relative = path.relative(tmp, full);
            // The skills are a separate package with its own suite, and
            // AGENTS.md is the file that is allowed to resolve npm scripts.
            if (relative === ".claude" || relative === "AGENTS.md") {
               continue;
            }
            if (entry.isDirectory()) {
               stack.push(full);
            } else if (entry.isFile()) {
               found.push(relative);
            }
         }
      }
      return found.sort();
   }

   test("no generated file names an npm script when npm start is not ours", () => {
      writePackageJson({ start: "vite" });
      const result = run({ name: "sales" });
      expect(result.hasStartScript).toBe(false);

      const offenders: string[] = [];
      for (const relative of generatedFiles()) {
         if (relative === "package.json") {
            // Theirs, untouched, and already asserted on elsewhere.
            continue;
         }
         const text = fs.readFileSync(path.join(tmp, relative), "utf8");
         for (const line of text.split("\n")) {
            if (/\bnpm (start|run)\b/.test(line)) {
               offenders.push(`${relative}: ${line.trim()}`);
            }
         }
      }
      // `npm start` here runs vite, so any file promising it is describing
      // something that does not happen.
      expect(offenders).toEqual([]);
   });
});

/**
 * An AGENTS.md already in the workspace is one of two very different files, and
 * only one of them is the user's. Deciding from "does this path exist" treats
 * the briefing this tool wrote 30 seconds earlier as a stranger's file: it is
 * left describing the previous run's package, the briefing for the new one is
 * written beside it under another name, and CLAUDE.md goes on pointing at the
 * stale one. Both flows below are documented in the README.
 */
describe("scaffold: an AGENTS.md this tool wrote is not the project's own", () => {
   test("a second package regenerates the briefing in place", () => {
      run({ name: "sales" });
      const first = agentsFile();
      const result = run({ name: "marketing" });
      const second = agentsFile();

      expect(second).not.toBe(first);
      expect(second).toContain("marketing");
      expect(result.agentsFile).toBe("AGENTS.md");
      expect(exists("AGENTS.malloy.md")).toBe(false);
      // It was there and it is now different, which is what this report calls
      // an overwrite everywhere else.
      expect(result.replaced.some((item) => item.startsWith("AGENTS.md"))).toBe(
         true,
      );
      expect(result.skipped).not.toContain("AGENTS.md");
   });

   test("a setup-only run then a package: the briefing gains a package section", () => {
      run({ name: undefined });
      expect(agentsFile()).not.toContain("## The package");
      const result = run({ name: "sales" });

      expect(agentsFile()).toContain("## The package");
      expect(agentsFile()).toContain("sales");
      expect(result.agentsFile).toBe("AGENTS.md");
      expect(exists("AGENTS.malloy.md")).toBe(false);
   });

   test("the regenerated briefing still names the package it replaced", () => {
      // Regenerating in place is what makes this reachable: the file is written
      // in full from this run's state, so a section written from this run alone
      // describes a one-package workspace over a config that serves two, and
      // the package named by the run before this one is described nowhere at
      // all, by any file in the directory.
      run({ name: "sales" });
      const result = run({ name: "marketing" });
      const config = readJson("publisher.config.json") as {
         environments: { packages: { name: string }[] }[];
      };
      expect(config.environments[0].packages.map((p) => p.name)).toEqual([
         "sales",
         "marketing",
      ]);

      const agents = agentsFile();
      expect(agents).toContain("marketing");
      if (!agents.includes("sales")) {
         throw new Error(
            `The workspace serves sales and marketing, and its only ` +
               `description of itself names marketing ` +
               `${agents.split("marketing").length - 1} times and sales not ` +
               `once. ${result.agentsFile} is what an agent entering this ` +
               `directory reads.`,
         );
      }
   });

   test("a setup-only run over a workspace that has packages names them", () => {
      run({ name: "sales" });
      run({ name: undefined });
      const agents = agentsFile();
      expect(agents).toContain("## The package");
      expect(agents).toContain("sales");
   });

   test("a hand-written AGENTS.md is still left alone", () => {
      const mine = "# my notes\n\nRun `npm start` for the API.\n";
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), mine);
      const result = run({ name: "metrics" });

      expect(agentsFile()).toBe(mine);
      expect(result.agentsFile).toBe("AGENTS.malloy.md");
      expect(result.skipped).toContain("AGENTS.md");
   });

   test("the briefing beside a hand-written AGENTS.md is refreshed every run", () => {
      fs.writeFileSync(path.join(tmp, "AGENTS.md"), "# my notes\n");
      run({ name: "sales" });
      const result = run({ name: "marketing" });

      const briefing = fs.readFileSync(
         path.join(tmp, "AGENTS.malloy.md"),
         "utf8",
      );
      expect(briefing).toContain("marketing");
      expect(
         result.replaced.some((item) => item.startsWith("AGENTS.malloy.md")),
      ).toBe(true);
   });
});

/**
 * needsReset answers "will a plain boot mount the package this run just
 * created", which is a question about a server having run in this directory.
 * publisher.config.json is this tool's own footprint: the setup-only run the
 * README documents as step one writes it, so reading it as evidence of a boot
 * makes step two print three paragraphs about a persisted package list that does
 * not exist, including the warning that the boot it offers drops materialized
 * tables and saved themes.
 */
describe("scaffold: needsReset is evidence of a boot, not of a config", () => {
   test("set up, then add a package, with no server ever run here", () => {
      run({ name: undefined });
      expect(exists("publisher.config.json")).toBe(true);
      const result = run({ name: "sales" });

      expect(exists("publisher_data")).toBe(false);
      expect(exists("publisher.db")).toBe(false);
      if (result.needsReset) {
         throw new Error(
            `Nothing in this directory has ever been read by a server: no ` +
               `publisher_data/, no publisher.db. The only thing here that ` +
               `predates this run is the publisher.config.json the run before ` +
               `it wrote, so the configExists disjunct in needsReset is ` +
               `answering "has this tool run here", not "has a server".`,
         );
      }
   });

   test("publisher_data/ from a real boot makes it true", () => {
      run({ name: "sales" });
      fs.mkdirSync(path.join(tmp, "publisher_data"), { recursive: true });
      expect(run({ name: "marketing" }).needsReset).toBe(true);
   });

   test("publisher.db is enough on its own", () => {
      run({ name: "sales" });
      fs.writeFileSync(path.join(tmp, "publisher.db"), "");
      expect(run({ name: "marketing" }).needsReset).toBe(true);
   });

   test("a run that creates no package never needs the reset", () => {
      fs.mkdirSync(path.join(tmp, "publisher_data"), { recursive: true });
      expect(run({ name: undefined }).needsReset).toBe(false);
   });
});

/**
 * Every generated workspace re-resolves the server on every boot, forever, and
 * there is no lockfile behind `npx -y`. Unpinned, that means the day a release
 * renames or drops a flag, workspaces generated months earlier change behaviour
 * on their next start while their own package.json, AGENTS.md and README still
 * describe the old one. The flag that matters here is --host: the server accepts
 * an unknown flag in silence and falls back to binding 0.0.0.0.
 */
describe("scaffold: the server version the generated boots resolve", () => {
   test("every generated command names a version, not the floating latest", () => {
      const result = run();
      const scripts = workspaceScripts();
      const commands: Record<string, string> = {
         startCommand: result.startCommand,
         resetCommand: result.resetCommand,
         "package.json start": scripts.start,
         "package.json reset": scripts.reset,
      };
      const floating = Object.entries(commands).filter(
         ([, command]) => !/@malloy-publisher\/server@[^\s]+/.test(command),
      );
      if (floating.length > 0) {
         throw new Error(
            `These boots resolve @malloy-publisher/server@latest on every ` +
               `start of every workspace this tool has ever generated:\n` +
               floating
                  .map(([label, command]) => `  ${label}: ${command}`)
                  .join("\n"),
         );
      }
   });

   test("AGENTS.md hands over the same command the result carries", () => {
      writePackageJson({ start: "vite", reset: "./wipe-db.sh" });
      const result = run();
      const agents = agentsFile();
      expect(agents).toContain(result.startCommand);
      expect(agents).toContain(result.resetCommand);
   });
});

/**
 * `.claude/skills/<name>` is deleted and rewritten on every run, which is the
 * containment fix and is correct: it is the only way a symlink planted anywhere
 * inside one is removed rather than written through. What the run reports about
 * it is the short part. "Local edits to them do not survive" reads as "changes
 * you made to a SKILL.md get reverted", and a reader who put a file of their own
 * in one of those directories has no reason to hear a warning in it.
 */
describe("scaffold: what the skills report says about the directory it rewrites", () => {
   function aSkillDirectory(): string {
      const root = path.join(tmp, ".claude", "skills");
      const names = fs
         .readdirSync(root, { withFileTypes: true })
         .filter((entry) => entry.isDirectory())
         .map((entry) => entry.name)
         .sort();
      expect(names.length).toBeGreaterThan(0);
      return names[0];
   }

   test("a file added inside a skill is removed, and the report says so", () => {
      run({ name: "sales" });
      const skill = aSkillDirectory();
      const mine = path.join(tmp, ".claude", "skills", skill, "MY-NOTES.md");
      const myData = path.join(
         tmp,
         ".claude",
         "skills",
         skill,
         "my-data",
         "x.csv",
      );
      fs.writeFileSync(mine, "# my notes\n");
      fs.mkdirSync(path.dirname(myData), { recursive: true });
      fs.writeFileSync(myData, "a,b\n1,2\n");

      // No --force: this is what an ordinary second run does.
      const result = run({ name: "marketing" });

      expect(fs.existsSync(mine)).toBe(false);
      expect(fs.existsSync(myData)).toBe(false);
      const reported = [...result.replaced, ...result.skipped].find((item) =>
         item.startsWith(path.join(".claude", "skills")),
      );
      if (reported === undefined || !/remov|delet/i.test(reported)) {
         throw new Error(
            `The run unlinked ${path.relative(tmp, mine)} and ` +
               `${path.relative(tmp, myData)}, neither of which this tool ` +
               `wrote. All it said about that was: ${
                  reported ?? "nothing at all"
               }`,
         );
      }
   });
});

/**
 * Blocker 2. Adoption used to be a substring test: any scripts entry containing
 * `@malloy-publisher/server` whose last `--host` looked loopback became `npm
 * start`, printed with an asserted description ("start Publisher (web UI + MCP)
 * with the package in watch mode") and written into AGENTS.md as the command an
 * autonomous agent runs. A substring test establishes neither the behaviour nor
 * the binding: everything after `&&`, `;`, `|`, `&`, a newline, a backtick or
 * `$(` rides along untouched, and a second listener started alongside the first
 * defeats the bind claim while there is still exactly one `--host` in the line.
 *
 * So each row here runs the script, through `sh -c` exactly as `npm run` does,
 * against a stub `npx` that records its arguments and exits 0. The sentinel file
 * proves the extra command really ran, on this machine, before anything is
 * claimed about it. Without that half the table would only be one string
 * compared against another, which is how the substring test passed review twice.
 */
describe("scaffold: a start script that runs more than Publisher", () => {
   /** POSIX shell only; Windows has no `sh`, and npm there runs cmd.exe. */
   const shellTest = process.platform === "win32" ? test.skip : test;

   const PUBLISHER_BOOT =
      `npx -y @malloy-publisher/server --server_root . ` +
      `--config ./publisher.config.json --host 127.0.0.1 --watch-env default`;

   /**
    * Run a scripts entry the way `npm run` would, with a stub `npx` first on
    * PATH so nothing is downloaded and no server is booted. Returns the argument
    * lines the stub was invoked with.
    */
   function runScript(script: string): string[] {
      const binDir = path.join(tmp, "stub-bin");
      const log = path.join(tmp, "stub-npx.log");
      fs.mkdirSync(binDir, { recursive: true });
      const stub = path.join(binDir, "npx");
      fs.writeFileSync(stub, `#!/bin/sh\nprintf '%s\\n' "$*" >> "${log}"\n`);
      fs.chmodSync(stub, 0o755);

      const proc = spawnSync("sh", ["-c", script], {
         cwd: tmp,
         encoding: "utf8",
         env: { ...process.env, PATH: `${binDir}:${process.env.PATH ?? ""}` },
         timeout: 20000,
      });
      if (proc.error) {
         throw proc.error;
      }
      // Polled, because `&` backgrounds the boot: `sh` returns before the stub
      // has written its line, and reading once turned a script that really did
      // run Publisher plus something else into "npx was never called".
      const deadline = Date.now() + 5000;
      let lines = readLog(log);
      while (lines.length === 0 && Date.now() < deadline) {
         sleep(25);
         lines = readLog(log);
      }
      return lines;
   }

   function readLog(log: string): string[] {
      return fs.existsSync(log)
         ? fs
              .readFileSync(log, "utf8")
              .split("\n")
              .filter((line) => line !== "")
         : [];
   }

   /** Synchronous, so the polling above stays inside a synchronous test. */
   function sleep(ms: number): void {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
   }

   const cases: { label: string; tail: string; sentinel: string }[] = [
      {
         label: "&& a second command",
         tail: "&& echo x > owned.txt",
         sentinel: "owned.txt",
      },
      {
         label: "; a second command",
         tail: "; echo x > owned.txt",
         sentinel: "owned.txt",
      },
      {
         label: "& a second listener in the background",
         tail: "& echo x > owned.txt",
         sentinel: "owned.txt",
      },
      { label: "a pipe", tail: "| cat > owned.txt", sentinel: "owned.txt" },
      {
         label: "command substitution",
         tail: "$(echo x > owned.txt)",
         sentinel: "owned.txt",
      },
      {
         label: "a backtick substitution",
         tail: "`echo x > owned.txt`",
         sentinel: "owned.txt",
      },
      {
         label: "a second line",
         tail: "\necho x > owned.txt",
         sentinel: "owned.txt",
      },
   ];

   for (const { label, tail, sentinel } of cases) {
      shellTest(`${label} is not adopted as npm start`, () => {
         const script = `${PUBLISHER_BOOT} ${tail}`;
         writePackageJson({ start: script });
         const result = run();

         // Executed first, so nothing below is claimed about a script that
         // might not do what this test says it does.
         const npxCalls = runScript(script);
         expect(npxCalls.length).toBe(1);
         expect(npxCalls[0]).toContain("@malloy-publisher/server");
         if (!fs.existsSync(path.join(tmp, sentinel))) {
            throw new Error(
               `The fixture script did not run its second command, so this ` +
                  `row proves nothing. Script: ${script}`,
            );
         }

         expect(result.hasStartScript).toBe(false);
         expect(result.declinedStartScript).toBeDefined();
         expect(agentsFile()).not.toContain("npm start");
         expect(agentsFile()).toContain(result.startCommand);
         expectEveryBootIsPrivatePublisher();
      });
   }

   /**
    * The bind claim, defeated with one `--host` in the line and no shell
    * metacharacter in a suspicious-looking place: the Publisher boot is
    * backgrounded and a second process holds the interesting port. Adoption on
    * "the last --host is loopback" says this workspace is private; the second
    * listener says otherwise.
    */
   shellTest(
      "a backgrounded boot beside a second listener is not adopted",
      () => {
         const script = `${PUBLISHER_BOOT} & echo listening > second-listener.txt`;
         writePackageJson({ start: script });
         const result = run();

         const npxCalls = runScript(script);
         expect(npxCalls.length).toBe(1);
         expect(fs.existsSync(path.join(tmp, "second-listener.txt"))).toBe(
            true,
         );

         expect(result.hasStartScript).toBe(false);
         expectEveryBootIsPrivatePublisher();
      },
   );

   /*
    * A plain Publisher boot carrying no --watch-env used to be checked here,
    * line by line, for a line holding both "npm start" and "watch mode". No such
    * line exists in any state: AGENTS.md names the command inside a fenced block
    * and makes the watch claim in the prose under it, so the filter matched
    * nothing and the test went green over the one claim it was written to catch.
    * That case is now a row in "what the run asserts about the boot it names"
    * below, which reads the claim out of the paragraph it is really made in.
    */
});

/**
 * Everything this run asserts about the boot it names, over a start script that
 * is a Publisher boot but not the boot this tool writes.
 *
 * The ports, the workspace root and the watch mode are three separate claims,
 * and all three were constants: PUBLISHER_PORT/MCP_PORT copied onto the result
 * and interpolated into AGENTS.md, .mcp.json and every printed curl, while the
 * adoption check read only the server name, the shell metacharacters, the
 * runner tokens and --host. A script carrying `--port 4100 --mcp_port 4140`
 * (which is exactly what AGENTS.md tells the reader to add, and to keep adding
 * on every later boot) was adopted as `npm start` and then described as serving
 * 4000 and 4040. Every check the tool prints then answers connection-refused, or
 * worse, answers from whichever other Publisher on this machine holds the
 * default ports: an agent told to poll 4000 and getting ECONNREFUSED starts a
 * second server, which is the publisher.db lock trap AGENTS.md warns about two
 * sections further down.
 *
 * Nothing below cares which fix landed. Declining these scripts and falling back
 * to the explicit command satisfies every assertion here, and so does reading
 * the flags off the script and following them. Asserting a port that the boot
 * this file names does not bind is the only thing that fails.
 */
describe("scaffold: what the run asserts about the boot it names", () => {
   /**
    * Publisher's own defaults, from packages/server/src/server.ts:
    * `Number(process.env.PUBLISHER_PORT || 4000)` and the MCP one beside it. A
    * boot with no --port binds these, so "no flag" is not "no claim".
    */
   const DEFAULT_PUBLISHER_PORT = 4000;
   const DEFAULT_MCP_PORT = 4040;

   /**
    * The value of the last whole-token `--<flag> <value>`, quotes stripped.
    *
    * Written from the server's argv walk rather than from what the line looks
    * like it means: it matches `arg === "--port" && args[i + 1]`, so the flag is
    * a whole argument and its value is the next one. A joined `--port=4100`
    * matches no branch, is dropped in silence, and the bind falls back to the
    * default, which is why the `=` form is deliberately not read here.
    */
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

   /** The two ports a command line really binds, defaults included. */
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

   /**
    * The boot AGENTS.md hands the reader under "Start the server first", as the
    * process it would really start: `npm start` resolved through this
    * workspace's own package.json, because that indirection is where the whole
    * defect lives.
    */
   function startBoot(): { command: string; resolved: string } {
      const commands = bootCommandsIn(agentsFile());
      const command = commands[0];
      if (command === undefined) {
         throw new Error(
            "AGENTS.md holds no boot command at all, so this test is asserting " +
               "nothing. The fenced-block extractor above has stopped matching.",
         );
      }
      const { script, resolved } = resolveCommand(command, workspaceScripts());
      if (resolved === undefined) {
         throw new Error(
            `AGENTS.md says to run "${command}", but this workspace's ` +
               `package.json defines no "${script}" script`,
         );
      }
      return { command, resolved };
   }

   /**
    * The distinct ports a text names in one shape of URL: one number when the
    * text is consistent, the whole list when it is not, so a disagreement inside
    * one file shows up in the diff rather than as "expected 4100, got 4000".
    */
   function portsMatching(text: string, pattern: RegExp): number | number[] {
      const found = [
         ...new Set([...text.matchAll(pattern)].map((m) => Number(m[1]))),
      ];
      return found.length === 1 ? found[0] : found;
   }

   /**
    * Every port this run wrote down, against the ports the boot it names binds.
    * The result fields are in here too: the CLI renders its curls off them, so a
    * result that disagrees with AGENTS.md is a printed check pointed at nothing.
    */
   function expectEveryPortClaimMatchesTheBoot(result: ScaffoldResult): void {
      const { command, resolved } = startBoot();
      const ports = portsOf(resolved);
      const agents = agentsFile();
      const mcp = readJson(".mcp.json") as {
         mcpServers: { malloy: { url: string } };
      };
      const observed = {
         "result.publisherPort": result.publisherPort,
         "result.mcpPort": result.mcpPort,
         "the status curl in AGENTS.md": portsMatching(
            agents,
            /http:\/\/localhost:(\d+)\/api\/v0\/status/g,
         ),
         "the REST URLs in AGENTS.md": portsMatching(
            agents,
            /http:\/\/localhost:(\d+)\/api\/v0\/environments/g,
         ),
         "the MCP endpoint in AGENTS.md": portsMatching(
            agents,
            /http:\/\/localhost:(\d+)\/mcp/g,
         ),
         "the url in .mcp.json": Number(
            new URL(mcp.mcpServers.malloy.url).port,
         ),
      };
      expect({
         boot: `${command} -> ${resolved}`,
         ...observed,
      }).toEqual({
         boot: `${command} -> ${resolved}`,
         "result.publisherPort": ports.publisher,
         "result.mcpPort": ports.mcp,
         "the status curl in AGENTS.md": ports.publisher,
         "the REST URLs in AGENTS.md": ports.publisher,
         "the MCP endpoint in AGENTS.md": ports.mcp,
         "the url in .mcp.json": ports.mcp,
      });
   }

   /**
    * The briefing describes the workspace it sits in, so the boot it names has
    * to be pointed at that workspace. A `--server_root` or `--config` outside it
    * serves someone else's packages and someone else's data under a file that
    * says "this workspace's packages", and the status response an agent is told
    * to trust looks identical.
    */
   function expectTheBootServesThisWorkspace(): void {
      const { command, resolved } = startBoot();
      // realpath, because macOS hands out /var/... and resolves to /private/var.
      const root = fs.realpathSync(tmp);
      for (const flag of ["--server_root", "--config"]) {
         const value = lastFlagValue(resolved, flag);
         if (value === undefined) {
            // No flag is the server's own default, which is this directory:
            // npm runs a script with the manifest's directory as cwd.
            continue;
         }
         const absolute = path.resolve(root, value);
         const inside =
            absolute === root || absolute.startsWith(root + path.sep);
         if (!inside) {
            throw new Error(
               `AGENTS.md describes this workspace and tells the reader to run ` +
                  `"${command}", which in this workspace runs "${resolved}" and ` +
                  `points ${flag} at ${absolute}, outside ${root}`,
            );
         }
      }
   }

   /**
    * Watch mode is a claim about a flag, not about Publisher. Without
    * `--watch-env` the model is not recompiled on edit at all, so "the last
    * version that compiled keeps serving" and "the failure goes only to the
    * server's stdout" describe a mode this workspace does not run, and an agent
    * following them skips the reload it actually needs.
    */
   function expectNoUnbackedWatchClaim(): void {
      const { command, resolved } = startBoot();
      if (/(?:^|\s)--watch-env\s+\S/.test(resolved)) {
         return;
      }
      const claims = agentsFile()
         .split(/\n{2,}/)
         .map((paragraph) => paragraph.trim())
         .filter((paragraph) => /watch[ -]mode/i.test(paragraph));
      expect({
         boot: `${command} -> ${resolved}`,
         watchClaims: claims,
      }).toEqual({ boot: `${command} -> ${resolved}`, watchClaims: [] });
   }

   const elsewhere = path.join(os.tmpdir(), "cmp-not-this-workspace");

   const cases: { label: string; start: string }[] = [
      {
         // The tool's own documented second-package flow: AGENTS.md hits a port
         // collision, offers the two flags, and says every later boot needs them
         // spelled out again, so the reader puts them in the start script.
         label: "the port pair the briefing itself hands the reader",
         start: `${OUR_START} --port 4100 --mcp_port 4140`,
      },
      {
         label: "a port pair nothing in this tool would guess",
         start: `${OUR_START} --port 5311 --mcp_port 5312`,
      },
      {
         label: "only the REST port moved",
         start: `${OUR_START} --port 4100`,
      },
      {
         label: "only the MCP port moved",
         start: `${OUR_START} --mcp_port 4140`,
      },
      {
         label: "a boot pointed at another workspace entirely",
         start:
            `npx -y @malloy-publisher/server@0.0.228 --server_root ${elsewhere} ` +
            `--config ${elsewhere}/publisher.config.json --host 127.0.0.1 ` +
            `--port 5555 --mcp_port 5556 --watch-env other`,
      },
      {
         label: "a boot that mounts nothing in watch mode",
         start: `npx @malloy-publisher/server --host localhost`,
      },
   ];

   for (const { label, start } of cases) {
      test(`${label}: nothing written claims more than that boot does`, () => {
         writePackageJson({ start });
         const result = run();

         // The script is the user's either way, adopted or not.
         expect(workspaceScripts().start).toBe(start);
         expectEveryBootIsPrivatePublisher();
         expectEveryPortClaimMatchesTheBoot(result);
         expectTheBootServesThisWorkspace();
         expectNoUnbackedWatchClaim();
      });
   }

   /**
    * The controls. Without them a helper above that threw on everything, or
    * matched nothing and compared two empty lists, would read as six passes.
    */
   test("a plain run into an empty directory satisfies all of it", () => {
      const result = run();
      // Nothing was there, so this run wrote the manifest and the start script
      // in it is the one it wrote.
      expect(result.hasStartScript).toBe(true);
      expectEveryPortClaimMatchesTheBoot(result);
      expectTheBootServesThisWorkspace();
      expectNoUnbackedWatchClaim();
   });

   test("a run over the start script this tool writes satisfies all of it", () => {
      writePackageJson({ start: OUR_START });
      const result = run();
      expect(result.hasStartScript).toBe(true);
      expectEveryPortClaimMatchesTheBoot(result);
      expectTheBootServesThisWorkspace();
      expectNoUnbackedWatchClaim();
   });

   /*
    * The three checks above, executed against the state they exist to catch. A
    * suite whose every row passes says nothing about whether the rows can fail,
    * and the defect they cover survived two rounds behind exactly that: checks
    * that compared one generated string against another and agreed with
    * themselves. So each of these adopts a script, lets the run write its files,
    * and then moves the script under them: the workspace is left in the state
    * the bug produced, with AGENTS.md describing a boot that is no longer the
    * one `npm start` runs, and the check has to say so.
    */
   test("the port check fails over a boot that moved after the run", () => {
      writePackageJson({ start: OUR_START });
      const result = run();
      expect(result.hasStartScript).toBe(true);
      writePackageJson({ start: `${OUR_START} --port 4100 --mcp_port 4140` });
      expect(() => expectEveryPortClaimMatchesTheBoot(result)).toThrow();
   });

   test("the workspace check fails over a boot pointed elsewhere", () => {
      writePackageJson({ start: OUR_START });
      const result = run();
      expect(result.hasStartScript).toBe(true);
      writePackageJson({
         start: OUR_START.replace(
            "--server_root .",
            `--server_root ${elsewhere}`,
         ),
      });
      expect(() => expectTheBootServesThisWorkspace()).toThrow();
   });

   test("the watch check fails over a boot carrying no --watch-env", () => {
      writePackageJson({ start: OUR_START });
      const result = run();
      expect(result.hasStartScript).toBe(true);
      writePackageJson({
         start: OUR_START.replace(" --watch-env default", ""),
      });
      expect(() => expectNoUnbackedWatchClaim()).toThrow();
   });
});

/**
 * `npx @malloy-publisher/server@<spec>` is not a version, it is anything npx can
 * resolve: a local directory, a git URL, a tarball over the network. The
 * allowance exists so a pinned `@0.0.228` reads as ours, and it admits all of
 * them, each adopted as `npm start` and described as "boots Publisher on a
 * loopback address" over code that is not Publisher and binds what it likes.
 *
 * Bounded, like every other hostile-manifest case here: a package.json whose
 * author is hostile already has npm lifecycle hooks. What it is not is honest,
 * and there is no shape of it this tool should be describing.
 */
describe("scaffold: a start script whose server spec is not a version", () => {
   const specs = [
      "file:/tmp/evil",
      "file:../evil",
      "git+ssh://git@example.com/evil.git",
      "https://example.com/evil.tgz",
   ];

   for (const spec of specs) {
      test(`@${spec} is not adopted as npm start`, () => {
         const script =
            `npx -y @malloy-publisher/server@${spec} --server_root . ` +
            `--config ./publisher.config.json --host 127.0.0.1 ` +
            `--watch-env default`;
         writePackageJson({ start: script });
         const result = run();

         expect(result.hasStartScript).toBe(false);
         expect(result.declinedStartScript).toBeDefined();
         expect(agentsFile()).not.toContain("npm start");
         expect(agentsFile()).toContain(result.startCommand);
         expectEveryBootIsPrivatePublisher();
      });
   }

   /** The control: the one spec form the allowance was added for. */
   test("a pinned version is still a Publisher boot", () => {
      const script =
         `npx -y @malloy-publisher/server@0.0.228 --server_root . ` +
         `--config ./publisher.config.json --host 127.0.0.1 ` +
         `--watch-env default`;
      writePackageJson({ start: script });
      const result = run();
      expect(result.declinedStartScript?.reason).not.toBe("not-publisher");
      expectEveryBootIsPrivatePublisher();
   });
});
