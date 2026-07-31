import { afterEach, describe, expect, it } from "bun:test";
import { execFileSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { logger } from "./logger";
import {
   ensureMcpConfig,
   logMcpConfigOutcome,
   MCP_CONFIG_FILENAME,
   mcpConfigEnabled,
   type McpConfigOutcome,
} from "./mcp_config";

const dirs: string[] = [];

/** A scratch directory that is deliberately NOT inside a git work tree. */
function tmpDir(): string {
   const dir = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "mcp-config-")),
   );
   dirs.push(dir);
   return dir;
}

const cfg = (dir: string) => path.join(dir, MCP_CONFIG_FILENAME);
const run = (dir: string, mcpPort = 4040, enabled = true) =>
   ensureMcpConfig({ dir, mcpPort, enabled });

afterEach(() => {
   while (dirs.length) {
      fs.rmSync(dirs.pop() as string, { recursive: true, force: true });
   }
});

describe("ensureMcpConfig", () => {
   it("creates the file when the directory has none", () => {
      const dir = tmpDir();
      expect(run(dir).action).toBe("created");
      expect(JSON.parse(fs.readFileSync(cfg(dir), "utf8"))).toEqual({
         mcpServers: {
            malloy: { type: "http", url: "http://localhost:4040/mcp" },
         },
      });
   });

   it("writes the port it was handed, not the 4040 default", () => {
      // "No ports to know" only holds if the file names the port really bound.
      const dir = tmpDir();
      run(dir, 15040);
      expect(fs.readFileSync(cfg(dir), "utf8")).toContain(
         "http://localhost:15040/mcp",
      );
   });

   describe("an existing file is left byte-identical and unread", () => {
      // The whole reason this module creates rather than merges. An earlier
      // version replaced the malloy entry wholesale and destroyed user fields
      // (auth headers among them) on every boot.
      it("does not touch a file holding a malloy entry with extra fields", () => {
         const dir = tmpDir();
         const body = JSON.stringify(
            {
               mcpServers: {
                  malloy: {
                     type: "http",
                     url: "http://localhost:4040/mcp",
                     headers: { Authorization: "Bearer SECRET" },
                  },
               },
            },
            null,
            2,
         );
         fs.writeFileSync(cfg(dir), body);
         const outcome = run(dir, 15040);
         expect(outcome.action).toBe("exists");
         expect(fs.readFileSync(cfg(dir), "utf8")).toBe(body);
      });

      it("does not touch a stdio bridge entry, which our own docs tell people to write", () => {
         const dir = tmpDir();
         const body = JSON.stringify({
            mcpServers: {
               malloy: { command: "npx", args: ["-y", "mcp-remote"] },
            },
         });
         fs.writeFileSync(cfg(dir), body);
         expect(run(dir).action).toBe("exists");
         expect(fs.readFileSync(cfg(dir), "utf8")).toBe(body);
      });

      it("does not touch a file it could not parse, and does not read it", () => {
         const dir = tmpDir();
         const body = "{ this is not json";
         fs.writeFileSync(cfg(dir), body);
         expect(run(dir).action).toBe("exists");
         expect(fs.readFileSync(cfg(dir), "utf8")).toBe(body);
      });
   });

   describe("symlinks", () => {
      it("does NOT create the target of a dangling symlink", () => {
         // The hazard `existsSync`-then-write could not see: the check says
         // absent, and the write follows the link and creates a file elsewhere.
         const dir = tmpDir();
         const victimDir = tmpDir();
         const victim = path.join(victimDir, "victim.json");
         fs.symlinkSync(victim, cfg(dir));

         const outcome = run(dir);
         expect(outcome.action).toBe("exists");
         expect(fs.existsSync(victim)).toBe(false);
      });

      it("does not modify the target of a symlink to a real file", () => {
         const dir = tmpDir();
         const victimDir = tmpDir();
         const victim = path.join(victimDir, "package.json");
         const body = JSON.stringify({ name: "someone-elses-file" });
         fs.writeFileSync(victim, body);
         fs.symlinkSync(victim, cfg(dir));

         expect(run(dir).action).toBe("exists");
         expect(fs.readFileSync(victim, "utf8")).toBe(body);
      });
   });

   it("returns promptly on a FIFO instead of hanging the process", () => {
      // A blocking read here stalls the event loop and takes /health with it.
      // `wx` never opens it for reading, so this must be fast and inert.
      const dir = tmpDir();
      try {
         execFileSync("mkfifo", [cfg(dir)]);
      } catch {
         return; // no mkfifo on this platform; nothing to assert
      }
      const started = Date.now();
      expect(run(dir).action).toBe("exists");
      expect(Date.now() - started).toBeLessThan(2000);
      expect(fs.lstatSync(cfg(dir)).isFIFO()).toBe(true);
   });

   describe("directories it stays out of", () => {
      it("skips a git work tree", () => {
         // A checkout belongs to somebody: a clone of this repo already ships a
         // .mcp.json, and an untracked one is a surprise in git status.
         const dir = tmpDir();
         fs.mkdirSync(path.join(dir, ".git"));
         expect(run(dir).action).toBe("skipped-git");
         expect(fs.existsSync(cfg(dir))).toBe(false);
      });

      it("skips a SUBDIRECTORY of a git work tree", () => {
         // The case that made this necessary: `bun run start` cds into
         // packages/server, so the write landed inside the repo but nowhere an
         // agent is ever opened.
         const dir = tmpDir();
         fs.mkdirSync(path.join(dir, ".git"));
         const sub = path.join(dir, "packages", "server");
         fs.mkdirSync(sub, { recursive: true });
         expect(run(sub).action).toBe("skipped-git");
         expect(fs.existsSync(cfg(sub))).toBe(false);
      });

      it("treats a .git FILE as a work tree, the way worktrees and submodules store it", () => {
         const dir = tmpDir();
         fs.writeFileSync(path.join(dir, ".git"), "gitdir: /elsewhere\n");
         expect(run(dir).action).toBe("skipped-git");
      });

      it("skips the home directory", () => {
         const outcome = ensureMcpConfig({
            dir: os.homedir(),
            mcpPort: 4040,
            enabled: true,
         });
         expect(outcome.action).toBe("skipped-home");
      });

      it("skips home even when the path is not normalised", () => {
         // The guard resolves before comparing; a raw string compare would miss.
         const outcome = ensureMcpConfig({
            dir: path.join(os.homedir(), "..", path.basename(os.homedir())),
            mcpPort: 4040,
            enabled: true,
         });
         expect(outcome.action).toBe("skipped-home");
      });

      it("does nothing when disabled", () => {
         const dir = tmpDir();
         expect(run(dir, 4040, false).action).toBe("disabled");
         expect(fs.existsSync(cfg(dir))).toBe(false);
      });
   });

   it("reports rather than throws when the directory cannot be written", () => {
      if (typeof process.getuid === "function" && process.getuid() === 0) {
         return; // root ignores the mode bits, so there is nothing to provoke
      }
      const dir = tmpDir();
      fs.chmodSync(dir, 0o500);
      try {
         expect(run(dir).action).toBe("failed");
      } finally {
         fs.chmodSync(dir, 0o700);
      }
   });
});

describe("logMcpConfigOutcome", () => {
   // These exist only to produce log text, so without a test nothing covers
   // whether the text is right or even emitted.
   const outcomes: McpConfigOutcome[] = [
      { action: "disabled" },
      { action: "skipped-home", dir: "/home/x", mcpPort: 4040 },
      { action: "skipped-git", dir: "/repo", mcpPort: 4040 },
      { action: "created", file: "/w/.mcp.json" },
      { action: "exists", file: "/w/.mcp.json", mcpPort: 4040 },
      { action: "failed", file: "/w/.mcp.json", problem: "EACCES" },
   ];

   /** Collect everything the logger is told, at either level. */
   function capture(run: () => void): string {
      const said: string[] = [];
      const info = logger.info.bind(logger);
      const warn = logger.warn.bind(logger);
      (logger as unknown as { info: (m: string) => void }).info = (m) => {
         said.push(m);
      };
      (logger as unknown as { warn: (m: string) => void }).warn = (m) => {
         said.push(m);
      };
      try {
         run();
      } finally {
         (logger as unknown as { info: unknown }).info = info;
         (logger as unknown as { warn: unknown }).warn = warn;
      }
      return said.join("\n");
   }

   it("handles every outcome variant without throwing", () => {
      for (const outcome of outcomes) {
         expect(() => logMcpConfigOutcome(outcome)).not.toThrow();
      }
   });

   it("names the real port in the command it tells the reader to run", () => {
      // The one branch whose text the user has to act on. A hardcoded 4040 here
      // would send them to a port nothing is listening on.
      const said: string[] = [];
      const info = logger.info.bind(logger);
      (logger as unknown as { info: (m: string) => void }).info = (m) => {
         said.push(m);
      };
      try {
         logMcpConfigOutcome({
            action: "exists",
            file: "/w/.mcp.json",
            mcpPort: 15040,
         });
      } finally {
         (logger as unknown as { info: unknown }).info = info;
      }
      expect(said.join("\n")).toContain(
         "claude mcp add --transport http malloy http://localhost:15040/mcp",
      );
   });

   it("says it wrote the file, and how to turn that off", () => {
      const said: string[] = [];
      const info = logger.info.bind(logger);
      (logger as unknown as { info: (m: string) => void }).info = (m) => {
         said.push(m);
      };
      try {
         logMcpConfigOutcome({ action: "created", file: "/w/.mcp.json" });
      } finally {
         (logger as unknown as { info: unknown }).info = info;
      }
      expect(said.join("\n")).toContain("/w/.mcp.json");
      expect(said.join("\n")).toContain("--no-mcp-config");
   });

   it("stays silent only when the user asked for silence", () => {
      // `disabled` is the one branch where nothing needs saying: the user passed
      // the flag, so no file is exactly what they asked for.
      expect(capture(() => logMcpConfigOutcome({ action: "disabled" }))).toBe(
         "",
      );
   });

   // Both skips were silent when this shipped for review, and three separate
   // reviewers landed on the same failure: no file, no tools, no message, and a
   // README that then sends you to relaunch the agent from a directory that has
   // nothing in it. Running inside your own project is the common case, not an
   // edge one, so these two lines are load-bearing.
   it("tells a user inside a git repo why there is no file, and what to run", () => {
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-git",
            dir: "/work/my-project",
            mcpPort: 15040,
         }),
      );
      expect(said).toContain("/work/my-project");
      expect(said).toContain("git working tree");
      expect(said).toContain(
         "claude mcp add --transport http malloy http://localhost:15040/mcp -s user",
      );
   });

   it("tells a user in their home directory the same thing", () => {
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-home",
            dir: "/Users/x",
            mcpPort: 15040,
         }),
      );
      expect(said).toContain("/Users/x");
      expect(said).toContain(
         "claude mcp add --transport http malloy http://localhost:15040/mcp -s user",
      );
   });

   it("never sends the reader to a hardcoded port on any branch that names one", () => {
      // The whole point of reading back the bound port. A 4040 leaking into any
      // remediation line sends the reader somewhere nothing is listening.
      const branches: McpConfigOutcome[] = [
         { action: "exists", file: "/w/.mcp.json", mcpPort: 19999 },
         { action: "skipped-git", dir: "/repo", mcpPort: 19999 },
         { action: "skipped-home", dir: "/home/x", mcpPort: 19999 },
      ];
      for (const outcome of branches) {
         const said = capture(() => logMcpConfigOutcome(outcome));
         expect(said).toContain("19999");
         expect(said).not.toContain("4040");
      }
   });
});

describe("mcpConfigEnabled", () => {
   // `!process.env.X` made PUBLISHER_NO_MCP_CONFIG=false disable the thing it
   // names, because every non-empty string is truthy.
   it("is on when the variable is not set", () => {
      expect(mcpConfigEnabled({})).toBe(true);
   });

   it("treats the spellings that mean 'leave it on' as leaving it on", () => {
      for (const value of ["false", "FALSE", "0", "", "  false  "]) {
         expect(mcpConfigEnabled({ PUBLISHER_NO_MCP_CONFIG: value })).toBe(
            true,
         );
      }
   });

   it("turns off for any other value, so =1 works as well as =true", () => {
      for (const value of ["1", "true", "yes", "please"]) {
         expect(mcpConfigEnabled({ PUBLISHER_NO_MCP_CONFIG: value })).toBe(
            false,
         );
      }
   });
});
