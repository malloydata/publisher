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
   mcpEndpoint,
   type McpConfigOutcome,
   resolveBoundPort,
   resolveClientHost,
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
const run = (dir: string, mcpPort = 4040) =>
   ensureMcpConfig({
      dir,
      endpoint: ep(mcpPort),
      requestedPort: mcpPort,
      boundPort: mcpPort,
   });

/** The endpoint the server would compute for a loopback bind on `port`. */
const ep = (port: number) => `http://127.0.0.1:${port}/mcp`;

const isWindows = process.platform === "win32";

/** Is `mkfifo` available? Absent on Windows and on minimal images. */
const hasMkfifo = ((): boolean => {
   const probe = fs.mkdtempSync(path.join(os.tmpdir(), "mcp-fifo-probe-"));
   try {
      execFileSync("mkfifo", [path.join(probe, "f")]);
      return true;
   } catch {
      return false;
   } finally {
      fs.rmSync(probe, { recursive: true, force: true });
   }
})();

/** Root ignores the mode bits, so an unwritable directory cannot be staged. */
const isRoot = typeof process.getuid === "function" && process.getuid() === 0;

/**
 * Can this process create a symlink? Windows needs a privilege or Developer
 * Mode for it, so several hazards here are unstageable on CI. Probed rather
 * than assumed, and used with `skipIf` so an unstageable test reports as
 * skipped instead of passing without asserting anything.
 */
const canSymlink = ((): boolean => {
   const probe = fs.mkdtempSync(path.join(os.tmpdir(), "mcp-symlink-probe-"));
   try {
      fs.symlinkSync(path.join(probe, "target"), path.join(probe, "link"));
      return true;
   } catch {
      return false;
   } finally {
      fs.rmSync(probe, { recursive: true, force: true });
   }
})();

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
            malloy: { type: "http", url: ep(4040) },
         },
      });
   });

   it("writes the port it was handed, not the 4040 default", () => {
      // "No ports to know" only holds if the file names the port really bound.
      const dir = tmpDir();
      run(dir, 15040);
      expect(fs.readFileSync(cfg(dir), "utf8")).toContain(ep(15040));
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
      it.skipIf(!canSymlink)(
         "does NOT create the target of a dangling symlink",
         () => {
            // The hazard `existsSync`-then-write could not see: the check says
            // absent, and the write follows the link and creates a file elsewhere.
            const dir = tmpDir();
            const victimDir = tmpDir();
            const victim = path.join(victimDir, "victim.json");
            fs.symlinkSync(victim, cfg(dir));
            expect(fs.existsSync(victim)).toBe(false); // precondition

            const outcome = run(dir);

            // The safety property, asserted AFTER the call. It was above the call
            // for one commit, where it proved nothing: on Windows, which accepts
            // "failed", a version that wrote through the link and then errored for
            // any other reason would have passed green.
            expect(fs.existsSync(victim)).toBe(false);
            // Windows returns ENOENT rather than EEXIST for a dangling reparse
            // point, so the label differs while the refusal does not. Both outcomes
            // leave the file alone; only POSIX can promise which one.
            expect(["exists", "failed"]).toContain(outcome.action);
            if (!isWindows) expect(outcome.action).toBe("exists");
         },
      );

      it.skipIf(!canSymlink)(
         "does not modify the target of a symlink to a real file",
         () => {
            const dir = tmpDir();
            const victimDir = tmpDir();
            const victim = path.join(victimDir, "package.json");
            const body = JSON.stringify({ name: "someone-elses-file" });
            fs.writeFileSync(victim, body);
            fs.symlinkSync(victim, cfg(dir));
            expect(run(dir).action).toBe("exists");
            expect(fs.readFileSync(victim, "utf8")).toBe(body);
         },
      );
   });

   it.skipIf(isWindows || !hasMkfifo)(
      "returns promptly on a FIFO instead of hanging the process",
      () => {
         // A blocking read here stalls the event loop and takes /health with it.
         // `wx` never opens it for reading, so this must be fast and inert.
         // Windows has no FIFO in the filesystem namespace (named pipes live under
         // \\.\pipe), and its mkfifo, if git-bash puts one on PATH, does not make
         // something Node reports as a FIFO. The hazard does not exist there.
         const dir = tmpDir();
         execFileSync("mkfifo", [cfg(dir)]);
         const started = Date.now();
         expect(run(dir).action).toBe("exists");
         expect(Date.now() - started).toBeLessThan(2000);
         expect(fs.lstatSync(cfg(dir)).isFIFO()).toBe(true);
      },
   );

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

      // These inject a scratch home rather than running against the real one.
      // Passing the developer's actual home in is only safe while the guard
      // returns before any write; if that ordering ever regressed, the old form
      // wrote .mcp.json into their home directory instead of going red.
      it("skips the home directory", () => {
         const home = tmpDir();
         const outcome = ensureMcpConfig({
            dir: home,
            homeDir: home,
            endpoint: ep(4040),
            requestedPort: 4040,
            boundPort: 4040,
         });
         expect(outcome.action).toBe("skipped-home");
         expect(fs.existsSync(cfg(home))).toBe(false);
      });

      it("skips home even when the path is not normalised", () => {
         // The guard resolves before comparing; a raw string compare would miss.
         const home = tmpDir();
         const outcome = ensureMcpConfig({
            dir: path.join(home, "..", path.basename(home), "."),
            homeDir: home,
            endpoint: ep(4040),
            requestedPort: 4040,
            boundPort: 4040,
         });
         expect(outcome.action).toBe("skipped-home");
         expect(fs.existsSync(cfg(home))).toBe(false);
      });

      it.skipIf(!canSymlink)(
         "skips a home directory reached through a symlink",
         () => {
            // Mutation-proven gap: swapping realpathSync back to path.resolve left
            // the whole suite green, because every other test resolves its tmpdir
            // first. An automounted /home is the real-world shape.
            const realHome = tmpDir();
            const linkedHome = path.join(tmpDir(), "home-link");
            fs.symlinkSync(realHome, linkedHome);
            const outcome = ensureMcpConfig({
               dir: realHome,
               homeDir: linkedHome,
               endpoint: ep(4040),
               requestedPort: 4040,
               boundPort: 4040,
            });
            expect(outcome.action).toBe("skipped-home");
            expect(fs.existsSync(cfg(realHome))).toBe(false);
         },
      );

      it("skips the filesystem root", () => {
         // Also mutation-proven: deleting the root guard left the suite green,
         // because nothing passed "/" in. systemd gives a unit that cwd.
         const root = path.parse(process.cwd()).root;
         const rootCfg = path.join(root, MCP_CONFIG_FILENAME);
         const preexisting = fs.existsSync(rootCfg);
         // try/finally, not a trailing statement: on the failure this cleanup
         // exists for, a regressed guard, the expect throws and anything after
         // it never runs. The home test below is the model.
         try {
            expect(
               ensureMcpConfig({
                  dir: root,
                  homeDir: tmpDir(),
                  endpoint: ep(4040),
                  requestedPort: 4040,
                  boundPort: 4040,
               }).action,
            ).toBe("skipped-root");
         } finally {
            if (!preexisting && fs.existsSync(rootCfg)) fs.rmSync(rootCfg);
         }
      });

      it.skipIf(!canSymlink)(
         "names a dangling-symlink config, which is what the write path also sees",
         () => {
            // Uncovered until now: swapping the probe's lstat back to existsSync
            // left the suite green, and the only difference is this case. lstat is
            // deliberate, so that reporting and the wx write agree about what
            // counts as present.
            const dir = tmpDir();
            fs.mkdirSync(path.join(dir, ".git"));
            fs.symlinkSync(path.join(dir, "nowhere.json"), cfg(dir));
            const outcome = run(dir);
            expect(outcome.action).toBe("skipped-git");
            if (outcome.action === "skipped-git") {
               expect(outcome.staleConfig).toBe(cfg(dir));
            }
         },
      );

      it("names a stale config that is already in the directory when it skips", () => {
         // Every skip returns before the write, so EEXIST never fires for them
         // and the file the docs call the main hazard went unmentioned.
         const dir = tmpDir();
         fs.mkdirSync(path.join(dir, ".git"));
         fs.writeFileSync(cfg(dir), "{}");
         const outcome = run(dir);
         expect(outcome.action).toBe("skipped-git");
         if (outcome.action === "skipped-git") {
            expect(outcome.staleConfig).toBe(cfg(dir));
         }
      });

      it("defaults the home guard to the real os.homedir()", () => {
         // Points at the developer's actual home, so it cleans up after itself:
         // if guard ordering ever regressed, this test would otherwise create
         // ~/.mcp.json on the way to going red, which is the very thing the
         // guard exists to prevent.
         const realCfg = path.join(os.homedir(), MCP_CONFIG_FILENAME);
         const preexisting = fs.existsSync(realCfg);
         try {
            expect(
               ensureMcpConfig({
                  dir: os.homedir(),
                  endpoint: ep(4040),
                  requestedPort: 4040,
                  boundPort: 4040,
               }).action,
            ).toBe("skipped-home");
         } finally {
            if (!preexisting && fs.existsSync(realCfg)) fs.rmSync(realCfg);
         }
      });

      // Every way the bound port can differ from the requested one. Checking
      // `requestedPort === 0` missed most of them, and the NaN case is not a
      // typo-only scenario: Kubernetes injects MCP_PORT=tcp://10.96.0.1:4040
      // into every pod in a namespace with a Service named `mcp`, and
      // `Number()` of that is NaN, which bun binds ephemerally.
      const unstable: Array<[string, number]> = [
         ["--mcp_port 0", 0],
         ["a non-numeric MCP_PORT (NaN)", Number("tcp://10.96.0.1:4040")],
         ["a negative port", -1],
      ];
      for (const [label, requestedPort] of unstable) {
         it(`skips ${label}, which would be stale from the next boot on`, () => {
            // Create-never-edit means the first boot's file is never corrected,
            // so a port that moves leaves a permanently wrong config.
            const dir = tmpDir();
            const outcome = ensureMcpConfig({
               dir,
               endpoint: ep(51234),
               requestedPort,
               boundPort: 51234,
            });
            expect(outcome.action).toBe("skipped-unstable-port");
            expect(fs.existsSync(cfg(dir))).toBe(false);
         });
      }

      it("still writes when the bound port is the one that was requested", () => {
         const dir = tmpDir();
         const outcome = ensureMcpConfig({
            dir,
            endpoint: ep(4040),
            requestedPort: 4040,
            boundPort: 4040,
         });
         expect(outcome.action).toBe("created");
      });
   });

   it("reports rather than throws when the directory does not exist", () => {
      // The portable half of the failure contract: no mode bits involved, so it
      // runs everywhere including Windows, where chmod does not restrict.
      const outcome = run(path.join(tmpDir(), "no", "such", "dir"));
      expect(outcome.action).toBe("failed");
      if (outcome.action === "failed")
         expect(outcome.problem).toContain("ENOENT");
   });

   // POSIX mode bits do not restrict on Windows; ACLs would.
   it.skipIf(isWindows || isRoot)(
      "reports rather than throws when the directory cannot be written",
      () => {
         const dir = tmpDir();
         fs.chmodSync(dir, 0o500);
         try {
            expect(run(dir).action).toBe("failed");
         } finally {
            fs.chmodSync(dir, 0o700);
         }
      },
   );
});

describe("logMcpConfigOutcome", () => {
   // These exist only to produce log text, so without a test nothing covers
   // whether the text is right or even emitted.
   const outcomes: McpConfigOutcome[] = [
      {
         action: "skipped-home",
         dir: "/home/x",
         endpoint: ep(4040),
         staleConfig: undefined,
      },
      {
         action: "skipped-root",
         dir: "/",
         endpoint: ep(4040),
         staleConfig: undefined,
      },
      {
         action: "skipped-git",
         dir: "/repo/sub",
         gitRoot: "/repo",
         rootConfig: undefined,
         endpoint: ep(4040),
         staleConfig: undefined,
      },
      {
         action: "skipped-git",
         dir: "/repo/sub",
         gitRoot: "/repo",
         rootConfig: "/repo/.mcp.json",
         endpoint: ep(4040),
         staleConfig: undefined,
      },
      {
         action: "skipped-unstable-port",
         dir: "/w",
         endpoint: ep(51234),
         requestedPort: 0,
         boundPort: 51234,
         staleConfig: undefined,
      },
      { action: "created", file: "/w/.mcp.json" },
      { action: "exists", file: "/w/.mcp.json", endpoint: ep(4040) },
      {
         action: "failed",
         file: "/w/.mcp.json",
         problem: "EACCES",
         endpoint: ep(4040),
      },
   ];

   /** Collect everything the logger is told, at any level. */
   function capture(run: () => void): string {
      const said: string[] = [];
      const info = logger.info.bind(logger);
      const warn = logger.warn.bind(logger);
      const debug = logger.debug.bind(logger);
      (logger as unknown as { debug: (m: string) => void }).debug = (m) => {
         said.push(m);
      };
      (logger as unknown as { info: (m: string) => void }).info = (m) => {
         said.push(m);
      };
      (logger as unknown as { warn: (m: string) => void }).warn = (m) => {
         said.push(m);
      };
      try {
         run();
      } finally {
         (logger as unknown as { debug: unknown }).debug = debug;
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
            endpoint: ep(15040),
         });
      } finally {
         (logger as unknown as { info: unknown }).info = info;
      }
      expect(said.join("\n")).toContain(
         `claude mcp add --transport http malloy '${ep(15040)}'`,
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

   // Both skips were silent when this shipped for review, and three separate
   // reviewers landed on the same failure: no file, no tools, no message, and a
   // README that then sends you to relaunch the agent from a directory that has
   // nothing in it. Running inside your own project is the common case, not an
   // edge one, so these lines are load-bearing.
   it("tells a user inside a git repo why there is no file, and what to run", () => {
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-git",
            dir: "/work/my-project/data",
            gitRoot: "/work/my-project",
            rootConfig: undefined,
            endpoint: ep(15040),
            staleConfig: undefined,
         }),
      );
      expect(said).toContain("/work/my-project/data");
      // Naming the .git that stopped it. With a dotfiles repo in $HOME the cause
      // can be many levels up, and `ls -a` in the working directory shows nothing.
      expect(said).toContain("/work/my-project");
      expect(said).toContain(
         `claude mcp add --transport http malloy '${ep(15040)}'`,
      );
   });

   it("names an up-tree config without pretending it covers this server", () => {
      // This branch was `logger.debug` for one commit, to keep a clone quiet on
      // every boot. That only looked quiet because the default level is debug:
      // under LOG_LEVEL=info it went silent, which is the very defect three
      // reviewers had already found. A config at the repo root may register an
      // unrelated server, or name a port this run is not on, and the module
      // never reads it to find out, so it must still give the endpoint.
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-git",
            dir: "/repo/packages/server",
            gitRoot: "/repo",
            rootConfig: "/repo/.mcp.json",
            endpoint: ep(4141),
            staleConfig: undefined,
         }),
      );
      expect(said).toContain("/repo/.mcp.json");
      expect(said).toContain("4141");
      expect(said).toContain("claude mcp add");
   });

   it("emits every no-file branch at info, never below it", () => {
      // The regression guard for the above. A branch logged at debug is invisible
      // the moment an operator sets LOG_LEVEL=info, which is the production shape,
      // and an invisible skip is indistinguishable from a broken install.
      const noFile = outcomes.filter((o) => o.action !== "created");
      for (const outcome of noFile) {
         const levels: string[] = [];
         const original = {
            debug: logger.debug.bind(logger),
            info: logger.info.bind(logger),
            warn: logger.warn.bind(logger),
         };
         for (const level of ["debug", "info", "warn"] as const) {
            (logger as unknown as Record<string, (m: string) => void>)[level] =
               (_m) => {
                  levels.push(level);
               };
         }
         try {
            logMcpConfigOutcome(outcome);
         } finally {
            Object.assign(logger, original);
         }
         expect(levels).toEqual(["info"]);
      }
   });

   it("tells a user in their home directory why there is no file", () => {
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-home",
            dir: "/Users/x",
            endpoint: ep(15040),
            staleConfig: undefined,
         }),
      );
      expect(said).toContain("/Users/x");
      expect(said).toContain(
         `claude mcp add --transport http malloy '${ep(15040)}'`,
      );
   });

   it("tells a stuck user what to run when the write itself failed", () => {
      // The branch where the user is most stuck was the only no-file case with a
      // diagnosis but no remedy.
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "failed",
            file: "/w/.mcp.json",
            problem: "EACCES: permission denied",
            endpoint: ep(15040),
         }),
      );
      expect(said).toContain("EACCES");
      expect(said).toContain(
         `claude mcp add --transport http malloy '${ep(15040)}'`,
      );
   });

   it("quotes the endpoint, because zsh treats an IPv6 bracket as a glob", () => {
      // `claude mcp add ... http://[::1]:4040/mcp` unquoted is `no matches
      // found` in zsh, the macOS default shell, and the docs tell the reader to
      // paste the line as printed. Unquoted was safe only while the command
      // interpolated a number rather than a URL string.
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-home",
            dir: "/Users/x",
            endpoint: "http://[::1]:4040/mcp",
            staleConfig: undefined,
         }),
      );
      expect(said).toContain("malloy 'http://[::1]:4040/mcp'");
   });

   // Mutation-proven gap: making staleNote return "" removed the feature
   // outright and all 46 tests stayed green, because every fixture set
   // staleConfig to undefined. These render it.
   it("names a stale config in the message, on every skip that can carry one", () => {
      const withStale: McpConfigOutcome[] = [
         {
            action: "skipped-home",
            dir: "/home/x",
            endpoint: ep(4040),
            staleConfig: "/home/x/.mcp.json",
         },
         {
            action: "skipped-root",
            dir: "/",
            endpoint: ep(4040),
            staleConfig: "/.mcp.json",
         },
         {
            action: "skipped-git",
            dir: "/repo/sub",
            gitRoot: "/repo",
            rootConfig: undefined,
            endpoint: ep(4040),
            staleConfig: "/repo/sub/.mcp.json",
         },
         {
            action: "skipped-unstable-port",
            dir: "/w",
            endpoint: ep(51234),
            requestedPort: 0,
            boundPort: 51234,
            staleConfig: "/w/.mcp.json",
         },
      ];
      for (const outcome of withStale) {
         const said = capture(() => logMcpConfigOutcome(outcome));
         expect(said).toContain("was not written by this run");
         expect(said).toContain(
            (outcome as { staleConfig: string }).staleConfig,
         );
         expect(said).not.toContain(".."); // the note must join cleanly
      }
   });

   it("says nothing about a stale config when there is none", () => {
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-home",
            dir: "/home/x",
            endpoint: ep(4040),
            staleConfig: undefined,
         }),
      );
      expect(said).not.toContain("was not written by this run");
   });

   it("does not name the same file twice when cwd is the repo root", () => {
      // A clone of this repo produces exactly this on every boot from its root.
      const said = capture(() =>
         logMcpConfigOutcome({
            action: "skipped-git",
            dir: "/repo",
            gitRoot: "/repo",
            rootConfig: "/repo/.mcp.json",
            endpoint: ep(4040),
            staleConfig: "/repo/.mcp.json",
         }),
      );
      expect(said.split("/repo/.mcp.json").length - 1).toBe(1);
   });

   it("never advises user scope, which the file that caused the message outranks", () => {
      // Claude Code resolves duplicates by precedence (local, then project, then
      // user) and takes the winning entry whole. Every branch printing this
      // command has, or may have, a project-scope .mcp.json in play, so `-s user`
      // is silently shadowed by the very file being reported.
      for (const outcome of outcomes) {
         const said = capture(() => logMcpConfigOutcome(outcome));
         expect(said).not.toContain("-s user");
      }
   });

   it("never sends the reader to a hardcoded port on any branch that names one", () => {
      // The whole point of reading back the bound port. A 4040 leaking into any
      // remediation line sends the reader somewhere nothing is listening.
      const branches: McpConfigOutcome[] = [
         { action: "exists", file: "/w/.mcp.json", endpoint: ep(19999) },
         {
            action: "skipped-git",
            dir: "/repo",
            gitRoot: "/repo",
            rootConfig: undefined,
            endpoint: ep(19999),
            staleConfig: undefined,
         },
         {
            action: "skipped-home",
            dir: "/home/x",
            endpoint: ep(19999),
            staleConfig: undefined,
         },
         {
            action: "skipped-unstable-port",
            dir: "/w",
            endpoint: ep(19999),
            requestedPort: 0,
            boundPort: 19999,
            staleConfig: undefined,
         },
         {
            action: "skipped-root",
            dir: "/",
            endpoint: ep(19999),
            staleConfig: undefined,
         },
         {
            action: "failed",
            file: "/w/.mcp.json",
            problem: "EACCES",
            endpoint: ep(19999),
         },
      ];
      for (const outcome of branches) {
         const said = capture(() => logMcpConfigOutcome(outcome));
         expect(said).toContain("19999");
         expect(said).not.toContain("4040");
      }
   });
});

describe("mcpConfigEnabled", () => {
   /** Set the variable for one call and restore it. */
   function withEnv(value: string | undefined, body: () => void): void {
      const saved = process.env.PUBLISHER_NO_MCP_CONFIG;
      if (value === undefined) delete process.env.PUBLISHER_NO_MCP_CONFIG;
      else process.env.PUBLISHER_NO_MCP_CONFIG = value;
      try {
         body();
      } finally {
         if (saved === undefined) delete process.env.PUBLISHER_NO_MCP_CONFIG;
         else process.env.PUBLISHER_NO_MCP_CONFIG = saved;
      }
   }

   it("is on when the variable is not set or empty", () => {
      for (const value of [undefined, "", "   "]) {
         withEnv(value, () => expect(mcpConfigEnabled()).toBe(true));
      }
   });

   it("stays on for every spelling of no", () => {
      // The bug this guards: `!process.env.X` made =false DISABLE the feature it
      // names, because every non-empty string is truthy.
      for (const value of ["false", "FALSE", "0", "no", "OFF", "  false  "]) {
         withEnv(value, () => expect(mcpConfigEnabled()).toBe(true));
      }
   });

   it("turns off for every spelling of yes", () => {
      for (const value of ["1", "true", "YES", "on"]) {
         withEnv(value, () => expect(mcpConfigEnabled()).toBe(false));
      }
   });

   it("reads the variable when called, not when the module was imported", () => {
      // server.ts sets this from --no-mcp-config in parseArgs() and reads it at
      // module scope afterwards. If the read were ever hoisted above parseArgs,
      // or the value captured at import, the flag would silently stop working.
      withEnv(undefined, () => {
         expect(mcpConfigEnabled()).toBe(true);
         process.env.PUBLISHER_NO_MCP_CONFIG = "true";
         expect(mcpConfigEnabled()).toBe(false);
      });
   });

   it("throws on a value that is neither, like every other flag in this server", () => {
      withEnv("please", () => expect(() => mcpConfigEnabled()).toThrow());
   });
});

describe("resolveClientHost", () => {
   // `localhost` resolves to BOTH 127.0.0.1 and ::1, while the server binds
   // exactly one family: 0.0.0.0 is the IPv4 wildcard and leaves ::1 free. A
   // local process squatting the same port on the other family then receives the
   // agent's traffic, because fetch prefers the IPv6 answer. Reproduced 3/3
   // before this was changed, so these assertions are the fix, not a preference.
   const v4 = (address: string) =>
      ({ address, family: "IPv4", port: 4040 }) as const;
   const v6 = (address: string) =>
      ({ address, family: "IPv6", port: 4040 }) as const;

   it("never writes the ambiguous name", () => {
      for (const addr of [v4("0.0.0.0"), v6("::"), v4("127.0.0.1")]) {
         expect(resolveClientHost(addr, "0.0.0.0")).not.toContain("localhost");
      }
   });

   it("maps each wildcard to the loopback of its own family", () => {
      expect(resolveClientHost(v4("0.0.0.0"), "0.0.0.0")).toBe("127.0.0.1");
      expect(resolveClientHost(v6("::"), "::")).toBe("[::1]");
   });

   it("keeps a specific bind address, which is the only one that reaches it", () => {
      expect(resolveClientHost(v4("127.0.0.1"), "0.0.0.0")).toBe("127.0.0.1");
      expect(resolveClientHost(v4("192.168.1.5"), "0.0.0.0")).toBe(
         "192.168.1.5",
      );
   });

   it("brackets an IPv6 literal so the URL is legal", () => {
      expect(resolveClientHost(v6("::1"), "::")).toBe("[::1]");
      expect(mcpEndpoint(resolveClientHost(v6("::1"), "::"), 4040)).toBe(
         "http://[::1]:4040/mcp",
      );
   });

   it("falls back to the configured host when there is no address", () => {
      expect(resolveClientHost(null, "127.0.0.1")).toBe("127.0.0.1");
      expect(resolveClientHost("/tmp/sock", "0.0.0.0")).toBe("127.0.0.1");
   });
});

describe("resolveBoundPort", () => {
   // The piece that makes "no ports to know" true. It was inline in the listen
   // callback and therefore untestable.
   it("prefers the port the OS actually bound", () => {
      expect(
         resolveBoundPort({ address: "::", family: "IPv6", port: 51234 }, 0),
      ).toBe(51234);
   });

   it("falls back to the requested port when the address is a pipe path", () => {
      expect(resolveBoundPort("/tmp/sock", 4040)).toBe(4040);
   });

   it("falls back when there is no address at all", () => {
      expect(resolveBoundPort(null, 4040)).toBe(4040);
   });
});
