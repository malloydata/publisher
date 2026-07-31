/**
 * Put this server in the host's MCP config, so an agent opened here finds the
 * `malloy_*` tools without being told how.
 *
 * An MCP client only knows a server exists if something registered it: a
 * `.mcp.json` in the directory the session starts in, or a manual
 * `claude mcp add`. A running server is otherwise invisible to it, and starting
 * the server first does not help, because registration and ordering are separate
 * requirements. The repo ships a committed `.mcp.json` and the scaffolder writes
 * one, so `npx @malloy-publisher/server` was the only way in that did not
 * register itself, which is the only reason getting-started had to teach
 * `claude mcp add` before a reader could ask a question.
 *
 * This CREATES, and never edits. An earlier version merged into an existing
 * file the way `scaffold.ts` does, and review found that nearly every hazard in
 * this module came from that one decision: replacing a `malloy` entry destroyed
 * user fields including auth headers, reading an existing file hung the whole
 * process when that file was a FIFO, and a repoint-on-every-boot rewrote
 * version-controlled files. The scaffolder can afford to merge because it runs
 * once, deliberately, at the user's request. A server boot cannot. So: write the
 * file when there is none, otherwise say so and leave it entirely alone,
 * unread.
 *
 * `wx` rather than `existsSync` then write, because those two disagree about
 * what they are naming when `.mcp.json` is a dangling symlink: the check says
 * absent, and the write follows the link and creates a file somewhere else on
 * the machine. `O_EXCL` refuses that, and closes the gap between the two calls.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { logger } from "./logger";

export const MCP_CONFIG_FILENAME = ".mcp.json";

/** What happened, so the caller can log it and tests can assert on it. */
export type McpConfigOutcome =
   | { action: "disabled" }
   | { action: "skipped-home"; dir: string; mcpPort: number }
   | { action: "skipped-git"; dir: string; mcpPort: number }
   | { action: "created"; file: string }
   | { action: "exists"; file: string; mcpPort: number }
   | { action: "failed"; file: string; problem: string };

/**
 * Always `localhost`, never the bound host. The client runs on this machine by
 * definition, `0.0.0.0` is not an address to dial, and writing a LAN address
 * would point other people's agents at this one's data.
 */
function malloyServer(mcpPort: number): Record<string, string> {
   return { type: "http", url: `http://localhost:${mcpPort}/mcp` };
}

/**
 * Is this directory inside a git working tree?
 *
 * A checkout belongs to somebody, and dropping an untracked file into one is both
 * a surprise in `git status` and a thing that gets committed by accident. A clone
 * of this repo is the sharp case: the documented `bun run start` chdirs into
 * `packages/server`, so without this the file landed there, untracked, and not at
 * the root where a session actually reads it.
 *
 * This is deliberately blunt: one `.git` anywhere above the working directory is
 * enough, so it also covers a user's own project, which is a normal place to run
 * the server. That case gets no file, so the caller logs the manual command
 * rather than leaving them with nothing.
 *
 * Walks up rather than shelling out to git, and treats `.git` as present whether
 * it is a directory or the file that worktrees and submodules use.
 */
function insideGitWorkTree(dir: string): boolean {
   let current = path.resolve(dir);
   for (;;) {
      if (fs.existsSync(path.join(current, ".git"))) return true;
      const parent = path.dirname(current);
      if (parent === current) return false;
      current = parent;
   }
}

/**
 * Is the feature on?
 *
 * A "stop writing to my filesystem" switch should honour `=1` as readily as
 * `=true`, so any value turns it off except the spellings a reader plainly means
 * as "leave it on". Bare truthiness got that backwards:
 * `PUBLISHER_NO_MCP_CONFIG=false` disabled the very thing it names, because every
 * non-empty string is truthy.
 */
export function mcpConfigEnabled(
   env: NodeJS.ProcessEnv = process.env,
): boolean {
   const raw = env.PUBLISHER_NO_MCP_CONFIG;
   if (raw === undefined) return true;
   const value = raw.trim().toLowerCase();
   return value === "" || value === "0" || value === "false";
}

/**
 * Create `<dir>/.mcp.json` if this directory should have one and does not.
 *
 * Never throws. A convenience file must not be able to stop a server booting, so
 * every failure path returns an outcome instead.
 */
export function ensureMcpConfig(options: {
   dir: string;
   mcpPort: number;
   enabled: boolean;
}): McpConfigOutcome {
   const { dir, mcpPort, enabled } = options;
   if (!enabled) return { action: "disabled" };

   const file = path.join(dir, MCP_CONFIG_FILENAME);
   try {
      // Inside the try because os.homedir() throws when HOME is unset and the uid
      // has no passwd entry, which is the ordinary distroless container shape.
      if (path.resolve(dir) === path.resolve(os.homedir())) {
         return { action: "skipped-home", dir, mcpPort };
      }
      if (insideGitWorkTree(dir))
         return { action: "skipped-git", dir, mcpPort };

      const body =
         JSON.stringify(
            { mcpServers: { malloy: malloyServer(mcpPort) } },
            null,
            2,
         ) + "\n";
      fs.writeFileSync(file, body, { encoding: "utf8", flag: "wx" });
      return { action: "created", file };
   } catch (error) {
      const code = (error as NodeJS.ErrnoException)?.code;
      // EEXIST covers both an ordinary existing file and a dangling symlink, and
      // the response to each is the same: leave it alone, unread.
      if (code === "EEXIST") return { action: "exists", file, mcpPort };
      return {
         action: "failed",
         file,
         problem: error instanceof Error ? error.message : String(error),
      };
   }
}

/**
 * Say whether an agent opened here will find this server, and what to do when it
 * will not. Announced rather than silent: a tool that writes into your directory
 * should say so, and the line is what tells a first-time reader that the
 * connection they were about to configure by hand is already done.
 */
export function logMcpConfigOutcome(outcome: McpConfigOutcome): void {
   switch (outcome.action) {
      case "created":
         logger.info(
            `Wrote ${outcome.file} so an agent started in this directory finds this server. Disable with --no-mcp-config.`,
         );
         return;
      case "exists":
         // Deliberately does not claim to know what is in it. Reading the file is
         // the thing this module refuses to do.
         logger.info(
            `Left the existing ${outcome.file} alone. If your agent cannot find this server, add it with: claude mcp add --transport http malloy http://localhost:${outcome.mcpPort}/mcp -s user`,
         );
         return;
      case "failed":
         logger.warn(
            `Could not write ${outcome.file} (${outcome.problem}). An agent started here will not find this server on its own.`,
         );
         return;
      // The two skips have to speak. They are the branches where the user gets no
      // file and no tools, and silence there sends them to the README's other
      // remedy, relaunching the agent from this directory, which cannot work
      // because there is nothing here to find. One `.git` anywhere above the
      // working directory reaches this line, which includes the ordinary case of
      // running inside your own project.
      case "skipped-git":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME} because ${outcome.dir} is inside a git working tree. To connect an agent, run: claude mcp add --transport http malloy http://localhost:${outcome.mcpPort}/mcp -s user`,
         );
         return;
      case "skipped-home":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME} into your home directory (${outcome.dir}). To connect an agent, run: claude mcp add --transport http malloy http://localhost:${outcome.mcpPort}/mcp -s user`,
         );
         return;
      case "disabled":
         return;
      default: {
         // A new variant must be handled above rather than logging nothing.
         const exhaustive: never = outcome;
         return exhaustive;
      }
   }
}
