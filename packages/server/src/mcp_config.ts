/**
 * Put this server in the host's MCP config, so an agent opened here finds the
 * `malloy_*` tools without being told how.
 *
 * Why the server does this at all. An MCP client only knows a server exists if
 * something registered it: a `.mcp.json` in the directory the session starts in,
 * or a manual `claude mcp add`. A running server on port 4040 is otherwise
 * invisible, and no amount of starting it first helps. Three of the four ways
 * into Publisher already register themselves (the repo ships a committed
 * `.mcp.json`, and the scaffolder writes one), and `npx @malloy-publisher/server`
 * was the one that did not, which is the only reason the getting-started docs
 * had to teach `claude mcp add` before a reader could ask a question.
 *
 * What it deliberately does NOT change. The file is only read from the directory
 * an agent session STARTED in, so "open your agent here" is still a real
 * requirement. And the host still prompts: on a directory it has not seen before,
 * measured as three, once each, in order: trust the folder, use the MCP server it
 * found, then allow the first `malloy_*` call. All three are the host's business,
 * not ours, and suppressing them is not something a server that reads your data
 * should want.
 *
 * Merging, not skipping, and that is a decision with history. An existing
 * `.mcp.json` is the common case in a project an agent already works in, and the
 * scaffolder learned that skipping one leaves the agent with no `malloy_*` tools
 * while the CLI cheerfully reports the endpoint wired. `scaffold.ts`'s
 * `writeMcpConfig` is the reference; this mirrors its rules so the two entry
 * points cannot drift into disagreeing about the same file.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { logger } from "./logger";

export const MCP_CONFIG_FILENAME = ".mcp.json";

/** What happened, so the caller can log it and tests can assert on it. */
export type McpConfigOutcome =
   | { action: "disabled" }
   | { action: "skipped-home"; dir: string }
   | { action: "created"; file: string }
   | { action: "merged"; file: string; otherServers: number }
   | { action: "updated"; file: string; from: string }
   | { action: "already-correct"; file: string }
   | { action: "unparseable"; file: string; problem: string; paste: string }
   | { action: "failed"; file: string; problem: string };

/**
 * Always `localhost`, never the bound host. The MCP client runs on this machine
 * by definition, `0.0.0.0` is not an address to dial, and writing a LAN address
 * into a config file would point other people's agents at this one's data.
 */
function malloyServer(mcpPort: number): Record<string, string> {
   return { type: "http", url: `http://localhost:${mcpPort}/mcp` };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
   return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Say why a file could not be merged into. A syntax error and a file that parses
 * but holds the wrong shape are different problems, and reporting both as "bad
 * JSON" sends the reader hunting for a broken quote that is not there.
 */
function describeProblem(parsed: unknown, parses: boolean): string {
   if (!parses) return "it is not valid JSON";
   if (!isPlainObject(parsed)) return "its top level is not a JSON object";
   return "its `mcpServers` is not a JSON object";
}

/**
 * Ensure `<dir>/.mcp.json` registers this server.
 *
 * Never throws: a convenience file must not be able to stop a server booting, so
 * every failure path returns an outcome instead.
 */
export function ensureMcpConfig(options: {
   dir: string;
   mcpPort: number;
   enabled: boolean;
}): McpConfigOutcome {
   const { dir, mcpPort, enabled } = options;
   if (!enabled) return { action: "disabled" };

   // Running from the home directory is the one case where "helpful" becomes
   // "invasive": `--server_root /data/packages` launched from ~ would otherwise
   // create ~/.mcp.json for a directory the user never meant as a workspace.
   if (path.resolve(dir) === path.resolve(os.homedir())) {
      return { action: "skipped-home", dir };
   }

   const file = path.join(dir, MCP_CONFIG_FILENAME);
   const desired = malloyServer(mcpPort);

   try {
      if (!fs.existsSync(file)) {
         const body =
            JSON.stringify({ mcpServers: { malloy: desired } }, null, 2) + "\n";
         fs.writeFileSync(file, body, "utf8");
         return { action: "created", file };
      }

      let parsed: unknown;
      let parses = true;
      try {
         parsed = JSON.parse(fs.readFileSync(file, "utf8"));
      } catch {
         parses = false;
      }

      const config = isPlainObject(parsed) ? parsed : undefined;
      const servers =
         config && "mcpServers" in config
            ? isPlainObject(config.mcpServers)
               ? config.mcpServers
               : undefined
            : {};
      if (!config || servers === undefined) {
         // Not ours to rewrite. These files hold other servers' credentials, and
         // "we cannot read it" is not "it is empty".
         return {
            action: "unparseable",
            file,
            problem: describeProblem(parsed, parses),
            paste: JSON.stringify({ mcpServers: { malloy: desired } }, null, 2),
         };
      }

      const existing = servers.malloy;
      if (JSON.stringify(existing) === JSON.stringify(desired)) {
         return { action: "already-correct", file };
      }

      // A merge, never a rewrite: every other server in here belongs to the user
      // and only the `malloy` key is ours to set.
      const merged = { ...config, mcpServers: { ...servers, malloy: desired } };
      fs.writeFileSync(file, JSON.stringify(merged, null, 2) + "\n", "utf8");

      if (existing !== undefined) {
         const from =
            isPlainObject(existing) && typeof existing.url === "string"
               ? existing.url
               : "a different entry";
         return { action: "updated", file, from };
      }
      return {
         action: "merged",
         file,
         otherServers: Object.keys(servers).filter((k) => k !== "malloy")
            .length,
      };
   } catch (error) {
      return {
         action: "failed",
         file,
         problem: error instanceof Error ? error.message : String(error),
      };
   }
}

/**
 * Log the outcome in the terms a reader cares about: whether an agent opened here
 * will find this server, and what to do when it will not.
 *
 * Announced rather than silent. A tool that writes a file into your directory
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
      case "merged":
         logger.info(
            `Added the malloy server to ${outcome.file}${
               outcome.otherServers > 0
                  ? `, alongside the ${outcome.otherServers} already there`
                  : ""
            }.`,
         );
         return;
      case "updated":
         logger.info(
            `Updated the malloy server in ${outcome.file} to this server's port (was ${outcome.from}).`,
         );
         return;
      case "unparseable":
         logger.warn(
            `Left ${outcome.file} alone because ${outcome.problem}. An agent started here will not find this server until you add:\n${outcome.paste}`,
         );
         return;
      case "failed":
         logger.warn(
            `Could not write ${outcome.file} (${outcome.problem}). An agent started here will not find this server; register it with: claude mcp add --transport http malloy ...`,
         );
         return;
      case "already-correct":
      case "skipped-home":
      case "disabled":
         return;
   }
}
