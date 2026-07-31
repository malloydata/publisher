import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { ensureMcpConfig, MCP_CONFIG_FILENAME } from "./mcp_config";

const dirs: string[] = [];

function tmpDir(): string {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mcp-config-"));
   dirs.push(dir);
   return dir;
}

function write(dir: string, body: string): string {
   const file = path.join(dir, MCP_CONFIG_FILENAME);
   fs.writeFileSync(file, body, "utf8");
   return file;
}

function read(dir: string): unknown {
   return JSON.parse(
      fs.readFileSync(path.join(dir, MCP_CONFIG_FILENAME), "utf8"),
   );
}

const run = (dir: string, mcpPort = 4040, enabled = true) =>
   ensureMcpConfig({ dir, mcpPort, enabled });

afterEach(() => {
   while (dirs.length) {
      fs.rmSync(dirs.pop() as string, { recursive: true, force: true });
   }
});

describe("ensureMcpConfig", () => {
   it("writes the file when the directory has none", () => {
      const dir = tmpDir();
      expect(run(dir).action).toBe("created");
      expect(read(dir)).toEqual({
         mcpServers: {
            malloy: { type: "http", url: "http://localhost:4040/mcp" },
         },
      });
   });

   it("uses the port it was handed, not the 4040 default", () => {
      // The whole promise is "no ports to know", which only holds if the file
      // points at the port the server actually bound.
      const dir = tmpDir();
      run(dir, 7340);
      expect(read(dir)).toEqual({
         mcpServers: {
            malloy: { type: "http", url: "http://localhost:7340/mcp" },
         },
      });
   });

   it("merges into an existing file and leaves other servers alone", () => {
      // The case the scaffolder learned the hard way: skipping an existing file
      // leaves the agent with no malloy_* tools while everything reports fine.
      const dir = tmpDir();
      write(
         dir,
         JSON.stringify({
            mcpServers: {
               other: { type: "http", url: "http://localhost:9/x" },
            },
         }),
      );
      const outcome = run(dir);
      expect(outcome.action).toBe("merged");
      expect(read(dir)).toEqual({
         mcpServers: {
            other: { type: "http", url: "http://localhost:9/x" },
            malloy: { type: "http", url: "http://localhost:4040/mcp" },
         },
      });
   });

   it("preserves unrelated top-level keys when merging", () => {
      const dir = tmpDir();
      write(dir, JSON.stringify({ note: "mine", mcpServers: {} }));
      run(dir);
      expect((read(dir) as { note: string }).note).toBe("mine");
   });

   it("does nothing when the malloy entry is already what we want", () => {
      const dir = tmpDir();
      write(
         dir,
         JSON.stringify({
            mcpServers: {
               malloy: { type: "http", url: "http://localhost:4040/mcp" },
            },
         }),
      );
      const before = fs.statSync(path.join(dir, MCP_CONFIG_FILENAME)).mtimeMs;
      expect(run(dir).action).toBe("already-correct");
      expect(fs.statSync(path.join(dir, MCP_CONFIG_FILENAME)).mtimeMs).toBe(
         before,
      );
   });

   it("repoints a malloy entry aimed at a different port", () => {
      // AGENTS.md tells readers to edit this url when they move ports, so a
      // stale entry is a state we create ourselves.
      const dir = tmpDir();
      write(
         dir,
         JSON.stringify({
            mcpServers: {
               malloy: { type: "http", url: "http://localhost:4040/mcp" },
            },
         }),
      );
      const outcome = run(dir, 7340);
      expect(outcome.action).toBe("updated");
      expect(read(dir)).toEqual({
         mcpServers: {
            malloy: { type: "http", url: "http://localhost:7340/mcp" },
         },
      });
   });

   it("never rewrites a file it cannot parse", () => {
      // These hold other servers' credentials. "We cannot read it" is not "it is
      // empty", and a BOM used to land exactly here.
      const dir = tmpDir();
      const body = "{ this is not json";
      write(dir, body);
      const outcome = run(dir);
      expect(outcome.action).toBe("unparseable");
      expect(fs.readFileSync(path.join(dir, MCP_CONFIG_FILENAME), "utf8")).toBe(
         body,
      );
      if (outcome.action === "unparseable") {
         expect(outcome.paste).toContain("localhost:4040/mcp");
      }
   });

   it("never rewrites a file whose mcpServers is the wrong shape", () => {
      const dir = tmpDir();
      const body = JSON.stringify({ mcpServers: "nope" });
      write(dir, body);
      expect(run(dir).action).toBe("unparseable");
      expect(fs.readFileSync(path.join(dir, MCP_CONFIG_FILENAME), "utf8")).toBe(
         body,
      );
   });

   it("skips the home directory", () => {
      // `--server_root /data` launched from ~ must not create ~/.mcp.json.
      const outcome = ensureMcpConfig({
         dir: os.homedir(),
         mcpPort: 4040,
         enabled: true,
      });
      expect(outcome.action).toBe("skipped-home");
      // And it really did not write: the guard runs before any fs call.
      expect(outcome).not.toHaveProperty("file");
   });

   it("does nothing when disabled", () => {
      const dir = tmpDir();
      expect(run(dir, 4040, false).action).toBe("disabled");
      expect(fs.existsSync(path.join(dir, MCP_CONFIG_FILENAME))).toBe(false);
   });

   it("reports rather than throws when the directory cannot be written", () => {
      // A convenience file must never be able to stop a server booting.
      const dir = tmpDir();
      fs.chmodSync(dir, 0o500);
      try {
         const outcome = run(dir);
         expect(outcome.action).toBe("failed");
      } finally {
         fs.chmodSync(dir, 0o700);
      }
   });
});
