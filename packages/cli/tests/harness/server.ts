/// <reference types="bun-types" />

import { spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// packages/cli/tests/harness -> packages/server and packages/cli/src/index.ts
const SERVER_DIR = path.resolve(__dirname, "../../../server");
const CLI_ENTRY = path.resolve(__dirname, "../../src/index.ts");
const PERSIST_FIXTURE = path.resolve(SERVER_DIR, "tests/fixtures/persist-test");

export const TEST_ENV = "it-env";
export const TEST_PKG = "persist-test";

export interface TestServer {
  baseUrl: string;
  stop(): Promise<void>;
}

/** Allocate an OS-assigned free TCP port (avoids fixed-port collisions). */
async function getFreePort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const srv = net.createServer();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const found = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() =>
        found ? resolve(found) : reject(new Error("no free port")),
      );
    });
  });
}

/**
 * Boot a real Publisher server for CLI integration tests.
 *
 * The CLI package does not depend on the server package, so the server cannot
 * be booted in-process (the way the server's own tests do). Instead we run it
 * as a subprocess from source (`bun src/server.ts`, no `--watch` so there is
 * no orphaned file watcher to clean up), on OS-assigned free ports, pointed at
 * a temp SERVER_ROOT seeded with the committed `persist-test` fixture.
 *
 * If `CLI_TEST_SERVER_URL` is set, reuse that already-running server instead of
 * spawning one (handy for CI). That server must be seeded with the
 * `it-env`/`persist-test` fixture and have no pre-existing materializations,
 * since the suite asserts on the empty initial state.
 */
export async function startTestServer(): Promise<TestServer> {
  const externalUrl = process.env.CLI_TEST_SERVER_URL;
  if (externalUrl) {
    const baseUrl = externalUrl.replace(/\/$/, "");
    await waitForPackage(baseUrl);
    return { baseUrl, stop: async () => {} };
  }

  const serverRoot = fs.mkdtempSync(path.join(os.tmpdir(), "cli-it-root-"));
  fs.writeFileSync(
    path.join(serverRoot, "publisher.config.json"),
    JSON.stringify({
      frozenConfig: false,
      environments: [
        {
          name: TEST_ENV,
          packages: [{ name: TEST_PKG, location: PERSIST_FIXTURE }],
          connections: [],
        },
      ],
    }),
  );

  const port = await getFreePort();
  let mcpPort = await getFreePort();
  while (mcpPort === port) {
    mcpPort = await getFreePort();
  }
  const baseUrl = `http://127.0.0.1:${port}`;
  const proc = spawn("bun", ["src/server.ts"], {
    cwd: SERVER_DIR,
    env: {
      ...process.env,
      NODE_ENV: "development",
      SERVER_ROOT: serverRoot,
      PUBLISHER_HOST: "127.0.0.1",
      PUBLISHER_PORT: String(port),
      MCP_PORT: String(mcpPort),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  // Buffer server output so a startup failure can surface a useful tail.
  let serverLog = "";
  const capture = (d: Buffer) => {
    serverLog = (serverLog + d.toString()).slice(-4000);
  };
  proc.stdout?.on("data", capture);
  proc.stderr?.on("data", capture);

  let exited = false;
  proc.on("exit", () => {
    exited = true;
  });

  try {
    await waitForPackage(baseUrl, () => exited);
  } catch (err) {
    proc.kill("SIGKILL");
    fs.rmSync(serverRoot, { recursive: true, force: true });
    const reason = err instanceof Error ? err.message : String(err);
    throw new Error(`${reason}\n--- server log tail ---\n${serverLog}`);
  }

  const stop = async (): Promise<void> => {
    await new Promise<void>((resolve) => {
      if (exited) {
        resolve();
        return;
      }
      // Backstop in case SIGTERM is ignored; cleared once the process exits.
      const backstop = setTimeout(() => proc.kill("SIGKILL"), 5_000);
      proc.on("exit", () => {
        clearTimeout(backstop);
        resolve();
      });
      proc.kill("SIGTERM");
    });
    fs.rmSync(serverRoot, { recursive: true, force: true });
  };

  return { baseUrl, stop };
}

/** Poll until the persist-test package is loaded and queryable. */
async function waitForPackage(
  baseUrl: string,
  hasExited: () => boolean = () => false,
): Promise<void> {
  const url = `${baseUrl}/api/v0/environments/${TEST_ENV}/packages/${TEST_PKG}/models`;
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    if (hasExited()) {
      throw new Error("Server process exited before becoming ready");
    }
    try {
      const res = await fetch(url);
      if (res.ok) {
        return;
      }
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error("Server did not become ready within 120s");
}

export interface CliResult {
  code: number;
  stdout: string;
  stderr: string;
  /** stdout + stderr, ANSI stripped, for substring assertions. */
  output: string;
}

// ESC (0x1b) built via fromCharCode to avoid a control-char literal in source.
const ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "g");

/**
 * Run the real CLI binary as a subprocess against `baseUrl`, capturing
 * stdout/stderr and the exit code. This exercises the full path: argument
 * parsing, command routing, the API client, HTTP, and the exit code.
 */
export async function runCli(
  args: string[],
  baseUrl: string,
): Promise<CliResult> {
  return new Promise<CliResult>((resolve, reject) => {
    const proc = spawn("bun", [CLI_ENTRY, ...args], {
      env: { ...process.env, NO_COLOR: "1", MALLOY_PUBLISHER_URL: baseUrl },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    proc.stdout?.on("data", (d) => {
      stdout += d.toString();
    });
    proc.stderr?.on("data", (d) => {
      stderr += d.toString();
    });
    proc.on("error", reject);
    proc.on("close", (code) => {
      const cleanOut = stdout.replace(ANSI, "");
      const cleanErr = stderr.replace(ANSI, "");
      resolve({
        code: code ?? -1,
        stdout: cleanOut,
        stderr: cleanErr,
        output: `${cleanOut}\n${cleanErr}`,
      });
    });
  });
}

/** Extract a materialization id (epoch-ms + suffix) from CLI output. */
export function extractMaterializationId(output: string): string | undefined {
  const m = output.match(/[0-9]{13}-[a-z0-9]+/);
  return m ? m[0] : undefined;
}
