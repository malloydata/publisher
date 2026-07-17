/**
 * The real acceptance: scaffold a package, boot an actual Publisher server against
 * the generated config, and confirm it serves the package, compiles the model,
 * answers a query, and exposes the MCP tools. A config that "looks right" can
 * silently serve nothing, so this verifies by executing, not by reading files.
 *
 * Prerequisites: the server bin (bun run build, or build:server-deploy for the
 * web app too) and the skills package (bun run build:skills) must be built. Run
 * with `bun run test:e2e`.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { scaffold } from "../../src/scaffold";

const here = path.dirname(fileURLToPath(import.meta.url));
const packageRoot = path.resolve(here, "..", "..");
const serverBin = path.resolve(
   packageRoot,
   "..",
   "server",
   "dist",
   "server.mjs",
);

let tmp: string;
let server: ChildProcess;
let serverLog = "";
let publisherPort = 0;
let mcpPort = 0;

/** The address the generated start script binds, passed to the server below. */
const BIND_HOST = "127.0.0.1";

const api = () => `http://localhost:${publisherPort}/api/v0`;
const pkgBase = () => `${api()}/environments/default/packages/sales`;

/**
 * The first non-internal IPv4 address of this machine: the address a laptop on
 * the same wifi, or another container on the same network, would use. Undefined
 * on a machine with no network interface at all.
 */
function lanAddress(): string | undefined {
   for (const infos of Object.values(os.networkInterfaces())) {
      for (const info of infos ?? []) {
         if (info.family === "IPv4" && !info.internal) {
            return info.address;
         }
      }
   }
   return undefined;
}

/** Whether a TCP connection to this address and port is accepted. */
function accepts(
   host: string,
   port: number,
   timeoutMs = 5000,
): Promise<boolean> {
   return new Promise((resolve) => {
      const socket = net.connect({ host, port });
      const finish = (accepted: boolean) => {
         socket.destroy();
         resolve(accepted);
      };
      socket.setTimeout(timeoutMs);
      socket.on("connect", () => finish(true));
      socket.on("timeout", () => finish(false));
      socket.on("error", () => finish(false));
   });
}

function freePort(): Promise<number> {
   return new Promise((resolve, reject) => {
      const srv = net.createServer();
      srv.on("error", reject);
      srv.listen(0, () => {
         const address = srv.address();
         const port = typeof address === "object" && address ? address.port : 0;
         srv.close(() => resolve(port));
      });
   });
}

/**
 * Read a response body as JSON, but surface the server's own diagnosis when the
 * request failed. Calling `res.json()` unconditionally turns a 500 whose body
 * explains exactly what is wrong with the generated config into an opaque parse
 * error, which is the failure mode this suite exists to prevent.
 */
async function readJson<T>(res: Response, what: string): Promise<T> {
   const text = await res.text();
   if (!res.ok) {
      throw new Error(
         `${what} failed with ${res.status} ${res.statusText}: ${text}`,
      );
   }
   try {
      return JSON.parse(text) as T;
   } catch {
      throw new Error(`${what} returned a non-JSON body: ${text}`);
   }
}

async function getJson<T>(url: string): Promise<T> {
   return readJson<T>(await fetch(url), `GET ${url}`);
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
   const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
   });
   return readJson<T>(res, `POST ${url}`);
}

async function waitForServing(timeoutMs: number): Promise<void> {
   const deadline = Date.now() + timeoutMs;
   while (Date.now() < deadline) {
      try {
         const res = await fetch(`${api()}/status`);
         if (res.ok) {
            const body = (await res.json()) as { operationalState?: string };
            if (body.operationalState === "serving") {
               return;
            }
         }
      } catch {
         // Server not accepting connections yet.
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
   }
   throw new Error(
      `Server did not report serving within ${timeoutMs}ms. Log:\n${serverLog}`,
   );
}

beforeAll(async () => {
   if (!fs.existsSync(serverBin)) {
      throw new Error(
         `Server bin not found at ${serverBin}. Run "bun run build" from the ` +
            `repo root before the e2e test.`,
      );
   }

   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-e2e-"));
   scaffold({ name: "sales", cwd: tmp, host: "claude-code", force: false });

   publisherPort = await freePort();
   mcpPort = await freePort();

   server = spawn(
      "node",
      [
         serverBin,
         "--server_root",
         ".",
         "--config",
         "./publisher.config.json",
         "--port",
         String(publisherPort),
         "--mcp_port",
         String(mcpPort),
         // The flag the generated start script carries, and the only thing
         // between this data and the rest of the network: Publisher's own
         // default is 0.0.0.0, and neither the REST API nor the MCP endpoint
         // authenticates anything. Without it this suite served an
         // unauthenticated read of the scaffolded workspace on every interface
         // of every machine that ran it, laptops and CI runners alike, and
         // nothing anywhere ever executed the flag the security story rests on:
         // the unit specs only check that the generated string contains it.
         "--host",
         BIND_HOST,
         "--watch-env",
         "default",
      ],
      {
         cwd: tmp,
         env: { ...process.env, NODE_ENV: "production" },
         stdio: "pipe",
      },
   );
   server.stdout?.on("data", (chunk) => (serverLog += chunk));
   server.stderr?.on("data", (chunk) => (serverLog += chunk));

   await waitForServing(150000);
});

afterAll(() => {
   if (server && server.exitCode === null) {
      server.kill("SIGKILL");
   }
   if (tmp) {
      fs.rmSync(tmp, { recursive: true, force: true });
   }
});

describe("generated project serves against a real server", () => {
   test("the server is serving and has the scaffolded package mounted", async () => {
      const status = await getJson<{ operationalState: string }>(
         `${api()}/status`,
      );
      expect(status.operationalState).toBe("serving");

      // A package the server refused to load is simply missing from this list
      // while the status stays "serving" and the reason goes only to the server
      // log, so the package list is the assertion that catches a config the
      // server accepted but could not mount.
      const packages = await getJson<{ name: string }[]>(
         `${api()}/environments/default/packages`,
      );
      const names = packages.map((p) => p.name);
      if (!names.includes("sales")) {
         throw new Error(
            `Server is serving but did not mount "sales". Packages: ` +
               `${JSON.stringify(names)}. Log:\n${serverLog}`,
         );
      }
   });

   test("the package's model loaded without a compile error", async () => {
      const models = await getJson<{ path: string; error?: string }[]>(
         `${pkgBase()}/models`,
      );
      const model = models.find((m) => m.path === "sales.malloy");
      expect(model).toBeDefined();
      expect(model?.error).toBeUndefined();
   });

   test("the starter model compiles with all its views", async () => {
      const model = await getJson<{
         sources: { name: string; views?: { name: string }[] }[];
      }>(`${pkgBase()}/models/sales.malloy`);
      const views = model.sources.flatMap((s) =>
         (s.views ?? []).map((v) => v.name),
      );
      expect(views).toEqual(
         expect.arrayContaining([
            "by_category",
            "by_region",
            "sales_by_month",
            "overview",
         ]),
      );
   });

   test("querying the overview view returns the expected totals", async () => {
      const response = await postJson<{ result: string }>(
         `${pkgBase()}/models/sales.malloy/query`,
         { sourceName: "sales", queryName: "overview" },
      );
      const result = JSON.parse(response.result) as {
         data: { array_value: { record_value: { number_value: number }[] }[] };
      };
      const rows = result.data.array_value;
      expect(rows).toHaveLength(1);
      // record_count over the 10-row sample CSV.
      expect(rows[0].record_value[0].number_value).toBe(10);
   });

   test("the time view breaks revenue into four months", async () => {
      const response = await postJson<{ result: string }>(
         `${pkgBase()}/models/sales.malloy/query`,
         { sourceName: "sales", queryName: "sales_by_month" },
      );
      const result = JSON.parse(response.result) as {
         data: { array_value: unknown[] };
      };
      expect(result.data.array_value).toHaveLength(4);
   });

   test("the MCP endpoint lists the five malloy tools", async () => {
      const client = new Client({ name: "cmp-e2e", version: "0.0.0" });
      const transport = new StreamableHTTPClientTransport(
         new URL(`http://localhost:${mcpPort}/mcp`),
      );
      await client.connect(transport);
      const { tools } = await client.listTools();
      await client.close();
      expect(tools.map((t) => t.name).sort()).toEqual([
         "malloy_compile",
         "malloy_executeQuery",
         "malloy_getContext",
         "malloy_reloadPackage",
         "malloy_searchDocs",
      ]);
   });
});

/**
 * The generated workspace's whole security story is one flag, `--host 127.0.0.1`,
 * on a server with no authentication in front of either the REST API or the MCP
 * endpoint. Every other spec in this repository checks that flag by looking for
 * the substring in a string this tool generated; these are the only ones that
 * boot a server with it and ask the network what happened. If the server ever
 * renames or drops the flag, every string check stays green while every generated
 * workspace serves the LAN.
 */
describe("the bind address is the one the generated workspace promises", () => {
   test("both ports answer on the loopback address", async () => {
      const status = await fetch(
         `http://${BIND_HOST}:${publisherPort}/api/v0/status`,
      );
      expect(status.ok).toBe(true);
      expect(await accepts(BIND_HOST, mcpPort)).toBe(true);
   });

   test("the URLs the scaffold hands out reach it", async () => {
      // AGENTS.md, the CLI's next steps, and the .mcp.json this run wrote all
      // name localhost, so a bind address those cannot resolve to would be a
      // workspace that documents an endpoint it does not serve.
      const mcp = JSON.parse(
         fs.readFileSync(path.join(tmp, ".mcp.json"), "utf8"),
      ) as { mcpServers: { malloy: { url: string } } };
      const url = new URL(mcp.mcpServers.malloy.url);
      expect(await accepts(url.hostname, mcpPort)).toBe(true);
      expect((await fetch(`${api()}/status`)).ok).toBe(true);
   });

   const lan = lanAddress();
   const lanTest = lan === undefined ? test.skip : test;

   lanTest(
      `neither port answers on this machine's LAN address (${lan})`,
      async () => {
         // Both listeners take the same flag, and the MCP one is the easier to
         // forget: it is a second express app with its own listen() call.
         expect(await accepts(lan as string, publisherPort)).toBe(false);
         expect(await accepts(lan as string, mcpPort)).toBe(false);
      },
   );
});

/**
 * The joined `--host=<address>` form, executed rather than reasoned about.
 *
 * The tool used to accept `--host=127.0.0.1` in an existing start script as a
 * loopback bind, adopt the script as `npm start`, suppress the exposure warning,
 * and write "this workspace is reachable from this machine only" into AGENTS.md.
 * Publisher's own parser reads `--host` and its address as two separate
 * arguments, so the joined form matches no branch, is dropped without a word,
 * and the bind falls back to `PUBLISHER_HOST || "0.0.0.0"`. Every unit spec on
 * the subject compares one string against another and would stay green either
 * way; this is the one place that boots a server with the flag and asks the
 * network what it did.
 *
 * The workspace booted here is deliberately setup-only, with no package in it,
 * because a passing run means an unauthenticated server really is answering on
 * this machine's LAN address for as long as it takes to assert that. It is
 * killed immediately afterwards.
 *
 * If this ever fails, the server has started honouring the joined form, and
 * scaffold.ts's parser should be brought back into line with it rather than the
 * other way round.
 */
describe("Publisher's own parser drops --host=<address>", () => {
   const lan = lanAddress();
   let joinedTmp: string;
   let joined: ChildProcess | undefined;
   let joinedPort = 0;
   let joinedMcpPort = 0;
   let joinedLog = "";

   beforeAll(async () => {
      if (lan === undefined) {
         return;
      }
      joinedTmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-e2e-host-"));
      // No package: nothing of anyone's is served while this runs.
      scaffold({
         name: undefined,
         cwd: joinedTmp,
         host: "claude-code",
         force: false,
      });
      joinedPort = await freePort();
      joinedMcpPort = await freePort();
      joined = spawn(
         "node",
         [
            serverBin,
            "--server_root",
            ".",
            "--config",
            "./publisher.config.json",
            "--port",
            String(joinedPort),
            "--mcp_port",
            String(joinedMcpPort),
            `--host=${BIND_HOST}`,
            "--watch-env",
            "default",
         ],
         {
            cwd: joinedTmp,
            env: { ...process.env, NODE_ENV: "production" },
            stdio: "pipe",
         },
      );
      joined.stdout?.on("data", (chunk) => (joinedLog += chunk));
      joined.stderr?.on("data", (chunk) => (joinedLog += chunk));

      const deadline = Date.now() + 150000;
      while (Date.now() < deadline) {
         if (await accepts("127.0.0.1", joinedPort, 1000)) {
            return;
         }
         await new Promise((resolve) => setTimeout(resolve, 500));
      }
      throw new Error(
         `Server with --host=${BIND_HOST} never accepted a connection. Log:\n` +
            joinedLog,
      );
   });

   afterAll(() => {
      if (joined && joined.exitCode === null) {
         joined.kill("SIGKILL");
      }
      if (joinedTmp) {
         fs.rmSync(joinedTmp, { recursive: true, force: true });
      }
   });

   const lanTest = lan === undefined ? test.skip : test;

   lanTest(
      `--host=${BIND_HOST} still serves this machine's LAN address (${lan})`,
      async () => {
         const reachable = await accepts(lan as string, joinedPort);
         if (!reachable) {
            throw new Error(
               `Publisher now honours --host=<address>. scaffold.ts declines a ` +
                  `start script carrying that form on the grounds that it does ` +
                  `not, so bring the two back into line. Log:\n${joinedLog}`,
            );
         }
         // The MCP listener takes the same flag and is the easier one to forget.
         expect(await accepts(lan as string, joinedMcpPort)).toBe(true);
      },
   );

   lanTest(
      "so a start script carrying that form is not adopted as npm start",
      () => {
         // The same string the server was just booted with, in the place the
         // tool reads it from. This is the assertion the observation above
         // exists to justify: it fails if the tool ever calls the joined form
         // loopback again.
         const dir = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-e2e-adopt-"));
         try {
            fs.writeFileSync(
               path.join(dir, "package.json"),
               JSON.stringify({
                  name: "someone-elses-app",
                  scripts: {
                     start:
                        `npx -y @malloy-publisher/server --server_root . ` +
                        `--config ./publisher.config.json --host=${BIND_HOST} ` +
                        `--watch-env default`,
                  },
               }) + "\n",
            );
            const result = scaffold({
               name: "sales",
               cwd: dir,
               host: "claude-code",
               force: false,
            });
            expect(result.hasStartScript).toBe(false);
            expect(result.declinedStartScript).toBeDefined();
            expect(
               fs.readFileSync(path.join(dir, "AGENTS.md"), "utf8"),
            ).not.toContain("npm start");
         } finally {
            fs.rmSync(dir, { recursive: true, force: true });
         }
      },
   );
});

/**
 * The ports the generated workspace documents, executed rather than compared.
 *
 * Every other check on this subject reads one generated string against another,
 * and that is exactly how the defect survived: PUBLISHER_PORT and MCP_PORT were
 * constants copied onto the result and interpolated into AGENTS.md, .mcp.json
 * and the CLI's curls, while the code that decides whether to adopt this
 * directory's `start` script read only the server name, the shell
 * metacharacters, the runner tokens and --host. A start script booting Publisher
 * on 4100/4140 was adopted as `npm start` and documented as serving 4000/4040,
 * and every string check agreed with itself.
 *
 * So this boots the command AGENTS.md hands the reader, and asks the URLs in the
 * same file whether they reach it. The command is translated to this checkout's
 * server bin (npx would go to the network for a version that is not this one);
 * every flag after the package name is passed through untouched, because those
 * flags are the thing under test.
 *
 * Nothing here assumes which fix landed. If the script is declined, AGENTS.md
 * documents the explicit boot with the default ports and this boots that; if it
 * is adopted, AGENTS.md documents the script's own ports and this boots those.
 * The failure is a documented URL that does not reach the documented boot.
 */
describe("the documented URLs reach the documented boot", () => {
   let portsTmp: string;
   let child: ChildProcess | undefined;
   let childLog = "";
   let statusUrl: string | undefined;
   let packagesUrl: string | undefined;
   let mcpUrl: string | undefined;
   /**
    * Set when the boot AGENTS.md names is on a port something else already
    * holds. Two Publishers answer /api/v0/status identically, so a run in that
    * state cannot tell this server from a stranger's, and the honest answer is
    * to say so rather than to pass or fail on someone else's process.
    */
   let unusable: string | undefined;

   /** The fenced boot commands of a generated AGENTS.md, in file order. */
   function bootCommandsIn(markdown: string): string[] {
      return [...markdown.matchAll(/```[a-z]*\r?\n([\s\S]*?)```/g)]
         .flatMap((match) => match[1].split("\n"))
         .map((line) => line.trim())
         .filter(
            (line) =>
               line.startsWith("npm ") ||
               line.includes("@malloy-publisher/server"),
         );
   }

   beforeAll(async () => {
      portsTmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-e2e-ports-"));
      const restPort = await freePort();
      const mcpPortForScript = await freePort();
      // A start script this tool did not write, of the shape its own AGENTS.md
      // tells the reader to write when the default ports are taken.
      fs.writeFileSync(
         path.join(portsTmp, "package.json"),
         JSON.stringify(
            {
               name: "someone-elses-app",
               scripts: {
                  start:
                     `npx -y @malloy-publisher/server --server_root . ` +
                     `--config ./publisher.config.json --host ${BIND_HOST} ` +
                     `--port ${restPort} --mcp_port ${mcpPortForScript} ` +
                     `--watch-env default`,
               },
            },
            null,
            2,
         ) + "\n",
      );
      scaffold({
         name: "sales",
         cwd: portsTmp,
         host: "claude-code",
         force: false,
      });

      const agents = fs.readFileSync(path.join(portsTmp, "AGENTS.md"), "utf8");
      statusUrl = /http:\/\/localhost:\d+\/api\/v0\/status/.exec(agents)?.[0];
      packagesUrl =
         /http:\/\/localhost:\d+\/api\/v0\/environments\/\S+?\/packages/.exec(
            agents,
         )?.[0];
      mcpUrl = (
         JSON.parse(
            fs.readFileSync(path.join(portsTmp, ".mcp.json"), "utf8"),
         ) as {
            mcpServers: { malloy: { url: string } };
         }
      ).mcpServers.malloy.url;
      if (statusUrl === undefined || packagesUrl === undefined) {
         throw new Error(`AGENTS.md documents no REST URLs:\n${agents}`);
      }

      // The boot the file hands the reader, resolved through this workspace's
      // own package.json where it is an npm script.
      const command = bootCommandsIn(agents)[0];
      const pkg = JSON.parse(
         fs.readFileSync(path.join(portsTmp, "package.json"), "utf8"),
      ) as { scripts?: Record<string, string> };
      const resolved = command?.startsWith("npm start")
         ? pkg.scripts?.start
         : command;
      if (resolved === undefined) {
         throw new Error(
            `AGENTS.md hands the reader "${command}", which this workspace ` +
               `cannot run`,
         );
      }

      const tokens = resolved.split(/\s+/).filter(Boolean);
      const index = tokens.findIndex((token) =>
         token.includes("@malloy-publisher/server"),
      );
      const args = tokens.slice(index + 1);
      const documentedPort = Number(new URL(statusUrl).port);
      if (await accepts("127.0.0.1", documentedPort, 1000)) {
         unusable =
            `Something is already listening on ${documentedPort}, the port ` +
            `AGENTS.md documents. Two Publishers answer /api/v0/status the ` +
            `same way, so this run cannot tell them apart. Stop it and rerun.`;
         return;
      }

      child = spawn("node", [serverBin, ...args], {
         cwd: portsTmp,
         env: { ...process.env, NODE_ENV: "production" },
         stdio: "pipe",
      });
      child.stdout?.on("data", (chunk) => (childLog += chunk));
      child.stderr?.on("data", (chunk) => (childLog += chunk));

      const deadline = Date.now() + 150000;
      while (Date.now() < deadline) {
         try {
            const res = await fetch(statusUrl);
            if (res.ok) {
               const body = (await res.json()) as {
                  operationalState?: string;
               };
               if (body.operationalState === "serving") {
                  return;
               }
            }
         } catch {
            // Not accepting connections yet.
         }
         await new Promise((resolve) => setTimeout(resolve, 500));
      }
      throw new Error(
         `The boot AGENTS.md documents ("${resolved}") never answered the ` +
            `status URL the same file documents (${statusUrl}) within 150s. ` +
            `Log:\n${childLog}`,
      );
   });

   afterAll(() => {
      if (child && child.exitCode === null) {
         child.kill("SIGKILL");
      }
      if (portsTmp) {
         fs.rmSync(portsTmp, { recursive: true, force: true });
      }
   });

   test("the package list the workspace documents holds the scaffolded package", async () => {
      if (unusable !== undefined) {
         throw new Error(unusable);
      }
      // Not just a 200: AGENTS.md tells an agent to read the package list before
      // trusting the server, because any Publisher on this port answers the
      // status the same way. The same check, run against the same URL.
      const packages = await getJson<{ name?: string }[]>(
         packagesUrl as string,
      );
      expect(packages.map((entry) => entry.name)).toContain("sales");
   });

   test("the MCP url this run wrote accepts a connection", async () => {
      if (unusable !== undefined) {
         throw new Error(unusable);
      }
      const url = new URL(mcpUrl as string);
      expect(await accepts(url.hostname, Number(url.port))).toBe(true);
   });
});
