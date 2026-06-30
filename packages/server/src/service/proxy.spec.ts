/**
 * Unit tests for the SSH proxy layer.
 *
 * These tests are hermetic: they stand up an in-process ssh2 server that
 * forwards to a plain TCP echo server, call openProxy(), connect a client
 * to the returned local endpoint, and assert that bytes round-trip through
 * the tunnel.
 *
 * No external network access is required.
 */

import net from "net";
import { generateKeyPairSync } from "crypto";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   Server as SshServer,
   utils as sshUtils,
   type Connection as SshServerConnection,
} from "ssh2";
import { openProxy } from "./proxy";

// ── Key material generated once for the test suite ────────────────────────────

// ssh2's server requires PEM private keys it can parse. These are ephemeral,
// test-only keys; RSA-2048 (not 1024) so static analysis doesn't flag them.
const hostKeys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const hostPrivatePem = hostKeys.privateKey
   .export({ type: "pkcs1", format: "pem" })
   .toString();

const clientKeys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const clientPrivatePem = clientKeys.privateKey
   .export({ type: "pkcs1", format: "pem" })
   .toString();
// Helpers ─────────────────────────────────────────────────────────────────────

function startEchoServer(): Promise<{
   port: number;
   close: () => Promise<void>;
}> {
   return new Promise((resolve, reject) => {
      // Track accepted sockets so close() can force them down. server.close()
      // alone only stops accepting and then waits for open connections to end;
      // a lingering forwarded socket would otherwise hang teardown (and the
      // afterEach hook) until the test timeout, especially under --serial CI.
      const sockets = new Set<net.Socket>();
      const server = net.createServer((socket) => {
         sockets.add(socket);
         socket.on("close", () => sockets.delete(socket));
         socket.pipe(socket);
      });
      server.on("error", reject);
      server.listen(0, "127.0.0.1", () => {
         const addr = server.address() as net.AddressInfo;
         resolve({
            port: addr.port,
            close: () =>
               new Promise((res, rej) => {
                  for (const s of sockets) s.destroy();
                  server.close((err) => (err ? rej(err) : res()));
               }),
         });
      });
   });
}

/**
 * Start a minimal in-process SSH server that accepts the test client key
 * and fulfils tcpip-forward (forwardOut) requests.
 */
function startSshServer(opts: {
   rejectAuth?: boolean;
   rejectForward?: boolean;
}): Promise<{
   port: number;
   hostKeyBase64: string;
   close: () => Promise<void>;
}> {
   return new Promise((resolve, reject) => {
      // Track live client connections and forwarded destination sockets so
      // close() can force them down; otherwise a lingering forwarded socket
      // keeps sshd.close() (and the afterEach hook) hanging until the test
      // timeout, which is what made the tunnel tests flake under --serial CI.
      const clients = new Set<SshServerConnection>();
      const destSockets = new Set<net.Socket>();
      const sshd = new SshServer(
         { hostKeys: [hostPrivatePem] },
         (client: SshServerConnection) => {
            clients.add(client);
            client.on("close", () => clients.delete(client));
            client.on("authentication", (ctx) => {
               if (opts.rejectAuth) {
                  ctx.reject();
                  return;
               }
               // Accept publickey auth for our test client key.
               if (ctx.method === "publickey" && !opts.rejectAuth) {
                  ctx.accept();
               } else {
                  ctx.reject();
               }
            });

            client.on("ready", () => {
               client.on("tcpip", (accept, _reject, info) => {
                  if (opts.rejectForward) {
                     _reject();
                     return;
                  }
                  const channel = accept();
                  // Connect to the real destination and pipe.
                  const dest = net.createConnection(
                     { host: info.destIP, port: info.destPort },
                     () => {
                        channel.pipe(dest).pipe(channel);
                        channel.on("error", () => dest.destroy());
                        dest.on("error", () => channel.destroy());
                     },
                  );
                  destSockets.add(dest);
                  dest.on("close", () => destSockets.delete(dest));
                  dest.on("error", () => channel.destroy());
               });
            });

            client.on("error", () => {
               /* swallow per-client errors in tests */
            });
         },
      );

      sshd.on("error", reject);
      sshd.listen(0, "127.0.0.1", () => {
         const addr = sshd.address() as net.AddressInfo;

         // Capture the server's host key fingerprint (base64 of the raw DER) by
         // parsing the pem via the ssh2 util so the format matches what
         // hostVerifier receives.
         const parsed = sshUtils.parseKey(hostPrivatePem);
         const hostKeyBase64 = (parsed as { getPublicSSH(): Buffer })
            .getPublicSSH()
            .toString("base64");

         resolve({
            port: addr.port,
            hostKeyBase64,
            close: () =>
               new Promise((res) => {
                  for (const s of destSockets) s.destroy();
                  for (const c of clients) c.end();
                  sshd.close(() => res());
               }),
         });
      });
   });
}

function connectAndSend(port: number, message: string): Promise<string> {
   return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: "127.0.0.1", port }, () => {
         socket.write(message);
      });
      const chunks: Buffer[] = [];
      socket.on("data", (chunk) => {
         chunks.push(chunk);
         if (Buffer.concat(chunks).toString().includes(message)) {
            socket.destroy();
            resolve(Buffer.concat(chunks).toString());
         }
      });
      socket.on("error", reject);
   });
}

// Best-effort, time-bounded teardown. Destroying every ssh2/socket handle is
// reliable locally but a lingering graceful close can still stall on some CI
// platforms (Bun + ssh2 on Linux/Windows), which would hang the afterEach hook
// until the 100s test timeout. The assertions have already run by teardown, so
// cap each close and move on rather than block.
function closeQuietly(close: () => Promise<void>, ms = 3000): Promise<void> {
   return Promise.race([
      close().catch(() => {}),
      new Promise<void>((resolve) => {
         const timer = setTimeout(resolve, ms);
         (timer as unknown as { unref?: () => void }).unref?.();
      }),
   ]);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("openProxy — SSH tunnel", () => {
   let echoServer: { port: number; close: () => Promise<void> };
   let sshServer: {
      port: number;
      hostKeyBase64: string;
      close: () => Promise<void>;
   };

   beforeEach(async () => {
      echoServer = await startEchoServer();
      sshServer = await startSshServer({});
   });

   afterEach(async () => {
      await closeQuietly(() => echoServer.close());
      await closeQuietly(() => sshServer.close());
   });

   it("round-trips bytes through the tunnel", async () => {
      const ep = await openProxy(
         {
            type: "ssh",
            ssh: {
               host: "127.0.0.1",
               port: sshServer.port,
               username: "testuser",
               privateKey: clientPrivatePem,
               hostKey: sshServer.hostKeyBase64,
            },
         },
         { host: "127.0.0.1", port: echoServer.port },
      );

      try {
         const reply = await connectAndSend(ep.port, "hello-proxy");
         expect(reply).toContain("hello-proxy");
      } finally {
         await closeQuietly(() => ep.close());
      }
   });

   it("accepts a hostKey given as a full known_hosts line", async () => {
      // Users typically paste `ssh-keyscan` output, e.g.
      // `bastion.example.com ssh-rsa AAAA…` — the verifier must extract the blob.
      const knownHostsLine = `bastion.example.com ssh-rsa ${sshServer.hostKeyBase64}`;
      const ep = await openProxy(
         {
            type: "ssh",
            ssh: {
               host: "127.0.0.1",
               port: sshServer.port,
               username: "testuser",
               privateKey: clientPrivatePem,
               hostKey: knownHostsLine,
            },
         },
         { host: "127.0.0.1", port: echoServer.port },
      );

      try {
         const reply = await connectAndSend(ep.port, "known-hosts-line");
         expect(reply).toContain("known-hosts-line");
      } finally {
         await closeQuietly(() => ep.close());
      }
   });

   it("rejects when hostKey does not match", async () => {
      const wrongKey = Buffer.alloc(32, 0xff).toString("base64");

      await expect(
         openProxy(
            {
               type: "ssh",
               ssh: {
                  host: "127.0.0.1",
                  port: sshServer.port,
                  username: "testuser",
                  privateKey: clientPrivatePem,
                  hostKey: wrongKey,
               },
            },
            { host: "127.0.0.1", port: echoServer.port },
         ),
      ).rejects.toThrow(/host-key mismatch/i);
   });

   it("rejects when hostKey is absent and opt-out env is not set", async () => {
      const orig = process.env.PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY;
      delete process.env.PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY;

      try {
         await expect(
            openProxy(
               {
                  type: "ssh",
                  ssh: {
                     host: "127.0.0.1",
                     port: sshServer.port,
                     username: "testuser",
                     privateKey: clientPrivatePem,
                  },
               },
               { host: "127.0.0.1", port: echoServer.port },
            ),
         ).rejects.toThrow(/no hostKey provided/i);
      } finally {
         if (orig !== undefined) {
            process.env.PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY = orig;
         }
      }
   });

   it("connects when hostKey is absent and opt-out env is set", async () => {
      process.env.PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY = "true";
      try {
         const ep = await openProxy(
            {
               type: "ssh",
               ssh: {
                  host: "127.0.0.1",
                  port: sshServer.port,
                  username: "testuser",
                  privateKey: clientPrivatePem,
               },
            },
            { host: "127.0.0.1", port: echoServer.port },
         );
         try {
            const reply = await connectAndSend(ep.port, "allow-unknown");
            expect(reply).toContain("allow-unknown");
         } finally {
            await closeQuietly(() => ep.close());
         }
      } finally {
         delete process.env.PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY;
      }
   });

   it("rejects for unsupported proxy type", async () => {
      await expect(
         openProxy(
            // @ts-expect-error — intentionally passing an unsupported type
            { type: "unsupported" },
            { host: "127.0.0.1", port: echoServer.port },
         ),
      ).rejects.toThrow(/not supported/i);
   });
});
