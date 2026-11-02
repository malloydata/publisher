// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import {
   ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV,
   allowUnverifiedHostKey,
   openProxy,
   type ProxyEndpoint,
} from "./proxy";
import { logger } from "../logger";
import {
   clientPrivatePem,
   closeQuietly,
   startEchoServer,
   startSshServer,
} from "../test_helpers/ssh_tunnel_harness";
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

function openTestProxy(
   server: { port: number; hostKeyBase64: string },
   targetPort: number,
): Promise<ProxyEndpoint> {
   return openProxy(
      {
         type: "ssh",
         ssh: {
            host: "127.0.0.1",
            port: server.port,
            username: "testuser",
            privateKey: clientPrivatePem,
            hostKey: server.hostKeyBase64,
         },
      },
      { host: "127.0.0.1", port: targetPort },
   );
}

// Resolves when the local socket closes; an error with no listener would fail
// the test as an uncaught exception, so one is always attached.
function dialAndAwaitClose(port: number, timeoutMs = 3000): Promise<void> {
   return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: "127.0.0.1", port });
      const timer = setTimeout(() => {
         socket.destroy();
         reject(new Error("local socket was not closed"));
      }, timeoutMs);
      socket.on("error", () => {});
      socket.on("close", () => {
         clearTimeout(timer);
         resolve();
      });
   });
}

function nextClose(ep: ProxyEndpoint): Promise<Error | undefined> {
   return new Promise((resolve) => ep.onClose(resolve));
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("openProxy — SSH tunnel", () => {
   let echoServer: { port: number; close: () => Promise<void> };
   let sshServer: Awaited<ReturnType<typeof startSshServer>>;

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

   it("accepts a hashed (|1|) known_hosts line — the hash only obscures the hostname", async () => {
      // `ssh-keyscan -H` output hashes the hostname (`|1|salt|hash`) but leaves
      // the key blob in the clear; the verifier extracts the blob token and never
      // matches hostnames, so hashed lines work the same as unhashed ones.
      const hashedLine = `|1|dNQdBg9dg8u7Vw8vq3B0uZ3example=|kZ2exampleHashValue0000000= ssh-rsa ${sshServer.hostKeyBase64}`;
      const ep = await openProxy(
         {
            type: "ssh",
            ssh: {
               host: "127.0.0.1",
               port: sshServer.port,
               username: "testuser",
               privateKey: clientPrivatePem,
               hostKey: hashedLine,
            },
         },
         { host: "127.0.0.1", port: echoServer.port },
      );

      try {
         const reply = await connectAndSend(ep.port, "hashed-line");
         expect(reply).toContain("hashed-line");
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
      ).rejects.toThrow(/host-key verification failed/i);
   });

   it("fails closed when hostKey is absent and the unverified opt-in is off", async () => {
      // No hostKey and no opt-in: an unverified host key admits a MITM on the
      // publisher->bastion hop, so the tunnel is refused rather than connected.
      const prev = process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
      delete process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
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
         ).rejects.toThrow(/host-key verification is required/);
      } finally {
         if (prev === undefined)
            delete process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
         else process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY = prev;
      }
   });

   it("refuses an unpinned tunnel without dialing the bastion", async () => {
      // The refusal must happen before the TCP connect, not in ssh2's
      // hostVerifier: unpinned-with-no-opt-in is a permanent misconfiguration, a
      // rejected build is evicted from the connection cache, and the passthrough
      // federate caller is not cached at all -- so a post-dial refusal re-pays a
      // real connect and key exchange against the tenant's bastion on every
      // query. Counting accepts on a stand-in listener is what distinguishes the
      // two positions; the message assertion above passes either way.
      const prev = process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
      delete process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
      let accepts = 0;
      const standIn = net.createServer((socket) => {
         accepts += 1;
         socket.destroy();
      });
      await new Promise<void>((done) => standIn.listen(0, "127.0.0.1", done));
      const standInPort = (standIn.address() as net.AddressInfo).port;
      try {
         await expect(
            openProxy(
               {
                  type: "ssh",
                  ssh: {
                     host: "127.0.0.1",
                     port: standInPort,
                     username: "testuser",
                     privateKey: clientPrivatePem,
                  },
               },
               { host: "127.0.0.1", port: echoServer.port },
            ),
         ).rejects.toThrow(/host-key verification is required/);
         expect(accepts).toBe(0);
      } finally {
         if (prev === undefined)
            delete process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
         else process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY = prev;
         await new Promise<void>((done) => standIn.close(() => done()));
      }
   });

   it("connects unpinned when the unverified opt-in is set", async () => {
      // The operator escape hatch: with PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY
      // set, an unpinned tunnel connects (the SSH transport is still encrypted).
      const prev = process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
      process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY = "true";
      let ep: ProxyEndpoint | undefined;
      try {
         ep = await openProxy(
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
         const reply = await connectAndSend(ep.port, "unpinned");
         expect(reply).toContain("unpinned");
      } finally {
         const opened = ep;
         if (opened) await closeQuietly(() => opened.close());
         if (prev === undefined)
            delete process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY;
         else process.env.PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY = prev;
      }
   });

   it("accepts a multi-line hostKey listing several keys — matches any (LB bastion)", async () => {
      // A load-balanced bastion presents a different host key per backend, so the
      // pin lists every backend's key and any match is accepted.
      const decoy = Buffer.alloc(32, 0xff).toString("base64");
      const multiKey = [
         `bastion.example.com ssh-ed25519 ${decoy}`,
         `bastion.example.com ssh-rsa ${sshServer.hostKeyBase64}`,
      ].join("\n");
      const ep = await openProxy(
         {
            type: "ssh",
            ssh: {
               host: "127.0.0.1",
               port: sshServer.port,
               username: "testuser",
               privateKey: clientPrivatePem,
               hostKey: multiKey,
            },
         },
         { host: "127.0.0.1", port: echoServer.port },
      );
      try {
         const reply = await connectAndSend(ep.port, "multi-key");
         expect(reply).toContain("multi-key");
      } finally {
         await closeQuietly(() => ep.close());
      }
   });

   it("fails closed when hostKey is set but parses to zero keys (comment/blank only)", async () => {
      // A set-but-degenerate pin must NOT fall through to the unpinned branch —
      // the security boundary is gated on hostKey being configured, not on it
      // parsing to >=1 key. (Config load rejects this earlier; this guards the
      // boundary itself.)
      const commentOnly = "# ssh-keyscan bastion.example.com timed out\n   \n";
      await expect(
         openProxy(
            {
               type: "ssh",
               ssh: {
                  host: "127.0.0.1",
                  port: sshServer.port,
                  username: "testuser",
                  privateKey: clientPrivatePem,
                  hostKey: commentOnly,
               },
            },
            { host: "127.0.0.1", port: echoServer.port },
         ),
      ).rejects.toThrow(/host-key verification failed/i);
   });

   it("rejects a multi-line hostKey when none of the listed keys match", async () => {
      const decoy1 = Buffer.alloc(32, 0xff).toString("base64");
      const decoy2 = Buffer.alloc(32, 0xaa).toString("base64");
      const multiKey = [
         `bastion.example.com ssh-ed25519 ${decoy1}`,
         `bastion.example.com ssh-rsa ${decoy2}`,
      ].join("\n");
      await expect(
         openProxy(
            {
               type: "ssh",
               ssh: {
                  host: "127.0.0.1",
                  port: sshServer.port,
                  username: "testuser",
                  privateKey: clientPrivatePem,
                  hostKey: multiKey,
               },
            },
            { host: "127.0.0.1", port: echoServer.port },
         ),
      ).rejects.toThrow(/host-key verification failed/i);
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

describe("openProxy - tunnel death", () => {
   let echoServer: { port: number; close: () => Promise<void> };
   let sshServer: Awaited<ReturnType<typeof startSshServer>>;
   let warn: ReturnType<typeof spyOn<typeof logger, "warn">>;

   beforeEach(async () => {
      echoServer = await startEchoServer();
      warn = spyOn(logger, "warn").mockImplementation(() => logger);
   });

   afterEach(async () => {
      warn.mockRestore();
      await closeQuietly(() => echoServer.close());
      await closeQuietly(() => sshServer.close());
   });

   for (const [label, kill] of [
      ["the server ends the client", (s: typeof sshServer) => s.endClients()],
      ["the socket is destroyed", (s: typeof sshServer) => s.destroySockets()],
   ] as const) {
      it(`fires onClose once, warns once and refuses new dials when ${label}`, async () => {
         sshServer = await startSshServer({});
         const ep = await openTestProxy(sshServer, echoServer.port);
         try {
            const calls: (Error | undefined)[] = [];
            ep.onClose((cause) => calls.push(cause));
            const closed = nextClose(ep);
            kill(sshServer);
            await closed;
            expect(calls).toHaveLength(1);
            expect(warn).toHaveBeenCalledTimes(1);
            const message = String(warn.mock.calls[0]?.[0]);
            expect(message).toContain("127.0.0.1");
            expect(message).toContain(String(echoServer.port));
            // The listener stays bound so the port cannot be reused by another tunnel.
            await dialAndAwaitClose(ep.port);
            expect(calls).toHaveLength(1);
         } finally {
            await closeQuietly(() => ep.close());
         }
      });
   }

   it("closes the local socket and stays alive when the server rejects direct-tcpip", async () => {
      sshServer = await startSshServer({ rejectForward: true });
      const ep = await openTestProxy(sshServer, echoServer.port);
      try {
         await dialAndAwaitClose(ep.port);
         // The tunnel itself is still usable for the next dial.
         await dialAndAwaitClose(ep.port);
      } finally {
         await closeQuietly(() => ep.close());
      }
   });

   it("rejects when the bastion closes the session before the tunnel is ready", async () => {
      sshServer = await startSshServer({ endOnAuth: true });
      await expect(openTestProxy(sshServer, echoServer.port)).rejects.toThrow();
   });

   it("neither fires onClose nor warns on its own close(), and close() is idempotent", async () => {
      sshServer = await startSshServer({});
      const ep = await openTestProxy(sshServer, echoServer.port);
      let fired = 0;
      ep.onClose(() => (fired += 1));
      await ep.close();
      await ep.close();
      await new Promise((r) => setTimeout(r, 100));
      expect(fired).toBe(0);
      expect(warn).not.toHaveBeenCalled();
   });

   it("fires an onClose listener registered after the tunnel died", async () => {
      sshServer = await startSshServer({});
      const ep = await openTestProxy(sshServer, echoServer.port);
      try {
         const first = nextClose(ep);
         sshServer.endClients();
         await first;
         const late = await nextClose(ep);
         expect(late === undefined || late instanceof Error).toBe(true);
      } finally {
         await closeQuietly(() => ep.close());
      }
   });

   it("does not fire a late onClose listener once close() has run", async () => {
      sshServer = await startSshServer({});
      const ep = await openTestProxy(sshServer, echoServer.port);
      const first = nextClose(ep);
      sshServer.endClients();
      await first;
      let fired = 0;
      ep.onClose(() => (fired += 1));
      await closeQuietly(() => ep.close());
      await new Promise((r) => setTimeout(r, 50));
      expect(fired).toBe(0);
   });
});

// The opt-in accepts the spellings every other boolean flag in this server
// accepts, and falls back to the secure default on anything else. It cannot
// throw: one caller is ssh2's hostVerifier, a synchronous callback inside a
// Promise constructor, where a throw escapes past fail() and settles the
// connection through a path this module does not control. The loud rejection of
// a misspelling lives at config load instead.
describe("allowUnverifiedHostKey env parsing", () => {
   const withEnv = (value: string | undefined, run: () => void) => {
      const prev = process.env[ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV];
      if (value === undefined)
         delete process.env[ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV];
      else process.env[ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV] = value;
      try {
         run();
      } finally {
         if (prev === undefined)
            delete process.env[ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV];
         else process.env[ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV] = prev;
      }
   };

   for (const value of ["true", "TRUE", " true ", "1", "yes", "on"]) {
      it(`opts in on ${JSON.stringify(value)}`, () => {
         withEnv(value, () => expect(allowUnverifiedHostKey()).toBe(true));
      });
   }

   for (const value of ["false", "0", "no", "off", "", "ture", "maybe"]) {
      it(`stays fail-closed on ${JSON.stringify(value)}`, () => {
         withEnv(value, () => expect(allowUnverifiedHostKey()).toBe(false));
      });
   }

   it("stays fail-closed when unset", () => {
      withEnv(undefined, () => expect(allowUnverifiedHostKey()).toBe(false));
   });

   it("does not throw on an unrecognised value", () => {
      // The property the hostVerifier depends on.
      withEnv("ture", () =>
         expect(() => allowUnverifiedHostKey()).not.toThrow(),
      );
   });
});
