// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Finding F-9: SSH proxy host-key verification defaults to accept-any.
//
// In `openSshProxy` the ssh2 `hostVerifier` returns `true` unconditionally when
// `ssh.hostKey` is unset, so an unpinned tunnel silently accepts whatever host
// key the far end presents -- a man-in-the-middle on the publisher-to-bastion hop
// is undetectable.
//
// The desired secure contract asserted below: with no host key pinned and no
// explicit opt-in to skip verification, the tunnel must NOT silently accept an
// arbitrary host key -- `openProxy` must fail closed. The fix is pin-by-default
// (reject an unverified host key unless a host key is pinned) or an explicit
// opt-in field on the SSH config that a caller must set to accept an unverified
// key. This test pins neither a host key nor any opt-in, so it stays correct
// whichever fix is chosen, and is red against the current accept-any default,
// which is the proof.
//
// The suite is hermetic (an in-process ssh2 server forwarding to a TCP echo
// server); it mirrors the harness in proxy.spec.ts. No external network access.

import net from "net";
import { generateKeyPairSync } from "crypto";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   Server as SshServer,
   type Connection as SshServerConnection,
} from "ssh2";
import { openProxy } from "./proxy";

// Ephemeral test-only key material. RSA-2048 (not 1024) so static analysis does
// not flag it.
const hostKeys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const hostPrivatePem = hostKeys.privateKey
   .export({ type: "pkcs1", format: "pem" })
   .toString();

const clientKeys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const clientPrivatePem = clientKeys.privateKey
   .export({ type: "pkcs1", format: "pem" })
   .toString();

function startEchoServer(): Promise<{
   port: number;
   close: () => Promise<void>;
}> {
   return new Promise((resolve, reject) => {
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
 * Minimal in-process SSH server that accepts the test client key and fulfils
 * forwardOut requests. Its host key is deliberately NOT pinned by the tests
 * below: the point is that an unpinned tunnel must not silently accept it.
 */
function startSshServer(): Promise<{
   port: number;
   close: () => Promise<void>;
}> {
   return new Promise((resolve, reject) => {
      const clients = new Set<SshServerConnection>();
      const destSockets = new Set<net.Socket>();
      const sshd = new SshServer(
         { hostKeys: [hostPrivatePem] },
         (client: SshServerConnection) => {
            clients.add(client);
            client.on("close", () => clients.delete(client));
            client.on("authentication", (ctx) => {
               if (ctx.method === "publickey") {
                  ctx.accept();
               } else {
                  ctx.reject();
               }
            });
            client.on("ready", () => {
               client.on("tcpip", (accept, _reject, info) => {
                  const channel = accept();
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
         resolve({
            port: addr.port,
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

// Best-effort, time-bounded teardown (mirrors proxy.spec.ts): destroying every
// handle is reliable locally but a lingering graceful close can stall on some CI
// platforms and would hang the afterEach hook until the test timeout.
function closeQuietly(close: () => Promise<void>, ms = 3000): Promise<void> {
   return Promise.race([
      close().catch(() => {}),
      new Promise<void>((resolve) => {
         const timer = setTimeout(resolve, ms);
         (timer as unknown as { unref?: () => void }).unref?.();
      }),
   ]);
}

describe("openProxy -- unpinned host-key verification (F-9)", () => {
   let echoServer: { port: number; close: () => Promise<void> };
   let sshServer: { port: number; close: () => Promise<void> };

   beforeEach(async () => {
      echoServer = await startEchoServer();
      sshServer = await startSshServer();
   });

   afterEach(async () => {
      await closeQuietly(() => echoServer.close());
      await closeQuietly(() => sshServer.close());
   });

   it("does not silently accept an arbitrary host key when none is pinned", async () => {
      // No hostKey and no opt-in to skip verification: the tunnel must fail
      // closed rather than accept whatever key the far end presents.
      let endpoint: { close: () => Promise<void> } | undefined;
      try {
         await expect(
            (async () => {
               endpoint = await openProxy(
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
            })(),
         ).rejects.toThrow();
      } finally {
         if (endpoint) await closeQuietly(() => endpoint!.close());
      }
   });
});
