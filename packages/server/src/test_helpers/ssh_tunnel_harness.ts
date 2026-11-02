// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// In-process ssh2 bastion and TCP echo server shared by the tunnel specs.

import net from "net";
import { generateKeyPairSync } from "crypto";
import {
   Server as SshServer,
   utils as sshUtils,
   type Connection as SshServerConnection,
} from "ssh2";

// ssh2's server requires PEM private keys it can parse. These are ephemeral,
// test-only keys; RSA-2048 (not 1024) so static analysis doesn't flag them.
const hostKeys = generateKeyPairSync("rsa", { modulusLength: 2048 });
const hostPrivatePem = hostKeys.privateKey
   .export({ type: "pkcs1", format: "pem" })
   .toString();

const clientKeys = generateKeyPairSync("rsa", { modulusLength: 2048 });
export const clientPrivatePem = clientKeys.privateKey
   .export({ type: "pkcs1", format: "pem" })
   .toString();
export function startEchoServer(): Promise<{
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
export function startSshServer(opts: {
   rejectAuth?: boolean;
   rejectForward?: boolean;
   endOnAuth?: boolean;
}): Promise<{
   port: number;
   hostKeyBase64: string;
   endClients: () => void;
   destroySockets: () => void;
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
               if (opts.endOnAuth) {
                  client.end();
                  return;
               }
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
            endClients: () => {
               for (const c of clients) c.end();
            },
            destroySockets: () => {
               // ssh2 exposes no public handle on a client's transport socket.
               for (const c of clients)
                  (c as unknown as { _sock: net.Socket })._sock.destroy();
            },
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
