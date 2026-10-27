// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * TCP proxy layer for publisher database connections. Only SSH bastion tunnels
 * (type "ssh") exist; another type needs only a new `openProxy` branch.
 *
 * A connection proxy is an ordinary connection capability, authorized by
 * whoever configures the connection, so it is deliberately not behind an env
 * gate (unlike the `publisher` type's PUBLISHER_ALLOW_PROXY_CONNECTIONS).
 *
 * Host-key policy: `ssh.hostKey` pins one or more bastion host keys and the
 * tunnel fails closed on mismatch. Without it the tunnel is refused, because
 * an unverified host key makes a MITM on the publisher-to-bastion hop
 * undetectable; an operator who accepts that risk (e.g. bastion egress is
 * allowlisted) sets PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY=true. The SSH
 * transport is encrypted either way.
 */

import { settings } from "../settings";
import net from "net";
import { Client as SshClient } from "ssh2";
import type { ConnectConfig, SyncHostVerifier } from "ssh2";
import { components } from "../api";
import { logger } from "../logger";

type ConnectionProxy = components["schemas"]["ConnectionProxy"];

// Off by default (fail closed). Accepting an unverified host key is an operator
// posture, so it is an env flag rather than a per-connection spec field.
export const ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV =
   "PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY";

/**
 * Whether this deployment accepts an unverified bastion host key. Accepts the
 * same spellings as every other boolean flag (`1`, `true`, `yes`, `on`).
 *
 * Never throws, though the setting does on an unrecognised value: one caller is
 * ssh2's synchronous `hostVerifier`, where a throw escapes past `fail()` and
 * loses the descriptive refusal. An unrecognised value falls back to the
 * secure default here; validateConnectionShape rejects it loudly at config
 * load.
 */
export function allowUnverifiedHostKey(): boolean {
   try {
      return settings.get("allow.unverifiedSshHostKey");
   } catch {
      return false;
   }
}

function unpinnedRefusal(host: string): Error {
   return new Error(
      `SSH host-key verification is required for ${host}: no hostKey is ` +
         `pinned. Pin the bastion host key on the connection's ssh.hostKey, or ` +
         `set ${ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV}=true to accept an unverified ` +
         `key (a MITM on the publisher-to-bastion hop is then undetectable).`,
   );
}

export interface ProxyEndpoint {
   host: string;
   port: number;
   close(): Promise<void>;
}

const SSH_CONNECT_TIMEOUT_MS = 15_000;
const SSH_KEEPALIVE_INTERVAL_MS = 15_000;

/**
 * Parse a stored hostKey into the base64 key blobs to accept, compared against
 * ssh2's `key.toString("base64")`. One known_hosts entry per line (a
 * load-balanced bastion presents a different key per backend); blank lines and
 * `#` comments are skipped. Each entry may be a full known_hosts line, a
 * `keytype AAAA…` pair, or a bare blob. Key blobs start with "AAAA" (the
 * length prefix of the key-type name), so that token wins, else the last one.
 */
export function parseHostKeys(raw: string): Set<string> {
   const keys = new Set<string>();
   for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const tokens = trimmed.split(/\s+/);
      const blob = tokens.find((t) => /^AAAA[A-Za-z0-9+/]+={0,2}$/.test(t));
      const key = blob ?? tokens[tokens.length - 1];
      if (key) keys.add(key);
   }
   return keys;
}

/**
 * Open a proxy tunnel described by `proxy` that ultimately delivers traffic to
 * `target`.  Returns a local `127.0.0.1:port` endpoint the DB driver should
 * connect to in place of the real host/port.
 */
export async function openProxy(
   proxy: ConnectionProxy,
   target: { host: string; port: number },
): Promise<ProxyEndpoint> {
   if (proxy.type === "ssh") {
      if (!proxy.ssh) {
         throw new Error(
            "ConnectionProxy type is 'ssh' but the 'ssh' config object is missing.",
         );
      }
      return openSshProxy(proxy.ssh, target);
   }
   throw new Error(
      `Proxy type '${proxy.type}' is not supported yet.  Only 'ssh' is implemented.`,
   );
}

function openSshProxy(
   ssh: components["schemas"]["SshProxyConfig"],
   target: { host: string; port: number },
): Promise<ProxyEndpoint> {
   return new Promise((resolve, reject) => {
      // Refuse an unpinned tunnel before dialing. It is a permanent
      // misconfiguration and failed builds are not cached, so leaving it to the
      // hostVerifier would cost a TCP connect and key exchange against the
      // bastion on every query, and a stalled handshake would bury the message
      // under a timeout. The hostVerifier branch stays as the backstop.
      if (!ssh.hostKey && !allowUnverifiedHostKey()) {
         reject(unpinnedRefusal(ssh.host));
         return;
      }
      const client = new SshClient();
      let localPort = 0;
      let server: net.Server | undefined;
      let settled = false;
      // Tracked so close() can force them down; server.close() alone waits for
      // sockets to drain and can hang teardown indefinitely.
      const sockets = new Set<net.Socket>();

      function fail(err: Error): void {
         if (settled) return;
         settled = true;
         server?.close();
         client.end();
         reject(err);
      }

      const connectConfig: ConnectConfig = {
         host: ssh.host,
         port: ssh.port ?? 22,
         username: ssh.username,
         privateKey: ssh.privateKey,
         ...(ssh.privateKeyPass ? { passphrase: ssh.privateKeyPass } : {}),
         readyTimeout: SSH_CONNECT_TIMEOUT_MS,
         // Keepalives stop NAT / firewall idle timeouts from silently reaping an
         // idle tunnel, which would surface as a confusing pg error on the next
         // query. 3 missed (~45s) and ssh2 considers the link dead.
         keepaliveInterval: SSH_KEEPALIVE_INTERVAL_MS,
         keepaliveCountMax: 3,
         hostVerifier: ((key: Buffer): boolean => {
            const presented = key.toString("base64");
            // "Pinned" means hostKey is non-empty, not that it parses to a key:
            // a hostKey of only blanks or comments must fail closed, never fall
            // through to the unpinned branch. Config load rejects that case too;
            // this is the backstop.
            if (ssh.hostKey) {
               const pinned = parseHostKeys(ssh.hostKey);
               if (pinned.has(presented)) {
                  return true;
               }
               fail(
                  new Error(
                     `SSH host-key verification failed for ${ssh.host}: the presented key ` +
                        `(${presented.slice(0, 24)}…) is not among the ${pinned.size} pinned ` +
                        `host key(s).`,
                  ),
               );
               return false;
            }
            // Unpinned: fail closed unless the deployment opted in (see header).
            if (allowUnverifiedHostKey()) {
               logger.warn(
                  `Connecting to SSH bastion ${ssh.host} without host-key verification ` +
                     `(no hostKey pinned; ${ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV}=true).`,
               );
               return true;
            }
            fail(unpinnedRefusal(ssh.host));
            return false;
         }) as SyncHostVerifier,
      };

      client.on("error", (err) => fail(err));

      client.on("ready", () => {
         server = net.createServer((socket) => {
            sockets.add(socket);
            socket.on("close", () => sockets.delete(socket));
            // Buffer incoming data immediately so we don't lose bytes that
            // arrive before the forwardOut channel is open (relevant in Bun
            // where resume() doesn't re-emit already-buffered data).
            const earlyData: Buffer[] = [];
            socket.on("data", (chunk: Buffer) => earlyData.push(chunk));

            client.forwardOut(
               "127.0.0.1",
               localPort,
               target.host,
               target.port,
               (err, channel) => {
                  if (err) {
                     socket.destroy(err);
                     return;
                  }
                  // The socket may have closed while the channel opened; attaching
                  // to it would leak the remote connection. The rest of this
                  // callback is synchronous, so no close can slip in after this.
                  if (socket.destroyed) {
                     channel.destroy();
                     return;
                  }
                  for (const chunk of earlyData) {
                     channel.write(chunk);
                  }
                  earlyData.length = 0;
                  socket.removeAllListeners("data");
                  // pipe() honors backpressure; on('data') => write() would
                  // buffer unbounded on a slow peer.
                  socket.pipe(channel);
                  channel.pipe(socket);
                  socket.on("error", () => channel.destroy());
                  channel.on("error", () => socket.destroy());
                  // Propagate teardown both ways so neither side is left half-open.
                  socket.on("close", () => channel.destroy());
                  channel.on("close", () => socket.destroy());
               },
            );
         });

         server.listen(0, "127.0.0.1", () => {
            const addr = server!.address();
            if (!addr || typeof addr === "string") {
               fail(
                  new Error(
                     "Failed to obtain local listen port for SSH proxy.",
                  ),
               );
               return;
            }
            localPort = addr.port;

            if (settled) return;
            settled = true;

            resolve({
               host: "127.0.0.1",
               port: localPort,
               close(): Promise<void> {
                  return new Promise((res) => {
                     // Destroy live sockets first or server.close() waits on them.
                     for (const s of sockets) s.destroy();
                     server!.close(() => {
                        client.end();
                        res();
                     });
                  });
               },
            });
         });

         server.on("error", (err) => fail(err));
      });

      client.connect(connectConfig);
   });
}
