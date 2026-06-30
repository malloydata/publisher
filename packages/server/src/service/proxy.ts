/**
 * Generic TCP proxy layer for publisher database connections.
 *
 * Currently supports SSH bastion tunnels (type "ssh").  Future connection
 * types (mysql, trino, …) only need a new `openProxy` branch — the caller
 * interface is unchanged.
 *
 * # Operator opt-in
 *
 * The proxy feature is default-deny (SSRF surface). Set
 *
 *   PUBLISHER_ALLOW_SSH_PROXY=true
 *
 * to enable it. This is a separate gate from PUBLISHER_ALLOW_PROXY_CONNECTIONS
 * (which controls the publisher HTTP multi-hop type).
 *
 * # Host-key policy
 *
 * Publisher is fail-closed by default: if `ssh.hostKey` is absent the tunnel
 * is refused at connect time to prevent MITM.  Set the environment variable
 *
 *   PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY=true
 *
 * to disable this check for development/testing.  Never set this in
 * production.
 */

import net from "net";
import { Client as SshClient } from "ssh2";
import type { ConnectConfig, SyncHostVerifier } from "ssh2";
import { components } from "../api";

type ConnectionProxy = components["schemas"]["ConnectionProxy"];

export interface ProxyEndpoint {
   host: string;
   port: number;
   close(): Promise<void>;
}

const SSH_CONNECT_TIMEOUT_MS = 15_000;

/**
 * Extract the base64 wire-format key from a stored hostKey, accepting any of:
 * a full known_hosts line (`[markers] host keytype AAAA…`), a `keytype AAAA…`
 * pair, or the bare base64 blob. SSH public-key blobs always base64-encode to a
 * string starting with "AAAA" (the 4-byte length prefix of the key-type name),
 * so we pick that token; otherwise fall back to the last whitespace-delimited
 * token. This matches what ssh2's hostVerifier hands us (`key.toString("base64")`).
 */
function extractHostKeyBase64(raw: string): string {
   const tokens = raw.trim().split(/\s+/);
   const blob = tokens.find((t) => /^AAAA[A-Za-z0-9+/]+={0,2}$/.test(t));
   return blob ?? tokens[tokens.length - 1] ?? "";
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
      const client = new SshClient();
      let localPort = 0;
      let server: net.Server | undefined;
      let settled = false;

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
         hostVerifier: ((key: Buffer): boolean => {
            // `key` is the raw Buffer of the host's public key.
            const presented = key.toString("base64");
            if (ssh.hostKey) {
               const expected = extractHostKeyBase64(ssh.hostKey);
               if (presented !== expected) {
                  fail(
                     new Error(
                        `SSH host-key mismatch for ${ssh.host}: expected ${expected.slice(0, 24)}… but got ${presented.slice(0, 24)}….`,
                     ),
                  );
                  return false;
               }
               return true;
            }
            // No hostKey provided.
            if (process.env.PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY === "true") {
               return true;
            }
            fail(
               new Error(
                  `SSH connection to ${ssh.host} refused: no hostKey provided and ` +
                     `PUBLISHER_SSH_ALLOW_UNKNOWN_HOSTKEY is not 'true'.  ` +
                     `Set hostKey to the bastion's host public key (an OpenSSH ` +
                     `known_hosts line or its base64 blob, e.g. from ssh-keyscan).`,
               ),
            );
            return false;
         }) as SyncHostVerifier,
      };

      client.on("error", (err) => fail(err));

      client.on("ready", () => {
         server = net.createServer((socket) => {
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
                  // The local socket may have closed while forwardOut was still
                  // opening the channel; if so, tear the channel down instead of
                  // attaching it to a dead socket (which would leak the remote
                  // connection). The rest of this callback is synchronous, so no
                  // close can slip in between this check and the listeners below.
                  if (socket.destroyed) {
                     channel.destroy();
                     return;
                  }
                  // Flush buffered data, then switch to live forwarding.
                  for (const chunk of earlyData) {
                     channel.write(chunk);
                  }
                  earlyData.length = 0;
                  socket.removeAllListeners("data");
                  socket.on("data", (chunk: Buffer) => channel.write(chunk));
                  channel.on("data", (chunk: Buffer) => socket.write(chunk));
                  socket.on("error", () => channel.destroy());
                  channel.on("error", () => socket.destroy());
                  // Propagate teardown both ways so neither side is left
                  // half-open when the other closes (e.g. pool eviction or the
                  // DB dropping the connection).
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
