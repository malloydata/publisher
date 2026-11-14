// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Test helper: a port on 127.0.0.1 that accepts each connection and closes it.
 *
 * Specs that need a database connection to fail point at this instead of at a
 * port nothing listens on. A refused connect fails at once on Linux and macOS,
 * but Windows retries the SYN before reporting the refusal and spends about
 * two seconds on each attempt. A connection the server closes fails at once on
 * every platform, and the client still names the address it tried, so an
 * error that echoes the connection string carries the same DSN either way.
 */

import * as net from "net";

export interface ClosingListener {
   host: string;
   port: number;
   close: () => Promise<void>;
}

export async function startClosingListener(): Promise<ClosingListener> {
   const server = net.createServer((socket) => socket.destroy());
   await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => resolve());
   });
   const { address, port } = server.address() as net.AddressInfo;
   return {
      host: address,
      port,
      close: () =>
         new Promise<void>((resolve, reject) =>
            server.close((err) => (err ? reject(err) : resolve())),
         ),
   };
}
