// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A query whose client goes away before the answer must still give back its
 * concurrency slot.
 *
 * This drives the real `/models/*?/query` route over a real socket, because the
 * failure is in what the runtime emits rather than in the route. On Bun 1.3.13 a
 * response whose client disconnected mid-request emits neither `close` nor
 * `finish` -- not at the disconnect, and not when the handler later writes its
 * answer to the dead socket. A slot released only on those events stayed held
 * until the process restarted, one per abandoned query, until the pod refused
 * every query at PUBLISHER_MAX_CONCURRENT_QUERIES.
 */

import { afterEach, describe, expect, it } from "bun:test";
import type { Server } from "http";
import net, { type AddressInfo } from "net";

import {
   getActiveQueryCount,
   resetActiveQueryCountForTesting,
} from "./query_concurrency";
import { API_PREFIX, buildRouteApp } from "./test_helpers/route_app";
import { installSettings } from "./test_helpers/settings_env";

/**
 * Puts back the settings a case installed, so a cap set here cannot leak into
 * another file.
 */
let restoreSettings: (() => void) | undefined;

/** Install the concurrency cap for this case, before its app is built. */
function installCap(cap: string): void {
   restoreSettings?.();
   restoreSettings = installSettings({
      env: { PUBLISHER_MAX_CONCURRENT_QUERIES: cap },
   }).restore;
}

afterEach(() => {
   restoreSettings?.();
   restoreSettings = undefined;
   resetActiveQueryCountForTesting();
});

/** A promise plus the handles that settle it, so a query can be held open. */
function deferred(): {
   promise: Promise<void>;
   resolve: () => void;
   reject: (error: Error) => void;
} {
   let resolve!: () => void;
   let reject!: (error: Error) => void;
   const promise = new Promise<void>((res, rej) => {
      resolve = res;
      reject = rej;
   });
   return { promise, resolve, reject };
}

/** Polls until the slot count reaches `count`, or gives up after `ms`. */
async function slotsReach(count: number, ms = 2000): Promise<number> {
   const deadline = Date.now() + ms;
   while (getActiveQueryCount() !== count && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10));
   }
   return getActiveQueryCount();
}

/**
 * Serves the real route table with the query parked on `hold`, sends one query
 * over a raw socket, and drops the socket once the route holds its slot -- the
 * way a caller that hit its own timeout lets go.
 */
async function abandonQuery(hold: Promise<void>): Promise<Server> {
   const server = buildRouteApp({
      queryController: {
         getQuery: async () => {
            await hold;
            return { result: "[]" } as never;
         },
      },
   }).listen(0);
   await new Promise((r) => server.once("listening", r));

   const body = JSON.stringify({
      query: "run: s -> { aggregate: n is count() }",
   });
   const socket = net.connect(
      (server.address() as AddressInfo).port,
      "127.0.0.1",
   );
   socket.on("error", () => undefined);
   await new Promise((r) => socket.once("connect", r));
   socket.write(
      `POST ${API_PREFIX}/environments/e/packages/p/models/m.malloy/query HTTP/1.1\r\n` +
         "Host: localhost\r\nContent-Type: application/json\r\n" +
         `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
   );
   expect(await slotsReach(1)).toBe(1);
   socket.destroy();
   // Let the server see the socket go before the answer is written. Settled in
   // the same tick, the answer beats the disconnect and goes out as a normal
   // response, which is not the case under test.
   await new Promise((r) => setTimeout(r, 200));
   return server;
}

describe("query concurrency slot after the client disconnects", () => {
   it("is released when the query answers after the client left", async () => {
      installCap("4");
      const hold = deferred();
      const server = await abandonQuery(hold.promise);
      try {
         hold.resolve();
         expect(await slotsReach(0)).toBe(0);
      } finally {
         server.close();
      }
   });

   it("is released when the query fails after the client left", async () => {
      // The shape seen in production: the connection's own statement timeout
      // cancels the query after the caller has given up, the driver reports
      // it as an error, and the route answers that error to a socket nobody
      // is reading.
      installCap("4");
      const hold = deferred();
      const server = await abandonQuery(hold.promise);
      try {
         hold.reject(new Error("SQL execution canceled"));
         expect(await slotsReach(0)).toBe(0);
      } finally {
         server.close();
      }
   });
});
