// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A query whose client goes away is cancelled, not left running.
 *
 * The `/models/*?/query` route aborts the signal it hands the query controller
 * when the response emits `close` before finishing, and the drivers cancel the
 * warehouse statement on that signal. This drives the real route over a real
 * socket and drops the client mid-query, so it pins the runtime half too: Bun
 * 1.3.13 never emitted `close` on a response whose client disconnected, so the
 * signal never fired and an abandoned query ran on the warehouse until it
 * finished, failed or timed out. Bun 1.4 emits it at the disconnect, as Node
 * does.
 */

import { afterEach, describe, expect, it } from "bun:test";
import net, { type AddressInfo } from "net";

import { resetActiveQueryCountForTesting } from "./query_concurrency";
import { API_PREFIX, buildRouteApp } from "./test_helpers/route_app";

afterEach(() => {
   resetActiveQueryCountForTesting();
});

/** Polls `predicate` until it holds, or gives up after `ms`. */
async function eventually(
   predicate: () => boolean,
   ms = 2000,
): Promise<boolean> {
   const deadline = Date.now() + ms;
   while (!predicate() && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10));
   }
   return predicate();
}

describe("query cancellation when the client disconnects", () => {
   it("aborts the signal the query controller was given", async () => {
      let signal: AbortSignal | undefined;
      const server = buildRouteApp({
         queryController: {
            getQuery: (...args: unknown[]) => {
               const received = args[args.length - 1] as AbortSignal;
               signal = received;
               // A query that only ends when it is cancelled.
               return new Promise((_, reject) => {
                  received.addEventListener(
                     "abort",
                     () => reject(received.reason),
                     { once: true },
                  );
               }) as never;
            },
         },
      }).listen(0);
      try {
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
         expect(await eventually(() => signal !== undefined)).toBe(true);

         socket.destroy();

         expect(await eventually(() => signal?.aborted === true)).toBe(true);
      } finally {
         server.close();
      }
   });
});
