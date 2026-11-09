// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, it, spyOn } from "bun:test";
import * as http from "http";
import { logListening, longTimeouts } from "./listen";
import { logger } from "./logger";

it("logs the address and port really bound, an IPv6 host bracketed", () => {
   const info = spyOn(logger, "info").mockImplementation(() => logger);
   try {
      const at = (address: unknown) => ({ address: () => address }) as never;
      expect(
         logListening("X", at({ address: "::1", port: 51234 }), "h", 0),
      ).toBe(51234);
      expect(logListening("X", at(null), "127.0.0.1", 4000)).toBe(4000);
      expect(info.mock.calls.map((c) => String(c[0]))).toEqual([
         "X listening at http://[::1]:51234",
         "X listening at http://127.0.0.1:4000",
      ]);
   } finally {
      info.mockRestore();
   }
});

it("gives a listener ten-minute request, header and keep-alive timeouts", () => {
   const server = longTimeouts(http.createServer());
   expect([
      server.timeout,
      server.keepAliveTimeout,
      server.headersTimeout,
   ]).toEqual([600_000, 600_000, 600_000]);
});
