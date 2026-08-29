// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   CONNECT_FAILED_HUMAN,
   MALFORMED_PAYLOAD_AGENT,
   MALFORMED_PAYLOAD_HUMAN,
   NO_PAYLOAD_AGENT,
   NO_PAYLOAD_HUMAN,
   RESULT_TOO_LARGE_AGENT,
   RESULT_TOO_LARGE_HUMAN,
} from "./messages";

describe("result-too-large copy", () => {
   it("tells the human to add a limit or filter (not 'failed to parse')", () => {
      expect(RESULT_TOO_LARGE_HUMAN).toMatch(/limit|filter/i);
      expect(RESULT_TOO_LARGE_HUMAN).not.toMatch(/failed to parse/i);
   });

   it("gives the agent an actionable, self-recovery instruction", () => {
      // The whole point: the agent must know to re-run a smaller query.
      expect(RESULT_TOO_LARGE_AGENT).toMatch(/re-?run/i);
      expect(RESULT_TOO_LARGE_AGENT).toMatch(/limit|filter/i);
   });

   it("names this server's tool, not Credible's", () => {
      // The copy was ported from a server whose tool is called execute_query.
      expect(RESULT_TOO_LARGE_AGENT).toContain("malloy_executeQuery");
   });
});

describe("no-payload copy", () => {
   it("does not tell anyone to shrink a query that was never the problem", () => {
      // The distinction this pair exists for. Nothing arrived to render, so
      // size advice would send both the human and the agent after a fault they
      // do not have, and the agent would burn a retry on an identical failure.
      expect(NO_PAYLOAD_HUMAN).not.toMatch(/too large|limit|filter/i);
      expect(NO_PAYLOAD_AGENT).not.toMatch(/\btruncated\b/i);
   });

   it("tells the agent explicitly that retrying smaller will not help", () => {
      expect(NO_PAYLOAD_AGENT).toMatch(/not a size problem/i);
      expect(NO_PAYLOAD_AGENT).toMatch(/will not help/i);
   });

   it("stays distinguishable from the too-large copy", () => {
      expect(NO_PAYLOAD_HUMAN).not.toBe(RESULT_TOO_LARGE_HUMAN);
      expect(NO_PAYLOAD_AGENT).not.toBe(RESULT_TOO_LARGE_AGENT);
   });

   it("names this server's tool, not Credible's", () => {
      expect(NO_PAYLOAD_AGENT).toContain("malloy_executeQuery");
   });
});

describe("malformed-payload copy", () => {
   // A payload that parsed but was not an object. Distinct from truncation and
   // from no payload, and it must not borrow either one's advice: both would send
   // the reader after a problem they do not have.
   it("does not tell the human to shrink the query", () => {
      expect(MALFORMED_PAYLOAD_HUMAN).not.toMatch(/limit|filter|too large/i);
   });

   it("tells the agent a smaller query will not help", () => {
      expect(MALFORMED_PAYLOAD_AGENT).toMatch(/not a size problem/i);
      expect(MALFORMED_PAYLOAD_AGENT).toMatch(/will not help/i);
   });

   it("names this server's tool", () => {
      expect(MALFORMED_PAYLOAD_AGENT).toContain("malloy_executeQuery");
   });

   it("is distinct from the other two failures", () => {
      // Three failures, three messages. Reusing one would misdiagnose the others.
      const all = [
         RESULT_TOO_LARGE_HUMAN,
         NO_PAYLOAD_HUMAN,
         MALFORMED_PAYLOAD_HUMAN,
         CONNECT_FAILED_HUMAN,
      ];
      expect(new Set(all).size).toBe(all.length);
   });
});

describe("connect-failed copy", () => {
   it("blames the client connection, not the query", () => {
      expect(CONNECT_FAILED_HUMAN).toMatch(/connect/i);
      expect(CONNECT_FAILED_HUMAN).not.toMatch(/limit|filter|too large/i);
   });
});
