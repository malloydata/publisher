// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { linesMentioning } from "./log_lines";

describe("linesMentioning", () => {
   it("keeps the lines naming the marker, in an Error's message too, and drops another spec's", () => {
      const spy = {
         mock: {
            calls: [
               // Another spec file's work, logged while this spy was active.
               ["Request failed", new Error("Environment env does not exist")],
               ["Request refused", { path: "/environments/probe-env/x" }],
               ["Internal error", { error: new Error("probe-env is broken") }],
            ] as unknown[][],
         },
      };
      expect(linesMentioning(spy, "probe-env").map(([text]) => text)).toEqual([
         "Request refused",
         "Internal error",
      ]);
   });
});
