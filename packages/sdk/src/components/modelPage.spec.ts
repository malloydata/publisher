// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { modelPageOf } from "./modelPage";

describe("modelPageOf", () => {
   it("is the page /status names, the Explorer unless it says viewer", () => {
      expect(modelPageOf({ modelPage: "viewer" }, false)).toBe("viewer");
      expect(modelPageOf({ modelPage: "explorer" }, false)).toBe("explorer");
      // A server that predates the setting sends no field.
      expect(modelPageOf({}, false)).toBe("explorer");
      expect(modelPageOf({ modelPage: "something new" }, false)).toBe(
         "explorer",
      );
   });

   // A deployment that chose viewer has chosen not to show the Explorer, and
   // the viewer writes no value into Malloy on any server.
   it("is the viewer when /status could not be read", () => {
      expect(modelPageOf(undefined, true)).toBe("viewer");
   });

   it("is the Explorer while /status has not answered", () => {
      expect(modelPageOf(undefined, false)).toBe("explorer");
   });
});
