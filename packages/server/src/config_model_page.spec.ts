// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { getModelPageMode } from "./config";

// The Explorer unless set to viewer, so a server that sets nothing shows the
// model page it did before the setting.
describe("PUBLISHER_MODEL_PAGE", () => {
   const saved = process.env.PUBLISHER_MODEL_PAGE;

   afterEach(() => {
      if (saved === undefined) delete process.env.PUBLISHER_MODEL_PAGE;
      else process.env.PUBLISHER_MODEL_PAGE = saved;
   });

   it("is explorer when unset or blank", () => {
      delete process.env.PUBLISHER_MODEL_PAGE;
      expect(getModelPageMode()).toBe("explorer");
      process.env.PUBLISHER_MODEL_PAGE = " ";
      expect(getModelPageMode()).toBe("explorer");
   });

   it("reads explorer and viewer, ignoring case and surrounding space", () => {
      process.env.PUBLISHER_MODEL_PAGE = " Viewer ";
      expect(getModelPageMode()).toBe("viewer");
      process.env.PUBLISHER_MODEL_PAGE = "explorer";
      expect(getModelPageMode()).toBe("explorer");
   });

   it("refuses a value outside explorer and viewer, naming the setting", () => {
      process.env.PUBLISHER_MODEL_PAGE = "new";
      expect(() => getModelPageMode()).toThrow(
         'PUBLISHER_MODEL_PAGE must be one of explorer | viewer (got "new")',
      );
   });
});
