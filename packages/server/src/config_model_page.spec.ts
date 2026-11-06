// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { getModelPageMode } from "./config";
import { withSettings } from "./test_helpers/settings_env";

// The Explorer unless set to viewer, so a server that sets nothing shows the
// model page it did before the setting.
describe("PUBLISHER_MODEL_PAGE", () => {
   it("is explorer when unset or blank", () => {
      withSettings({}, () => expect(getModelPageMode()).toBe("explorer"));
      withSettings({ env: { PUBLISHER_MODEL_PAGE: " " } }, () =>
         expect(getModelPageMode()).toBe("explorer"),
      );
   });

   it("reads explorer and viewer, ignoring case and surrounding space", () => {
      withSettings({ env: { PUBLISHER_MODEL_PAGE: " Viewer " } }, () =>
         expect(getModelPageMode()).toBe("viewer"),
      );
      withSettings({ env: { PUBLISHER_MODEL_PAGE: "explorer" } }, () =>
         expect(getModelPageMode()).toBe("explorer"),
      );
   });

   it("refuses a value outside explorer and viewer, naming the setting", () => {
      withSettings({ env: { PUBLISHER_MODEL_PAGE: "new" } }, () =>
         expect(() => getModelPageMode()).toThrow(
            'modelPage from PUBLISHER_MODEL_PAGE: expected one of "explorer" or "viewer", got "new"',
         ),
      );
   });
});
