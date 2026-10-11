// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it } from "bun:test";
import { BrowserDocumentStorage } from "../DocumentStorage";
import { locatorFor } from "../DocumentCreate/documentPath";
import { findDraftType } from "./useDraftType";

describe("findDraftType", () => {
   beforeEach(() => localStorage.clear());

   it("finds the type a draft was saved under, which the listing cannot say", async () => {
      const storage = new BrowserDocumentStorage();
      // createDocument files a linear report under `notebook`.
      await storage.saveDocument(
         locatorFor("notebook", "Local", "env", "pkg", "story.malloy"),
         "## artifact { layout=linear }",
      );
      await storage.saveDocument(
         locatorFor("dashboard", "Local", "env", "pkg", "sales/q3.malloy"),
         "## artifact { layout=grid }",
      );
      expect(await findDraftType(storage, "env", "pkg", "story.malloy")).toBe(
         "notebook",
      );
      expect(
         await findDraftType(storage, "env", "pkg", "sales/q3.malloy"),
      ).toBe("dashboard");
   });

   it("is undefined when the host keeps no copy, or keeps it for another package", async () => {
      const storage = new BrowserDocumentStorage();
      await storage.saveDocument(
         locatorFor("notebook", "Local", "env", "other", "story.malloy"),
         "x",
      );
      expect(
         await findDraftType(storage, "env", "pkg", "story.malloy"),
      ).toBeUndefined();
   });
});
