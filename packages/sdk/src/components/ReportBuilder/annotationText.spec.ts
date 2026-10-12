// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { AUTHORIZE_TAG_LIKE as SERVER_AUTHORIZE_TAG_LIKE } from "../../../../server/src/service/authorize";
import { annotationTextProblem, AUTHORIZE_TAG_LIKE } from "./annotationText";

describe("annotationTextProblem", () => {
   it("is the server's pattern, character for character", () => {
      expect(AUTHORIZE_TAG_LIKE).toBe(SERVER_AUTHORIZE_TAG_LIKE);
   });

   it("refuses a line break and what reads as an access-control tag", () => {
      expect(annotationTextProblem("tile label", "a\nb")).toContain(
         "tile label is one line",
      );
      expect(annotationTextProblem("tile label", "a\rb")).toContain("one line");
      for (const text of [
         "# authorize",
         "x ##(row_authorize) y",
         "#| access_filter",
      ])
         expect(annotationTextProblem("tile label", text)).toContain(
            "access-control",
         );
   });

   it("accepts ordinary titles", () => {
      expect(
         annotationTextProblem("tile label", "Revenue (authorized) by region"),
      ).toBeUndefined();
   });
});
