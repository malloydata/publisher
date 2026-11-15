// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { annotatedJoinedOutput } from "./joined_field_output";

const annotated = new Map([["j", new Set(["name"])]]);
const ref = (...path: string[]) => ({ type: "fieldref", path });
const stage = (...queryFields: unknown[]) => ({ type: "reduce", queryFields });

describe("annotatedJoinedOutput", () => {
   it("finds an annotated joined field returned as a column", () => {
      expect(
         annotatedJoinedOutput([stage(ref("j", "name"))], [], annotated),
      ).toEqual({ output: { path: ["j", "name"] }, unreadable: false });
   });

   it("ignores an unannotated joined field, another join, and a renamed read", () => {
      expect(
         annotatedJoinedOutput(
            [
               stage(
                  ref("j", "other"),
                  ref("k", "name"),
                  ref("j", "name", "deeper"),
                  // `x is j.name`: an expression, which carries no notes live.
                  {
                     type: "string",
                     name: "x",
                     e: { node: "field", path: ["j", "name"] },
                  },
               ),
            ],
            [],
            annotated,
         ),
      ).toEqual({ unreadable: false });
   });

   it("finds one in a nest, a later stage, and a view read by name", () => {
      const nest = {
         type: "turtle",
         name: "n",
         pipeline: [stage(ref("j", "name"))],
      };
      expect(
         annotatedJoinedOutput([stage(ref("id"), nest)], [], annotated).output,
      ).toEqual({ path: ["j", "name"] });
      expect(
         annotatedJoinedOutput(
            [stage(ref("id")), stage(ref("j", "name"))],
            [],
            annotated,
         ).output,
      ).toEqual({ path: ["j", "name"] });
      const view = {
         type: "turtle",
         name: "v",
         pipeline: [stage(ref("j", "name"))],
      };
      expect(
         annotatedJoinedOutput([stage(ref("v"))], [view], annotated).output,
      ).toEqual({ path: ["j", "name"] });
      // A view the query does not read is not its output.
      expect(
         annotatedJoinedOutput([stage(ref("id"))], [view], annotated),
      ).toEqual({ unreadable: false });
   });

   it("fails closed on what it cannot read", () => {
      expect(annotatedJoinedOutput(undefined, [], annotated).unreadable).toBe(
         true,
      );
      expect(
         annotatedJoinedOutput([{ queryFields: "x" }], [], annotated)
            .unreadable,
      ).toBe(true);
      expect(
         annotatedJoinedOutput(
            [stage({ type: "fieldref", path: [1] })],
            [],
            annotated,
         ).unreadable,
      ).toBe(true);
      // A view whose pipeline is missing.
      expect(
         annotatedJoinedOutput(
            [stage(ref("v"))],
            [{ type: "turtle", name: "v" }],
            annotated,
         ).unreadable,
      ).toBe(true);
   });
});
