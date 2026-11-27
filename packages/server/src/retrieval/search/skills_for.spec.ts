// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { WorkflowsUnavailableError } from "../../errors";
import { skillsFor } from "./response";

const ctxOf = (listSkills: () => unknown[]) =>
   ({ pkgIndex: { pkg: { listSkills } } }) as unknown as Parameters<
      typeof skillsFor
   >[0];

describe("skillsFor", () => {
   it("lists the package's advertised skills by name and description", () => {
      const reply = skillsFor(
         ctxOf(() => [{ name: "house", description: "House rules." }]),
      );
      expect(reply.skills).toEqual([
         { name: "house", description: "House rules." },
      ]);
   });

   it("attaches nothing, and does not throw, when the package's workflows could not be read at load", () => {
      expect(
         skillsFor(
            ctxOf(() => {
               throw new WorkflowsUnavailableError("EACCES");
            }),
         ),
      ).toEqual({});
   });

   it("lets any other failure through", () => {
      expect(() =>
         skillsFor(
            ctxOf(() => {
               throw new Error("boom");
            }),
         ),
      ).toThrow("boom");
   });
});
