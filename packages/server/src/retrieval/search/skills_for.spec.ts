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

   it("says the package's guidance could not be read, with the reason, instead of looking empty", () => {
      const reply = skillsFor(
         ctxOf(() => {
            throw new WorkflowsUnavailableError("EACCES on sets/a");
         }),
      );
      expect(reply).toEqual({
         skills_note: expect.stringContaining("EACCES on sets/a"),
      });
      expect(reply).not.toHaveProperty("skills");
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
