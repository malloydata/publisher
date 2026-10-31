// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { joinAccess } from "./joinAccess";

const browsable = (entries: Record<string, string[]>) =>
   new Map(
      Object.entries(entries).map(([path, names]) => [path, new Set(names)]),
   );

describe("joinAccess", () => {
   it("a served source gets a page", () => {
      expect(
         joinAccess(
            { modelPath: "flights.malloy", sourceName: "carriers" },
            browsable({ "flights.malloy": ["flights", "carriers"] }),
         ),
      ).toEqual({ kind: "link" });
   });

   // The whole point: the entities artifact records every join the model
   // compiles, including into sources the package's `explores`/`export {}`
   // kept off the served surface. Following such a link lands on a
   // "no source named …" page, so the row must not offer one.
   it("a source the model did not serve is join-only", () => {
      expect(
         joinAccess(
            { modelPath: "flights.malloy", sourceName: "aircraft_models" },
            browsable({ "flights.malloy": ["flights", "carriers"] }),
         ),
      ).toEqual({ kind: "join-only" });
   });

   it("judges a cross-model target against ITS model's served list", () => {
      const sets = browsable({
         "flights.malloy": ["flights"],
         "airports.malloy": ["airports"],
      });
      expect(
         joinAccess(
            { modelPath: "airports.malloy", sourceName: "airports" },
            sets,
         ),
      ).toEqual({
         kind: "link",
      });
      // Same NAME, served by the other model — the model path is what decides.
      expect(
         joinAccess(
            { modelPath: "flights.malloy", sourceName: "airports" },
            sets,
         ),
      ).toEqual({
         kind: "join-only",
      });
   });

   // Absent means in flight *or failed*, not absent-from-the-surface.
   // Rendering the note here would flash "only accessible via join" on every
   // cross-model join before its model answers — and a timeout would look
   // like the package curated the source away.
   it("says nothing about a model that has not answered yet", () => {
      expect(
         joinAccess(
            { modelPath: "airports.malloy", sourceName: "airports" },
            browsable({}),
         ),
      ).toEqual({ kind: "unknown" });
   });

   it("a model that served nothing is join-only, not unknown", () => {
      expect(
         joinAccess(
            { modelPath: "internal.malloy", sourceName: "staging" },
            browsable({ "internal.malloy": [] }),
         ),
      ).toEqual({ kind: "join-only" });
   });

   it("incomplete join metadata is unknown", () => {
      const sets = browsable({ "flights.malloy": ["carriers"] });
      expect(joinAccess(undefined, sets)).toEqual({ kind: "unknown" });
      expect(
         joinAccess({ modelPath: "", sourceName: "carriers" }, sets),
      ).toEqual({
         kind: "unknown",
      });
      expect(
         joinAccess({ modelPath: "flights.malloy", sourceName: "" }, sets),
      ).toEqual({
         kind: "unknown",
      });
   });
});
