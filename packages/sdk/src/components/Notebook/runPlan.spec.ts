// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { planRun } from "./runPlan";

const DOC = "uri|1";
const key = (givens: string) => `${DOC}|${givens}`;

describe("planRun", () => {
   it("disarms a pending run when the notebook is not loaded", () => {
      // Handled here rather than by an early return in the effect, because an
      // early return skipped the cancellation: navigating away inside the
      // settle window left a timer holding the previous notebook's closure.
      expect(
         planRun({
            ready: false,
            lastRunKey: key(""),
            pendingKey: key("REGION=West"),
            runKey: key("REGION=West"),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: true, dispatch: "none" });
   });

   it("does nothing at all when there is nothing pending and nothing loaded", () => {
      expect(
         planRun({
            ready: false,
            lastRunKey: null,
            pendingKey: undefined,
            runKey: key(""),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: false, dispatch: "none" });
   });

   it("runs the first pass immediately", () => {
      expect(
         planRun({
            ready: true,
            lastRunKey: null,
            pendingKey: undefined,
            runKey: key(""),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: false, dispatch: "now" });
   });

   it("makes a changed parameter wait for the value to settle", () => {
      expect(
         planRun({
            ready: true,
            lastRunKey: key(""),
            pendingKey: undefined,
            runKey: key("REGION=West"),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: false, dispatch: "settle" });
   });

   it("does not make a different notebook wait", () => {
      // A document change is not a burst, so delaying it is only a blank screen.
      expect(
         planRun({
            ready: true,
            lastRunKey: "uri|1|",
            pendingKey: undefined,
            runKey: "other|1|",
            documentKey: "other|1",
         }).dispatch,
      ).toBe("now");
   });

   it("does not make a package reload wait", () => {
      // A reload bumps the generation, which changes the document key.
      expect(
         planRun({
            ready: true,
            lastRunKey: "uri|1|REGION=West",
            pendingKey: undefined,
            runKey: "uri|2|REGION=West",
            documentKey: "uri|2",
         }).dispatch,
      ).toBe("now");
   });

   it("lets a pending run for the same key finish its clock", () => {
      // Restarting it on every unrelated re-render could hold it off forever.
      expect(
         planRun({
            ready: true,
            lastRunKey: key(""),
            pendingKey: key("REGION=West"),
            runKey: key("REGION=West"),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: false, dispatch: "none" });
   });

   it("supersedes a pending run when the value changes again", () => {
      expect(
         planRun({
            ready: true,
            lastRunKey: key(""),
            pendingKey: key("REGION=We"),
            runKey: key("REGION=West"),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: true, dispatch: "settle" });
   });

   it("cancels a pending run when the user reverts to what already ran", () => {
      // The defect this function was extracted for. Typing a value and deleting
      // it again inside the settle window lands on an already-run key. Cancelling
      // has to happen even though there is nothing to dispatch, or the abandoned
      // value's results arrive and nothing is left to correct them.
      expect(
         planRun({
            ready: true,
            lastRunKey: key(""),
            pendingKey: key("REGION=West"),
            runKey: key(""),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: true, dispatch: "none" });
   });

   it("cancels a pending run when the DOCUMENT changes under it", () => {
      // The reachable case the first table missed: a parameter edit arms a
      // timer, then the reader navigates to another notebook inside the window.
      // The pending run belongs to the notebook they just left.
      expect(
         planRun({
            ready: true,
            lastRunKey: "uri|1|",
            pendingKey: "uri|1|REGION=West",
            runKey: "other|1|",
            documentKey: "other|1",
         }),
      ).toEqual({ cancelPending: true, dispatch: "now" });
   });

   it("does nothing when the state is unchanged", () => {
      expect(
         planRun({
            ready: true,
            lastRunKey: key("REGION=West"),
            pendingKey: undefined,
            runKey: key("REGION=West"),
            documentKey: DOC,
         }),
      ).toEqual({ cancelPending: false, dispatch: "none" });
   });
});
