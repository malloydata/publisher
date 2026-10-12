// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import { renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import type { GivenValue } from "../../hooks/givenValue";
import {
   reportEmitter,
   ReportEventsContext,
   type ReportEvent,
} from "../Report/telemetry";
import { useDrillSelf } from "./useDrillSelf";

const TYPES = new Map<string, string | undefined>([
   ["REGION", "filter<string>"],
   ["MIN_AMOUNT", "number"],
]);

function hook(
   setGiven = mock((_n: string, _v: GivenValue) => {}),
   onEvent?: (event: ReportEvent) => void,
) {
   const events = reportEmitter(
      { path: "overview.malloy", layout: "grid" },
      onEvent,
   );
   const view = renderHook(
      () =>
         useDrillSelf({
            declaredTypes: TYPES,
            setGiven,
            documentName: "overview",
         }),
      {
         wrapper: ({ children }: { children: ReactNode }) => (
            <ReportEventsContext.Provider value={events}>
               {children}
            </ReportEventsContext.Provider>
         ),
      },
   );
   return { ...view.result.current, setGiven };
}

describe("useDrillSelf", () => {
   let warn: ReturnType<typeof spyOn> | undefined;
   afterEach(() => warn?.mockRestore());

   it("resolves a tag's name as declared, or by folding case", () => {
      const { resolveGiven, canSelf } = hook();
      expect(resolveGiven("REGION")).toBe("REGION");
      expect(resolveGiven("region")).toBe("REGION");
      expect(resolveGiven("Min_Amount")).toBe("MIN_AMOUNT");
      expect(resolveGiven("STATE")).toBeUndefined();
      expect(canSelf("region")).toBe(true);
      expect(canSelf("STATE")).toBe(false);
   });

   it("sets the given under the DECLARED name, encoded for its type", () => {
      const { onSelf, setGiven } = hook();
      onSelf("region", "West");
      expect(setGiven).toHaveBeenCalledWith("REGION", "West");
      onSelf("min_amount", 42);
      expect(setGiven).toHaveBeenLastCalledWith("MIN_AMOUNT", 42);
   });

   it("declines, aloud, a name the document does not declare", () => {
      warn = spyOn(console, "warn").mockImplementation(() => {});
      const { onSelf, setGiven } = hook();
      onSelf("STATE", "CA");
      expect(setGiven).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("'overview'"));
   });

   it("declines, aloud, a value the given's type cannot hold", () => {
      warn = spyOn(console, "warn").mockImplementation(() => {});
      const { onSelf, setGiven } = hook();
      onSelf("MIN_AMOUNT", "not a number");
      expect(setGiven).not.toHaveBeenCalled();
      expect(warn).toHaveBeenCalledWith(
         expect.stringContaining("Drill declined"),
      );
   });

   describe("inside a report", () => {
      const outcomes = (onEvent: ReturnType<typeof mock>) =>
         (onEvent.mock.calls as [ReportEvent][]).map(([event]) => event);

      it("reports a drill that set the given as resolved", () => {
         const onEvent = mock((_event: ReportEvent) => {});
         hook(undefined, onEvent).onSelf("region", "West");
         expect(outcomes(onEvent)).toEqual([
            {
               type: "report.drill",
               path: "overview.malloy",
               layout: "grid",
               to: "self",
               outcome: "resolved",
            },
         ]);
      });

      it("reports an undeclared name as unresolved and an unencodable value as malformed, instead of warning", () => {
         warn = spyOn(console, "warn").mockImplementation(() => {});
         const onEvent = mock((_event: ReportEvent) => {});
         const { onSelf } = hook(undefined, onEvent);
         onSelf("STATE", "CA");
         onSelf("MIN_AMOUNT", "not a number");
         expect(outcomes(onEvent).map((event) => event)).toMatchObject([
            { type: "report.drill", outcome: "unresolved" },
            { type: "report.drill", outcome: "malformed" },
         ]);
         expect(warn).not.toHaveBeenCalled();
      });
   });
});
