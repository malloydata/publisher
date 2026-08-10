import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { useGivensState } from "./useGivensState";

const TYPES = new Map<string, string | undefined>([
   ["REGION", "filter<string>"],
   ["MIN_AMOUNT", "number"],
]);

const entries = (values: Map<string, unknown>) => Object.fromEntries(values);

/**
 * A host that behaves like `NotebookPage` and `DashboardPage`: it keeps the
 * applied values in its URL and feeds that URL straight back in as `params`.
 *
 * Testing the hook without that loop misses most of what it does. Every Reset
 * defect below is invisible to a test that treats `params` as a constant,
 * because the second half of each one happens when the values the hook just
 * reported arrive back through the front door.
 */
function renderWithUrlHost(options: {
   autorun: boolean;
   startingValues?: Record<string, string>;
   initialUrl?: Record<string, string>;
}) {
   let url: Record<string, string> = options.initialUrl ?? {};
   const urlWrites: Record<string, string>[] = [];
   // Mounted with no starting values, then given them a render later. That is
   // how it really happens: they arrive with the notebook, after the first
   // render, and the hook's report guard is seeded for exactly that order.
   // Handing them over at mount instead makes the first render look like the
   // steady state and hides whether they ever reach the URL.
   const arrival: { starting?: Record<string, string> } = {};
   const view = renderHook(
      ({
         p,
         s,
      }: {
         p: Record<string, string>;
         s: Record<string, string> | undefined;
      }) =>
         useGivensState({
            declaredTypes: TYPES,
            startingValues: s,
            params: p,
            onParamsChange: (next) => {
               url = next;
               urlWrites.push(next);
            },
            autorun: options.autorun,
         }),
      { initialProps: { p: url, s: arrival.starting } },
   );
   // Let the reported values propagate back through the host, repeatedly, so a
   // change that keeps provoking another URL write shows up as more writes
   // rather than as a stable-looking end state.
   const settle = () => {
      for (let i = 0; i < 5; i++)
         act(() => view.rerender({ p: url, s: arrival.starting }));
   };
   settle();
   arrival.starting = options.startingValues;
   settle();
   return {
      get current() {
         return view.result.current;
      },
      settle,
      urlWrites,
      get url() {
         return url;
      },
   };
}

describe("useGivensState: where the controls start", () => {
   it("takes the document's starting values when the URL is empty", () => {
      const host = renderWithUrlHost({
         autorun: true,
         startingValues: { REGION: "West" },
      });
      expect(entries(host.current.applied)).toEqual({ REGION: "West" });
   });

   it("lets the URL win, so a shared link shows what the sender saw", () => {
      const host = renderWithUrlHost({
         autorun: true,
         startingValues: { REGION: "West" },
         initialUrl: { REGION: "Northeast" },
      });
      expect(entries(host.current.applied)).toEqual({ REGION: "Northeast" });
   });

   it("reads each value as its declared type", () => {
      const host = renderWithUrlHost({
         autorun: true,
         initialUrl: { REGION: "West", MIN_AMOUNT: "100" },
      });
      expect(host.current.applied.get("REGION")).toBe("West");
      expect(host.current.applied.get("MIN_AMOUNT")).toBe(100);
   });

   it("ignores a parameter the model does not declare", () => {
      // A tracking tag on the page URL must not reach a query, which would
      // fail outright on an unknown given.
      const host = renderWithUrlHost({
         autorun: true,
         initialUrl: { REGION: "West", utm_source: "slack" },
      });
      expect(entries(host.current.applied)).toEqual({ REGION: "West" });
   });

   it("reports the starting values outward once, and then stops", () => {
      const host = renderWithUrlHost({
         autorun: true,
         startingValues: { REGION: "West" },
      });
      expect(host.urlWrites).toEqual([{ REGION: "West" }]);
   });
});

describe("useGivensState: editing", () => {
   it("applies immediately under autorun", () => {
      const host = renderWithUrlHost({ autorun: true });
      act(() => host.current.setGiven("REGION", "Northeast"));
      host.settle();

      expect(entries(host.current.applied)).toEqual({ REGION: "Northeast" });
      expect(host.current.pending).toBe(false);
      expect(host.url).toEqual({ REGION: "Northeast" });
   });

   it("batches behind Apply when autorun is off", () => {
      const host = renderWithUrlHost({ autorun: false });
      act(() => host.current.setGiven("REGION", "Northeast"));
      act(() => host.current.setGiven("MIN_AMOUNT", 100));
      host.settle();

      expect(entries(host.current.draft)).toEqual({
         REGION: "Northeast",
         MIN_AMOUNT: 100,
      });
      expect(entries(host.current.applied)).toEqual({});
      expect(host.current.pending).toBe(true);
      // Two edits, and the URL has not moved: nothing has been run yet.
      expect(host.urlWrites).toEqual([]);

      act(() => host.current.apply());
      host.settle();

      expect(entries(host.current.applied)).toEqual({
         REGION: "Northeast",
         MIN_AMOUNT: 100,
      });
      expect(host.current.pending).toBe(false);
      // One write for two edits, which is the whole point of batching.
      expect(host.urlWrites).toEqual([
         { REGION: "Northeast", MIN_AMOUNT: "100" },
      ]);
   });

   it("keeps the other starting values when one control is edited", () => {
      const host = renderWithUrlHost({
         autorun: true,
         startingValues: { REGION: "West", MIN_AMOUNT: "5" },
      });
      act(() => host.current.setGiven("REGION", "Northeast"));
      host.settle();

      expect(entries(host.current.applied)).toEqual({
         REGION: "Northeast",
         MIN_AMOUNT: 5,
      });
   });

   it("drops an override on null, back to the model default", () => {
      const host = renderWithUrlHost({
         autorun: true,
         initialUrl: { REGION: "West" },
      });
      act(() => host.current.setGiven("REGION", null));
      host.settle();

      expect(host.current.applied.has("REGION")).toBe(false);
      expect(host.url).toEqual({});
   });

   it("drops values for givens the model no longer declares", () => {
      // What a package reload can do while the page is open. Sending a given
      // the model no longer declares fails the query outright.
      let url: Record<string, string> = { REGION: "West", MIN_AMOUNT: "100" };
      const { result, rerender } = renderHook(
         ({ types }: { types: Map<string, string | undefined> }) =>
            useGivensState({
               declaredTypes: types,
               params: url,
               onParamsChange: (next) => {
                  url = next;
               },
               autorun: true,
            }),
         { initialProps: { types: TYPES } },
      );
      expect(result.current.applied.size).toBe(2);

      const reloaded = new Map([["REGION", "filter<string>"]]);
      act(() => rerender({ types: reloaded }));

      expect(entries(result.current.applied)).toEqual({ REGION: "West" });
   });
});

describe("useGivensState: Reset", () => {
   // Two symptoms were reported against #935's `clearAll`, and they turned out
   // to be one defect: it cleared to an EMPTY map rather than to the starting
   // point, and it only committed under autorun. Each test below fails against
   // that version, in the way named in its comment.

   it("puts the document's starting values back, and runs once", () => {
      // Was: cleared to empty, the empty state went out to the URL, the
      // starting values then flowed back in from `initial`, and the queries
      // ran a second time. Two URL writes where there should be one.
      const host = renderWithUrlHost({
         autorun: true,
         startingValues: { REGION: "West" },
      });
      act(() => host.current.setGiven("REGION", "Northeast"));
      host.settle();
      const writesBefore = host.urlWrites.length;

      act(() => host.current.reset());
      host.settle();

      expect(entries(host.current.applied)).toEqual({ REGION: "West" });
      expect(host.url).toEqual({ REGION: "West" });
      expect(host.urlWrites.length - writesBefore).toBe(1);
   });

   it("takes effect with autorun off, instead of doing nothing at all", () => {
      // Was: `applied` was left untouched whenever autorun was false, so the
      // URL never changed and no query re-ran. Reset was inert.
      const host = renderWithUrlHost({ autorun: false });
      act(() => host.current.setGiven("REGION", "Northeast"));
      act(() => host.current.apply());
      host.settle();
      expect(host.url).toEqual({ REGION: "Northeast" });
      const writesBefore = host.urlWrites.length;

      act(() => host.current.reset());
      host.settle();

      expect(entries(host.current.applied)).toEqual({});
      expect(host.url).toEqual({});
      expect(host.urlWrites.length - writesBefore).toBe(1);
   });

   it("leaves nothing pending, so Apply does not light up after a Reset", () => {
      const host = renderWithUrlHost({
         autorun: false,
         startingValues: { REGION: "West" },
      });
      act(() => host.current.setGiven("REGION", "Northeast"));
      host.settle();
      expect(host.current.pending).toBe(true);

      act(() => host.current.reset());
      host.settle();

      expect(host.current.pending).toBe(false);
      expect(entries(host.current.draft)).toEqual({ REGION: "West" });
   });

   it("clears a URL-carried value rather than treating it as the start", () => {
      // The URL is the thing being reset away from, so a link's values are not
      // the starting point even though they win on arrival.
      const host = renderWithUrlHost({
         autorun: true,
         startingValues: { REGION: "West" },
         initialUrl: { REGION: "Northeast" },
      });
      expect(entries(host.current.applied)).toEqual({ REGION: "Northeast" });

      act(() => host.current.reset());
      host.settle();

      expect(entries(host.current.applied)).toEqual({ REGION: "West" });
   });
});

describe("useGivensState: settling", () => {
   it("stops reporting once the host echoes the values back", () => {
      // The hook reports applied values out for the host to put in the URL,
      // and the host feeds that URL back in. Without the `sameParams` guard
      // that is a loop.
      const host = renderWithUrlHost({
         autorun: true,
         initialUrl: { REGION: "West" },
      });
      host.settle();
      host.settle();
      expect(host.urlWrites).toEqual([]);
   });

   it("settles when its inputs are fresh objects on every render", () => {
      // Carried over from `useGivensForm.spec.ts`, which this hook replaces.
      // That hook infinite-looped on an unmemoized argument: its effect
      // returned a fresh Map every run, which changed state identity, which
      // re-rendered, until React gave up with "Maximum update depth exceeded"
      // after 645k warnings. This is a published SDK export, so a consumer can
      // reasonably pass an object literal.
      let renders = 0;
      const { result } = renderHook(() => {
         renders++;
         return useGivensState({
            declaredTypes: new Map([["REGION", "filter<string>"]]),
            startingValues: { REGION: "West" },
            params: {},
            onParamsChange: () => {},
            autorun: true,
         });
      });

      expect(entries(result.current.applied)).toEqual({ REGION: "West" });
      expect(renders).toBeLessThan(5);
   });
});
