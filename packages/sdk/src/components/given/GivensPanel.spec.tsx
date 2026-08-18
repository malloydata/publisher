import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { useState } from "react";
import type { Given } from "../../client";
import { useGivensState } from "../../hooks/useGivensState";
import { decodeFilterList } from "./filterValue";
import { GivensPanel } from "./GivensPanel";

const TYPES = new Map<string, string | undefined>([
   ["BRAND", "filter<string>"],
]);

const MULTISELECT: Given = {
   name: "BRAND",
   type: "filter<string>",
   control: "multiselect",
};

let lastDraft: string | undefined;

/**
 * The panel wired to the state hook through a host that keeps applied values in
 * its "URL" and feeds them straight back in, which is what `NotebookPage` does
 * through react-router. The loop is the point: a control's next value is
 * computed from the value it is currently rendering, so anything that delays
 * that value getting back to it is where a lost click would come from.
 */
function Harness({ options }: { options?: Map<string, string[]> }) {
   const [params, setParams] = useState<Record<string, string>>({});
   const { draft, setGiven } = useGivensState({
      declaredTypes: TYPES,
      params,
      onParamsChange: setParams,
      autorun: true,
   });
   lastDraft = draft.get("BRAND") as string | undefined;
   return (
      <GivensPanel
         givens={[MULTISELECT]}
         values={draft}
         onChange={setGiven}
         onReset={() => {}}
         options={options ?? new Map([["BRAND", ["Loft", "Aurora", "Nike"]]])}
      />
   );
}

// The combobox by role, not by label: once a value is picked the control also
// renders a chip carrying the same accessible name, and getByLabelText then
// matches two elements.
const combobox = () => screen.getByRole("combobox");

function pick(label: string) {
   fireEvent.keyDown(combobox(), { key: "ArrowDown" });
   fireEvent.click(screen.getByRole("option", { name: label }));
}

describe("GivensPanel: multiselect", () => {
   it("joins a second pick onto the first rather than replacing it", () => {
      // A Playwright spec on the originating branch documented the opposite,
      // "a second click sent immediately lands on the pre-first-pick selection
      // and replaces Loft instead of joining it", and worked around it by
      // awaiting a URL change between the clicks. It does not reproduce here,
      // with the host's round trip in place and no await between the clicks, so
      // this pins the behaviour that is actually observable at this level
      // rather than encoding a race nobody can currently demonstrate.
      render(<Harness />);

      pick("Loft");
      expect(decodeFilterList(lastDraft ?? "")).toEqual(["Loft"]);

      pick("Aurora");
      expect(decodeFilterList(lastDraft ?? "")).toEqual(["Loft", "Aurora"]);
   });

   it("encodes the selection as a filter Malloy reads as 'any of these'", () => {
      render(<Harness options={new Map([["BRAND", ["Ben & Jerry, Inc"]]])} />);

      pick("Ben & Jerry, Inc");

      // Escaped, so the comma stays inside one value instead of splitting it
      // into two brands.
      expect(lastDraft).toBe("Ben\\ &\\ Jerry\\,\\ Inc");
      expect(decodeFilterList(lastDraft ?? "")).toEqual(["Ben & Jerry, Inc"]);
   });
});

describe("GivensPanel: a suggest query that failed", () => {
   it("says so, instead of showing an empty dropdown", () => {
      // An empty option list and a failed query render identically, and mean
      // opposite things. Without this the reader sees a picker with nothing in
      // it and concludes the data has no values.
      render(
         <GivensPanel
            givens={[MULTISELECT]}
            values={new Map()}
            onChange={() => {}}
            onReset={() => {}}
            options={new Map()}
            optionsFailed={new Set(["BRAND"])}
         />,
      );

      expect(
         screen.getByText(/Could not load the options for this control/),
      ).toBeDefined();
   });

   it("says nothing when the dimension simply has no values", () => {
      render(
         <GivensPanel
            givens={[MULTISELECT]}
            values={new Map()}
            onChange={() => {}}
            onReset={() => {}}
            options={new Map([["BRAND", []]])}
         />,
      );

      expect(screen.queryAllByText(/Could not load the options/)).toHaveLength(
         0,
      );
   });
});

describe("layout and title", () => {
   // Both props exist for the dashboard surface, which does not land in this
   // slice, so nothing in the repo passes either one. Exercised here so the
   // first render of the `bar` branch is not an external consumer's.
   const props = {
      givens: [MULTISELECT],
      values: new Map(),
      onChange: () => {},
      onReset: () => {},
      options: new Map([["BRAND", ["Nike"]]]),
   };

   it('titles the panel "Parameters" by default', () => {
      render(<GivensPanel {...props} />);
      expect(screen.getByText("Parameters")).toBeDefined();
   });

   it("uses a given title in place of the default", () => {
      render(<GivensPanel {...props} title="Filters" />);
      expect(screen.getByText("Filters")).toBeDefined();
      expect(screen.queryAllByText("Parameters")).toHaveLength(0);
   });

   it("renders the bar layout with its controls and no heading", () => {
      render(<GivensPanel {...props} layout="bar" />);
      // The bar is meant to sit inline above a dashboard, so it carries the
      // controls but drops the heading the panel layout shows.
      expect(screen.getByRole("combobox")).toBeDefined();
      expect(screen.queryAllByText("Parameters")).toHaveLength(0);
   });

   it("still offers Reset from the bar layout", () => {
      let reset = 0;
      render(
         <GivensPanel
            {...props}
            layout="bar"
            values={new Map([["BRAND", "Nike"]])}
            onReset={() => {
               reset += 1;
            }}
         />,
      );
      fireEvent.click(screen.getByRole("button", { name: "Reset" }));
      expect(reset).toBe(1);
   });
});
