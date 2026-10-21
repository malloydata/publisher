// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

   it('titles the panel "Filters" by default', () => {
      render(<GivensPanel {...props} />);
      expect(screen.getByText("Filters")).toBeDefined();
   });

   it("uses a given title in place of the default", () => {
      render(<GivensPanel {...props} title="Refine" />);
      expect(screen.getByText("Refine")).toBeDefined();
      expect(screen.queryAllByText("Filters")).toHaveLength(0);
   });

   it("renders the bar layout as filter buttons that open their controls, with no heading", () => {
      render(<GivensPanel {...props} layout="bar" />);
      // The bar sits inline above a report: a button per control, naming it
      // and its value, and no heading. The control is in its popover.
      expect(screen.queryByRole("combobox")).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: /^BRAND: / }));
      expect(screen.getByRole("combobox")).toBeDefined();
      expect(screen.queryAllByText("Filters")).toHaveLength(0);
   });

   it("offers no editing outside the builder", () => {
      render(<GivensPanel {...props} layout="bar" />);
      expect(screen.queryByLabelText(/^Remove filter/)).toBeNull();
      expect(screen.queryByLabelText("Add filter")).toBeNull();
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

   it("offers no Reset when the values are already where it would put them", () => {
      // A value the document starts with is set, but Reset would change
      // nothing: offered anyway, it was a button that did nothing.
      render(
         <GivensPanel
            {...props}
            layout="bar"
            values={new Map([["BRAND", "Nike"]])}
            canReset={false}
         />,
      );
      expect(screen.queryByRole("button", { name: "Reset" })).toBeNull();
   });

   it("offers Reset when the values have moved off the start, even to empty", () => {
      render(<GivensPanel {...props} layout="bar" canReset />);
      expect(screen.getByRole("button", { name: "Reset" })).toBeDefined();
   });
});

describe("GivensPanel: Apply with autorun off", () => {
   const TEXT: Given = { name: "REGION", type: "string" };

   function ApplyHarness({ onApplied }: { onApplied: () => void }) {
      const [params, setParams] = useState<Record<string, string>>({});
      const { draft, setGiven, apply, pending } = useGivensState({
         declaredTypes: new Map([["REGION", "string"]]),
         params,
         onParamsChange: setParams,
         autorun: false,
      });
      return (
         <GivensPanel
            givens={[TEXT]}
            values={draft}
            onChange={setGiven}
            onReset={() => {}}
            apply={{
               onApply: () => {
                  onApplied();
                  apply();
               },
               pending,
            }}
         />
      );
   }

   it("applies a click that lands right after typing, before the draft settles", () => {
      let applied = 0;
      render(<ApplyHarness onApplied={() => applied++} />);
      const field = screen.getByRole("textbox");
      const button = screen.getByRole("button", { name: "Apply" });

      field.focus();
      fireEvent.change(field, { target: { value: "CA" } });
      // The browser's order: mousedown on the button blurs the field, which
      // flushes the draft, and only then does the click fire.
      fireEvent.mouseDown(button);
      fireEvent.blur(field);
      fireEvent.mouseUp(button);
      fireEvent.click(button);

      expect(applied).toBe(1);
      expect(button.getAttribute("aria-disabled")).toBe("true");
   });

   it("does nothing on a click while nothing is pending", () => {
      let applied = 0;
      render(<ApplyHarness onApplied={() => applied++} />);
      const button = screen.getByRole("button", { name: "Apply" });

      fireEvent.click(button);

      expect(applied).toBe(0);
      expect(button.getAttribute("aria-disabled")).toBe("true");
   });
});
