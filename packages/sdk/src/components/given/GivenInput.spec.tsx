import { describe, expect, it, mock } from "bun:test";
import { fireEvent, render, screen } from "@testing-library/react";
import type { GivenValue } from "../../hooks/useGivensForm";
import { GivenInput } from "./GivenInput";

/**
 * Covers the three-state contract GivenInput's own doc comment specifies:
 * unset vs. an explicit override (including `""` and `false`) vs. a revert.
 * None of it had any verification before this suite.
 *
 * The distinction is not cosmetic: unset omits the given from the request so
 * the server applies the model default, while an explicit `""` is sent
 * verbatim. The only thing telling a user which state they are in is whether
 * the clear (x) affordance is showing, so each test below asserts on it.
 */

const clearButtons = () => screen.queryAllByLabelText("clear value");

describe("GivenInput: unset vs. explicit override", () => {
   it("unset shows the model default as a placeholder and offers no revert", () => {
      render(
         <GivenInput
            given={{ name: "region", type: "string", default: "'WN'" }}
            value={undefined}
            onChange={() => {}}
         />,
      );

      const input = screen.getByLabelText("region") as HTMLInputElement;
      expect(input.value).toBe("");
      expect(input.placeholder).toBe("WN");
      // Nothing to revert to: the widget is already showing the default.
      expect(clearButtons()).toHaveLength(0);
   });

   it("an explicit empty string is an override, not an unset", () => {
      render(
         <GivenInput
            given={{ name: "region", type: "string", default: "'WN'" }}
            value=""
            onChange={() => {}}
         />,
      );

      const input = screen.getByLabelText("region") as HTMLInputElement;
      expect(input.value).toBe("");
      // The ghost default is suppressed and the revert appears, which is the
      // only thing distinguishing this from the unset case above: both render
      // an empty box.
      expect(input.placeholder).toBe("");
      expect(clearButtons()).toHaveLength(1);
   });

   it("the revert affordance reverts rather than blanking the field", () => {
      // Typed, not `mock(() => {})`: an untyped zero-arg mock makes
      // `mock.calls` an array of empty tuples, so reading `[0][0]` below does
      // not compile and nothing constrains what the component passes.
      const onChange = mock((_next: GivenValue) => {});
      render(
         <GivenInput
            given={{ name: "region", type: "string" }}
            value="CA"
            onChange={onChange}
         />,
      );

      fireEvent.click(clearButtons()[0]);

      // null, not "". `useGivensForm` deletes the key on null, which is what
      // returns the given to "use the model default".
      expect(onChange).toHaveBeenCalledTimes(1);
      expect(onChange.mock.calls[0][0]).toBeNull();
   });
});

describe("GivenInput: boolean is three-state, not truthy/falsy", () => {
   it("unset reflects the model default so the box matches what will run", () => {
      render(
         <GivenInput
            given={{ name: "active", type: "boolean", default: "true" }}
            value={undefined}
            onChange={() => {}}
         />,
      );

      const box = screen.getByLabelText("active") as HTMLInputElement;
      expect(box.checked).toBe(true);
      expect(clearButtons()).toHaveLength(0);
   });

   it("an explicit false is an override, distinguishable from unset", () => {
      render(
         <GivenInput
            given={{ name: "active", type: "boolean", default: "true" }}
            value={false}
            onChange={() => {}}
         />,
      );

      const box = screen.getByLabelText("active") as HTMLInputElement;
      expect(box.checked).toBe(false);
      // Guards the `typeof value === "boolean"` test in the component. A
      // truthiness check would read `false` as unset and hide this revert,
      // stranding the user on a value the server is being sent.
      expect(clearButtons()).toHaveLength(1);
   });
});

describe("GivenInput: helper text", () => {
   it("surfaces a description annotation verbatim", () => {
      render(
         <GivenInput
            given={{
               name: "since",
               type: "string",
               annotations: ['#(description="Earliest report date")'],
            }}
            value={undefined}
            onChange={() => {}}
         />,
      );

      expect(screen.getByText("Earliest report date")).toBeDefined();
   });

   it("shows an explicit empty-string default as (empty), not as no default", () => {
      render(
         <GivenInput
            given={{ name: "region", type: "string", default: "''" }}
            value={undefined}
            onChange={() => {}}
         />,
      );

      // Guards the `!== undefined` test in the component: `is ''` renders as
      // "", and a truthiness check would drop the caption entirely, making a
      // real empty-string default look like no default at all.
      expect(screen.getByText("Default: (empty)")).toBeDefined();
   });
});

describe("GivenInput: number", () => {
   it("treats 0 as a value, not as absent", () => {
      render(
         <GivenInput
            given={{ name: "limit", type: "number" }}
            value={0}
            onChange={() => {}}
         />,
      );

      const input = screen.getByLabelText("limit") as HTMLInputElement;
      expect(input.value).toBe("0");
      expect(clearButtons()).toHaveLength(1);
   });

   it("still offers a revert when the value is not a number", () => {
      // Reachable through the public component API: GivenInput and GivenValue
      // are both published SDK exports, and GivenValue is a union permitting a
      // string while `type` says number.
      render(
         <GivenInput
            given={{ name: "limit", type: "number" }}
            value={"12abc"}
            onChange={() => {}}
         />,
      );

      // The box reads empty because the component substitutes "" for a
      // non-number, and passing the raw string instead would not help: a
      // browser discards it anyway on input[type=number] (checked in
      // Chromium). So the revert is the user's only route out, and it used to
      // be hidden too, because it keyed off the rendered string being
      // non-empty rather than off a value being overridden.
      expect(clearButtons()).toHaveLength(1);
   });
});

describe("GivenInput: which types are actually reachable", () => {
   it("renders a multi-value widget only for the array<...> spelling", () => {
      render(
         <GivenInput
            given={{ name: "tags", type: "array<string>" }}
            value={undefined}
            onChange={() => {}}
         />,
      );
      expect(screen.queryAllByRole("combobox")).toHaveLength(1);
   });

   it("falls back to a text box for the bare array type the server emits", () => {
      // The array<...> branch is unreachable twice over. Malloy's grammar emits
      // only scalar parameter types, so no array given exists to send; and if
      // one did, `given.ts` renders the wire type as the bare discriminator
      // "array", which does not match the component's `startsWith("array<")`.
      render(
         <GivenInput
            given={{ name: "tags", type: "array" }}
            value={undefined}
            onChange={() => {}}
         />,
      );
      expect(screen.queryAllByRole("combobox")).toHaveLength(0);
      expect(screen.getByLabelText("tags")).toBeDefined();
   });
});
