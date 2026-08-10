import { describe, expect, it, mock } from "bun:test";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
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

      // null, not "". `useGivensState` deletes the key on null, which is what
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

describe("GivenInput, which types are actually reachable", () => {
   // There used to be an `array<...>` branch here rendering a multi-value
   // widget, and it was unreachable twice over. Malloy's grammar emits only
   // scalar parameter types, so no array given exists to send; and if one did,
   // `given.ts` renders the wire type as the bare discriminator "array", which
   // never matched that branch's `startsWith("array<")` test. It is gone, and
   // both spellings now land on the text box every unknown type gets.
   for (const type of ["array<string>", "array", "record", "geography"]) {
      it(`falls back to a text box for ${type}`, () => {
         render(
            <GivenInput
               given={{ name: "tags", type }}
               value={undefined}
               onChange={() => {}}
            />,
         );
         expect(screen.queryAllByRole("combobox")).toHaveLength(0);
         expect(screen.getByLabelText("tags")).toBeDefined();
      });
   }
});

describe("GivenInput: a control= the widget cannot honour falls through", () => {
   // The picker speaks two languages: plain strings and the STRING filter
   // grammar. Honouring `control=select` on anything else corrupts the value,
   // a `filter<date>` would get string-grammar escaping the temporal parser
   // rejects, and a `number` given would get a string `givensToRequest` forwards
   // verbatim for Malloy to refuse. Nothing populates `control` yet, so these
   // pin the refusal before the slice that starts populating it.
   const pickerCount = () => screen.queryAllByRole("combobox").length;

   for (const type of ["string", "filter<string>"]) {
      it(`renders a picker for ${type}`, () => {
         render(
            <GivenInput
               given={{ name: "g", type, control: "select" }}
               value={undefined}
               onChange={() => {}}
            />,
         );
         expect(pickerCount()).toBe(1);
      });
   }

   for (const type of [
      "number",
      "boolean",
      "date",
      "timestamp",
      "filter<number>",
      "filter<date>",
   ]) {
      it(`refuses a picker for ${type} and uses the type's own widget`, () => {
         render(
            <GivenInput
               given={{ name: "g", type, control: "multiselect" }}
               value={undefined}
               onChange={() => {}}
            />,
         );
         expect(pickerCount()).toBe(0);
      });
   }
});

describe("GivenInput: a slider only claims a filter it can represent", () => {
   it("renders a slider for a lower-bound filter", () => {
      render(
         <GivenInput
            given={{
               name: "n",
               type: "filter<number>",
               rangeMin: 0,
               rangeMax: 10,
            }}
            value=">= 5"
            onChange={() => {}}
         />,
      );
      expect(screen.queryAllByRole("slider")).toHaveLength(1);
   });

   it("falls through for a filter it cannot represent", () => {
      // Otherwise `decodeAtLeast` returns undefined, the control reads "Any"
      // while a range is in force, and the first drag replaces it silently.
      render(
         <GivenInput
            given={{
               name: "n",
               type: "filter<number>",
               rangeMin: 0,
               rangeMax: 10,
            }}
            value="1 to 5"
            onChange={() => {}}
         />,
      );
      expect(screen.queryAllByRole("slider")).toHaveLength(0);
      expect((screen.getByLabelText("n") as HTMLInputElement).value).toBe(
         "1 to 5",
      );
   });
});

describe("GivenInput: multi-pick needs a type that can carry several values", () => {
   it("is single-pick for a plain string given, even when asked for multi", () => {
      // A real array cannot survive the URL: `givenToParam` joins on `,` and
      // reading it back yields the joined string, not the list. So a plain
      // `string` given renders single-pick rather than emitting something the
      // address bar would corrupt.
      const onChange = mock((_next: GivenValue) => {});
      render(
         <GivenInput
            given={{ name: "g", type: "string", control: "multiselect" }}
            value={undefined}
            onChange={onChange}
         />,
      );
      const input = screen.getByRole("combobox");
      fireEvent.keyDown(input, { key: "ArrowDown" });
      // No chips container: a single-pick Autocomplete has no multiple slot.
      expect(screen.queryAllByRole("button", { name: /^g/ })).toHaveLength(0);
   });

   it("is multi-pick for a filter<string> given", () => {
      render(
         <GivenInput
            given={{
               name: "g",
               type: "filter<string>",
               control: "multiselect",
            }}
            value={undefined}
            onChange={() => {}}
            options={["a", "b"]}
         />,
      );
      expect(screen.queryAllByRole("combobox")).toHaveLength(1);
   });
});

describe("GivenInput: the whole type x control matrix", () => {
   // Six guards decide which widget a given gets (`pickableType`,
   // `multiplePickable`, `filterIsPickable`, `bareDate`, `dateIsPickable`,
   // `sliderIsRepresentable`), each added for its own reason. Read one at a
   // time they are each defensible. What nothing checked is what they do as a
   // SET, so this enumerates every combination rather than sampling, and
   // the property that matters most is the last assertion: no combination falls
   // through every branch and renders nothing at all.
   const widget = () => {
      if (screen.queryAllByRole("combobox").length) return "picker";
      if (screen.queryAllByRole("slider").length) return "slider";
      if (screen.queryAllByRole("checkbox").length) return "checkbox";
      if (screen.queryAllByRole("spinbutton").length) return "number";
      if (screen.queryAllByRole("textbox").length) return "text";
      return "NONE";
   };

   const show = (
      type: string | undefined,
      control?: string,
      range?: boolean,
      value?: GivenValue,
   ) => {
      // Explicitly, because Testing Library's automatic cleanup runs BETWEEN
      // tests: several renders inside one test otherwise stack up and every
      // query sees the previous widget too.
      cleanup();
      render(
         <GivenInput
            given={
               {
                  name: "g",
                  type,
                  control,
                  ...(range ? { rangeMin: 0, rangeMax: 10 } : {}),
               } as Given
            }
            value={value}
            onChange={() => {}}
         />,
      );
      return widget();
   };

   it("gives a picker only to the string family", () => {
      expect(show("string", "select")).toBe("picker");
      expect(show("filter<string>", "multiselect")).toBe("picker");
      // An undeclared type defaults to string, so it may pick too.
      expect(show(undefined, "select")).toBe("picker");
      // Everything else refuses, because the picker only speaks strings.
      for (const type of [
         "number",
         "boolean",
         "date",
         "timestamp",
         "timestamptz",
         "filter<number>",
         "filter<date>",
      ]) {
         expect(show(type, "select")).not.toBe("picker");
      }
   });

   it("gives a slider to a bounded number, control= or not", () => {
      expect(show("number", undefined, true)).toBe("slider");
      expect(show("filter<number>", undefined, true)).toBe("slider");
      // A `control=select` on a bounded number still gets the slider: the
      // picker refuses the type, and the slider is what the type implies.
      expect(show("number", "select", true)).toBe("slider");
      // Without bounds there is no slider to give.
      expect(show("number", undefined, false)).toBe("number");
   });

   it("leaves every other combination on its type's own widget", () => {
      expect(show("boolean", "multiselect")).toBe("checkbox");
      expect(show("date", "select")).toBe("text");
      expect(show("record", "select")).toBe("text");
   });

   it("routes on the VALUE too, not just the type and control", () => {
      // Three of the six guards read the value rather than the declaration, so
      // a matrix that only ever passes `undefined` cannot see them.
      expect(show("filter<string>", "select", false, "Nike")).toBe("picker");
      // A filter the picker cannot represent falls through to the text box, so
      // the author's filter stays visible rather than being re-encoded.
      expect(show("filter<string>", "select", false, "-Nike")).toBe("text");
      // A lower-bound filter keeps the slider; a range cannot be a threshold.
      expect(show("filter<number>", undefined, true, ">= 5")).toBe("slider");
      expect(show("filter<number>", undefined, true, "1 to 5")).toBe("text");
   });

   it("never renders nothing, for any combination", () => {
      const types = [
         "string",
         "number",
         "boolean",
         "date",
         "timestamp",
         "timestamptz",
         "filter<string>",
         "filter<number>",
         "filter<date>",
         "filter<timestamp>",
         "array<string>",
         "record",
         undefined,
      ];
      const values = [
         undefined,
         "",
         "Nike",
         "-Nike",
         ">= 5",
         "1 to 5",
         "2024-01-15",
         5,
         true,
      ] as GivenValue[];
      for (const type of types) {
         for (const control of [undefined, "select", "multiselect"]) {
            for (const range of [false, true]) {
               for (const value of values) {
                  expect(show(type, control, range, value)).not.toBe("NONE");
               }
            }
         }
      }
   });
});

describe("GivenInput: a date value the codec cannot read", () => {
   // Four states (readable, unreadable, empty, unset) and only two of them,
   // empty and unset, look alike: both render a blank picker. The state worth
   // pinning is UNREADABLE, because a DatePicker can only display a date, so
   // the obvious rendering of `?D=last month` is a blank picker whose own
   // `clearable` never appears, which would filter every cell while being
   // invisible and impossible to revert. What this branch must do instead is
   // show the offending value and offer a revert, which is what follows.
   const DATE: Given = { name: "D", type: "date" };

   it("shows the offending value instead of rendering blank", () => {
      render(
         <GivenInput given={DATE} value="last month" onChange={() => {}} />,
      );
      expect(screen.getByDisplayValue("last month")).toBeDefined();
      expect(screen.getByText(/Not a date/)).toBeDefined();
   });

   it("does not let the user edit the unreadable value in place", () => {
      // An editable version of this field swapped itself for the picker the
      // moment the typed text first parsed, taking the cursor with it.
      render(
         <GivenInput given={DATE} value="last month" onChange={() => {}} />,
      );
      expect(screen.getByDisplayValue("last month")).toHaveProperty(
         "readOnly",
         true,
      );
   });

   it("keeps the calendar button on a readable value", () => {
      // Putting the revert in the picker's own `InputProps` REPLACED this
      // button, leaving a date control with no way to open a calendar.
      render(
         <GivenInput given={DATE} value="2024-01-05" onChange={() => {}} />,
      );
      expect(
         screen.getByRole("button", { name: /choose date/i }),
      ).toBeDefined();
   });

   it("offers a revert, which the picker's own clearable does not", () => {
      const onChange = mock((_next: GivenValue) => {});
      render(
         <GivenInput given={DATE} value="last month" onChange={onChange} />,
      );

      fireEvent.click(screen.getByRole("button", { name: "clear value" }));

      expect(onChange.mock.calls[0][0]).toBeNull();
   });

   it("does not pin the picker's own error state when the value is fine", () => {
      // `error` passed through `slotProps.textField` wins over MUI's internal
      // validation for BOTH values, so a hardcoded `false` silently suppressed
      // the picker's real errors.
      render(
         <GivenInput given={DATE} value="2024-01-05" onChange={() => {}} />,
      );
      expect(screen.getByRole("textbox").getAttribute("aria-invalid")).not.toBe(
         "true",
      );
   });

   it("reads a plain date string as a date rather than an error", () => {
      // `# drill { to=self }` onto a date given hands over a bare `YYYY-MM-DD`
      // STRING, not a Date, so treating every non-Date as unreadable painted an
      // error over a value the product itself had just produced.
      render(
         <GivenInput given={DATE} value="2024-01-05" onChange={() => {}} />,
      );
      expect(screen.queryAllByText(/Not a date/)).toHaveLength(0);
   });

   it("treats an empty date value as an empty box, not an error", () => {
      render(<GivenInput given={DATE} value="" onChange={() => {}} />);
      expect(screen.queryAllByText(/Not a date/)).toHaveLength(0);
   });

   it("says nothing when the given is simply unset", () => {
      render(<GivenInput given={DATE} value={undefined} onChange={() => {}} />);
      expect(screen.queryAllByText(/Not a date/)).toHaveLength(0);
      expect(
         screen.queryAllByRole("button", { name: "clear value" }),
      ).toHaveLength(0);
   });
});

describe("GivenInput: a number value the control cannot show", () => {
   // The same hazard as the date branch, and for a long time handled a
   // different way: the box substituted a blank, so the value went on filtering
   // every cell while the control looked empty and said nothing about it.
   const NUM: Given = { name: "N", type: "number" };

   it("shows the offending value rather than a blank box", () => {
      render(<GivenInput given={NUM} value="lots" onChange={() => {}} />);
      expect(screen.getByDisplayValue("lots")).toBeDefined();
      expect(screen.getByText(/Not a number/)).toBeDefined();
   });

   it("offers a revert", () => {
      const onChange = mock((_next: GivenValue) => {});
      render(<GivenInput given={NUM} value="lots" onChange={onChange} />);
      fireEvent.click(screen.getByRole("button", { name: "clear value" }));
      expect(onChange.mock.calls[0][0]).toBeNull();
   });

   it("treats an empty value as an empty box, not an error", () => {
      // `?N=` in a URL arrives as the empty string. Calling that "Not a number"
      // painted a red error over a field showing nothing at all.
      render(<GivenInput given={NUM} value="" onChange={() => {}} />);
      expect(screen.queryAllByText(/Not a number/)).toHaveLength(0);
   });

   it("leaves an ordinary number alone", () => {
      render(<GivenInput given={NUM} value={42} onChange={() => {}} />);
      expect(screen.queryAllByText(/Not a number/)).toHaveLength(0);
      expect(screen.getByDisplayValue("42")).toBeDefined();
   });
});

describe("GivenInput: a boolean value the control cannot show", () => {
   const FLAG: Given = { name: "F", type: "boolean", default: "true" };

   it("does not render the model default for a value it is still sending", () => {
      // `?F=yes` reaches here as a string, which the codec passes through on
      // purpose. The checkbox used to show the DEFAULT for it, so the box said
      // one thing while the request carried "yes" and every cell failed.
      render(<GivenInput given={FLAG} value="yes" onChange={() => {}} />);
      expect(screen.getByDisplayValue("yes")).toBeDefined();
      expect(screen.getByText(/Not a true or false/)).toBeDefined();
   });

   it("offers a revert", () => {
      const onChange = mock((_next: GivenValue) => {});
      render(<GivenInput given={FLAG} value="yes" onChange={onChange} />);
      fireEvent.click(screen.getByRole("button", { name: "clear value" }));
      expect(onChange.mock.calls[0][0]).toBeNull();
   });

   it("leaves a real boolean, and an unset given, alone", () => {
      render(<GivenInput given={FLAG} value={false} onChange={() => {}} />);
      expect(screen.queryAllByText(/Not a true or false/)).toHaveLength(0);
      cleanup();
      render(<GivenInput given={FLAG} value={undefined} onChange={() => {}} />);
      expect(screen.queryAllByText(/Not a true or false/)).toHaveLength(0);
   });
});

describe("GivenInput: a slider value the control cannot place", () => {
   // Latent today, because nothing populates rangeMin/rangeMax, which is why it
   // is worth pinning before anything does.
   const SLIDER: Given = {
      name: "MIN",
      type: "number",
      rangeMin: 0,
      rangeMax: 100,
   };

   it('does not read as "Any" for a value it is still sending', () => {
      render(<GivenInput given={SLIDER} value="lots" onChange={() => {}} />);
      expect(screen.getByDisplayValue("lots")).toBeDefined();
      expect(screen.getByText(/Not a number/)).toBeDefined();
   });

   it("still renders a slider for a value it can place", () => {
      render(<GivenInput given={SLIDER} value={42} onChange={() => {}} />);
      expect(screen.queryAllByText(/Not a number/)).toHaveLength(0);
      expect(screen.getByRole("slider")).toBeDefined();
   });
});
