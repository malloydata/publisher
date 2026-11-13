// Review probe (PR #1703, second review): the shadow's metadata rule must not
// flag a nested field whose only difference is the reference id Malloy mints
// per compile (every nest query would read as a difference).

import { describe, expect, it } from "bun:test";
import { shadowFieldDifference } from "./v1_shadow";

const nest = (refId: string) => [
   {
      name: "x",
      type: {
         kind: "array_type",
         element_type: {
            kind: "record_type",
            fields: [
               {
                  name: "d",
                  annotations: [
                     {
                        value: `#(malloy) reference_id = "${refId}" drill_expression { kind = field_reference name = d path = [x] code = d }\n`,
                     },
                  ],
                  type: { kind: "date_type" },
               },
            ],
         },
      },
      annotations: [],
   },
];

describe("shadowFieldDifference over a nest", () => {
   it("two compiles of one nest query are equal", () => {
      expect(
         shadowFieldDifference(
            nest("6b35ece0-770f-4987-afdd-155867a7bb84"),
            nest("9fda3b84-333a-47ef-998a-63e825a0b22a"),
         ),
      ).toBeUndefined();
   });
});
