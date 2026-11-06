// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as malloy from "@malloydata/malloy";
import { namedQueryCode as queryCodeOf } from "./namedQueryCode";
import { parseModelText } from "./modelTextParse";

const namedQueryCode = (text: string | undefined, name: string) =>
   queryCodeOf(
      text === undefined ? undefined : parseModelText(malloy, text),
      name,
   );

const FILE = `source: flights is duckdb.table('flights.parquet') extend {
  measure: flight_count is count()
}

#(doc) Top carriers by flights
query: top_carriers is flights -> {
  group_by: carrier
  aggregate: flight_count
  where: carrier != "}"
}

query: by_month is flights
  -> { group_by: dep_month is dep_time.month }
  -> { select: * }

run: flights -> { aggregate: flight_count }
`;

describe("namedQueryCode", () => {
   it("cuts a named query's declaration out of its model file", () => {
      expect(namedQueryCode(FILE, "top_carriers")).toBe(
         `query: top_carriers is flights -> {\n  group_by: carrier\n  aggregate: flight_count\n  where: carrier != "}"\n}`,
      );
   });

   it("keeps a pipeline continued on the next lines", () => {
      expect(namedQueryCode(FILE, "by_month")).toBe(
         "query: by_month is flights\n  -> { group_by: dep_month is dep_time.month }\n  -> { select: * }",
      );
   });

   it("reads each of several queries one `query:` declares", () => {
      const text =
         "query: a is flights -> { group_by: carrier }, b is flights -> { group_by: origin }\n";
      expect(namedQueryCode(text, "b")).toBe(
         "query: b is flights -> { group_by: origin }",
      );
   });

   it("has nothing to show for a name the file doesn't declare, or no file", () => {
      expect(namedQueryCode(FILE, "nope")).toBeUndefined();
      expect(namedQueryCode(undefined, "top_carriers")).toBeUndefined();
   });
});
