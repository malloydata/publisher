// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type * as Malloy from "@malloydata/malloy-interfaces";
import {
   neutralizeFormula,
   resultToCsv,
   rowsFileName,
   serverCappedAt,
} from "./rowsCsv";

const field = (name: string): Malloy.FieldInfo => ({
   kind: "dimension",
   name,
   type: { kind: "string_type" },
});

const result = (names: string[], rows: Malloy.Cell[][]): Malloy.Result => ({
   connection_name: "duckdb",
   schema: { fields: names.map(field) },
   data: {
      kind: "array_cell",
      array_value: rows.map((record_value) => ({
         kind: "record_cell",
         record_value,
      })),
   },
});

const str = (string_value: string): Malloy.Cell => ({
   kind: "string_cell",
   string_value,
});

describe("the rows as CSV", () => {
   it("quotes a value holding a comma, a quote or a line break", () => {
      const csv = resultToCsv(
         result(
            ["owner"],
            [
               [str("Bob's team")],
               [str('say "hi"')],
               [str("a,b")],
               [str("one\ntwo")],
            ],
         ),
      );
      expect(csv).toBe(
         '\uFEFFowner\r\nBob\'s team\r\n"say ""hi"""\r\n"a,b"\r\n"one\ntwo"\r\n',
      );
   });

   it("writes a null as an empty field, and a number, boolean, date and timestamp as they are", () => {
      const csv = resultToCsv(
         result(
            ["a", "b", "c", "d", "e"],
            [
               [
                  { kind: "null_cell" },
                  { kind: "number_cell", number_value: -12.5 },
                  { kind: "boolean_cell", boolean_value: false },
                  { kind: "date_cell", date_value: "2025-12-01" },
                  {
                     kind: "timestamp_cell",
                     timestamp_value: "2025-12-01T08:30:00.000Z",
                  },
               ],
            ],
         ),
      );
      expect(csv).toBe(
         "\uFEFFa,b,c,d,e\r\n,-12.5,false,2025-12-01,2025-12-01T08:30:00.000Z\r\n",
      );
   });

   it("prefixes text a spreadsheet would run as a formula, but never a number", () => {
      expect(neutralizeFormula('=HYPERLINK("http://x","x")')).toBe(
         `'=HYPERLINK("http://x","x")`,
      );
      for (const lead of ["=", "+", "-", "@"])
         expect(neutralizeFormula(`${lead}1`)).toBe(`'${lead}1`);
      expect(neutralizeFormula("West")).toBe("West");
      const csv = resultToCsv(
         result(["=name"], [[str("@SUM(A1)")], [str("-2+3")]]),
      );
      expect(csv).toBe("\uFEFF'=name\r\n'@SUM(A1)\r\n'-2+3\r\n");
   });

   it("names the file by the source and each value, in plain characters", () => {
      expect(rowsFileName("orders", ["Complete", "2025-12"])).toBe(
         "orders_rows_Complete_2025-12",
      );
      expect(rowsFileName("owners", ["West", "Bob's team"])).toBe(
         "owners_rows_West_Bob_s_team",
      );
   });

   it("says the server cut a result short only when its own default did", () => {
      expect(serverCappedAt(1000, 1000, "server_default")).toBe(1000);
      expect(serverCappedAt(999, 1000, "server_default")).toBeUndefined();
      expect(serverCappedAt(1000, 1000, "query")).toBeUndefined();
      expect(serverCappedAt(5, undefined, undefined)).toBeUndefined();
   });
});
