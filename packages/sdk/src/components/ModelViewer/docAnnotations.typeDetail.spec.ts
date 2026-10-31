// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type * as Malloy from "@malloydata/malloy-interfaces";
import {
   atomicTypeDetail,
   getFieldTagLines,
   getModelDocString,
   recordFieldsTip,
} from "./docAnnotations";
import { modelDocOf } from "./useModelSources";

// A field's type chip says what the kind alone drops, and its tag pills are the
// author's display tags, never the platform's persistence tags.
const t = (type: unknown) => type as Malloy.AtomicType;

describe("atomicTypeDetail", () => {
   it("keeps a plain kind as it is", () => {
      expect(atomicTypeDetail(t({ kind: "string_type" }))).toBe("string");
      expect(atomicTypeDetail(t({ kind: "number_type" }))).toBe("number");
   });

   it("adds a number's subtype and a time field's timeframe", () => {
      expect(
         atomicTypeDetail(t({ kind: "number_type", subtype: "integer" })),
      ).toBe("number · integer");
      expect(
         atomicTypeDetail(t({ kind: "date_type", timeframe: "month" })),
      ).toBe("date · month");
      expect(
         atomicTypeDetail(t({ kind: "timestamp_type", timeframe: "hour" })),
      ).toBe("timestamp · hour");
   });

   it("names an array's element type and a native column's SQL type", () => {
      expect(
         atomicTypeDetail(
            t({ kind: "array_type", element_type: { kind: "string_type" } }),
         ),
      ).toBe("array<string>");
      expect(
         atomicTypeDetail(
            t({ kind: "sql_native_type", sql_type: "GEOGRAPHY" }),
         ),
      ).toBe("sql native · GEOGRAPHY");
   });

   it("counts a record's fields, and lists them for its tooltip", () => {
      const record = t({
         kind: "record_type",
         fields: [
            { name: "city", type: { kind: "string_type" } },
            { name: "zip", type: { kind: "number_type", subtype: "integer" } },
         ],
      });
      expect(atomicTypeDetail(record)).toBe("record · 2 fields");
      expect(recordFieldsTip(record)).toBe("city string\nzip number · integer");
      expect(recordFieldsTip(t({ kind: "string_type" }))).toBeUndefined();
   });
});

describe("getFieldTagLines", () => {
   it("keeps display tags and drops docs, namespaced and persistence tags", () => {
      const annotations = [
         { value: '# label="Revenue"\n' },
         { value: "# currency\n" },
         { value: '#(doc) "Total revenue"\n' },
         { value: "#(index)\n" },
         { value: '#@ preaggregate grain="status"\n' },
      ];
      expect(getFieldTagLines(annotations)).toEqual([
         'label="Revenue"',
         "currency",
      ]);
   });
});

describe("getModelDocString", () => {
   it("reads a model's ##(doc) and ##\" lines, and leaves other ## lines out", () => {
      const annotations = [
         { value: "##! experimental.persistence\n" },
         {
            value: '##(doc) "Orders, with the rules that govern who reads them."\n',
         },
         { value: '##" Built from public.orders.\n' },
         { value: '#(doc) "a source doc, not the model\'s"\n' },
      ];
      expect(getModelDocString(annotations)).toBe(
         "Orders, with the rules that govern who reads them.\nBuilt from public.orders.",
      );
      expect(
         getModelDocString([{ value: "##! experimental\n" }]),
      ).toBeUndefined();
   });
});

describe("modelDocOf", () => {
   it("prefers modelInfo's annotations, and reads the file's ## lines when it has none", () => {
      const withDoc = JSON.stringify({
         entries: [],
         annotations: [{ value: '##(doc) "From modelInfo"\n' }],
      });
      expect(modelDocOf(withDoc, '##(doc) "From text"')).toBe("From modelInfo");
      const without = JSON.stringify({ entries: [] });
      expect(
         modelDocOf(
            without,
            '##! experimental\n##(doc) "From text"\nsource: x is y',
         ),
      ).toBe("From text");
      expect(modelDocOf(undefined, "source: x is y")).toBeUndefined();
   });
});
