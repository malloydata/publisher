// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import type { Database } from "../../client";
import {
   columnTypeIcon,
   DataSchemaDialog,
   formatRowCount,
} from "./DataSchemaDialog";

const file = (columnCount: number): Database => ({
   path: "data/customers.parquet",
   info: {
      rowCount: 1000,
      columns: Array.from({ length: columnCount }, (_, i) => ({
         name: i === 0 ? "customer_id" : `col_${i}`,
         type: i === 0 ? "number" : "string",
      })),
   },
});

describe("DataSchemaDialog", () => {
   it("titles itself with the file and lists its columns", () => {
      render(<DataSchemaDialog database={file(3)} onClose={() => {}} />);
      const dialog = screen.getByRole("dialog");
      expect(within(dialog).getByText("customers.parquet")).toBeTruthy();
      expect(within(dialog).getByText("data · 1.0 K rows")).toBeTruthy();
      const list = within(dialog).getByRole("list", { name: /Columns/ });
      expect(within(list).getAllByRole("listitem")).toHaveLength(3);
      expect(within(list).getByText("customer_id")).toBeTruthy();
      // A narrow file has nothing to filter.
      expect(within(dialog).queryByLabelText("Filter columns")).toBeNull();
   });

   it("filters a wide file's columns by name", () => {
      render(<DataSchemaDialog database={file(20)} onClose={() => {}} />);
      fireEvent.change(screen.getByLabelText("Filter columns"), {
         target: { value: "customer" },
      });
      const list = screen.getByRole("list", { name: /Columns/ });
      expect(within(list).getAllByRole("listitem")).toHaveLength(1);
   });

   it("shows a file's read error instead of columns", () => {
      render(
         <DataSchemaDialog
            database={{ path: "data/bad.csv", error: "Could not read" }}
            onClose={() => {}}
         />,
      );
      expect(screen.getByText("Could not read")).toBeTruthy();
      expect(screen.queryByRole("list")).toBeNull();
   });

   it("is closed with no file", () => {
      render(<DataSchemaDialog database={null} onClose={() => {}} />);
      expect(screen.queryByRole("dialog")).toBeNull();
   });
});

describe("columnTypeIcon", () => {
   it("tells the Malloy types apart and falls back for an unknown one", () => {
      const kinds = [
         "string",
         "number",
         "date",
         "timestamp",
         "boolean",
         "array<string>",
         "record",
      ].map(columnTypeIcon);
      expect(new Set(kinds).size).toBe(kinds.length);
      expect(columnTypeIcon("sql native")).toBe(columnTypeIcon(undefined));
   });
});

describe("formatRowCount", () => {
   it("abbreviates like the Data section does", () => {
      expect(formatRowCount(12)).toBe("12 rows");
      expect(formatRowCount(25_400)).toBe("25.4 K rows");
      expect(formatRowCount(3_000_000)).toBe("3.0 M rows");
   });
});
