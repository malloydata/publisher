// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The rows dialog's actions: what a host's `rowsActions` is handed, and the
 * dialog's own Download CSV when a host passes none.
 */
import { afterEach, beforeEach, expect, it, mock } from "bun:test";
import {
   act,
   cleanup,
   fireEvent,
   render,
   screen,
   waitFor,
} from "@testing-library/react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { RowsActionsContext, RowsRequest } from "./RowsDialog";

const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: {
         query?: string;
         givens?: Record<string, unknown>;
         versionId?: string;
      },
      _includeHidden?: boolean,
      _options?: { signal?: AbortSignal },
   ): Promise<unknown> => pending(),
);

mockServerProvider({ models: { executeQueryModel } });

const { RowsDialog, rowsQuery } = await import("./RowsDialog");
const { withPreamble } = await import("./textSource");

const request: RowsRequest = {
   target: { source: "orders", filters: ["region ~ $REGION"] },
   field: "status",
   rawValue: "Complete",
   label: "Complete",
   dimensions: [
      {
         code: "status",
         value: { kind: "string_literal", string_value: "Complete" },
      },
   ],
   drillClauses: [
      {
         filter: {
            kind: "literal_equality",
            expression: { kind: "field_reference", name: "status" },
            value: { kind: "string_literal", string_value: "Complete" },
         },
      },
   ],
};

const givens = { REGION: "West" };

beforeEach(() => {
   clearCache();
   executeQueryModel.mockClear();
});
afterEach(cleanup);

function mount(extra: {
   preamble?: string;
   actions?: (context: RowsActionsContext) => null;
}) {
   render(
      <RowsDialog
         request={request}
         environmentName="env"
         packageName="pkg"
         versionId="v3"
         modelPath="report.malloy"
         givens={givens}
         onClose={() => {}}
         {...extra}
      />,
      { wrapper: serverWrapper },
   );
}

it("hands a host the rows query with no limit, and the givens the dialog sends", async () => {
   const seen: RowsActionsContext[] = [];
   mount({
      actions: (context) => {
         seen.push(context);
         return null;
      },
   });
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   const context = seen.at(-1)!;
   expect(context.query).toBe(rowsQuery(request, null)!);
   expect(context.query).not.toContain("limit:");
   expect(context.preamble).toBeUndefined();
   const sent = executeQueryModel.mock.calls[0][3];
   // The host's query is the dialog's own, less its limit.
   expect(sent.query).toBe(rowsQuery(request)!);
   expect(context.givens).toEqual(sent.givens!);
   expect(context).toMatchObject({
      environmentName: "env",
      packageName: "pkg",
      modelPath: "report.malloy",
      versionId: "v3",
      fileName: "orders_rows_Complete",
   });
   // A host action replaces the dialog's own download.
   expect(screen.queryByRole("button", { name: /Download CSV/ })).toBeNull();
});

it("hands a host the preamble in text-source mode, apart from the query", async () => {
   const seen: RowsActionsContext[] = [];
   const preamble = "source: orders_tiles is orders extend {}";
   mount({
      preamble,
      actions: (context) => {
         seen.push(context);
         return null;
      },
   });
   await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
   const context = seen.at(-1)!;
   expect(context.preamble).toBe(preamble);
   expect(context.query).toBe(rowsQuery(request, null)!);
   expect(executeQueryModel.mock.calls[0][3].query).toBe(
      withPreamble(preamble, rowsQuery(request)!),
   );
});

it("downloads every row as CSV by default, and says when the server's cap cut it short", async () => {
   const saved: { text: string; name: string }[] = [];
   const blobs = new Map<string, Blob>();
   const originalCreate = URL.createObjectURL;
   const originalRevoke = URL.revokeObjectURL;
   URL.createObjectURL = (blob: Blob) => {
      const url = `blob:${blobs.size}`;
      blobs.set(url, blob);
      return url;
   };
   URL.revokeObjectURL = () => {};
   const originalClick = HTMLAnchorElement.prototype.click;
   HTMLAnchorElement.prototype.click = function (this: HTMLAnchorElement) {
      const blob = blobs.get(this.href)!;
      void blob
         .text()
         .then((text) => saved.push({ text, name: this.download }));
   };
   try {
      mount({});
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      const rows = Array.from({ length: 3 }, () => ({
         kind: "record_cell",
         record_value: [{ kind: "string_cell", string_value: "Complete" }],
      }));
      executeQueryModel.mockImplementationOnce(async () => ({
         data: {
            result: JSON.stringify({
               connection_name: "duckdb",
               schema: {
                  fields: [
                     {
                        kind: "dimension",
                        name: "status",
                        type: { kind: "string_type" },
                     },
                  ],
               },
               data: { kind: "array_cell", array_value: rows },
            }),
            queryRowLimit: 3,
            queryRowLimitSource: "server_default",
         },
      }));
      await act(async () => {
         fireEvent.click(screen.getByRole("button", { name: /Download CSV/ }));
      });
      await waitFor(() => expect(saved.length).toBe(1));
      const download = executeQueryModel.mock.calls.at(-1)![3];
      expect(download.query).toBe(rowsQuery(request, null)!);
      expect(download.givens).toEqual(givens);
      expect(download.versionId).toBe("v3");
      expect(saved[0].name).toBe(
         "orders_rows_Complete_first_3_rows_server_limit.csv",
      );
      expect(saved[0].text).toBe(
         "\uFEFFstatus\r\nComplete\r\nComplete\r\nComplete\r\n",
      );
      fireEvent.mouseOver(screen.getByRole("button", { name: /Download CSV/ }));
      expect(
         await screen.findByText("First 3 rows (server limit)"),
      ).toBeTruthy();
   } finally {
      URL.createObjectURL = originalCreate;
      URL.revokeObjectURL = originalRevoke;
      HTMLAnchorElement.prototype.click = originalClick;
   }
});
