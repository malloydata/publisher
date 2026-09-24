// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Explorer sends the control row's values with every Run, and surfaces a
 * default note, a server error and a missing-given hint as banners rather than
 * swallowing them.
 *
 * `@malloydata/malloy-explorer` is stubbed because it is lazy-loaded (a
 * dynamic `import()` inside `SourceExplorerComponent`) and pulls in a real
 * query-builder and renderer this spec has no business exercising; only
 * `mock.module` reaches a dynamic import the same way it reaches a static one.
 * It is process-global, so this is the one spec in the package allowed to
 * import either module — see test/README.md.
 */
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import {
   clearCache,
   mockServerProvider,
   pending,
   serverWrapper,
} from "../../../test/serverProvider";
import type { CompiledModel } from "../../client";

mock.module("@malloydata/malloy-explorer", () => ({
   MalloyExplorerProvider: ({ children }: { children: ReactNode }) => (
      <>{children}</>
   ),
   QueryPanel: ({ runQuery }: { runQuery: () => void }) => (
      <button onClick={runQuery}>Run</button>
   ),
   ResizableCollapsiblePanel: ({ children }: { children: ReactNode }) => (
      <>{children}</>
   ),
   SourcePanel: () => null,
   ResultPanel: ({
      submittedQuery,
   }: {
      submittedQuery?: {
         response?: { messages?: { title: string }[] };
         onCancel: () => void;
      };
   }) => (
      <div data-testid="result-panel">
         {submittedQuery && (
            <button onClick={submittedQuery.onCancel}>Cancel</button>
         )}
         {submittedQuery?.response?.messages?.map((message) => (
            <div key={message.title}>{message.title}</div>
         ))}
      </div>
   ),
}));

// Never reached when every test's query is a plain string (see below), but
// stubbed anyway so the module's own dynamic import resolves to something
// light rather than the real WASM-backed builder.
mock.module("@malloydata/malloy-query-builder", () => ({
   ASTQuery: class {
      toMalloy() {
         return "run: orders -> by_month";
      }
   },
}));

const executeQueryModel = mock(
   (
      _environmentName: string,
      _packageName: string,
      _modelPath: string,
      _request: { versionId?: string; givens?: Record<string, unknown> },
   ) => pending<{ data: { result: string } }>(),
);

mockServerProvider({ models: { executeQueryModel } });

// Imported after the stubs above: a static import would hoist above them.
const { ModelExplorer } = await import("./ModelExplorer");

const URI = "publisher://environments/env/packages/pkg/models/orders.malloy";

const modelWith = (givens?: CompiledModel["givens"]): CompiledModel => ({
   sourceInfos: [JSON.stringify({ name: "orders" })],
   ...(givens ? { givens } : {}),
});

/** As a string, so `SourceExplorerComponentInner` never reaches `ASTQuery`. */
const EXISTING_QUERY = {
   query: "run: orders -> by_month",
   malloyQuery: "run: orders -> by_month",
   malloyResult: undefined,
};

const run = () => fireEvent.click(screen.getByRole("button", { name: "Run" }));

beforeEach(() => {
   clearCache();
   executeQueryModel.mockReset();
   executeQueryModel.mockImplementation(() => pending());
});

const reject = (status: number, message: string) =>
   executeQueryModel.mockImplementation(() =>
      Promise.reject(
         Object.assign(new Error("fallback"), {
            status,
            data: { code: status, message },
         }),
      ),
   );

describe("a given with no value and no default", () => {
   // Givens are model-wide but read per source, so a blank one may be fine for
   // the source being explored: the server decides, not the client.
   it("still sends Run, and names the given when the server refuses", async () => {
      reject(403, 'Access denied for source "regions".');
      render(
         <ModelExplorer
            data={modelWith([{ name: "TENANT", type: "string" }])}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
         />,
         { wrapper: serverWrapper },
      );

      await screen.findByLabelText("TENANT");
      run();

      expect(
         await screen.findByText('Access denied for source "regions".'),
      ).toBeTruthy();
      expect(
         screen.getByText(
            "This source may need a value for the given TENANT. Set it in the parameters above.",
         ),
      ).toBeTruthy();
      expect(executeQueryModel).toHaveBeenCalledTimes(1);
   });

   it("adds no hint to an error that is not a refusal", async () => {
      reject(400, "syntax error near 'aggregat'");
      render(
         <ModelExplorer
            data={modelWith([{ name: "TENANT", type: "string" }])}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
         />,
         { wrapper: serverWrapper },
      );

      await screen.findByLabelText("TENANT");
      run();

      expect(
         await screen.findByText("syntax error near 'aggregat'"),
      ).toBeTruthy();
      expect(screen.queryByText(/may need a value/)).toBeNull();
   });
});

describe("a given left blank with a default", () => {
   it("runs, and notes the default that was used", async () => {
      executeQueryModel.mockImplementation(() =>
         Promise.resolve({ data: { result: JSON.stringify({}) } }),
      );
      render(
         <ModelExplorer
            data={modelWith([
               { name: "TENANT", type: "string", default: "'acme'" },
            ])}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
         />,
         { wrapper: serverWrapper },
      );

      await screen.findByLabelText("TENANT");
      run();

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(
         await screen.findByText("Ran with the default TENANT = acme"),
      ).toBeTruthy();
   });
});

describe("a model that declares givens", () => {
   it("sends the control's value with Run", async () => {
      executeQueryModel.mockImplementation(() =>
         Promise.resolve({ data: { result: JSON.stringify({}) } }),
      );
      render(
         <ModelExplorer
            data={modelWith([{ name: "TENANT", type: "string" }])}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
         />,
         { wrapper: serverWrapper },
      );

      fireEvent.change(await screen.findByLabelText("TENANT"), {
         target: { value: "acme" },
      });
      run();

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(executeQueryModel.mock.calls[0][3].givens).toEqual({
         TENANT: "acme",
      });
   });
});

describe("a model with no givens", () => {
   it("renders no Parameters row, and sends no givens key", async () => {
      executeQueryModel.mockImplementation(() =>
         Promise.resolve({ data: { result: JSON.stringify({}) } }),
      );
      render(
         <ModelExplorer
            data={modelWith()}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
         />,
         { wrapper: serverWrapper },
      );

      // Nothing to wait on the label for, so wait on the button instead.
      await screen.findByRole("button", { name: "Run" });
      expect(screen.queryByText("Parameters")).toBeNull();
      run();

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(executeQueryModel.mock.calls[0][3]).not.toHaveProperty("givens");
   });
});

describe("a rejected run", () => {
   it("shows the server's message rather than clearing the pane", async () => {
      const apiError = Object.assign(new Error("fallback"), {
         status: 403,
         data: { code: 403, message: "Not authorized: missing TENANT" },
      });
      executeQueryModel.mockImplementation(() => Promise.reject(apiError));
      render(
         <ModelExplorer
            data={modelWith([
               { name: "TENANT", type: "string", default: "'x'" },
            ])}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
         />,
         { wrapper: serverWrapper },
      );

      await screen.findByLabelText("TENANT");
      run();

      expect(
         await screen.findByText("Not authorized: missing TENANT"),
      ).toBeTruthy();
   });
});

describe("a cancelled run", () => {
   it("stays cleared when its request fails afterwards", async () => {
      let fail: (reason: unknown) => void = () => {};
      executeQueryModel.mockImplementation(
         () =>
            new Promise((_resolve, reject) => {
               fail = reject;
            }),
      );
      render(
         <ModelExplorer
            data={modelWith()}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
         />,
         { wrapper: serverWrapper },
      );

      await screen.findByRole("button", { name: "Run" });
      run();
      fireEvent.click(await screen.findByRole("button", { name: "Cancel" }));
      fail(
         Object.assign(new Error("fallback"), {
            status: 500,
            data: { code: 500, message: "late failure" },
         }),
      );

      // Give the rejection a chance to land before asserting it changed nothing.
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(screen.queryByText("late failure")).toBeNull();
      expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
   });
});

describe("host-supplied givens", () => {
   it("seeds the control, and reports a change with the given in `managed`", async () => {
      const onGivensChange = mock(
         (_givens: Record<string, string>, _managed: readonly string[]) => {},
      );
      render(
         <ModelExplorer
            data={modelWith([{ name: "TENANT", type: "string" }])}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
            givens={{ TENANT: "acme" }}
            onGivensChange={onGivensChange}
         />,
         { wrapper: serverWrapper },
      );

      const input = (await screen.findByLabelText(
         "TENANT",
      )) as HTMLInputElement;
      expect(input.value).toBe("acme");

      fireEvent.change(input, { target: { value: "beta" } });

      await waitFor(() =>
         expect(onGivensChange).toHaveBeenCalledWith({ TENANT: "beta" }, [
            "TENANT",
         ]),
      );
   });
});

describe("startingGivens", () => {
   it("seeds the control, and Run sends it", async () => {
      executeQueryModel.mockImplementation(() =>
         Promise.resolve({ data: { result: JSON.stringify({}) } }),
      );
      render(
         <ModelExplorer
            data={modelWith([{ name: "TENANT", type: "string" }])}
            existingQuery={EXISTING_QUERY}
            resourceUri={URI}
            startingGivens={{ TENANT: "acme" }}
         />,
         { wrapper: serverWrapper },
      );

      const input = (await screen.findByLabelText(
         "TENANT",
      )) as HTMLInputElement;
      expect(input.value).toBe("acme");

      run();

      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(executeQueryModel.mock.calls[0][3].givens).toEqual({
         TENANT: "acme",
      });
   });
});
