// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";

const string = { kind: "string_type" };
const number = { kind: "number_type" };
const items = {
   name: "items",
   schema: {
      fields: [
         {
            kind: "dimension",
            name: "name",
            type: string,
            annotations: [{ value: "#(index)\n" }],
         },
         { kind: "measure", name: "item_count", type: number },
         { kind: "calculate", name: "running_total", type: number },
         { kind: "view", name: "by_name", schema: { fields: [] } },
      ],
   },
};
const other = {
   name: "other",
   schema: { fields: [{ kind: "dimension", name: "code", type: string }] },
};
const sourceText = `source: items is duckdb.table('items.parquet') extend {
  dimension: name is upper(raw_name)
  measure: item_count is count()
  view: by_name is { group_by: name }
}
source: other is duckdb.table('other.parquet')
query: all_names is items -> by_name
`;
const model = {
   data: {
      sourceInfos: [JSON.stringify(items), JSON.stringify(other)],
      queries: [{ name: "all_names", sourceName: "items" }],
      sourceText,
   },
};
const result = JSON.stringify({
   schema: { fields: [{ kind: "dimension", name: "name", type: string }] },
   data: {
      kind: "array_cell",
      array_value: [
         {
            kind: "record_cell",
            record_value: [
               { kind: "string_cell", string_value: `27" Monitor` },
            ],
         },
      ],
   },
});

const getModel = mock(
   (..._args: unknown[]): Promise<{ data: unknown }> => Promise.resolve(model),
);
const executeQueryModel = mock((..._args: unknown[]) =>
   Promise.resolve({ data: { result } }),
);
mockServerProvider({ models: { getModel, executeQueryModel } });

const { ModelViewer } = await import("./ModelViewer");

const URI = "publisher://environments/env/packages/pkg/models/shop.malloy";
const sentQueries = () =>
   executeQueryModel.mock.calls.map(
      (call) => (call[3] as { query: string }).query,
   );

beforeEach(() => {
   clearCache();
   getModel.mockClear();
   executeQueryModel.mockClear();
});

describe("ModelViewer", () => {
   it("opens on the source with views, its sections in reading order", async () => {
      render(<ModelViewer resourceUri={URI} />, { wrapper: serverWrapper });
      expect(
         await screen.findByRole("heading", { name: "items", level: 1 }),
      ).toBeTruthy();
      const sections = screen
         .getAllByRole("region")
         .map((r) => r.getAttribute("aria-label"));
      expect(sections).toEqual([
         "Views",
         "Queries",
         "Measures",
         "Calculations",
         "Dimensions",
      ]);
      // Browsing costs nothing: no row runs until it is opened.
      expect(executeQueryModel).not.toHaveBeenCalled();
   });

   it("runs an opened view by name, with no value in the text", async () => {
      render(<ModelViewer resourceUri={URI} />, { wrapper: serverWrapper });
      fireEvent.click(await screen.findByRole("button", { name: /by_name/ }));
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(sentQueries()).toEqual(["run: `items` -> `by_name`"]);
   });

   it("runs a named query by name", async () => {
      render(<ModelViewer resourceUri={URI} />, { wrapper: serverWrapper });
      fireEvent.click(await screen.findByRole("button", { name: /all_names/ }));
      await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
      expect(sentQueries()).toEqual(["run: `all_names`"]);
   });

   it("lists a tagged dimension's values from one quoted query", async () => {
      render(<ModelViewer resourceUri={URI} />, { wrapper: serverWrapper });
      fireEvent.click(await screen.findByRole("button", { name: /^name/ }));
      expect(await screen.findByText(`27" Monitor`)).toBeTruthy();
      const [query] = sentQueries();
      expect(query).toContain("group_by: `name`");
      expect(query).not.toContain("Monitor");
   });

   it("shows the source a host names, and says so when the model has none by that name", async () => {
      const { rerender } = render(
         <ModelViewer resourceUri={URI} sourceName="other" />,
         { wrapper: serverWrapper },
      );
      expect(
         await screen.findByRole("heading", { name: "other", level: 1 }),
      ).toBeTruthy();
      rerender(<ModelViewer resourceUri={URI} sourceName="nope" />);
      expect(
         await screen.findByText("This model has no source named nope."),
      ).toBeTruthy();
      expect(screen.getByRole("button", { name: "items" })).toBeTruthy();
   });

   it("hands a picked source to its host", async () => {
      const onSourceChange = mock((_name: string) => {});
      render(
         <ModelViewer
            resourceUri={URI}
            sourceName="items"
            onSourceChange={onSourceChange}
         />,
         { wrapper: serverWrapper },
      );
      fireEvent.click(await screen.findByRole("button", { name: /2 sources/ }));
      fireEvent.click(
         await screen.findByRole("button", { name: /Open source other/ }),
      );
      expect(onSourceChange.mock.calls).toEqual([["other"]]);
   });

   it("says what the server said when the model isn't there", async () => {
      getModel.mockImplementationOnce(() =>
         Promise.reject({
            response: {
               status: 404,
               data: { message: "gone.malloy does not exist" },
            },
         }),
      );
      render(
         <ModelViewer resourceUri="publisher://environments/env/packages/pkg/models/gone.malloy" />,
         { wrapper: serverWrapper },
      );
      expect(
         await screen.findByText("gone.malloy does not exist"),
      ).toBeTruthy();
      expect(screen.queryByRole("button", { name: "Retry" })).toBeNull();
   });

   it("draws a host's additions on a field's line", async () => {
      render(
         <ModelViewer
            resourceUri={URI}
            extensions={{
               renderFieldStatus: ({ field }) =>
                  field.name === "item_count" ? <span>Rolled up</span> : null,
            }}
         />,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("Rolled up")).toBeTruthy();
   });
});

// A model whose `items` reads a given with no default, and a named query the
// model gives no source, over `other`, which reads none.
const gatedText = `source: items is duckdb.table('items.parquet') extend {
  where: name = $REGION
  dimension: name is upper(raw_name)
  view: by_name is { group_by: name }
}
source: other is duckdb.table('other.parquet')
query: other_codes is other -> { group_by: code }
`;
const gatedModel = (given: Record<string, unknown> = {}) => ({
   data: {
      sourceInfos: [JSON.stringify(items), JSON.stringify(other)],
      queries: [{ name: "other_codes" }],
      sourceText: gatedText,
      givens: [{ name: "REGION", type: "string", ...given }],
   },
});
const sentBodies = () =>
   executeQueryModel.mock.calls.map(
      (call) =>
         call[3] as {
            query: string;
            givens?: Record<string, unknown>;
            queryMetadata?: Record<string, string>;
         },
   );

describe("ModelViewer givens", () => {
   it("sends a host's given values as the request's givens, never in the text", async () => {
      getModel.mockImplementation(() => Promise.resolve(gatedModel()));
      try {
         render(
            <ModelViewer
               resourceUri={URI}
               // `source` is the host's own parameter, not a given.
               givens={{ REGION: `x" or "a" = "a`, source: "items" }}
            />,
            { wrapper: serverWrapper },
         );
         fireEvent.click(
            await screen.findByRole("button", { name: /by_name/ }),
         );
         await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
         const [body] = sentBodies();
         expect(body.query).toBe("run: `items` -> `by_name`");
         expect(body.givens).toEqual({ REGION: `x" or "a" = "a` });
      } finally {
         getModel.mockImplementation(() => Promise.resolve(model));
      }
   });

   it("reports applied values to its host, with every name it manages", async () => {
      getModel.mockImplementation(() => Promise.resolve(gatedModel()));
      const onGivensChange = mock(
         (_givens: Record<string, string>, _managed: readonly string[]) => {},
      );
      try {
         render(
            <ModelViewer
               resourceUri={URI}
               givens={{ REGION: "West" }}
               onGivensChange={onGivensChange}
            />,
            { wrapper: serverWrapper },
         );
         fireEvent.click(await screen.findByRole("button", { name: "Reset" }));
         await waitFor(() =>
            expect(onGivensChange.mock.calls.at(-1)).toEqual([{}, ["REGION"]]),
         );
      } finally {
         getModel.mockImplementation(() => Promise.resolve(model));
      }
   });

   it("holds a run, and a dimension's values, that need a given with no value", async () => {
      getModel.mockImplementation(() => Promise.resolve(gatedModel()));
      try {
         render(<ModelViewer resourceUri={URI} />, { wrapper: serverWrapper });
         fireEvent.click(
            await screen.findByRole("button", { name: /by_name/ }),
         );
         expect(
            await screen.findByText(/This needs a value for the given REGION/),
         ).toBeTruthy();
         fireEvent.click(screen.getByRole("button", { name: /^name/ }));
         expect(
            (await screen.findByTestId("model-viewer-values-unavailable"))
               .textContent,
         ).toContain("REGION");
         expect(executeQueryModel).not.toHaveBeenCalled();
      } finally {
         getModel.mockImplementation(() => Promise.resolve(model));
      }
   });

   it("runs a named query with no source on the givens its text reads, not the model's every one", async () => {
      getModel.mockImplementation(() => Promise.resolve(gatedModel()));
      try {
         render(<ModelViewer resourceUri={URI} />, { wrapper: serverWrapper });
         fireEvent.click(
            await screen.findByRole("button", { name: /other_codes/ }),
         );
         await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
         expect(sentQueries()).toEqual(["run: `other_codes`"]);
      } finally {
         getModel.mockImplementation(() => Promise.resolve(model));
      }
   });

   it("leaves a given the host fills out of the section and the gate", async () => {
      getModel.mockImplementation(() => Promise.resolve(gatedModel()));
      try {
         render(<ModelViewer resourceUri={URI} hostGivenNames={["REGION"]} />, {
            wrapper: serverWrapper,
         });
         fireEvent.click(
            await screen.findByRole("button", { name: /by_name/ }),
         );
         await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
         expect(screen.queryByTestId("model-givens-section")).toBeNull();
      } finally {
         getModel.mockImplementation(() => Promise.resolve(model));
      }
   });

   it("reads a picker's options when it is opened, never on render", async () => {
      getModel.mockImplementation(() =>
         Promise.resolve(
            gatedModel({
               default: "West",
               control: "select",
               suggest: { source: "items", dimension: "name" },
            }),
         ),
      );
      try {
         render(<ModelViewer resourceUri={URI} />, { wrapper: serverWrapper });
         const picker = await screen.findByRole("button", { name: /^REGION/ });
         await new Promise((resolve) => setTimeout(resolve, 50));
         expect(executeQueryModel).not.toHaveBeenCalled();
         fireEvent.click(picker);
         await waitFor(() => expect(executeQueryModel).toHaveBeenCalled());
         expect(sentQueries()[0]).toContain("group_by: `name`");
      } finally {
         getModel.mockImplementation(() => Promise.resolve(model));
      }
   });
});

describe("ModelViewer host props", () => {
   it("reads a hidden file, and keeps it apart from the curated read", async () => {
      render(<ModelViewer resourceUri={URI} includeHiddenFilesAndSources />, {
         wrapper: serverWrapper,
      });
      await screen.findByRole("heading", { name: "items", level: 1 });
      expect(getModel.mock.calls[0][4]).toBe(true);
   });

   it("sends its labels with every run and reports each run that settles", async () => {
      const onQueryRun = mock((_event: unknown) => {});
      render(
         <ModelViewer
            resourceUri={URI}
            queryMetadata={{ surface: "test" }}
            onQueryRun={onQueryRun}
         />,
         { wrapper: serverWrapper },
      );
      fireEvent.click(await screen.findByRole("button", { name: /by_name/ }));
      await waitFor(() => expect(onQueryRun).toHaveBeenCalled());
      expect(sentBodies()[0].queryMetadata).toEqual({ surface: "test" });
      expect(onQueryRun.mock.calls[0][0]).toMatchObject({ outcome: "success" });
   });

   it("shows a host's entities: a field's code, the table a source reads, a join's way out", async () => {
      const joinHref = mock(
         (target: { modelPath: string; sourceName: string }) =>
            `/models/${target.modelPath}?source=${target.sourceName}`,
      );
      const withJoin = {
         ...items,
         schema: {
            fields: [
               ...items.schema.fields,
               {
                  kind: "join",
                  name: "maker",
                  relationship: "one",
                  schema: {
                     fields: [
                        { kind: "dimension", name: "country", type: string },
                     ],
                  },
               },
            ],
         },
      };
      getModel.mockImplementation(() =>
         Promise.resolve({
            data: { ...model.data, sourceInfos: [JSON.stringify(withJoin)] },
         }),
      );
      try {
         const { container } = render(
            <ModelViewer
               resourceUri={URI}
               joinHref={joinHref}
               entities={[
                  {
                     name: "item_count",
                     source: "items",
                     fieldType: "measure",
                     code: "measure: item_count is count(distinct id)",
                     table: {
                        connectionName: "duckdb",
                        tableName: "items",
                     },
                  },
                  {
                     name: "maker",
                     source: "items",
                     fieldType: "join",
                     code: "join_one: maker is makers on maker_id = maker.id",
                     joinTarget: {
                        modelPath: "makers.malloy",
                        sourceName: "makers",
                     },
                  },
               ]}
            />,
            { wrapper: serverWrapper },
         );
         expect(
            (await screen.findByTestId("model-viewer-source-table"))
               .textContent,
         ).toBe("duckdb.items");
         fireEvent.click(screen.getByRole("button", { name: /^item_count/ }));
         expect(
            (await screen.findByTestId("model-viewer-definition")).textContent,
         ).toContain("count(distinct id)");
         expect(joinHref.mock.calls[0][0]).toEqual({
            modelPath: "makers.malloy",
            sourceName: "makers",
         });
         expect(
            container.querySelector(
               'a[href="/models/makers.malloy?source=makers"]',
            ),
         ).toBeTruthy();
      } finally {
         getModel.mockImplementation(() => Promise.resolve(model));
      }
   });

   it("marks a host's column description as generated", async () => {
      render(
         <ModelViewer
            resourceUri={URI}
            sourceName="other"
            entities={[
               {
                  name: "code",
                  source: "other",
                  fieldType: "column",
                  code: "duckdb.table('other.parquet')",
               },
            ]}
            columnDescriptions={
               new Map([["other", new Map([["code", "The product code"]])]])
            }
         />,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("The product code")).toBeTruthy();
      expect(
         screen.getByTestId("model-viewer-field-doc-generated"),
      ).toBeTruthy();
   });
});

describe("ModelViewer extensions", () => {
   it("draws a host's chips and card on the source, and its summary and chips in the picker", async () => {
      render(
         <ModelViewer
            resourceUri={URI}
            renderSourceStatus={(name) => <span>Status of {name}</span>}
            extensions={{
               renderSourceDetail: ({ sourceName }) => (
                  <span>Built from {sourceName}</span>
               ),
               describePickerSource: ({ sourceName }) =>
                  sourceName === "other"
                     ? {
                          summary: "Other things.",
                          status: <span>Fresh</span>,
                       }
                     : undefined,
            }}
         />,
         { wrapper: serverWrapper },
      );
      expect(await screen.findByText("Status of items")).toBeTruthy();
      expect(screen.getByText("Built from items")).toBeTruthy();
      fireEvent.click(screen.getByRole("button", { name: /2 sources/ }));
      expect(await screen.findByText("Other things.")).toBeTruthy();
      expect(screen.getByText("Fresh")).toBeTruthy();
      expect(
         screen.getByTestId("model-sources-summary-generated"),
      ).toBeTruthy();
   });

   it("lists only the rows a host shows, and drops a section it empties", async () => {
      render(
         <ModelViewer
            resourceUri={URI}
            extensions={{
               showField: ({ field }) => field.name !== "item_count",
            }}
         />,
         { wrapper: serverWrapper },
      );
      await screen.findByRole("heading", { name: "items", level: 1 });
      expect(screen.queryByRole("button", { name: /^item_count/ })).toBeNull();
      // Its only measure not listed, the section goes with it.
      expect(screen.queryByRole("region", { name: "Measures" })).toBeNull();
      expect(screen.getByRole("region", { name: "Dimensions" })).toBeTruthy();
   });

   it("puts a host's action on a field's line, and its card in place of the row's", async () => {
      render(
         <ModelViewer
            resourceUri={URI}
            extensions={{
               renderFieldAction: ({ field }) =>
                  field.name === "item_count" ? (
                     <button type="button">Rebuild</button>
                  ) : null,
               renderFieldDetail: ({ field }, builtIn) =>
                  field.name === "item_count" ? (
                     <span>Host card</span>
                  ) : field.name === "running_total" ? null : (
                     builtIn
                  ),
            }}
         />,
         { wrapper: serverWrapper },
      );
      expect(
         await screen.findByRole("button", { name: "Rebuild" }),
      ).toBeTruthy();
      fireEvent.click(screen.getByRole("button", { name: /^item_count/ }));
      expect(await screen.findByText("Host card")).toBeTruthy();
      expect(screen.queryByTestId("model-viewer-field-card")).toBeNull();
      // Nothing to show: the row doesn't open.
      expect(
         screen
            .getByRole("region", { name: "Calculations" })
            .querySelector("button[aria-expanded]"),
      ).toBeNull();
   });

   it("hands a run's actions its givens and cache key", async () => {
      const renderRunActions = mock((_run: unknown) => (
         <button type="button">Download</button>
      ));
      render(
         <ModelViewer resourceUri={URI} extensions={{ renderRunActions }} />,
         {
            wrapper: serverWrapper,
         },
      );
      fireEvent.click(await screen.findByRole("button", { name: /by_name/ }));
      expect(
         await screen.findByRole("button", { name: "Download" }),
      ).toBeTruthy();
      const run = renderRunActions.mock.calls.at(-1)?.[0] as {
         query: string;
         queryKey: readonly unknown[];
      };
      expect(run.query).toBe("run: `items` -> `by_name`");
      expect(run.queryKey).toContain("run: `items` -> `by_name`");
   });

   it("shows a host's count of values, and its reason when there are none", async () => {
      const fetchSamples = mock((request: { fieldName: string }) =>
         Promise.resolve(
            request.fieldName === "name"
               ? { values: ["a", "b"], total: 40 }
               : { values: [] },
         ),
      );
      render(<ModelViewer resourceUri={URI} extensions={{ fetchSamples }} />, {
         wrapper: serverWrapper,
      });
      fireEvent.click(await screen.findByRole("button", { name: /^name/ }));
      expect(
         await screen.findByText("The most common of 40 values"),
      ).toBeTruthy();
      expect(executeQueryModel).not.toHaveBeenCalled();
   });
});
