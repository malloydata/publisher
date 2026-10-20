// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, render, screen } from "@testing-library/react";
import { createElement, Fragment } from "react";
import { afterAll, describe, expect, it, setDefaultTimeout } from "bun:test";
import { parseAnnotation } from "@malloydata/malloy-tag";
import type * as MalloyInterfaces from "@malloydata/malloy-interfaces";
import {
   drillClickRelay,
   reportRowsWithoutFilters,
} from "../drill/drillClickRelay";
import type { DrillClickPayload, EntryDrill } from "../drill/resolveDrill";
import { useDrill, type UseDrillResult } from "../drill/useDrill";
import { rowsGateFor } from "../Dashboard/RowsDialog";
import {
   clickDimensions,
   clickMenuPreview,
   type ParseTags,
} from "./clickActions";

/**
 * The builder's preview of a tile's click menu against the menu itself: the
 * tile's query runs on DuckDB, the real renderer draws it, the first row's
 * cell of each dimension is clicked through the dashboard's own binding, and
 * the menu that opens must read exactly as the preview said it would.
 */

setDefaultTimeout(60_000);

const MODEL = `
source: sales is duckdb.sql("""
  select * from (values
    (DATE '2026-01-15', 'West', 'Ben & Jerry''s', 10.0),
    (DATE '2026-02-03', 'East', 'Plain', 20.0)
  ) t(sale_date, region, product, amount)
""") extend {
  # label="Region"
  # drill { to=self given=REGION }
  dimension: region_name is region
  dimension: sales_month is sale_date.month
  measure: revenue is amount.sum()
  view: by_region_month is {
    group_by: region_name, sales_month
    aggregate: revenue
    order_by: region_name desc
  }
  view: nested is {
    group_by: region_name
    aggregate: revenue
    nest: products is { group_by: product; aggregate: revenue; order_by: product }
    order_by: region_name desc
  }
}
`;

const { SingleConnectionRuntime, API } = await import("@malloydata/malloy");
const { DuckDBConnection } = await import("@malloydata/db-duckdb");
const { MalloyRenderer } = (await import(
   `${Bun.resolveSync("@malloydata/render", import.meta.dir)}?real`
)) as typeof import("@malloydata/render");
const connection = new DuckDBConnection("duckdb", ":memory:");
const model = new SingleConnectionRuntime({ connection } as never).loadModel(
   MODEL,
);
afterAll(() => connection.close());

const SELF_LABEL = "Filter to";

/**
 * The menu the first click on each field opens, by field name, as the dashboard
 * wires a tile: `bindingFor` with the tile's rows gate and its entry drills.
 */
async function firstMenus(
   tile: string,
   entries: Record<string, EntryDrill> | undefined,
) {
   const result = await model.loadQuery(`run: ${tile}`).run();
   const wrapped = API.util.wrapResult(result) as MalloyInterfaces.Result;
   const ref = {} as { current: UseDrillResult };
   function Host() {
      ref.current = useDrill({
         onRows: () => {},
         onSelf: () => {},
         canSelf: () => true,
         onNavigate: () => {},
         selfLabel: SELF_LABEL,
      });
      return createElement(Fragment, null, ref.current.drillMenu);
   }
   const mounted = render(createElement(Host));
   const binding = ref.current.bindingFor(tile, rowsGateFor(tile), entries);
   const clicked: string[] = [];
   const relay = drillClickRelay((payload: DrillClickPayload) => {
      if (payload.field) clicked.push(payload.field.name);
      binding.onClick(payload);
   });
   const viz = new MalloyRenderer({
      onClick: relay.onClick as never,
      onDrill: relay.onDrill,
      tableConfig: { enableDrill: true, disableVirtualization: true },
   }).createViz();
   viz.setResult(wrapped);
   reportRowsWithoutFilters(viz.getMetadata()?.getRootField() as never);
   const element = document.createElement("div");
   document.body.appendChild(element);
   viz.render(element);
   await new Promise((resolve) => setTimeout(resolve, 300));
   const menus = new Map<string, string[]>();
   for (const cell of Array.from(
      element.querySelectorAll<HTMLElement>(".cell-content"),
   )) {
      const before = clicked.length;
      await act(async () => {
         cell.click();
         await Promise.resolve();
      });
      const field = clicked[before];
      const items = screen
         .queryAllByRole("menuitem")
         .map((item) => item.getAttribute("aria-label") ?? "");
      if (field !== undefined && !menus.has(field) && items.length > 0)
         menus.set(field, items);
      // Close whatever opened before the next click.
      await act(async () => {
         document.dispatchEvent(
            new KeyboardEvent("keydown", { key: "Escape", bubbles: true }),
         );
         await Promise.resolve();
      });
   }
   mounted.unmount();
   viz.remove();
   element.remove();
   return { menus, result: wrapped };
}

async function expectPreviewMatches(
   tile: string,
   entries?: Record<string, EntryDrill>,
) {
   const { menus, result } = await firstMenus(tile, entries);
   const dimensions = clickDimensions(
      result,
      parseAnnotation as unknown as ParseTags,
   );
   expect(dimensions.length).toBeGreaterThan(0);
   for (const dimension of dimensions) {
      const preview = clickMenuPreview(
         dimension,
         entries?.[dimension.name] ?? dimension.modelDrill,
         {
            selfLabel: SELF_LABEL,
            rows: true,
            headerOf: (name) => dimensions.find((d) => d.name === name)?.header,
         },
      );
      expect({
         field: dimension.name,
         menu: menus.get(dimension.name),
      }).toEqual({ field: dimension.name, menu: preview });
   }
   return dimensions;
}

describe("the builder's preview is the menu a click opens", () => {
   it("for a tagged dimension and a month, by the model's tag", async () => {
      const dimensions = await expectPreviewMatches("sales -> by_region_month");
      expect(dimensions.map((d) => d.label)).toEqual(["Region", "Sales month"]);
   });

   it("for a tile entry that replaces the tag and adds one", async () => {
      await expectPreviewMatches("sales -> by_region_month", {
         region_name: { to: ["regions/detail"] },
         sales_month: { to: ["self"], given: "MONTH" },
      });
   });

   it("for a tile entry that turns the tag off", async () => {
      await expectPreviewMatches("sales -> by_region_month", {
         region_name: { to: [] },
      });
   });

   it("for a nested row, its parent's value included", async () => {
      await expectPreviewMatches("sales -> nested");
   });
});
