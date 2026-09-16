// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";

const getModel = mock(() =>
   Promise.resolve({
      data: {
         modelPath: "storefront.malloy",
         sources: [
            {
               name: "order_items",
               views: [{ name: "by_category" }, { name: "by_brand" }],
            },
            { name: "products", views: [] },
         ],
      },
   }),
);
const putModelSource = mock((_env: string, _pkg: string, path: string) =>
   Promise.resolve({ data: { path, contentHash: "h", created: true } }),
);
mockServerProvider({ models: { getModel, putModelSource } });

const { NewDashboardDialog } = await import("./NewDashboardDialog");

const pick = (label: RegExp, option: string | RegExp) => {
   fireEvent.mouseDown(screen.getByRole("combobox", { name: label }));
   fireEvent.click(screen.getByRole("option", { name: option }));
};

beforeEach(() => {
   clearCache();
   putModelSource.mockClear();
});

describe("NewDashboardDialog", () => {
   it("writes the picked view as the first tile and hands back the slug", async () => {
      const onCreated = mock((_slug: string) => {});
      render(
         <NewDashboardDialog
            open
            environmentName="env"
            packageName="pkg"
            models={["storefront.malloy", "other.malloy"]}
            existing={["overview"]}
            onClose={() => {}}
            onCreated={onCreated}
         />,
         { wrapper: serverWrapper },
      );
      await waitFor(() =>
         expect(
            screen
               .getByRole("combobox", { name: /Source/ })
               .getAttribute("aria-disabled"),
         ).not.toBe("true"),
      );
      pick(/Source/, "order_items");
      pick(/First tile/, "by_category");
      // The title follows the view until typed over; the slug follows the title.
      expect(screen.getByLabelText("Dashboard title")).toHaveProperty(
         "value",
         "by category",
      );
      fireEvent.change(screen.getByLabelText("Dashboard title"), {
         target: { value: "Sales by Region" },
      });
      expect(
         screen.getByText("Written as dashboards/sales-by-region.malloy"),
      ).toBeDefined();

      fireEvent.click(screen.getByRole("button", { name: "Create" }));
      await waitFor(() =>
         expect(onCreated).toHaveBeenCalledWith("sales-by-region"),
      );
      const [, , path, body] = putModelSource.mock.calls[0] as unknown as [
         string,
         string,
         string,
         { source: string },
      ];
      expect(path).toBe("dashboards/sales-by-region.malloy");
      expect(body.source).toContain(
         'tiles=["order_items_tiles -> by_category_tile"]',
      );
      expect(body.source).toContain(
         'import { order_items } from "../storefront.malloy"',
      );
   });

   it("refuses a title whose file already exists", async () => {
      render(
         <NewDashboardDialog
            open
            environmentName="env"
            packageName="pkg"
            models={["storefront.malloy"]}
            existing={["overview"]}
            onClose={() => {}}
            onCreated={() => {}}
         />,
         { wrapper: serverWrapper },
      );
      fireEvent.change(screen.getByLabelText("Dashboard title"), {
         target: { value: "Overview" },
      });
      expect(screen.getByText(/already exists in this package/)).toBeDefined();
      expect(
         screen
            .getByRole("button", { name: "Create" })
            .hasAttribute("disabled"),
      ).toBe(true);
   });
});
