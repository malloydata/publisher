// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, it } from "bun:test";
import { getTreeView } from "./FileTreeView";

const noop = () => {};

it("shows a served notebook as a notebook, not a model", () => {
   const tree = getTreeView(
      [{ path: "notebooks/tour.malloy" }, { path: "storefront.malloy" }],
      noop,
      [{ path: "notebooks/tour.malloy" }],
   );

   const notebooks = tree.find((item) => item.id === "notebooks/");
   expect(notebooks?.children?.map((c) => [c.id, c.fileType])).toEqual([
      ["notebooks/tour.malloy", "notebook"],
   ]);
   expect(tree.find((item) => item.id === "storefront.malloy")?.fileType).toBe(
      "model",
   );
});

it("lists a served notebook the models listing leaves out", () => {
   const tree = getTreeView([], noop, [{ path: "notebooks/tour.malloy" }]);

   expect(tree[0].children?.[0].fileType).toBe("notebook");
});
