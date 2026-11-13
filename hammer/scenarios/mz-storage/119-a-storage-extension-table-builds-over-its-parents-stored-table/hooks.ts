// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Markdown cannot read a run record's failure text, so this hook does.

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

const PKG = "mzx";

/**
 * The refused build names the parent by its author's name, `orders`, and no
 * run record names a generated build source (`<src>__mat`).
 */
export async function refusalNamesTheAuthorsParent(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const rest = await api.use("on");
   const records = JSON.stringify(await rest.listMaterializations(PKG));
   assert.ok(
      "the refusal names the author's parent",
      records.includes("persisted upstream 'orders'"),
      records.slice(0, 2000),
   );
   assert.ok(
      "no run record names a generated build source",
      !records.includes("__mat"),
      records.slice(
         Math.max(0, records.indexOf("__mat") - 300),
         records.indexOf("__mat") + 300,
      ),
   );
}
