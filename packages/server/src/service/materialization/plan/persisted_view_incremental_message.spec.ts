// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A persisted view's incremental-policy rejection, which fails the load, names
 * the author's view and the source it is declared on, never the generated
 * build source (`<source>__<view>__view`) the plan holds it under.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../../package_load/package_load_pool";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";

let rootDir: string;
let restore: () => void;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-pv-msg-"));
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE_ENABLED: "true" },
   }).restore;
   await __setPackageLoadPoolForTests(new PackageLoadPool(1));
});

afterEach(async () => {
   await __setPackageLoadPoolForTests(null);
   restore();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

const MODEL = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""SELECT 1 AS amount, 'A' AS category""") extend {
  #@ persist name="by_category_t" refresh="incremental"
  view: by_category is { group_by: category; aggregate: total is amount.sum() }
}
`;

describe("a persisted view's incremental-policy rejection", () => {
   it("names the author's view and its source, never the generated build source", async () => {
      writePackageFiles(
         path.join(rootDir, "pkg"),
         { "model.malloy": MODEL },
         { manifest: { name: "pkg", description: "fixture" } },
      );
      const env = await Environment.create("testEnv", rootDir, []);
      const error = await env.addPackage("pkg").then(
         () => undefined,
         (e: unknown) => e as Error,
      );
      expect(error?.message).toContain(
         '#@ persist on view "by_category" of source "orders" declares refresh="incremental" but no watermark=',
      );
      expect(error?.message).not.toContain("__view");
      expect(error?.message).not.toContain("orders__by_category");
   }, 60_000);
});
