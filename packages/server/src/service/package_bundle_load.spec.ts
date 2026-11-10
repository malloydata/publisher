// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   decodeBundleStamp,
   encodeBundle,
   decodeBundle,
} from "../package_load/bundle";
import {
   duckdbMalloyConfig,
   writeTempPackage,
} from "../test_helpers/temp_package";
import { unrecordedAudit } from "./access_record";
import { Package } from "./package";

/**
 * A package loaded from a compiled bundle serves what the compile did, and a
 * bundle that does not fit this server or this tree is a miss that compiles.
 */
describe("Package.create with a compiled bundle", () => {
   const files = {
      "base.malloy": [
         "##! experimental.givens",
         "given:",
         "  ROLE :: string",
         "",
         "#(authorize) 'analyst' = $ROLE",
         "source: rows is duckdb.sql(\"select 1 as id, 'a' as k union all select 2, 'b'\") extend {",
         "  measure: n is count()",
         "  view: by_k is { group_by: k; aggregate: n }",
         "}",
      ].join("\n"),
      "derived.malloy": [
         "##! experimental.givens",
         'import "base.malloy"',
         "// Inherits the base's gate through extend.",
         "source: rows_ext is rows extend { view: every is { group_by: id; aggregate: n } }",
      ].join("\n"),
   };
   let dir: string;
   let bundlePath: string;
   let cleanup: () => void;
   const env = duckdbMalloyConfig();

   beforeAll(async () => {
      ({ dir, cleanup } = writeTempPackage(files, { name: "bundled" }));
      bundlePath = path.join(os.tmpdir(), `bundled-${process.pid}.bundle`);
      await Package.compileToBundle(
         "env",
         "bundled",
         dir,
         env.malloyConfig,
         bundlePath,
      );
   });
   afterAll(async () => {
      fs.rmSync(bundlePath, { force: true });
      cleanup();
      await env.duckdb.close();
   });

   /** Rows of a view, as the caller `role` sees them; a refused gate throws. */
   async function queryRows(
      pkg: Package,
      source: string,
      view: string,
      role: string,
   ) {
      const model = pkg.getModel("derived.malloy")!;
      const result = await model.runQuery(unrecordedAudit(), {
         sourceName: source,
         queryName: view,
         filterParams: {},
         givens: { ROLE: role } as never,
      });
      return result.compactResult as unknown as Array<Record<string, unknown>>;
   }

   it("hydrates from a bundle whose stamp matches, with the same answers and gates", async () => {
      const compiled = await Package.create(
         "env",
         "bundled",
         dir,
         env.malloyConfig,
      );
      const hydrated = await Package.create(
         "env",
         "bundled",
         dir,
         env.malloyConfig,
         false,
         {
            bundleLocation: bundlePath,
         },
      );
      expect(hydrated.getOperationalFields().modelBundle?.status).toBe("hit");
      expect(compiled.getOperationalFields().modelBundle?.status).toBe(
         "compiled",
      );
      const fromBundle = await queryRows(
         hydrated,
         "rows_ext",
         "every",
         "analyst",
      );
      expect(fromBundle.length).toBe(2);
      expect(fromBundle).toEqual(
         await queryRows(compiled, "rows_ext", "every", "analyst"),
      );
      // The inherited gate denies the same caller on both.
      await expect(
         queryRows(hydrated, "rows_ext", "every", "viewer"),
      ).rejects.toThrow();
      await expect(
         queryRows(compiled, "rows_ext", "every", "viewer"),
      ).rejects.toThrow();
   });

   it("compiles, and says why, when the bundle is missing, from another compiler, or for other source", async () => {
      const absent = await Package.create(
         "env",
         "bundled",
         dir,
         env.malloyConfig,
         false,
         {
            bundleLocation: path.join(os.tmpdir(), "nope.bundle"),
         },
      );
      expect(absent.getOperationalFields().modelBundle?.status).toBe("absent");

      const bytes = fs.readFileSync(bundlePath);
      const { stamp, result } = decodeBundle(bytes);
      const otherCompiler = path.join(
         os.tmpdir(),
         `bundled-old-${process.pid}.bundle`,
      );
      fs.writeFileSync(
         otherCompiler,
         encodeBundle({ ...stamp, compilerVersion: "0.0.1" }, result),
      );
      try {
         const stale = await Package.create(
            "env",
            "bundled",
            dir,
            env.malloyConfig,
            false,
            {
               bundleLocation: otherCompiler,
            },
         );
         expect(stale.getOperationalFields().modelBundle?.status).toBe(
            "stale_compiler",
         );
         expect(stale.getOperationalFields().modelBundle?.detail).toContain(
            "0.0.1",
         );
      } finally {
         fs.rmSync(otherCompiler, { force: true });
      }

      const edited = writeTempPackage(
         {
            ...files,
            "derived.malloy": files["derived.malloy"] + "\n// edited",
         },
         { name: "bundled" },
      );
      try {
         const stale = await Package.create(
            "env",
            "bundled",
            edited.dir,
            env.malloyConfig,
            false,
            {
               bundleLocation: bundlePath,
            },
         );
         expect(stale.getOperationalFields().modelBundle?.status).toBe(
            "stale_source",
         );
      } finally {
         edited.cleanup();
      }
   });

   it("finds the bundle an install recorded in publisher.json when none is passed", async () => {
      const recorded = writeTempPackage(files, {
         manifest: { name: "bundled", modelBundleLocation: bundlePath },
      });
      try {
         const pkg = await Package.create(
            "env",
            "bundled",
            recorded.dir,
            env.malloyConfig,
         );
         expect(pkg.getOperationalFields().modelBundle?.status).toBe("hit");
         expect(
            decodeBundleStamp(fs.readFileSync(bundlePath)).packageName,
         ).toBe("bundled");
      } finally {
         recorded.cleanup();
      }
   });
});
