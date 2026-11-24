// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Every module of the data app kit (skills/malloy-html-data-apps/assets/data-app/kit) parses as an ES module. The
 * kit is copied into apps as it is and runs in the browser with no build step, so a syntax error in any one module
 * leaves every app built from it blank.
 */
import { describe, expect, it } from "bun:test";
import { readdirSync, readFileSync } from "fs";
import path from "path";

const KIT = path.resolve(
   import.meta.dir,
   "../../../../../skills/malloy-html-data-apps/assets/data-app/kit",
);
const modules = readdirSync(KIT).filter((f) => f.endsWith(".js"));

describe("the data app kit", () => {
   it("has its modules", () => {
      expect(modules).toContain("kit.js");
   });

   for (const file of modules) {
      it(`parses ${file}`, () => {
         const source = readFileSync(path.join(KIT, file), "utf8");
         expect(() =>
            new Bun.Transpiler({ loader: "js" }).transformSync(source),
         ).not.toThrow();
      });
   }
});
