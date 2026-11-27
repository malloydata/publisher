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

   const exportsOf = (file: string) =>
      new Bun.Transpiler({ loader: "js" }).scan(
         readFileSync(path.join(KIT, file), "utf8"),
      ).exports;

   it("the worked app imports only names the kit exports", () => {
      const app = readFileSync(path.join(KIT, "..", "app.js"), "utf8");
      const have = exportsOf("kit.js");
      for (const m of app.matchAll(
         /import\s*\{([^}]*)\}\s*from\s*'\.\/kit\/kit\.js'/g,
      )) {
         for (const name of m[1]
            .split(",")
            .map((s) => s.trim().split(/\s+as\s+/)[0])
            .filter(Boolean)) {
            expect(have, `app.js imports ${name}`).toContain(name);
         }
      }
   });

   for (const file of modules) {
      it(`${file} imports and re-exports only names its sibling modules export`, () => {
         const source = readFileSync(path.join(KIT, file), "utf8");
         for (const m of source.matchAll(
            /(?:import|export)\s*\{([^}]*)\}\s*from\s*'\.\/([^']+)'/g,
         )) {
            const have = exportsOf(m[2]);
            for (const name of m[1]
               .split(",")
               .map((s) => s.trim().split(/\s+as\s+/)[0])
               .filter(Boolean)) {
               expect(have, `${file} imports ${name} from ${m[2]}`).toContain(
                  name,
               );
            }
         }
      });

      it(`parses ${file}`, () => {
         const source = readFileSync(path.join(KIT, file), "utf8");
         expect(() =>
            new Bun.Transpiler({ loader: "js" }).transformSync(source),
         ).not.toThrow();
      });
   }
});
