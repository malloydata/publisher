// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import config from "../vite.config";

/**
 * The build plugin that makes the widget one self-contained file.
 *
 * Under test because it is the only part of this package that is platform
 * sensitive, and it runs on Windows in CI. It matches an HTML tag against a
 * rollup bundle key, which is a URL compared against a bundle path: comparing
 * them whole made the build depend on rollup emitting forward slashes in
 * `fileName` on every platform. That contract holds, but it had only ever been
 * observed on macOS, and a separator mismatch does not degrade here, it
 * hard-fails the build.
 *
 * So the separator cases below are the point, and the two guard cases are what
 * stops the fix from being a loosening: an unreferenced chunk must still be a
 * build error, because inlining a bundle while silently dropping a chunk ships a
 * widget that is missing code.
 */

interface FakeChunk {
   type: "chunk";
   code: string;
}
interface FakeAsset {
   type: "asset";
   source: string;
}
type FakeBundle = Record<string, FakeChunk | FakeAsset>;

const plugin = (() => {
   const plugins = (config as { plugins?: unknown[] }).plugins ?? [];
   const found = plugins
      .flat()
      .find(
         (p): p is { name: string; generateBundle: unknown } =>
            typeof p === "object" &&
            p !== null &&
            (p as { name?: string }).name === "publisher-inline-widget-assets",
      );
   if (!found) throw new Error("inline plugin not found in vite.config");
   return found;
})();

function inline(
   key: string,
   tagSrc: string | null,
   extraChunkKey?: string,
): { html: string } {
   const bundle: FakeBundle = {
      "execute-query.html": {
         type: "asset",
         source: `<html><body>${
            tagSrc === null
               ? ""
               : `<script type="module" crossorigin src="${tagSrc}"></script>`
         }</body></html>`,
      },
      [key]: { type: "chunk", code: "console.log(1)" },
   };
   if (extraChunkKey) {
      bundle[extraChunkKey] = { type: "chunk", code: "console.log(2)" };
   }
   // Rollup calls generateBundle with the plugin context as `this`; `error`
   // throws in the real one, which is what the guards rely on.
   const ctx = {
      error: (m: unknown) => {
         throw new Error(typeof m === "string" ? m : String(m));
      },
   };
   (
      plugin.generateBundle as (
         this: typeof ctx,
         o: unknown,
         b: FakeBundle,
      ) => void
   ).call(ctx, {}, bundle);
   return { html: String((bundle["execute-query.html"] as FakeAsset).source) };
}

describe("inlineWidgetAssets", () => {
   // Each of the four separator combinations must inline, because the key and
   // the tag come from different layers and either could carry either separator.
   const combinations: Array<[string, string, string]> = [
      ["both forward slash", "assets/eq-Hb60.js", "./assets/eq-Hb60.js"],
      ["backslash in the key", "assets\\eq-Hb60.js", "./assets/eq-Hb60.js"],
      ["backslash in the tag", "assets/eq-Hb60.js", ".\\assets\\eq-Hb60.js"],
      ["backslash in both", "assets\\eq-Hb60.js", ".\\assets\\eq-Hb60.js"],
   ];

   for (const [label, key, tagSrc] of combinations) {
      it(`inlines the chunk with ${label}`, () => {
         const { html } = inline(key, tagSrc);
         expect(html).toContain("console.log(1)");
         // The whole purpose: no external reference may survive, or the widget
         // renders blank in a host that cannot fetch it.
         expect(html).not.toContain("src=");
      });
   }

   it("errors when no tag references the chunk", () => {
      // Not a loosening: an unreferenced chunk means the HTML and the bundle
      // disagree, and inlining anyway would ship a widget missing code.
      expect(() => inline("assets/eq-Hb60.js", null)).toThrow(
         /no <script> tag references/,
      );
   });

   it("errors when the bundle was split into a second chunk", () => {
      // The case inlineDynamicImports exists to prevent. The second chunk has no
      // tag of its own, so it would be dropped silently.
      expect(() =>
         inline("assets/eq-Hb60.js", "./assets/eq-Hb60.js", "assets/split.js"),
      ).toThrow(/no <script> tag references assets\/split\.js/);
   });
});
