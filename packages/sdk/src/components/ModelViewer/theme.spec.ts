// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readdirSync, readFileSync } from "fs";
import path from "path";

/**
 * The viewer is drawn inside whatever theme its host gives it, light or dark,
 * so its colours are MUI's own palette keys, which every theme defines in both
 * modes. A hex value, or a key only some hosts add (`background.muted`,
 * `text.tertiary`, `focus.ring`), drew a near-white pill under light text in
 * the Console's dark mode, and a hover tint that never appeared.
 */
const SOURCES = readdirSync(import.meta.dir)
   .filter((file) => /\.tsx?$/.test(file) && !/\.spec\.tsx?$/.test(file))
   .map((file) => ({
      file,
      text: readFileSync(path.join(import.meta.dir, file), "utf8"),
   }));

describe("the viewer's colours", () => {
   it("are never a hex value", () => {
      const hex = SOURCES.filter(({ text }) =>
         /["'`]#[0-9a-fA-F]{3,8}\b/.test(text),
      ).map(({ file }) => file);
      expect(hex).toEqual([]);
   });

   it("name only palette keys every MUI theme defines", () => {
      const custom =
         /["'](background\.(muted|strong|subtle)|text\.(tertiary|faint)|focus\.ring)["']/;
      const found = SOURCES.filter(({ text }) => custom.test(text)).map(
         ({ file }) => file,
      );
      expect(found).toEqual([]);
   });
});
