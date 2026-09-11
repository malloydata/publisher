// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readdirSync } from "fs";
import { join } from "path";
import { DOC_LINKS } from "./docLinks";

const REPO_ROOT = join(import.meta.dir, "..", "..", "..", "..");
const REPO_DOCS_PREFIX =
   "https://github.com/malloydata/publisher/blob/main/docs/";

/**
 * Every DOC_LINKS entry pointing into this repo names a file that is actually
 * here. These are rendered as links on the Console home page, so a doc renamed
 * or moved without updating the constant ships a 404 to every reader, and
 * nothing else in the build would notice: the value is a string, and a string
 * is as valid wrong as right.
 *
 * What this pins, stated precisely, because the obvious reading is wider than
 * the check: the target exists **in this working tree**. It cannot pin that the
 * URL resolves today, because the URL names `blob/main` and a doc added on a
 * branch is here and not on `main` until that branch merges. That gap is a
 * merge-ordering question, handled by sequencing the pull request, not by a
 * test. What this does catch is the durable half: a rename, a deletion, or a
 * typo, from the moment it happens.
 *
 * Compared against exact basenames rather than with `existsSync`, because macOS
 * is case-insensitive: `dashboards.md` mistyped as `Dashboards.md` passes an
 * existence check on the author's machine and 404s on GitHub, which is
 * case-sensitive. Linux CI would catch it, but only after the author had seen
 * green.
 */
describe("DOC_LINKS", () => {
   const repoDocs = Object.entries(DOC_LINKS).filter(([, url]) =>
      url.startsWith(REPO_DOCS_PREFIX),
   );

   // Guards the filter itself. Were the prefix to drift from the constant, every
   // case below would vanish and the suite would still be green: zero passing
   // assertions reads identically to zero broken links.
   it("covers the in-repo docs", () => {
      expect(repoDocs.length).toBeGreaterThan(0);
      expect(repoDocs.length).toBe(
         Object.values(DOC_LINKS).filter((url) => url.includes("/blob/"))
            .length,
      );
   });

   it.each(repoDocs)("%s resolves to a file in docs/", (_key, url) => {
      const relative = url.slice(REPO_DOCS_PREFIX.length);
      expect(readdirSync(join(REPO_ROOT, "docs"))).toContain(relative);
   });
});
