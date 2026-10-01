// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The sources a model exports. `sources` also lists names the model only
 * imports, and `import { x } from "../model"` cannot reach those; no
 * `modelInfo`, or one that does not parse, offers nothing rather than a
 * choice that would write a file that does not compile.
 */
export function exportedSources(modelInfo: string | undefined): Set<string> {
   try {
      const parsed = JSON.parse(modelInfo ?? "") as {
         entries?: { kind?: string; name?: string }[];
      };
      return new Set(
         (parsed.entries ?? []).flatMap((entry) =>
            entry.kind === "source" && typeof entry.name === "string"
               ? [entry.name]
               : [],
         ),
      );
   } catch {
      return new Set();
   }
}
