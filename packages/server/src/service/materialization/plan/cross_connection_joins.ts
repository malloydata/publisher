// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
/**
 * Loading an author's model whose files join sources across connections:
 * with the materialize switch on, the load is tried again with those joins,
 * and what reads them, left out (`text/cross_connection_text.ts`), and a
 * file's text is read as that compile read it.
 *
 * Read by the package-load worker, `Model.create` and the main-thread build
 * plan, so discovery, gates, the build plan and live all read the same build
 * view.
 */

import { packageTextReader } from "../../../utils";
import { getMaterializeEnabled } from "../../../config";
import { overlayKey } from "../../overlay_key";
import { type LeftOutDefinition } from "../text/definition_cut";
import { leaveOutCrossConnectionJoins } from "../text/cross_connection_text";

export type { LeftOutDefinition };

/**
 * A file's text by URL, as a compile read it: `overlay`'s (keyed by
 * {@link overlayKey}) when it holds the file, else the file on disk, read
 * only when it lies inside `packageRoot`.
 */
export function compiledFileText(
   packageRoot: string,
   url: string,
   overlay?: ReadonlyMap<string, string>,
): string | undefined {
   const held = overlay?.get(overlayKey(url));
   if (held !== undefined) return held;
   return packageTextReader(packageRoot)(url);
}

/**
 * Load a model whose files join sources across connections (#6982): with the
 * materialize switch on, `load` again with those joins, and what reads them,
 * left out (the build view), so builds are planned from it and the model
 * serves every query that does not need them. `firstError` is the failure of
 * `load` with no overlay; `readText` gives each file's text as that compile
 * read it, which the problems' positions index. Undefined when the switch is
 * off, it is any other failure, or the build view does not compile either;
 * the caller then reports `firstError`.
 *
 * The one entry point for every load of an author's model: the package-load
 * worker, `Model.create` and the main-thread build plan, so discovery, gates,
 * the build plan and live all read the same build view.
 */
export async function loadLeavingOutCrossConnectionJoins<T>(args: {
   firstError: unknown;
   readText: (url: string) => string | undefined;
   load: (overlay: ReadonlyMap<string, string>) => Promise<T>;
}): Promise<
   | { result: T; overlay: Map<string, string>; leftOut: LeftOutDefinition[] }
   | undefined
> {
   if (!getMaterializeEnabled()) return undefined;
   let result: T | undefined;
   let loaded = false;
   const kept = await leaveOutCrossConnectionJoins({
      firstError: args.firstError,
      readText: args.readText,
      compile: async (overlay) => {
         result = await args.load(overlay);
         loaded = true;
      },
   }).catch(() => undefined);
   if (!kept || !loaded) return undefined;
   return { result: result as T, overlay: kept.overlay, leftOut: kept.leftOut };
}
