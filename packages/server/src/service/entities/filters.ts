// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Explore as MalloyExplore } from "@malloydata/malloy";
import {
   type FilterNoteSource,
   parseFilters,
   sourceFilterNotes,
} from "../filter";
import type { Filter } from "./internal_types";

/**
 * A source's `#(filter)`s exactly as the query path reads them: its block notes
 * across the `extend` chain, an extending source's filter overriding its base's
 * by name. Throws on a malformed annotation, as the query path does.
 */
export function sourceFilters(explore: MalloyExplore): Filter[] {
   const annotations = (
      explore.structDef as { annotations?: FilterNoteSource } | undefined
   )?.annotations;
   return parseFilters(sourceFilterNotes(annotations));
}
