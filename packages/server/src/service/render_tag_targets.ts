// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { quoteMalloyIdentifier } from "./authorize";

/** One query whose compiled result the render-tag check reads. */
export interface RenderTagTarget {
   /** Names the target in a finding: the query, or `source -> view`. */
   label: string;
   queryString: string;
}

/**
 * The queries a model's render-tag check prepares: every annotated top-level
 * named query and every annotated view declared on a source. Shared by the
 * package-load worker, which prepares them alongside the model's compile, and
 * `Model.validateRenderTags`, which prepares them itself when the worker did
 * not, so the two can never disagree about what is checked.
 */
export function renderTagTargets(
   queries: readonly { name?: string; annotations?: unknown[] }[] | undefined,
   sources:
      | readonly {
           name?: string;
           views?: { name?: string; annotations?: unknown[] }[];
        }[]
      | undefined,
): RenderTagTarget[] {
   const targets: RenderTagTarget[] = [];
   for (const query of queries ?? []) {
      // Only an annotated, named query can carry a render tag to validate;
      // skip the rest rather than compiling every query in the package.
      if (!query.name || !query.annotations?.length) {
         continue;
      }
      // Quote the identifier (see quoteMalloyIdentifier) so a name needing
      // Malloy quoting still lexes and cannot break out of the quotes.
      targets.push({
         label: query.name,
         queryString: `run: ${quoteMalloyIdentifier(query.name)}`,
      });
   }
   for (const source of sources ?? []) {
      if (!source.name) continue;
      for (const view of source.views ?? []) {
         // Render tags live on the view's own or inherited annotations, not
         // via source-to-view inheritance, so an unannotated view has nothing
         // to validate and need not be compiled. (A model-level `##` tag is the
         // one case this gate doesn't reach, but those are theme/config, not
         // the child-only chart tags this guards against.)
         if (!view.name || !view.annotations?.length) {
            continue;
         }
         // Quote both identifiers (see quoteMalloyIdentifier): an unquoted
         // name like `gated-source` fails to lex, and the prepare would then
         // fail and silently skip the very view this is meant to validate.
         targets.push({
            label: `${source.name} -> ${view.name}`,
            queryString: `run: ${quoteMalloyIdentifier(source.name)} -> ${quoteMalloyIdentifier(view.name)}`,
         });
      }
   }
   return targets;
}
