// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { LocalGiven } from "./document";

/**
 * A `given:` a report declares itself, as the `Given` spec its filter control
 * is drawn from: the spec the server would publish for the same declaration
 * once the report is served.
 *
 * Exported for a host that holds a report as text and draws its controls
 * itself, so a filter it declares looks and behaves as it will once the
 * report is published.
 */
export function localGivenSpec(local: LocalGiven): Given {
   const shown = displayDefault(local);
   return {
      name: local.name,
      type: local.type,
      ...(shown === undefined ? {} : { default: shown }),
      ...(local.label === undefined ? {} : { label: local.label }),
      ...(local.description === undefined
         ? {}
         : { description: local.description }),
      ...(local.control === undefined
         ? {}
         : { control: local.control as Given["control"] }),
      ...(local.suggest
         ? {
              suggest: {
                 ...(local.suggest.source === undefined
                    ? {}
                    : { source: local.suggest.source }),
                 ...(local.suggest.query === undefined
                    ? {}
                    : { query: local.suggest.query }),
                 dimension: local.suggest.dimension,
              },
           }
         : {}),
      ...(local.rangeMin === undefined ? {} : { rangeMin: local.rangeMin }),
      ...(local.rangeMax === undefined ? {} : { rangeMax: local.rangeMax }),
   };
}

/**
 * The default the server would publish: it unwraps a filter's `f'…'`, so an
 * empty one is `""` (filters nothing, shows no caption). Never undefined for a
 * declaration with a default: an absent `default` means the given is required.
 */
function displayDefault(local: LocalGiven): string | undefined {
   if (!local.type?.startsWith("filter<")) return local.default;
   const body = local.default?.match(/^f(['"])([\s\S]*)\1$/)?.[2];
   return body ?? local.default;
}
