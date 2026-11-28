// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect } from "bun:test";
import type { components } from "../api";

type PackageWarning = components["schemas"]["PackageWarning"];

/**
 * The one warning with `code` and `subject` whose message holds `token`, found
 * among `warnings`: a warning is asserted by what a consumer reads, never by
 * its whole sentence, which is prose that may be reworded.
 */
export function expectWarning(
   warnings: readonly PackageWarning[],
   code: string,
   subject: string | undefined,
   token: string,
): PackageWarning {
   const found = warnings.filter(
      (w) =>
         w.code === code && w.subject === subject && w.message.includes(token),
   );
   if (found.length !== 1) {
      throw new Error(
         `expected one '${code}' warning on '${subject}' whose message holds '${token}', found ${found.length} among ${JSON.stringify(warnings.map((w) => [w.code, w.subject, w.message]))}`,
      );
   }
   expect(found[0]!.severity).toBe("warn");
   return found[0]!;
}
