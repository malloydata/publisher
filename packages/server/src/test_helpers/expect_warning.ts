// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect } from "bun:test";
import type { components } from "../api";

type PackageWarning = components["schemas"]["PackageWarning"];

/**
 * The one warning with `code` and `subject`, which must hold `token`, found
 * among `warnings`: a warning is asserted by what a consumer reads, never by
 * its whole sentence, which is prose that may be reworded. A second warning of
 * the same code and subject, whatever it says, fails it: a finding is said once.
 */
export function expectWarning(
   warnings: readonly PackageWarning[],
   code: string,
   subject: string | undefined,
   token: string,
): PackageWarning {
   const same = warnings.filter(
      (w) => w.code === code && w.subject === subject,
   );
   const found = same.filter((w) => w.message.includes(token));
   if (same.length !== 1 || found.length !== 1) {
      throw new Error(
         `expected one '${code}' warning on '${subject}', holding '${token}', found ${same.length} (${found.length} holding it) among ${JSON.stringify(warnings.map((w) => [w.code, w.subject, w.message]))}`,
      );
   }
   expect(found[0]!.severity).toBe("warn");
   return found[0]!;
}
