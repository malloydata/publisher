// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect } from "bun:test";
import type { components } from "../api";

type PackageWarning = components["schemas"]["PackageWarning"];

/** Refusals are said once per subject; a set's findings are not, since its name is the subject of each. */
const SAID_ONCE_PER_SUBJECT = new Set([
   "workflow-not-served",
   "skill-set-not-served",
]);

/**
 * The one warning with `code` and `subject` holding `token`, found among
 * `warnings`: a warning is asserted by what a consumer reads, never by its
 * whole sentence, which is prose that may be reworded. A refusal code also
 * fails when its subject has a second warning of it, whatever that says.
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
   if (
      found.length !== 1 ||
      (SAID_ONCE_PER_SUBJECT.has(code) && same.length !== 1)
   ) {
      throw new Error(
         `expected one '${code}' warning on '${subject}', holding '${token}', found ${same.length} (${found.length} holding it) among ${JSON.stringify(warnings.map((w) => [w.code, w.subject, w.message]))}`,
      );
   }
   expect(found[0]!.severity).toBe("warn");
   return found[0]!;
}
