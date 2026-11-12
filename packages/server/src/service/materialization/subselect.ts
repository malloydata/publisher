// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `FROM (<select>) AS <alias>`, safe for any SELECT a build produces: a
 * trailing terminator would end the subselect early, and the closing paren goes
 * on its own line so a trailing `--` comment cannot swallow it. One spelling for
 * every build-side wrapper (the partitioned insert's ORDER BY, the declared-type
 * cast), so a fix to one cannot miss the other.
 */
export function fromSubselect(selectSQL: string, alias: string): string {
   const inner = selectSQL.replace(/[\s;]+$/, "");
   return `FROM (\n${inner}\n) AS ${alias}`;
}
