// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PublisherClient } from "../api/client.js";
import { logInfo, logOutput } from "../utils/logger.js";

/**
 * Show the package's materialization schedule and the policy around it: the cron
 * (or none), the persist scope, whether a freshness policy is declared, and
 * whether the package is control-plane managed (in which case the standalone
 * scheduler never fires it).
 */
export async function viewSchedule(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const pkg = await client.getPackage(environmentName, packageName);
  const schedule = pkg?.materialization?.schedule ?? null;
  const scope = pkg?.scope ?? "package";
  const hasFreshness = Boolean(pkg?.materialization?.freshness);
  const orchestrated = Boolean(pkg?.manifestLocation);

  logOutput(`Package:   ${packageName}`);
  logOutput(`Scope:     ${scope}`);
  logOutput(`Schedule:  ${schedule ?? "(none: publish / on-demand only)"}`);
  logOutput(`Freshness: ${hasFreshness ? "set" : "(none)"}`);
  if (orchestrated) {
    logInfo(
      "Control-plane managed (manifestLocation set): the standalone scheduler does not fire this package.",
    );
  }
}
