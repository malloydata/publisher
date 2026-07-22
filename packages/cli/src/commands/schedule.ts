import { PublisherClient } from "../api/client.js";
import { PackageScopeEnum } from "../api/generated";
import { logInfo, logOutput, logSuccess } from "../utils/logger.js";

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

/**
 * Set the package's materialization schedule. A cron is legal only on a
 * version-scoped package, so this also sets scope: version (mirrors the SDK
 * schedule card). The server enforces the publish-gate rules (a valid 5-field
 * UNIX cron, version scope, and no coexisting freshness) and rejects an invalid
 * request with 400, which the CLI surfaces. The current description is carried
 * through so the edit does not drop it.
 *
 * A schedule and a freshness policy are mutually exclusive, and the request
 * replaces the whole `materialization` object, so setting a schedule drops any
 * existing freshness. That is intended (they cannot coexist) but we warn first
 * so it is never silent. An empty cron is rejected up front: every server rule
 * is truthiness-guarded, so `""` would otherwise skip them all and persist.
 */
export async function setSchedule(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  cron: string,
): Promise<void> {
  if (!cron.trim()) {
    throw new Error(
      "A schedule requires a non-empty 5-field UNIX cron, e.g. `0 6 * * *`.",
    );
  }
  const current = await client.getPackage(environmentName, packageName);
  if (current?.materialization?.freshness) {
    logInfo(
      `Replacing the existing freshness policy on ${packageName} with a schedule (a package cannot have both).`,
    );
  }
  await client.updatePackage(environmentName, packageName, {
    name: packageName,
    description: current?.description,
    scope: PackageScopeEnum.Version,
    materialization: { schedule: cron },
  });
  logSuccess(`Schedule set on ${packageName}: "${cron}" (scope: version).`);
}

/**
 * Clear the package's materialization schedule (revert to publish / on-demand
 * only). No-ops when no schedule is set: clearing sends a fresh
 * `materialization` object and the server replaces the stored one wholesale, so
 * PATCHing when there is nothing to clear would wipe an unrelated freshness
 * policy on a freshness-only package. Scope is left as version; to return a
 * package to `scope: package`, edit `scope` in its publisher.json (or PATCH the
 * package directly); the CLI does not expose a scope switch.
 */
export async function clearSchedule(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const current = await client.getPackage(environmentName, packageName);
  if (!current?.materialization?.schedule) {
    logInfo(`No schedule set on ${packageName}; nothing to clear.`);
    return;
  }
  await client.updatePackage(environmentName, packageName, {
    name: packageName,
    description: current?.description,
    materialization: { schedule: null },
  });
  logSuccess(`Schedule cleared on ${packageName}.`);
}
