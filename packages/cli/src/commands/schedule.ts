// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PublisherClient } from "../api/client.js";
import { type Package, PackageScopeEnum } from "../api/generated";
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
  if (pkg?.versionId) {
    logOutput(`Version:   ${pkg.versionId}`);
  }
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
 * Refuse, before any PATCH, to change the schedule of a published version.
 * A version is immutable, so the server answers a schedule PATCH on one with
 * 409 PACKAGE_IS_VERSIONED; its schedule is part of its content, declared in
 * its publisher.json. This names that path instead of the server's generic
 * refusal. A response carries `versionId` only for a published version.
 */
function refuseOnPublishedVersion(
  pkg: Package | undefined,
  packageName: string,
): void {
  if (!pkg?.versionId) {
    return;
  }
  throw new Error(
    `Package ${packageName} is published version ${pkg.versionId}, which is immutable, so its schedule cannot be changed in place. ` +
      `A version's schedule is \`materialization.schedule\` in its publisher.json, with \`"scope": "version"\` at the root: ` +
      `set it there, bump \`version\`, and publish the new version.`,
  );
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
 * so it is never silent. Everything else in the block that the caller is not
 * changing is resent verbatim. An empty cron is rejected up front: every server
 * rule is truthiness-guarded, so `""` would otherwise skip them all and persist.
 *
 * Only a package with no published versions takes a schedule this way (see
 * refuseOnPublishedVersion).
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
  refuseOnPublishedVersion(current, packageName);
  if (current?.materialization?.freshness) {
    logInfo(
      `Replacing the existing freshness policy on ${packageName} with a schedule (a package cannot have both).`,
    );
  }
  await client.updatePackage(environmentName, packageName, {
    name: packageName,
    description: current?.description,
    scope: PackageScopeEnum.Version,
    // The request replaces the whole block, so anything not resent is dropped.
    // Freshness is dropped deliberately (warned above); query metadata is
    // orthogonal to scheduling and must survive, or setting a schedule would
    // silently untag every statement the package's builds issue.
    materialization: {
      schedule: cron,
      ...(current?.materialization?.queryMetadata
        ? { queryMetadata: current.materialization.queryMetadata }
        : {}),
    },
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
 *
 * Presence is `schedule != null`, not truthiness: an empty-string schedule is
 * still present and must be clearable. Until the server rejects `""` at the
 * publish gate a package can get stuck with `schedule: ""`, and a truthiness
 * guard would refuse to clear the very state it needs to unstick.
 */
export async function clearSchedule(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const current = await client.getPackage(environmentName, packageName);
  if (current?.materialization?.schedule == null) {
    logInfo(`No schedule set on ${packageName}; nothing to clear.`);
    return;
  }
  refuseOnPublishedVersion(current, packageName);
  await client.updatePackage(environmentName, packageName, {
    name: packageName,
    description: current?.description,
    // Carried for the same reason as in setSchedule: clearing a schedule must
    // not also clear the package's query metadata.
    materialization: {
      schedule: null,
      ...(current?.materialization?.queryMetadata
        ? { queryMetadata: current.materialization.queryMetadata }
        : {}),
    },
  });
  logSuccess(`Schedule cleared on ${packageName}.`);
}
