// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import {
  logSuccess,
  logInfo,
  logOutput,
  logWarning,
  truncate,
} from "../utils/logger.js";

// A build settles at MANIFEST_FILE_READY or a failure/cancellation. For an
// auto-run, MANIFEST_FILE_READY means the tables are built and the package
// serves them: the manifest was loaded into it before the status moved (its
// metadata says `autoLoad: "failed"` when that load did not take, as when a
// host manifest was bound to the package while the run built). A package or
// version already bound to a host manifest refuses the run up front (409).
const SETTLED_STATUSES = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];
const SUCCESS_STATUSES = ["MANIFEST_FILE_READY"];

/**
 * The exit code of `--wait` for a run that built but whose manifest did not
 * load into the package (`metadata.autoLoad: "failed"`): not 0, since queries
 * are not answered from the new tables, and not 1, which is a failed build.
 */
export const BUILT_NOT_SERVED_EXIT_CODE = 2;

// Each run records metadata.trigger (scheduler | on_demand; servers before
// 0.15 wrote SCHEDULER | ON_DEMAND) so a scheduled rebuild is distinguishable
// from a manual one; it lives in the free-form metadata object rather than a
// top-level field. Shown lower-cased, so both spellings read the same.
function triggerOf(m: any): string {
  return (
    (m?.metadata as { trigger?: string } | null)?.trigger ?? ""
  ).toLowerCase();
}

export async function listMaterializations(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  options: { limit?: number; offset?: number } = {},
): Promise<void> {
  const materializations = await client.listMaterializations(
    environmentName,
    packageName,
    options.limit,
    options.offset,
  );

  if (materializations.length === 0) {
    logInfo(`No materializations in package: ${packageName}`);
    return;
  }

  const table = new Table({
    head: ["ID", "Status", "Trigger", "Started", "Completed", "Error"],
  });

  materializations.forEach((m: any) => {
    table.push([
      m.id ?? "",
      m.status ?? "",
      triggerOf(m),
      m.startedAt ?? "",
      m.completedAt ?? "",
      m.error ? truncate(m.error) : "",
    ]);
  });

  logOutput(table.toString());
}

export async function getMaterialization(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  materializationId: string,
): Promise<void> {
  const materialization = await client.getMaterialization(
    environmentName,
    packageName,
    materializationId,
  );
  logOutput(JSON.stringify(materialization, null, 2));
}

/**
 * Create a materialization (auto-run): the publisher compiles, self-assigns
 * table names, builds every persist source, and auto-loads the manifest,
 * settling at MANIFEST_FILE_READY. With `wait`, poll until the run settles;
 * without it, return immediately with a status hint.
 *
 * Resolves to the process exit code the caller should set: 0, or
 * BUILT_NOT_SERVED_EXIT_CODE. A failed or timed-out build throws instead,
 * and so does a refused one: a package or version bound to a host manifest
 * answers 409 and nothing is built, so the CLI exits 1, `--wait` or not.
 */
export async function materialize(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  options: {
    forceRefresh?: boolean;
    reseed?: boolean;
    wait?: boolean;
    pollIntervalMs?: number;
    timeoutMs?: number;
  } = {},
): Promise<number> {
  const created = await client.createMaterialization(
    environmentName,
    packageName,
    {
      forceRefresh: options.forceRefresh,
      reseed: options.reseed,
    },
  );
  const id = created.id as string | undefined;
  if (!id) {
    throw new Error("Publisher returned a materialization with no id");
  }
  logSuccess(`Created materialization: ${id} (status: ${created.status})`);

  if (!options.wait) {
    logInfo(
      `Run "malloy-pub get materialization ${id} --environment ${environmentName} --package ${packageName}" to check status.`,
    );
    return 0;
  }

  const settledStatuses = SETTLED_STATUSES;
  const successStatuses = SUCCESS_STATUSES;

  const pollIntervalMs = options.pollIntervalMs ?? 2000;
  const timeoutMs = options.timeoutMs ?? 120000;
  const deadline = Date.now() + timeoutMs;

  let current = created;
  while (!settledStatuses.includes(current.status) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    current = await client.getMaterialization(environmentName, packageName, id);
  }

  // Always print the final record for visibility.
  logOutput(JSON.stringify(current, null, 2));

  if (successStatuses.includes(current.status)) {
    // Built, but its manifest did not load into the package: queries are not
    // answered from the new tables. Not a failure of the build, so its own
    // exit code, distinct from 1.
    const metadata = (current.metadata ?? {}) as Record<string, unknown>;
    if (metadata.autoLoad === "failed") {
      logWarning(
        `Materialization ${id} built but not served: ${String(
          metadata.autoLoadError ?? current.error ?? "the manifest load failed",
        )}`,
      );
      return BUILT_NOT_SERVED_EXIT_CODE;
    }
    logSuccess(`Materialization ${id} ready: ${current.status}`);
    return 0;
  }

  // Non-success: throw so the CLI exits non-zero. The index.ts action's catch
  // turns a thrown error into logError + process.exit(1), which is what a
  // CI/automation caller relying on --wait's exit code needs.
  if (!settledStatuses.includes(current.status)) {
    throw new Error(
      `Timed out waiting for materialization ${id} after ${Math.round(
        timeoutMs / 1000,
      )}s (last status: ${current.status}). The build may still be running.`,
    );
  }
  throw new Error(
    `Materialization ${id} finished: ${current.status}${
      current.error ? ` - ${current.error}` : ""
    }`,
  );
}

export async function stopMaterialization(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  materializationId: string,
): Promise<void> {
  const result = await client.materializationAction(
    environmentName,
    packageName,
    materializationId,
    "stop",
  );
  logSuccess(
    `Stopped materialization: ${materializationId} (status: ${result.status})`,
  );
}

export async function deleteMaterialization(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  materializationId: string,
  dropTables?: boolean,
): Promise<void> {
  await client.deleteMaterialization(
    environmentName,
    packageName,
    materializationId,
    dropTables,
  );
  logSuccess(
    dropTables
      ? `Deleted materialization and dropped its tables: ${materializationId}`
      : `Deleted materialization: ${materializationId}`,
  );
}
