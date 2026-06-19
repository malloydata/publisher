import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logSuccess, logInfo, logOutput, truncate } from "../utils/logger.js";

// Auto-run (default) settles at MANIFEST_FILE_READY (the full build + load
// completed) or a failure. In pauseBetweenPhases mode the publisher pauses at
// BUILD_PLAN_READY for the control plane to drive Round 2, so that is also a
// settled (success) state for a publisher-only client.
const AUTO_SETTLED_STATUSES = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];
const AUTO_SUCCESS_STATUSES = ["MANIFEST_FILE_READY"];
const PAUSE_SETTLED_STATUSES = [
  "BUILD_PLAN_READY",
  "MANIFEST_FILE_READY",
  "FAILED",
  "CANCELLED",
];
const PAUSE_SUCCESS_STATUSES = ["BUILD_PLAN_READY", "MANIFEST_FILE_READY"];

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
    head: ["ID", "Status", "Started", "Completed", "Error"],
  });

  materializations.forEach((m: any) => {
    table.push([
      m.id ?? "",
      m.status ?? "",
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
 * Create a materialization. By default (auto-run) the publisher runs all phases
 * — compile, plan, self-assign table names, build, and auto-load the manifest —
 * settling at MANIFEST_FILE_READY. With `pauseBetweenPhases`, it pauses at
 * BUILD_PLAN_READY for the control plane to drive Round 2. With `wait`, poll
 * until the run settles; without it, return immediately with a status hint.
 */
export async function materialize(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  options: {
    forceRefresh?: boolean;
    pauseBetweenPhases?: boolean;
    wait?: boolean;
    pollIntervalMs?: number;
    timeoutMs?: number;
  } = {},
): Promise<void> {
  const created = await client.createMaterialization(
    environmentName,
    packageName,
    {
      forceRefresh: options.forceRefresh,
      pauseBetweenPhases: options.pauseBetweenPhases,
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
    return;
  }

  const settledStatuses = options.pauseBetweenPhases
    ? PAUSE_SETTLED_STATUSES
    : AUTO_SETTLED_STATUSES;
  const successStatuses = options.pauseBetweenPhases
    ? PAUSE_SUCCESS_STATUSES
    : AUTO_SUCCESS_STATUSES;

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
    logSuccess(`Materialization ${id} ready: ${current.status}`);
    return;
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
