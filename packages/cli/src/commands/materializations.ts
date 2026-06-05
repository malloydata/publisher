import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logSuccess, logInfo, logOutput, truncate } from "../utils/logger.js";

const TERMINAL_STATUSES = ["SUCCESS", "FAILED", "CANCELLED"];

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
 * Create a materialization and start it. With `wait`, poll until the build
 * reaches a terminal state (mirrors the UI's create-then-start flow). Without
 * `wait`, return immediately with a hint for checking status, since live
 * polling is awkward in a non-interactive CLI.
 */
export async function materialize(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
  options: {
    forceRefresh?: boolean;
    autoLoadManifest?: boolean;
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
      autoLoadManifest: options.autoLoadManifest,
    },
  );
  const id = created.id as string | undefined;
  if (!id) {
    throw new Error("Publisher returned a materialization with no id");
  }
  logSuccess(`Created materialization: ${id}`);

  let current = await client.materializationAction(
    environmentName,
    packageName,
    id,
    "start",
  );
  logInfo(`Started materialization: ${id} (status: ${current.status})`);

  if (!options.wait) {
    logInfo(
      `Run "malloy-pub get materialization ${id} --environment ${environmentName} --package ${packageName}" to check status.`,
    );
    return;
  }

  const pollIntervalMs = options.pollIntervalMs ?? 2000;
  const timeoutMs = options.timeoutMs ?? 120000;
  const deadline = Date.now() + timeoutMs;

  while (!TERMINAL_STATUSES.includes(current.status) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    current = await client.getMaterialization(environmentName, packageName, id);
  }

  // Always print the final record for visibility.
  logOutput(JSON.stringify(current, null, 2));

  if (current.status === "SUCCESS") {
    logSuccess(`Materialization ${id} completed: SUCCESS`);
    return;
  }

  // Non-success: throw so the CLI exits non-zero. The index.ts action's catch
  // turns a thrown error into logError + process.exit(1), which is what a
  // CI/automation caller relying on --wait's exit code needs.
  if (!TERMINAL_STATUSES.includes(current.status)) {
    throw new Error(
      `Timed out waiting for materialization ${id} after ${Math.round(
        timeoutMs / 1000,
      )}s (last status: ${current.status}). It may still be running.`,
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
): Promise<void> {
  await client.deleteMaterialization(
    environmentName,
    packageName,
    materializationId,
  );
  logSuccess(`Deleted materialization: ${materializationId}`);
}
