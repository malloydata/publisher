import { PublisherClient } from "../api/client.js";
import Table from "cli-table3";
import { logInfo, logOutput } from "../utils/logger.js";

export async function listDatabases(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const databases = await client.listDatabases(environmentName, packageName);

  if (databases.length === 0) {
    logInfo(`No databases in package: ${packageName}`);
    return;
  }

  // A file the server could not probe is listed carrying `error` instead of
  // `info`. Only widen the table when one actually failed, so the common
  // all-healthy output is unchanged.
  const hasErrors = databases.some((d: any) => d.error);

  const table = new Table({
    head: hasErrors ? ["Path", "Type", "Error"] : ["Path", "Type"],
  });

  databases.forEach((d: any) => {
    const row = [d.path ?? "", d.type ?? ""];
    if (hasErrors) {
      row.push(d.error ?? "");
    }
    table.push(row);
  });

  logOutput(table.toString());
}
