// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PublisherClient } from "../api/client.js";
import { logOutput } from "../utils/logger.js";

/**
 * `malloy-pub get governance --environment <env> --package <pkg>`: the
 * package's governance export, printed as JSON exactly as the server answers
 * it, so a pipeline can write it to a file, check it in, and diff it.
 */
export async function getGovernance(
  client: PublisherClient,
  environmentName: string,
  packageName: string,
): Promise<void> {
  const governance = await client.getGovernance(environmentName, packageName);
  logOutput(JSON.stringify(governance, null, 2));
}
