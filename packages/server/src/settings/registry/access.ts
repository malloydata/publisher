// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { setting } from "../setting";
import { boolean, integer, oneOf, string } from "../types";

/** The configuration design's `allow`, `authorize`, `logging`, `shutdown` table. */
export const access = [
   setting({
      key: "allow.proxyConnections",
      type: boolean(),
      default: false,
      description:
         "Allow publisher connections, which make this server call a configured connectionUri.",
      aliases: {
         env: ["PUBLISHER_ALLOW_PROXY_CONNECTIONS"],
         today: { reads: "exactly-true" },
      },
   }),
   setting({
      key: "allow.duckdbSetupSql",
      type: boolean(),
      default: false,
      description: "Allow setupSQL on environment-authored DuckDB connections.",
      aliases: { env: ["PUBLISHER_ALLOW_DUCKDB_SETUP_SQL"] },
   }),
   setting({
      key: "allow.unverifiedSshHostKey",
      type: boolean(),
      default: false,
      description: "Accept a bastion host key no connection pins.",
      aliases: { env: ["PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY"] },
   }),
   setting({
      key: "allow.s3CredentialChain",
      type: oneOf(["any", "destinations-only", "off"]),
      // The one permission on by default: deployments that read S3 through
      // the host's own role depend on it.
      default: "any",
      description:
         "Where an s3 connection may use the host's credentials: any, destinations-only or off.",
      aliases: { env: ["S3_CREDENTIAL_CHAIN_POLICY"] },
   }),
   setting({
      key: "authorize.bypassSecret",
      type: string(),
      secret: true,
      description:
         "Secret a request presents in x-publisher-bypass-authorize to skip #(authorize) gates.",
      aliases: { env: ["PUBLISHER_BYPASS_AUTHORIZE_SECRET"] },
   }),
   setting({
      key: "logging.level",
      type: oneOf(["error", "warn", "info", "verbose", "debug", "silly"]),
      default: "debug",
      description: "Least severe level logged.",
      aliases: { env: ["LOG_LEVEL"], today: { invalid: "warn" } },
   }),
   setting({
      key: "logging.responseBodiesEnabled",
      type: boolean(),
      default: true,
      description: "Log response bodies.",
      aliases: {
         env: ["DISABLE_RESPONSE_LOGGING"],
         negated: true,
         today: { reads: "true-or-1" },
      },
   }),
   setting({
      key: "shutdown.drainDurationSeconds",
      type: integer({ min: 0 }),
      default: 0,
      description:
         "Seconds to report draining before closing the listeners on shutdown.",
      aliases: {
         env: ["SHUTDOWN_DRAIN_DURATION_SECONDS"],
         flags: ["--shutdown_drain_duration_seconds"],
         today: { reads: "unchecked-Number" },
      },
   }),
   setting({
      key: "shutdown.gracefulCloseTimeoutSeconds",
      type: integer({ min: 0 }),
      default: 0,
      description:
         "Seconds to wait after closing the listeners before exiting.",
      aliases: {
         env: ["SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS"],
         flags: ["--shutdown_graceful_close_timeout_seconds"],
         today: { reads: "unchecked-Number" },
      },
   }),
];
