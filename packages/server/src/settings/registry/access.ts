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
      notes: "Set to `true` to allow `publisher`-type proxy connections (Publisher-to-Publisher). See [connections.md](connections.md).",
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
      notes: "Set to `true` to let environment-level `duckdb` connections carry `setupSQL`. Unset, a connection with `setupSQL` is refused at config load, on create and update, and by the connection test. `setupSQL` runs arbitrary DuckDB statements on the server, including writing host files and loading extensions, so enable it only where the people who author connections are trusted with the host. Refused under `EXTENSION_FETCH_POLICY=local-only` either way. Same boolean spellings as `PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY`; any other value fails config load.",
      aliases: { env: ["PUBLISHER_ALLOW_DUCKDB_SETUP_SQL"] },
   }),
   setting({
      key: "allow.unverifiedSshHostKey",
      type: boolean(),
      default: false,
      description: "Accept a bastion host key no connection pins.",
      notes: "Set to `true` to let an SSH-proxy connection whose `ssh.hostKey` is not pinned connect anyway, logging a warning each time. Unset, such a tunnel is refused, because an unverified host key means a MITM on the publisher-to-bastion hop cannot be detected. Accepts the same spellings as every other boolean here -- `true`, `1`, `yes`, `on`, case-insensitive and trimmed. Any other value fails config load rather than opting in silently. See [connections.md](connections.md).",
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
      notes: "Where an S3 connection or attachment may use `provider: credential_chain`, which makes DuckDB resolve the **host's own** credentials (an EKS pod's IRSA role, an EC2 instance profile) instead of a configured key pair. `any`: allowed everywhere. `destinations-only`: allowed on a [storage destination](connections.md#storage-destinations) and on an admin-API catalog, and refused on an environment connection or attachment, which an author may supply. A destination is declared by whoever writes the environment (its config file, or `storageDestinations` on `POST`/`PATCH /environments`), so under this policy keep those writes to operators. `off`: refused everywhere. A refused config fails at load, on create and update, and by the connection test, with an error naming the policy. Any other value fails startup.",
      aliases: { env: ["S3_CREDENTIAL_CHAIN_POLICY"] },
   }),
   setting({
      key: "authorize.bypassSecret",
      type: string(),
      secret: true,
      description:
         "Secret a request presents in x-publisher-bypass-authorize to skip #(authorize) gates.",
      notes: "A secret that, sent in the `x-publisher-bypass-authorize` header, skips `#(authorize)` gates for that request. Unset, the header does nothing. See [authorize-bypass-deployment.md](authorize-bypass-deployment.md).",
      aliases: { env: ["PUBLISHER_BYPASS_AUTHORIZE_SECRET"] },
   }),
   setting({
      key: "logging.level",
      type: oneOf(["error", "warn", "info", "verbose", "debug", "silly"]),
      default: "debug",
      description: "Least severe level logged.",
      notes: "One of `error`, `warn`, `info`, `verbose`, `debug`, `silly`.",
      aliases: { env: ["LOG_LEVEL"], today: { invalid: "warn" } },
   }),
   setting({
      key: "logging.responseBodiesEnabled",
      type: boolean(),
      default: true,
      description: "Log response bodies.",
      notes: "Set to `true` or `1` to suppress response-body logging.",
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
      notes: "After SIGTERM, how long to keep serving in-flight and new requests (readiness reports not-ready immediately) before the server starts refusing new traffic.",
      aliases: {
         env: ["SHUTDOWN_DRAIN_DURATION_SECONDS"],
         flags: ["--shutdown_drain_duration_seconds"],
      },
   }),
   setting({
      key: "shutdown.gracefulCloseTimeoutSeconds",
      type: integer({ min: 0 }),
      default: 0,
      description:
         "Seconds to wait after closing the listeners before exiting.",
      notes: "Time to wait for in-flight requests to drain before forcing close.",
      aliases: {
         env: ["SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS"],
         flags: ["--shutdown_graceful_close_timeout_seconds"],
      },
   }),
];
