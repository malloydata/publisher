// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Variables Publisher reads but does not own. They keep their names and stay
 * outside the registry, as the configuration design says. They are read live,
 * not from the settings loaded at startup, because the libraries that own them
 * (Node, the Google and Postgres clients, OpenTelemetry) read `process.env`
 * themselves, and Publisher has to see what they see.
 */
export type ExternalVariable =
   | "NODE_ENV"
   | "NODE_EXTRA_CA_CERTS"
   | "OTEL_EXPORTER_OTLP_ENDPOINT"
   | "GOOGLE_APPLICATION_CREDENTIALS"
   | "PGSSLMODE"
   | "PGSERVICE";

export function externalEnv(name: ExternalVariable): string | undefined {
   return process.env[name];
}

/**
 * A variable the config file names as `${VAR}`, which is content (a
 * credential, a bucket) rather than a setting, so any name may be asked for.
 */
export function referencedVariable(name: string): string | undefined {
   return process.env[name];
}
