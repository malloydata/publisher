// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a setting's value came from. `name` is what the operator wrote: the
 * flag (`--mcp_port`), the variable (`MCP_PORT`) or the config file's path, so
 * a message points at the place to fix. A default has no name to point at.
 */
export type SettingSource =
   | { kind: "flag"; name: string }
   | { kind: "env"; name: string }
   | { kind: "file"; name: string }
   | { kind: "default" };

/** How a source reads in a message: `MCP_PORT`, `--mcp_port`, the file's path. */
export function describeSource(source: SettingSource): string {
   return source.kind === "default" ? "its default" : source.name;
}

/**
 * One value that did not parse as its setting's type, in the configuration
 * design's format:
 *
 *    mcp.port from MCP_PORT: expected an integer from 0 to 65535, got "40a0"
 *
 * A secret's value is never echoed: it reads `got (set)`.
 */
export class SettingError extends Error {
   constructor(
      readonly key: string,
      readonly source: SettingSource,
      readonly expected: string,
      readonly got: unknown,
      readonly secret = false,
   ) {
      super(
         `${key} from ${describeSource(source)}: expected ${expected}, got ${
            secret ? "(set)" : formatGot(got)
         }`,
      );
      this.name = "SettingError";
   }
}

/** A string as the operator wrote it, quoted; a file value as JSON. */
function formatGot(got: unknown): string {
   if (got === undefined) return "nothing";
   return JSON.stringify(got) ?? String(got);
}

/** Every setting that failed at one startup, reported together, one per line. */
export class SettingsError extends Error {
   constructor(readonly errors: readonly Error[]) {
      super(errors.map((error) => error.message).join("\n"));
      this.name = "SettingsError";
   }
}
