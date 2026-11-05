// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { applyCliArgs } from "./cli_args";

const apply = (args: string[], env: NodeJS.ProcessEnv = {}) => {
   applyCliArgs(args, env);
   return env;
};

describe("applyCliArgs", () => {
   it.each([
      ["--port", "PUBLISHER_PORT"],
      ["--host", "PUBLISHER_HOST"],
      ["--server_root", "SERVER_ROOT"],
      ["--config", "PUBLISHER_CONFIG_PATH"],
      ["--mcp_port", "MCP_PORT"],
      ["--mcp_host", "MCP_HOST"],
      ["--admin_port", "PUBLISHER_ADMIN_PORT"],
      ["--admin_host", "PUBLISHER_ADMIN_HOST"],
      ["--shutdown_drain_duration_seconds", "SHUTDOWN_DRAIN_DURATION_SECONDS"],
      [
         "--shutdown_graceful_close_timeout_seconds",
         "SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS",
      ],
   ])("sets %s into %s", (flag, variable) => {
      expect(apply([flag, "x"], { NODE_ENV: "test" })[variable]).toBe("x");
   });

   it("ignores a value flag with no value", () => {
      expect(apply(["--port"], { NODE_ENV: "test" }).PUBLISHER_PORT).toBe(
         undefined,
      );
   });

   it("maps the boolean flags", () => {
      const env = apply(["--init", "--no-mcp-config"], { NODE_ENV: "test" });
      expect(env.INITIALIZE_STORAGE).toBe("true");
      expect(env.PUBLISHER_NO_MCP_CONFIG).toBe("true");
   });

   it("maps each API switch to its variable, on bare and off with no-, overriding the variable", () => {
      const env = apply(["--no-serving-api-enabled", "--admin-api-enabled"], {
         NODE_ENV: "test",
         PUBLISHER_SERVING_API_ENABLED: "true",
         PUBLISHER_ADMIN_API_ENABLED: "false",
      });
      expect(env.PUBLISHER_SERVING_API_ENABLED).toBe("false");
      expect(env.PUBLISHER_ADMIN_API_ENABLED).toBe("true");
      const off = apply(["--serving-api-enabled", "--no-admin-api-enabled"], {
         NODE_ENV: "test",
      });
      expect(off.PUBLISHER_SERVING_API_ENABLED).toBe("true");
      expect(off.PUBLISHER_ADMIN_API_ENABLED).toBe("false");
   });

   it("appends repeated --watch-env to an existing PUBLISHER_WATCH", () => {
      const env = apply(["--watch-env", "b", "--watch-env", "c"], {
         NODE_ENV: "test",
         PUBLISHER_WATCH: "a",
      });
      expect(env.PUBLISHER_WATCH).toBe("a,b,c");
   });

   it("opts a zero-config run into the bundled sample config", () => {
      expect(apply([], {}).PUBLISHER_USE_BUNDLED_DEFAULT).toBe("true");
   });

   it.each([["--server_root"], ["--config"]])(
      "does not use the bundled config when %s names a location",
      (flag) => {
         expect(apply([flag, "/x"], {}).PUBLISHER_USE_BUNDLED_DEFAULT).toBe(
            undefined,
         );
      },
   );

   it("never uses the bundled config under NODE_ENV=test", () => {
      expect(
         apply([], { NODE_ENV: "test" }).PUBLISHER_USE_BUNDLED_DEFAULT,
      ).toBe(undefined);
   });
});
