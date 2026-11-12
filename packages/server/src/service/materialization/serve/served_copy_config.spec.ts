// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { MalloyConfig } from "@malloydata/malloy";
import {
   isServedCopyAlias,
   servedCopyAlias,
   servedCopyConfig,
} from "./served_copy_config";
import { classifyServedCopyFailure } from "./served_copy_serve";

function fakeConnection(name: string) {
   return { name } as never;
}

function destinations(): MalloyConfig {
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(() => ({
      lookupConnection: async (name?: string) => {
         if (name === "lake") return fakeConnection("lake");
         throw new Error(`no destination ${name}`);
      },
   }));
   return config;
}

const pkg = new Map([["duckdb", fakeConnection("duckdb")]]);

describe("servedCopyConfig", () => {
   it("reaches a destination only through its alias", async () => {
      const alias = servedCopyAlias("lake");
      expect(isServedCopyAlias(alias)).toBe(true);
      expect(alias).not.toContain("lake");
      const config = servedCopyConfig(pkg, destinations(), new Set(["lake"]));
      expect(
         (
            (await config.connections.lookupConnection(alias)) as {
               name: string;
            }
         ).name,
      ).toBe("lake");
   });

   it("never resolves the destination's real name, which an author could write", async () => {
      const config = servedCopyConfig(pkg, destinations(), new Set(["lake"]));
      await expect(
         config.connections.lookupConnection("lake"),
      ).rejects.toThrow();
   });

   it("resolves the package's own connections, including the default", async () => {
      const config = servedCopyConfig(pkg, destinations(), new Set(["lake"]));
      expect(
         (
            (await config.connections.lookupConnection("duckdb")) as {
               name: string;
            }
         ).name,
      ).toBe("duckdb");
   });

   it("refuses an alias for a destination this served copy does not read", async () => {
      const other = servedCopyAlias("other_lake");
      const config = servedCopyConfig(pkg, destinations(), new Set(["lake"]));
      await expect(config.connections.lookupConnection(other)).rejects.toThrow(
         /not available/,
      );
   });

   it("keeps one alias per destination for the life of the process", () => {
      expect(servedCopyAlias("lake")).toBe(servedCopyAlias("lake"));
      expect(servedCopyAlias("lake")).not.toBe(servedCopyAlias("other_lake"));
   });
});

describe("classifyServedCopyFailure", () => {
   it("reads a refused cross-connection join as waiting for the other side", () => {
      expect(
         classifyServedCopyFailure(
            "Cannot join 'customers', which is on connection 'wh_b', into a source on connection 'wh_a'",
         ),
      ).toBe("cross_connection_unbuilt");
      expect(classifyServedCopyFailure("'region' is not defined")).toBe(
         "compile_failed",
      );
   });
});
