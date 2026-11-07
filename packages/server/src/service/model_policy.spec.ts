// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import type { Connection } from "@malloydata/malloy";
import {
   createTempModel,
   writeTempPackage,
} from "../test_helpers/temp_package";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { unrecordedAudit } from "./access_record";
import { NotQueryableError } from "../errors";
import {
   Model,
   ModelPolicy,
   PackageModelPolicy,
   QueryPrivileges,
} from "./model";

const MODEL = `source: orders is duckdb.sql("select 1 as id") extend {
   view: v is { aggregate: n is count() }
}
`;

describe("ModelPolicy.standalone", () => {
   it("is uncurated, admits every declared name, and serves live", () => {
      const policy = ModelPolicy.standalone();
      expect(policy.origin).toBe("standalone");
      expect(policy.discoveryCurationEnabled()).toBe(false);
      expect(policy.queryBoundary()).toEqual({
         mode: "all",
         exploresDeclared: false,
         isQueryEntryPoint: true,
      });
      expect(policy.siblingModelDefResolver()).toBeUndefined();
      expect(policy.freshBuildManifestEntries()).toBeUndefined();
      expect(policy.preaggregateEntityIds()).toBeUndefined();
   });

   it("fixed() overrides only the fields it names", () => {
      const policy = ModelPolicy.fixed({ discoveryCuration: true });
      expect(policy.origin).toBe("fixed");
      expect(policy.discoveryCurationEnabled()).toBe(true);
      expect(policy.queryBoundary().mode).toBe("all");
   });
});

describe("PackageModelPolicy", () => {
   it("refuses everything until the package decides a boundary", () => {
      const view = new PackageModelPolicy().forModel("m.malloy");
      expect(view.origin).toBe("package");
      expect(view.queryBoundary()).toMatchObject({
         mode: "declared",
         exploresDeclared: true,
         isQueryEntryPoint: false,
      });
      expect(view.queryBoundary().packageCuratedSources?.size).toBe(0);
   });

   it("is a live view: a decision reaches a model built before it", () => {
      const policy = new PackageModelPolicy();
      const view = policy.forModel("m.malloy");
      const other = policy.forModel("other.malloy");
      const decided = {
         mode: "all" as const,
         exploresDeclared: false,
         isQueryEntryPoint: true,
      };
      policy.setQueryBoundaries(new Map([["m.malloy", decided]]));
      policy.setDiscoveryCuration(true);
      expect(view.queryBoundary()).toBe(decided);
      expect(view.discoveryCurationEnabled()).toBe(true);
      // A path the package did not decide stays refused.
      expect(other.queryBoundary().isQueryEntryPoint).toBe(false);
      expect(view.freshBuildManifestEntries()).toBeUndefined();
      policy.setFreshnessResolvers({
         manifest: () => ({}),
         preaggregateEntityIds: () => new Set(["x"]),
      });
      expect(view.freshBuildManifestEntries()).toEqual({});
      expect(view.preaggregateEntityIds()).toEqual(new Set(["x"]));
   });
});

describe("Model built with a policy", () => {
   let standalone: Awaited<ReturnType<typeof createTempModel>>;
   let undecided: Model;
   let cleanupDir: () => void;
   let duckdb: DuckDBConnection;

   beforeAll(async () => {
      standalone = await createTempModel({ "m.malloy": MODEL }, "m.malloy");
      duckdb = standalone.duckdb;
      const written = writeTempPackage({ "m.malloy": MODEL });
      cleanupDir = written.cleanup;
      undecided = await Model.create(
         "pkg",
         written.dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
         { policy: new PackageModelPolicy().forModel("m.malloy") },
      );
   });

   afterAll(async () => {
      cleanupDir();
      await standalone.cleanup();
   });

   it("standalone runs a declared view", async () => {
      const { compactResult } = await standalone.model.runQuery(
         unrecordedAudit(),
         { sourceName: "orders", queryName: "v", responseShape: "compact" },
      );
      expect(compactResult).toEqual([{ n: 1 }]);
   });

   it("an undecided package policy refuses the same query", async () => {
      await expect(
         undecided.runQuery(unrecordedAudit(), {
            sourceName: "orders",
            queryName: "v",
         }),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });
});

describe("QueryPrivileges", () => {
   it("none grants nothing and cannot be widened", () => {
      expect(QueryPrivileges.none).toEqual({
         bypassFilters: false,
         bypassAuthorize: false,
         includeHiddenFilesAndSources: false,
      });
      expect(Object.isFrozen(QueryPrivileges.none)).toBe(true);
   });

   it("of() grants only what is named true", () => {
      expect(
         QueryPrivileges.of({
            bypassAuthorize: true,
            bypassFilters: undefined,
         }),
      ).toEqual({
         bypassFilters: false,
         bypassAuthorize: true,
         includeHiddenFilesAndSources: false,
      });
   });

   it("getQueryResults maps its positions onto runQuery", async () => {
      const model = Model.fromCompilationError(
         "pkg",
         "m.malloy",
         "model",
         new Error("unused"),
         { policy: ModelPolicy.standalone() },
      );
      const run = spyOn(model, "runQuery").mockResolvedValue(
         undefined as never,
      );
      const audit = unrecordedAudit();
      const signal = new AbortController().signal;
      await model.getQueryResults(
         audit,
         "s",
         "q",
         "run: s -> v",
         { f: "1" },
         true,
         { G: "g" },
         signal,
         undefined,
         "compact",
         false,
         true,
      );
      expect(run).toHaveBeenCalledWith(
         audit,
         {
            sourceName: "s",
            queryName: "q",
            query: "run: s -> v",
            filterParams: { f: "1" },
            givens: { G: "g" },
            abortSignal: signal,
            queryMetadataInput: undefined,
            responseShape: "compact",
         },
         {
            bypassFilters: true,
            bypassAuthorize: false,
            includeHiddenFilesAndSources: true,
         },
      );
   });
});
