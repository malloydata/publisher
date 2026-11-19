// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization: the exact package warning and refusal record each
 * mechanism's refusal of a valid declaration produces, byte for byte, for a
 * build that reads a given (a rollup, a persisted view, a persisted query)
 * and for a `#(materialize)` declaration refused alone. Drives
 * `Package.create` on the worker path with PUBLISHER_MATERIALIZE on.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../../package_load/package_load_pool";
import { installSettings } from "../../../test_helpers/settings_env";
import { writeTempPackage } from "../../../test_helpers/temp_package";
import { Package } from "../../package";
import { materializeRefusalFinding } from "./materialize_plan";
import {
   persistedQueryRefusalFinding,
   persistedViewRefusalFinding,
} from "./persisted_view";
import { rollupRefusalFinding } from "./rollup_plan";

const ROWS = `duckdb.sql("select * from (values (1,'east','A',10.0),(2,'west','B',20.0)) as t(id, region, category, amount)")`;

const MODEL = `##! experimental.givens
given: REGION :: string is 'east'

source: baked is ${ROWS} -> { where: region = $REGION; select: * } extend {
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}

source: viewed is ${ROWS} extend {
  #@ persist
  view: by_region is { where: region = $REGION; group_by: category; aggregate: n is count() }
}

#@ persist
query: per_caller is viewed -> { where: region = $REGION; group_by: category }

#(materialize) refresh="incremental" watermark="id"
source: no_delta is ${ROWS}

#(materialize)
source: m_baked is ${ROWS} -> { where: region = $REGION; select: * }
`;

describe("refusal findings, byte for byte", () => {
   let tempDir: string;
   let pool: PackageLoadPool;
   let duckdb: DuckDBConnection;
   let restore: () => void;
   let pkg: Package;

   beforeAll(async () => {
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      tempDir = writeTempPackage(
         {},
         {
            prefix: "publisher-refusal-findings-",
            manifest: { name: "pkg", description: "refusal findings" },
         },
      ).dir;
      fs.writeFileSync(path.join(tempDir, "m.malloy"), MODEL);
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      pkg = await Package.create("env", "pkg", tempDir, config);
   });

   afterAll(async () => {
      restore();
      await duckdb.close();
      await __setPackageLoadPoolForTests(null);
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   /** `value` as JSON with the package directory written `<DIR>`. */
   function normalized(value: unknown): unknown {
      // A source's URL spells the directory as a file URL (`file:///D:/…` on
      // Windows, where the path itself has backslashes and a drive letter),
      // so replace that form first, then the plain path as JSON escapes it.
      const dirUrl = pathToFileURL(tempDir).href;
      const dirJson = JSON.stringify(tempDir).slice(1, -1);
      return JSON.parse(
         JSON.stringify(value)
            .split(dirUrl)
            .join("file://<DIR>")
            .split(dirJson)
            .join("<DIR>"),
      );
   }

   const MAT_GIVEN =
      "Source 'm_baked' cannot be materialized (database '#@ persist'): its persisted query references a given. A given the persisted query is built with is substituted at BUILD time, so the table would hold the default's rows and serve them to every caller, ignoring the value each one supplies. Keep the given OUT of the persisted query and apply it when the source is read — a `where:` in the source's extend block, or a dimension, measure or join declared there — where it binds per caller over the materialized rows. Otherwise stop persisting this source.";
   const VIEW_GIVEN =
      '#@ persist on view "by_region" of source "viewed" is not built, so the view is served live: its result reads a given, so it differs per caller and a table holds one caller\'s result.';
   const QUERY_GIVEN =
      '#@ persist on query "per_caller" is not built, so the query is answered live: its result reads a given, so it differs per caller and a table holds one caller\'s result. To store its result, persist a source over it instead (`#@ persist` on `source: per_caller_result is per_caller`) and query that source.';
   const ROLLUP_GIVEN =
      '#(rollup) rollup of source "baked" at (category) is not built, so its measures are served live: its rows read a given, and a table holds one value\'s rows.';
   const MAT_INCREMENTAL =
      '#(materialize) on source "no_delta" is not built, so the source is served live: it declares refresh="incremental", which is not supported on dialect "duckdb". Incremental refresh reads a bounded watermark range out of the source and applies it transactionally, so it is enabled only where the publisher has proven both: postgres, snowflake, standardsql (BigQuery). Use refresh="full" to build it; the supported set widens in a later release.';

   it("warns once per refusal, in this order, with these exact words", () => {
      expect(normalized(pkg.getPackageMetadata().warnings)).toEqual([
         {
            message: MAT_GIVEN,
            code: "materialize-not-built",
            reason: "reads_given",
            severity: "warn",
            model: "m.malloy",
            subject: "m_baked",
         },
         {
            model: "m.malloy",
            subject: "viewed -> by_region",
            message: VIEW_GIVEN,
            code: "persist-view-not-built",
            reason: "reads_given",
            severity: "warn",
         },
         {
            model: "m.malloy",
            subject: "per_caller",
            message: QUERY_GIVEN,
            code: "persist-query-not-built",
            reason: "reads_given",
            severity: "warn",
         },
         {
            model: "m.malloy",
            subject: "baked",
            message: ROLLUP_GIVEN,
            code: "rollup-not-built",
            reason: "reads_given",
            severity: "warn",
         },
         {
            model: "m.malloy",
            subject: "baked",
            message: ROLLUP_GIVEN,
            code: "preaggregate-not-built",
            reason: "reads_given",
            severity: "warn",
         },
         {
            model: "m.malloy",
            subject: "no_delta",
            message: MAT_INCREMENTAL,
            code: "materialize-not-built",
            reason: "incremental_unsupported",
            severity: "warn",
         },
      ]);
   });

   it("records each refusal under its own key", () => {
      const record = (
         name: string,
         file: string,
         reason: string,
         message: string,
         origin: string,
      ) => ({
         [`${name}@file://<DIR>/${file}`]: {
            name,
            sourceID: `${name}@file://<DIR>/${file}`,
            modelPath: "m.malloy",
            tier: "database",
            reason,
            message,
            origin,
         },
      });
      expect(normalized(pkg.getBuildPlan()?.refusedSources)).toEqual({
         ...record(
            "m_baked",
            "m.malloy",
            "reads_given",
            MAT_GIVEN,
            "materialize",
         ),
         ...record(
            "viewed__by_region__view",
            "m.malloy.persist-view.malloy",
            "reads_given",
            VIEW_GIVEN,
            "view",
         ),
         ...record(
            "per_caller__query",
            "m.malloy.persist-view.malloy",
            "reads_given",
            QUERY_GIVEN,
            "query",
         ),
         ...record(
            "baked__rollup__category__3cda4adc",
            "m.malloy.preaggregate.malloy",
            "reads_given",
            ROLLUP_GIVEN,
            "rollup",
         ),
         ...record(
            "no_delta",
            "m.malloy",
            "incremental_unsupported",
            MAT_INCREMENTAL,
            "materialize",
         ),
      });
   });
});

describe("each mechanism's refusal projector", () => {
   const URL = "file:///pkg/m.malloy";
   const COMPANION = `${URL}.persist-view.malloy`;

   it("#(materialize): invalid, invalid unless relaxed, and not built", () => {
      expect(
         materializeRefusalFinding(
            {
               sourceName: "s",
               sourceID: `s@${URL}`,
               reason: "bad key",
               outcome: { malformed: "materialize-invalid" },
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "materialize",
         tier: "malformed",
         code: "materialize-invalid",
         message: '#(materialize) on source "s" is invalid: bad key.',
         subject: "s",
         model: "m.malloy",
      });
      expect(
         materializeRefusalFinding(
            {
               sourceName: "s",
               sourceID: `s@${URL}`,
               reason: "collides",
               outcome: {
                  malformed: "materialization-table-collision",
                  whenRelaxed: {
                     reason: "table_name_collision",
                     tier: "storage",
                  },
               },
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "materialize",
         tier: "malformed",
         code: "materialization-table-collision",
         message: '#(materialize) on source "s" is invalid: collides.',
         subject: "s",
         model: "m.malloy",
         refused: {
            sourceID: `s@${URL}`,
            name: "s",
            tier: "storage",
            reason: "table_name_collision",
         },
      });
      // No sourceID: no refusal record.
      expect(
         materializeRefusalFinding(
            {
               sourceName: "s",
               reason: "no id",
               outcome: {
                  refused: { reason: "hidden_field", tier: "database" },
               },
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "materialize",
         tier: "unservable",
         code: "materialize-not-built",
         message:
            '#(materialize) on source "s" is not built, so the source is served live: no id.',
         subject: "s",
         model: "m.malloy",
      });
   });

   it("a persisted view: invalid, and not built in its destination", () => {
      const view = { sourceID: `s@${URL}`, sourceName: "s", viewName: "v" };
      expect(
         persistedViewRefusalFinding(
            {
               ...view,
               reason: "unreadable",
               outcome: { malformed: "persist-invalid" },
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "@",
         tier: "malformed",
         code: "persist-invalid",
         message:
            '#@ persist on view "v" of source "s" is invalid: unreadable.',
         subject: "s -> v",
         model: "m.malloy",
      });
      expect(
         persistedViewRefusalFinding(
            {
               ...view,
               reason: "off",
               outcome: { refused: "materialization_off" },
               storage: "lake",
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "@",
         tier: "unservable",
         code: "persist-view-not-built",
         message:
            '#@ persist on view "v" of source "s" is not built, so the view is served live: off.',
         subject: "s -> v",
         model: "m.malloy",
         refused: {
            sourceID: `s__v__view@${COMPANION}`,
            name: "s__v__view",
            tier: "storage",
            reason: "materialization_off",
         },
      });
   });

   it("a persisted query: not built", () => {
      expect(
         persistedQueryRefusalFinding(
            {
               queryName: "q",
               reason: "multi",
               refusal: "multi_line_annotation",
               declaringURL: URL,
               storage: "lake",
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "@",
         tier: "unservable",
         code: "persist-query-not-built",
         message:
            '#@ persist on query "q" is not built, so the query is answered live: multi. To store its result, persist a source over it instead (`#@ persist` on `source: q_result is q`) and query that source.',
         subject: "q",
         model: "m.malloy",
         refused: {
            sourceID: `q__query@${COMPANION}`,
            name: "q__query",
            tier: "storage",
            reason: "multi_line_annotation",
         },
      });
   });

   it("a rollup: its own message, under the measure's path", () => {
      expect(
         rollupRefusalFinding(
            {
               tier: "malformed",
               code: "rollup-invalid",
               message: "m1",
               sourceName: "s",
               measure: "t",
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "rollup",
         tier: "malformed",
         code: "rollup-invalid",
         message: "m1",
         subject: "s.t",
         model: "m.malloy",
      });
      expect(
         rollupRefusalFinding(
            {
               tier: "deprecated",
               code: "deprecated-preaggregate-annotation",
               message: "m2",
               sourceName: "s",
            },
            "m.malloy",
         ),
      ).toEqual({
         route: "rollup",
         tier: "deprecated",
         code: "deprecated-preaggregate-annotation",
         message: "m2",
         subject: "s",
         model: "m.malloy",
      });
   });
});
