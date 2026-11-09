// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, spyOn, test } from "bun:test";
import fs from "fs";
import { Malloy, Model } from "@malloydata/malloy";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { type EntitiesModelFile, extractPackageEntities } from "./extract";
import { FileURLReader } from "./field_helpers";
import {
   ConnectionLookup,
   type FilePointer,
   packageCompilationService,
   UNCURATED_QUERY_ROUTE,
   type Workspace,
   writeWorkspace,
} from "./test_support";

const ORDERS_CSV =
   "order_id,channel_name,region,rep_id,tenant_id\n1,web,us,1,t1\n2,store,eu,2,t2\n";
const REPS_CSV = "rep_id,rep_name\n1,Ann\n2,Bo\n";

// Every kind of fact the extraction reports: a doc, a required filter, an
// access filter on a secure given, two #(index) dimensions,
// a measure, a view, a table join and an inline-extend join.
const RICH_MODEL = `
##! experimental.givens

#(secure)
given: TENANT :: string[]

#(doc) Orders placed in any channel.
#(filter) name=channel dimension=channel_name type=equal required
#(access_filter) tenant_id in $TENANT
source: orders is duckdb.table('orders.csv') extend {
  #(index)
  dimension: channel_upper is upper(channel_name)
  #(index)
  #(doc) The region, labelled.
  dimension: region_label is concat('r-', region)
  measure: order_count is count()
  join_one: reps is duckdb.table('reps.csv') on rep_id = reps.rep_id
  join_one: rep_names is duckdb.table('reps.csv') extend {
    dimension: shout is upper(rep_name)
  } on rep_id = rep_names.rep_id
  view: by_channel is { group_by: channel_name; aggregate: order_count }
}
`;

let ws: Workspace | undefined;
let lookup: ConnectionLookup | undefined;

afterEach(async () => {
   await lookup?.dispose();
   await ws?.cleanup();
   ws = undefined;
   lookup = undefined;
});

async function workspace(models: Record<string, string>): Promise<{
   files: FilePointer[];
   lookup: ConnectionLookup;
}> {
   ws = await writeWorkspace([
      { relpath: "orders.csv", contents: ORDERS_CSV },
      { relpath: "reps.csv", contents: REPS_CSV },
      ...Object.entries(models).map(([relpath, contents]) => ({
         relpath,
         contents,
      })),
   ]);
   lookup = await ConnectionLookup.getInstance([], ws.dir);
   const files = Object.keys(models).map((relpath) => ({
      packageRootPath: ws!.dir,
      absolutePath: path.join(ws!.dir, relpath),
      relativePath: relpath,
   }));
   return { files, lookup };
}

describe("extractPackageEntities", () => {
   test("a model rebuilt from its definition reports exactly what the compiled model does", async () => {
      // A load hands the extraction models rebuilt from the definitions the
      // worker compiled, not the compiled objects. Nothing the extraction
      // reads may differ between the two, the index fingerprints least of all.
      const { files, lookup } = await workspace({
         "orders.malloy": RICH_MODEL,
      });
      const compiled = await packageCompilationService.compilePackageModelFiles(
         { modelFiles: files },
         lookup,
      );
      expect(compiled[0].errorMessage).toBeUndefined();
      expect(compiled[0].compileStatus).toBe("SUCCESS");

      const file = files[0];
      const url = new URL("file://" + file.absolutePath);
      const urlReader = new FileURLReader();
      const model = await Malloy.compile({
         urlReader,
         connections: lookup,
         parse: await Malloy.parse({
            url,
            urlReader,
            options: { importBaseURL: new URL("file://" + ws!.dir + "/") },
         }),
      });
      const rebuilt: EntitiesModelFile = {
         relativePath: file.relativePath,
         absolutePath: file.absolutePath,
         model: new Model(model._modelDef, [], []),
         text: await readFile(file.absolutePath, "utf-8"),
      };
      const fromDefinition = await extractPackageEntities({
         packageRoot: ws!.dir,
         modelFiles: [rebuilt],
         queryRoute: UNCURATED_QUERY_ROUTE,
         connections: lookup,
      });

      expect(fromDefinition.results).toEqual(compiled);
      // The comparison means something only if the fingerprints are there.
      const fingerprinted = fromDefinition.results[0]!.entities.filter(
         (e) => e.dim_index_fingerprint,
      );
      expect(fingerprinted.map((e) => e.name).sort()).toEqual([
         "channel_upper",
         "region_label",
      ]);
      for (const entity of fingerprinted) {
         expect(entity.dim_index_fingerprint!.sql_fingerprint).toMatch(
            /^[0-9a-f]{64}$/,
         );
         expect(
            entity.dim_index_fingerprint!.required_filter_dimensions,
            // `tenant_id in $TENANT` is a list term, not a scalar-equality gate
            // column, so only the required filter partitions the index.
         ).toEqual(["channel_name"]);
      }
   }, 60_000);

   test("reports every annotation an index cannot honor, across files, as findings, and keeps the entities", async () => {
      const { files, lookup } = await workspace({
         "a.malloy": `
#(filter) name=c1 dimension=nope type=equal required
source: a is duckdb.table('orders.csv')
`,
         "b.malloy": `
source: b is duckdb.table('orders.csv') extend {
  #(index)
  measure: n is count()
}
`,
      });
      const { results, findings } =
         await packageCompilationService.extractPackage(
            { modelFiles: files },
            lookup,
         );
      expect(findings.map((f) => [f.modelPath, f.reason])).toEqual([
         ["a.malloy", "required_filter_missing_dimension"],
         ["b.malloy", "not_a_dimension"],
      ]);
      expect(findings[0]!.message).toContain("Fix:");
      expect(results.map((r) => r.compileStatus)).toEqual([
         "SUCCESS",
         "SUCCESS",
      ]);
      // The measure is still an entity; it only gets no fingerprint.
      const n = results[1]!.entities.find((e) => e.name === "n");
      expect(n?.dim_index_fingerprint).toBeUndefined();
   }, 60_000);

   test("a dropped required filter is not a partition key: the source's #(index) dimension is still fingerprinted", async () => {
      const { files, lookup } = await workspace({
         "a.malloy": `
#(filter) name=c1 dimension=nope type=equal required
source: a is duckdb.table('orders.csv') extend {
  #(index)
  dimension: region_label is concat('r-', region)
}
`,
      });
      const { results, findings } =
         await packageCompilationService.extractPackage(
            { modelFiles: files },
            lookup,
         );
      expect(findings.map((f) => f.reason)).toEqual([
         "required_filter_missing_dimension",
      ]);
      const label = results[0]!.entities.find((e) => e.name === "region_label");
      expect(label?.dim_index_fingerprint?.sql_fingerprint).toMatch(
         /^[0-9a-f]{64}$/,
      );
      expect(label?.dim_index_fingerprint?.required_filter_dimensions).toEqual(
         [],
      );
   }, 60_000);

   test("reports every finding within one file, not only the first", async () => {
      const { files, lookup } = await workspace({
         "a.malloy": `
source: a is duckdb.table('orders.csv') extend {
  #(index)
  measure: n is count()
  #(index)
  measure: m is count(region)
  #(index) sharing=all
  dimension: r is region
}
`,
      });
      const { findings } = await packageCompilationService.extractPackage(
         { modelFiles: files },
         lookup,
      );
      expect(findings.map((f) => f.reason)).toEqual([
         "not_a_dimension",
         "not_a_dimension",
         "sharing_not_allowed_on_dimension",
      ]);
   }, 60_000);

   test("a required range, and a child overriding its base's required filter, load as the query path reads them", async () => {
      const { files, lookup } = await workspace({
         "a.malloy": `
#(filter) name=min dimension=rep_id type=greater_than required
#(filter) name=max dimension=rep_id type=less_than required
source: base is duckdb.table('orders.csv') extend {
  #(index)
  dimension: region_label is concat('r-', region)
}
#(filter) name=min dimension=rep_id type=greater_than
source: child is base extend {}
`,
      });
      const { results, findings } =
         await packageCompilationService.extractPackage(
            { modelFiles: files },
            lookup,
         );
      expect(findings).toEqual([]);
      const required = (source: string) =>
         results[0]!.sourceMetadataEntries
            .filter(
               (m) => m.sourceName === source && m.metadataType === "FILTERS",
            )
            .flatMap((m) => m.filters ?? [])
            .filter((f) => f.required)
            .map((f) => f.name);
      expect(required("base").sort()).toEqual(["max", "min"]);
      expect(required("child")).toEqual(["max"]);
      // One partition key for the range, not two.
      const label = results[0]!.entities.find(
         (e) => e.source === "base" && e.name === "region_label",
      );
      expect(label?.dim_index_fingerprint?.required_filter_dimensions).toEqual([
         "rep_id",
      ]);
   }, 60_000);

   test("a parameterized source joining an indexed source does not inherit its own required parameter onto the join", async () => {
      const { files, lookup } = await workspace({
         "a.malloy": `##! experimental.parameters
source: reps is duckdb.table('reps.csv') extend {
  #(index)
  dimension: rep_label is concat('r-', rep_name)
}
source: report(start::number) is duckdb.table('orders.csv') extend {
  where: rep_id > start
  join_one: reps on rep_id = reps.rep_id
}
`,
      });
      const { results, findings } =
         await packageCompilationService.extractPackage(
            { modelFiles: files },
            lookup,
         );
      expect(findings).toEqual([]);
      const label = results[0]!.entities.find(
         (e) => e.source === "reps" && e.name === "rep_label",
      );
      expect(label?.dim_index_fingerprint?.sql_fingerprint).toMatch(
         /^[0-9a-f]{64}$/,
      );
   }, 60_000);

   test("reads no model file from disk when it has each model's compiled text", async () => {
      const { files, lookup } = await workspace({
         "base.malloy": `source: base is duckdb.table('orders.csv') extend {
  dimension: channel_upper is upper(channel_name)
  measure: order_count is count()
}
`,
         "index.malloy": `import "base.malloy"
source: served is base extend {
  dimension: region_label is concat('r-', region)
}
`,
      });
      const read = spyOn(fs, "readFileSync");
      try {
         await packageCompilationService.compilePackageModelFiles(
            { modelFiles: files },
            lookup,
         );
         const modelReads = read.mock.calls
            .map(([file]) => String(file))
            .filter((file) => file.endsWith(".malloy"));
         expect(modelReads).toEqual([]);
      } finally {
         read.mockRestore();
      }
   }, 60_000);

   test("an #(index) fingerprint is the SHA-256 of the compiled scan query", async () => {
      // Pinned: a change here re-keys every dimension index a host derives an
      // id from (a Malloy upgrade that changes the generated SQL, or a change
      // to the fingerprint query). Update it only on purpose.
      const { files, lookup } = await workspace({
         "orders.malloy": RICH_MODEL,
      });
      const [result] = await packageCompilationService.compilePackageModelFiles(
         { modelFiles: files },
         lookup,
      );
      const channelUpper = result.entities.find(
         (e) => e.name === "channel_upper",
      );
      expect(channelUpper?.dim_index_fingerprint?.sql_fingerprint).toBe(
         "7e55c92c0b088768e6094b40595578c2c27607f4ebf32770aeda0b85d267f53d",
      );
   }, 60_000);

   test("reports a file that did not compile without failing the others", async () => {
      const { files, lookup } = await workspace({
         "good.malloy": "source: good is duckdb.table('orders.csv')",
         "bad.malloy":
            "source: bad is duckdb.table('orders.csv') extend { dimension: x is nope }",
      });
      const results = await packageCompilationService.compilePackageModelFiles(
         { modelFiles: files },
         lookup,
      );
      const byPath = new Map(results.map((r) => [r.modelFilePath, r]));
      expect(byPath.get("good.malloy")!.compileStatus).toBe("SUCCESS");
      expect(byPath.get("bad.malloy")!.compileStatus).toBe("ERROR");
      expect(byPath.get("bad.malloy")!.errorMessage).toContain("nope");
   }, 60_000);

   test("asking a curated package's query route never writes the package directory it is handed", async () => {
      const { files, lookup } = await workspace({
         "index.malloy": 'import "orders.malloy"\nexport { orders }\n',
         "orders.malloy": "source: orders is duckdb.table('orders.csv')",
         "bad.malloy":
            "source: bad is duckdb.table('orders.csv') extend { dimension: x is nope }",
         "publisher.json": JSON.stringify({ name: "shared", explores: [] }),
      });
      const snapshot = () =>
         fs
            .readdirSync(ws!.dir, { recursive: true })
            .map(String)
            .sort()
            .map((rel) => {
               const abs = path.join(ws!.dir, rel);
               const stat = fs.statSync(abs);
               // The modification time too: a file rewritten and restored to
               // the same text was still written.
               return stat.isFile()
                  ? `${rel}@${stat.mtimeMs}:${fs.readFileSync(abs, "utf-8")}`
                  : rel;
            });
      const before = snapshot();
      const extraction = await packageCompilationService.extractPackage(
         {
            modelFiles: files.filter((f) => f.relativePath.endsWith(".malloy")),
            manifest: { explores: ["index.malloy"] },
         },
         lookup,
      );
      // The route was a real curated one: the surface's source is queryable.
      const index = extraction.results.find(
         (r) => r.modelFilePath === "index.malloy",
      );
      expect(index?.compileStatus).toBe("SUCCESS");
      expect(snapshot()).toEqual(before);
   }, 60_000);
});
