// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, test } from "bun:test";
import { Malloy, Model } from "@malloydata/malloy";
import { readFile } from "node:fs/promises";
import path from "node:path";
import {
   type EntitiesModelFile,
   EntitiesValidationError,
   extractPackageEntities,
} from "./extract";
import { FileURLReader } from "./malloy_indexing_helpers";
import {
   ConnectionLookup,
   type FilePointer,
   packageCompilationService,
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
         connections: lookup,
      });

      expect(fromDefinition).toEqual(compiled);
      // The comparison means something only if the fingerprints are there.
      const fingerprinted = fromDefinition[0].entities.filter(
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

   test("refuses every file's invalid annotation at once", async () => {
      const { files, lookup } = await workspace({
         "a.malloy": `
#(filter) name=c1 dimension=channel_name type=equal required
#(filter) name=c2 dimension=channel_name type=equal required
source: a is duckdb.table('orders.csv')
`,
         "b.malloy": `
source: b is duckdb.table('orders.csv') extend {
  #(index)
  measure: n is count()
}
`,
      });
      const error = await packageCompilationService
         .compilePackageModelFiles({ modelFiles: files }, lookup)
         .then(
            () => undefined,
            (e: unknown) => e,
         );
      expect(error).toBeInstanceOf(EntitiesValidationError);
      const messages = (error as EntitiesValidationError).messages;
      expect(messages).toHaveLength(2);
      expect(messages[0]).toContain("multiple required filters");
      expect(messages[1]).toContain("is not a dimension");
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
});
