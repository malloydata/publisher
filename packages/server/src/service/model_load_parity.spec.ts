// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The two compile paths must agree: production loads every model in the
 * package-load worker (`Package.create`), while `Model.create` compiles in
 * process for `compile_source` and specs. Each fixture is loaded both ways and
 * the served shape, or the load-time refusal, compared.
 */
import { describe, expect, it } from "bun:test";
import {
   createTempModel,
   loadTempPackage,
   usePackageLoadPool,
} from "../test_helpers/temp_package";
import { type Model } from "./model";

const ROWS = `duckdb.sql("select * from (values (1,1,'a'),(2,2,'b')) as t(id, org_id, brand)")`;

const GATED = `##! experimental.givens
#(secure)
given: ROLE :: string
#(secure)
given: GROUPS :: number[]
given: ORG :: number

#(authorize) 'analyst' = $ROLE
source: locked is ${ROWS} extend { measure: c is count() }

#(access_filter) org_id in $GROUPS
source: filtered is ${ROWS} extend {
   measure: c is count()
   view: by_brand is { group_by: brand; aggregate: c }
}

source: derived is locked extend { dimension: b2 is brand }

query: for_org is filtered -> { where: org_id = $ORG; aggregate: c }
`;

const SUGGESTED = `##! experimental.givens
#(secure)
given: GROUPS :: number[]
# control=select suggest { source=filtered dimension=brand }
given: BRAND :: string

#(access_filter) org_id in $GROUPS
source: filtered is ${ROWS} extend { measure: c is count() }
`;

const LIB = `source: kept is ${ROWS}
source: skipped is ${ROWS}
`;

const NOTEBOOK = `>>>markdown
# Notebook
>>>malloy
import { kept } from "lib.malloy"
>>>malloy
source: own is ${ROWS}
>>>malloy
run: own -> { aggregate: n is count() }
`;

const REFUSED: Record<string, string> = {
   "a misplaced #(authorize) on a query": `##! experimental.givens
given: ROLE :: string
source: s is ${ROWS}
#(authorize) 'a' = $ROLE
query: q is s -> { aggregate: n is count() }
`,
   "a legacy string gate": `##! experimental.givens
given: ROLE :: string
#(authorize) "'a' = $ROLE"
source: s is ${ROWS}
`,
   "a gate naming an unknown given": `##! experimental.givens
#(authorize) 'a' = $MISSING
source: s is ${ROWS}
`,
   "a retired route marker": `#(partition) day
source: s is ${ROWS}
`,
};

/** A cell's `newSources` entry is a serialized `SourceInfo`. */
function sourceName(source: unknown): string {
   return (JSON.parse(String(source)) as { name: string }).name;
}

/** A probe compile names its throwaway model by a fresh id. */
function withoutProbeIds(message: string | undefined): string | undefined {
   return message?.replace(/internal:\/\/loadModel\/[0-9a-f-]+/g, "<probe>");
}

/** What a load serves, as both paths can report it. */
async function servedShape(model: Model): Promise<Record<string, unknown>> {
   const cells = model.isNotebook()
      ? (await model.getNotebook()).notebookCells?.map((cell) => ({
           type: cell.type,
           newSources: cell.newSources?.map(sourceName),
        }))
      : undefined;
   return JSON.parse(
      JSON.stringify({
         sources: model.getSources(true),
         queries: model.getQueries(),
         givens: model.getGivens(),
         sourceInfos: model.getSourceInfos(true)?.map((info) => info.name),
         cells,
      }),
   );
}

describe("Model.create and the worker load the same model", () => {
   usePackageLoadPool();

   async function bothPaths(
      files: Record<string, string>,
      entry: string,
   ): Promise<{
      inProcess: Model;
      worker: Model;
      cleanup: () => Promise<void>;
   }> {
      const temp = await createTempModel(files, entry);
      const loaded = await loadTempPackage(files);
      return {
         inProcess: temp.model,
         worker: loaded.pkg.getModel(entry)!,
         cleanup: async () => {
            await temp.cleanup();
            await loaded.cleanup();
         },
      };
   }

   const cases: [string, Record<string, string>, string][] = [
      [
         "gated sources, a derived source and a query",
         { "m.malloy": GATED },
         "m.malloy",
      ],
      [
         "a given whose suggest reads a gated source",
         { "m.malloy": SUGGESTED },
         "m.malloy",
      ],
      [
         "a notebook with a selective import",
         { "lib.malloy": LIB, "nb.malloynb": NOTEBOOK },
         "nb.malloynb",
      ],
   ];
   for (const [name, files, entry] of cases) {
      it(`serves the same shape: ${name}`, async () => {
         const { inProcess, worker, cleanup } = await bothPaths(files, entry);
         try {
            expect(inProcess.getCompilationError()).toBeUndefined();
            expect(await servedShape(inProcess)).toEqual(
               await servedShape(worker),
            );
         } finally {
            await cleanup();
         }
      });
   }

   for (const [name, text] of Object.entries(REFUSED)) {
      it(`refuses with the same message: ${name}`, async () => {
         const files = { "m.malloy": text };
         const temp = await createTempModel(files, "m.malloy");
         const inProcess = withoutProbeIds(
            temp.model.getCompilationError()?.message,
         );
         await temp.cleanup();
         const worker = await loadTempPackage(files).then(
            async (loaded) => {
               await loaded.cleanup();
               return undefined;
            },
            (error: Error) => withoutProbeIds(error.message),
         );
         expect(inProcess).toBeDefined();
         expect(inProcess).toBe(worker);
      });
   }
});
