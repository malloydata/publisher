// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `#(carries)` ceiling is read on the compile that answers. When the
 * served copy answers, that is the served copy's own compile, and live as
 * well whenever live compiles; the live compile of a cross-connection model
 * is its build view, which lacks the joins it leaves out, so a query through
 * one does not compile there and is read on the served copy alone. Anything
 * the served copy's compile cannot show is refused.
 */
import { describe, expect, it } from "bun:test";
import type { ModelMaterializer, QueryMaterializer } from "@malloydata/malloy";
import { BadRequestError } from "../../errors";
import { withAuthorizeModel } from "../../test_helpers/authorize_fixture";
import { unrecordedAudit } from "../access_record";
import type { Model } from "../model";
import type { CompiledQuery, GovernRequest } from "./query_governor";

const MODEL = `##! experimental { access_modifiers }
#(carries) ["none"]
source: s is duckdb.table("child") include { internal: * } extend {
   #(classification) "phi"
   dimension: secret is name
   #(classification) "none"
   dimension: sid is id
   #(classification) "none"
   measure: c is count()
}
`;

const UNCARRIED = `##! experimental { access_modifiers }
source: s is duckdb.table("child") include { internal: * } extend {
   #(classification) "none"
   dimension: sid is id
   #(classification) "none"
   measure: c is count()
}
`;

const CLEAN = "run: s -> { group_by: sid; aggregate: c }";
const LEAK = "run: s -> { group_by: secret }";
/** Does not compile: what a build view answers for a left-out join. */
const BROKEN = "run: s -> { group_by: u.missing }";

function compile(model: Model, text: string): QueryMaterializer {
   const mm = (model as unknown as { modelMaterializer: ModelMaterializer })
      .modelMaterializer;
   return mm.loadRestrictedQuery(text);
}

/** `runnable`, its prepared query stripped of the model it compiled in. */
function withoutModelDef(runnable: QueryMaterializer): QueryMaterializer {
   return Object.create(runnable, {
      getPreparedQuery: {
         value: async () => {
            const prepared = await runnable.getPreparedQuery();
            return Object.create(prepared, {
               _modelDef: { get: () => undefined },
            });
         },
      },
   }) as QueryMaterializer;
}

function govern(
   model: Model,
   compiled: CompiledQuery,
   answeredBy?: "served_copy",
) {
   const request: GovernRequest = {
      givens: {},
      ceiling: {
         audit: unrecordedAudit(),
         source: () => "s",
         ...(answeredBy ? { answeredBy } : {}),
      },
   };
   return model.governance().govern(request, compiled);
}

async function refusal(promise: Promise<unknown>): Promise<string> {
   const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
   );
   expect(err).toBeInstanceOf(BadRequestError);
   return (err as Error).message;
}

describe("the #(carries) ceiling on a served-copy answer", () => {
   it("reads the served copy when live does not compile, and passes a clean query", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const served = compile(model, CLEAN);
         const { runnable } = await govern(
            model,
            { runnable: served, live: compile(model, BROKEN) },
            "served_copy",
         );
         expect(runnable).toBeDefined();
      });
   });

   it("refuses a served copy read past the ceiling when live does not compile", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const message = await refusal(
            govern(
               model,
               { runnable: compile(model, LEAK), live: compile(model, BROKEN) },
               "served_copy",
            ),
         );
         expect(message).toContain("carries [none]; this query reads secret");
      });
   });

   it("still reads live when it compiles", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         // Contrived (the two are the same text in practice): live past the
         // ceiling, the served copy clean. Both are read, so it is refused.
         const message = await refusal(
            govern(
               model,
               { runnable: compile(model, CLEAN), live: compile(model, LEAK) },
               "served_copy",
            ),
         );
         expect(message).toContain("this query reads secret");
      });
   });

   it("reads live, as before, when the served copy did not answer", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         // A routed shape other than the served copy carries no annotations:
         // the ceiling is read on live, which does not compile here.
         const err = await govern(model, {
            runnable: compile(model, CLEAN),
            live: compile(model, BROKEN),
         }).then(
            () => undefined,
            (e: unknown) => e,
         );
         expect(err).toBeDefined();
         expect(err).not.toBeInstanceOf(BadRequestError);
      });
   });

   it("reads live when the served-copy claim names no separate compile", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const leak = compile(model, LEAK);
         const message = await refusal(
            govern(model, { runnable: leak, live: leak }, "served_copy"),
         );
         expect(message).toContain("this query reads secret");
      });
   });

   it("refuses a served compile that carries no model to read", async () => {
      for (const text of [MODEL, UNCARRIED]) {
         await withAuthorizeModel(text, async ({ model }) => {
            const message = await refusal(
               govern(
                  model,
                  {
                     runnable: withoutModelDef(compile(model, CLEAN)),
                     live: compile(model, BROKEN),
                  },
                  "served_copy",
               ),
            );
            expect(message).toContain("cannot be read completely");
         });
      }
   });
});
