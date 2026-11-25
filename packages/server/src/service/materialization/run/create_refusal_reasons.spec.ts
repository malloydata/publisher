// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every deterministic refusal of a materialization create's body or build
 * instructions answers 400 with `reason: BUILD_INSTRUCTIONS_INVALID`, the
 * message still naming the case, so a host branches on the reason, not on
 * prose.
 */

import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import type { components } from "../../../api";
import { MaterializationController } from "../../../controller/materialization.controller";
import { internalErrorToHttpError } from "../../../errors";
import type { MaterializationService } from "./materialization_service";
import { validateInstructions } from "./run_instructions";

type BuildPlan = components["schemas"]["BuildPlan"];

/** The answer a thrown error would get. */
function answerOf(error: unknown) {
   return internalErrorToHttpError(error as Error, { log: false });
}

async function refusalOfBody(body: Record<string, unknown>) {
   const service = {
      createMaterialization: sinon.stub().resolves({ id: "m1" }),
   } as unknown as MaterializationService;
   return new MaterializationController(service)
      .createMaterialization("env", "pkg", body)
      .then(
         () => undefined,
         (error: unknown) => error,
      );
}

const INSTRUCTION = {
   sourceEntityId: "e1",
   physicalTableName: "t1",
   realization: "COPY",
};

describe("a malformed create body", () => {
   const cases: [string, Record<string, unknown>][] = [
      ["forceRefresh not a boolean", { forceRefresh: "yes" }],
      ["reseed not a boolean", { reseed: 1 }],
      ["sourceNames not strings", { sourceNames: [1] }],
      ["runContext not an object", { runContext: "x" }],
      ["buildInstructions not an object", { buildInstructions: "x" }],
      [
         "an instruction that is not an object",
         { buildInstructions: { sources: ["x"] } },
      ],
      [
         "an unusable physicalTableName",
         {
            buildInstructions: {
               sources: [{ ...INSTRUCTION, physicalTableName: "a..b" }],
            },
         },
      ],
      [
         "a ledger that is not an array",
         { buildInstructions: { sources: [INSTRUCTION], ledger: { a: 1 } } },
      ],
   ];
   for (const [label, body] of cases) {
      it(`answers ${label} 400 BUILD_INSTRUCTIONS_INVALID`, async () => {
         const refused = await refusalOfBody(body);
         expect(refused).toBeDefined();
         expect(answerOf(refused).json).toMatchObject({
            code: 400,
            reason: "BUILD_INSTRUCTIONS_INVALID",
         });
      });
   }
});

describe("instructions the build plan refuses", () => {
   const plan = {
      graphs: [],
      sources: {
         "orders@file:///m.malloy": {
            name: "orders",
            sourceID: "orders@file:///m.malloy",
            sourceEntityId: "e1",
            connectionName: "duckdb",
            refresh: null,
         },
         "daily@file:///m.malloy": {
            name: "daily",
            sourceID: "daily@file:///m.malloy",
            sourceEntityId: "e2",
            connectionName: "duckdb",
            refresh: "incremental",
         },
      },
   } as unknown as BuildPlan;
   const ledger = (over: Record<string, unknown>) => ({
      sourceEntityId: "e2",
      connectionName: "duckdb",
      physicalTableName: "daily_t",
      ...over,
   });
   const daily = {
      sourceEntityId: "e2",
      physicalTableName: "daily_t",
      realization: "COPY",
   };
   const cases: [
      string,
      BuildPlan | null,
      Record<string, unknown>[],
      Record<string, unknown>[] | undefined,
      RegExp,
   ][] = [
      ["no persist sources", null, [INSTRUCTION], undefined, /no persist/],
      [
         "an unknown sourceEntityId",
         plan,
         [{ ...INSTRUCTION, sourceEntityId: "nope" }],
         undefined,
         /unknown sourceEntityId/,
      ],
      [
         "realization=SNAPSHOT",
         plan,
         [{ ...INSTRUCTION, realization: "SNAPSHOT" }],
         undefined,
         /SNAPSHOT/,
      ],
      [
         "a duplicated ledger entry",
         plan,
         [daily],
         [ledger({}), ledger({})],
         /more than once/,
      ],
      [
         "a ledger entry for a table the run does not build",
         plan,
         [daily],
         [ledger({ physicalTableName: "other_t" })],
         /do not build/,
      ],
      [
         "a ledger entry for a source that is not incremental",
         plan,
         [INSTRUCTION],
         [ledger({ sourceEntityId: "e1", physicalTableName: "t1" })],
         /refresh="incremental"/,
      ],
      [
         "a ledger entry measured under another definition",
         plan,
         [daily],
         [ledger({ sourceEntityId: "e-old" })],
         /different definition/,
      ],
   ];
   for (const [label, p, instructions, entries, message] of cases) {
      it(`answers ${label} 400 BUILD_INSTRUCTIONS_INVALID, naming the case`, () => {
         let refused: unknown;
         try {
            validateInstructions(
               p,
               instructions as never,
               entries as never,
               false,
            );
         } catch (error) {
            refused = error;
         }
         expect((refused as Error).message).toMatch(message);
         expect(answerOf(refused).json).toMatchObject({
            code: 400,
            reason: "BUILD_INSTRUCTIONS_INVALID",
         });
      });
   }
});
