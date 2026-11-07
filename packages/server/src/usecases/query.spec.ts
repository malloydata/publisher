// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * QueryUseCase runs one sequence for both transports; each documented
 * difference is an option. These cases pin what each option does, over a
 * real compiled model behind a fake store, reading the access record off the
 * logger line it writes.
 */
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import sinon from "sinon";
import { BadRequestError, ModelNotFoundError } from "../errors";
import { logger } from "../logger";
import { getActiveQueryCount } from "../query_concurrency";
import type { AccessRecord } from "../service/access_record";
import type { EnvironmentStore } from "../service/environment_store";
import type { Model } from "../service/model";
import { positionalQueryArgs } from "../test_helpers/positional_query_args";
import { withSettings } from "../test_helpers/settings_env";
import {
   loadTempPackage,
   type LoadedTempPackage,
} from "../test_helpers/temp_package";
import {
   type QueryCommand,
   type QueryTransportOptions,
   QueryUseCase,
   REST_QUERY_OPTIONS,
} from "./query";

let temp: LoadedTempPackage;
let model: Model;

beforeAll(async () => {
   temp = await loadTempPackage({
      "m.malloy":
         "source: s is duckdb.sql('select 1 as a') extend { view: v is { group_by: a } }\n",
   });
   model = temp.pkg.getModel("m.malloy")!;
});

afterAll(async () => {
   await temp.cleanup();
});

let info: ReturnType<typeof spyOn>;
beforeEach(() => {
   info = spyOn(logger, "info").mockImplementation(() => logger);
});
afterEach(() => {
   info.mockRestore();
   sinon.restore();
});

function records(): AccessRecord[] {
   return (info.mock.calls as unknown[][])
      .filter(([message]) => message === "Access record")
      .map(([, record]) => record as AccessRecord);
}

function useCaseOver(version?: string) {
   const pkg = {
      getModel: (path: string) => (path === "m.malloy" ? model : undefined),
      getDeclaredQueryMetadata: () => null,
      getVersionId: () => version,
   };
   const environment = {
      assertCanAdmitQuery: () => {},
      getPackage: async () => pkg,
      getApiConnection: () => {
         throw new Error("no such connection");
      },
   };
   const store = {
      getEnvironment: async () => environment,
   } as unknown as EnvironmentStore;
   return new QueryUseCase(store);
}

const COMMAND: QueryCommand = {
   environmentName: "env",
   packageName: "pkg",
   modelPath: "m.malloy",
   sourceName: "s",
   queryName: "v",
   identity: {},
};

const REST: QueryTransportOptions = { ...REST_QUERY_OPTIONS, shape: "full" };

/** What Model.runQuery was called with, in `getQueryResults`'s positional order. */
function spyRun() {
   const spy = sinon.spy(model, "runQuery");
   return () => {
      const args = positionalQueryArgs(...spy.firstCall.args);
      return {
         sourceName: args[1],
         queryName: args[2],
         query: args[3],
         metadata: args[8] as { version?: string } | undefined,
         shape: args[9],
         bypassAuthorize: args[10],
      };
   };
}

describe("QueryUseCase", () => {
   it("runs the query, records it, and hands the outcome to present", async () => {
      const args = spyRun();
      const out = await useCaseOver("1.0.0").run(COMMAND, REST, (o) => o);
      expect(out.version).toBe("1.0.0");
      expect(out.result.serializedResult).toContain('"a"');
      expect(args()).toMatchObject({
         sourceName: "s",
         queryName: "v",
         shape: "full",
         metadata: { version: "1.0.0" },
      });
      expect(records()).toHaveLength(1);
      expect(records()[0]).toMatchObject({
         outcome: "ok",
         version: "1.0.0",
         requestedSource: "s",
         adHoc: false,
      });
   });

   it("refuses a missing model with the transport's wording, after recording the version", async () => {
      const err = await useCaseOver("2.0.0")
         .run({ ...COMMAND, modelPath: "nope.malloy" }, REST, (o) => o)
         .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ModelNotFoundError);
      expect((err as Error).message).toBe('No queryable model "nope.malloy".');
      expect(records()[0]).toMatchObject({ status: 404, version: "2.0.0" });
   });

   it("refuseNotebook refuses a missing model while resolving, before the version is recorded", async () => {
      const onSetupError = sinon.spy();
      const err = await useCaseOver("2.0.0")
         .run(
            { ...COMMAND, modelPath: "nope.malloy" },
            {
               ...REST,
               refuseNotebook: true,
               modelNotFound: () => new ModelNotFoundError("early"),
               onSetupError,
            },
            (o) => o,
         )
         .catch((e: unknown) => e);
      expect((err as Error).message).toBe("early");
      expect(records()[0]).toMatchObject({ status: 404 });
      expect(records()[0].version).toBeUndefined();
      // The transport's own refusal is not a setup error.
      expect(onSetupError.called).toBe(false);
   });

   it("eagerCompile reports a compile failure to onSetupError and rethrows it", async () => {
      const broken = new Error("does not compile");
      sinon.stub(model, "getModel").rejects(broken);
      const onSetupError = sinon.spy();
      const err = await useCaseOver("2.0.0")
         .run(COMMAND, { ...REST, eagerCompile: true, onSetupError }, (o) => o)
         .catch((e: unknown) => e);
      expect(err).toBe(broken);
      expect(onSetupError.calledOnceWith(broken)).toBe(true);
      expect(records()[0].version).toBeUndefined();
   });

   it("throws the precondition inside the record, before resolving", async () => {
      const resolveModel = sinon.stub().rejects(new Error("not reached"));
      const err = await useCaseOver()
         .run(
            COMMAND,
            {
               ...REST,
               resolveModel,
               precondition: new BadRequestError("bad args"),
            },
            (o) => o,
         )
         .catch((e: unknown) => e);
      expect((err as Error).message).toBe("bad args");
      expect(resolveModel.called).toBe(false);
      expect(records()[0]).toMatchObject({ status: 400 });
   });

   it("refuses malformed query metadata as a 400", async () => {
      const err = await useCaseOver()
         .run({ ...COMMAND, queryClass: "nonsense" }, REST, (o) => o)
         .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BadRequestError);
   });

   it("honors the bypass only when the transport does", async () => {
      const honored = spyRun();
      await useCaseOver().run(
         { ...COMMAND, bypassAuthorize: true },
         REST,
         (o) => o,
      );
      expect(honored().bypassAuthorize).toBe(true);
      sinon.restore();
      const ignored = spyRun();
      await useCaseOver().run(
         { ...COMMAND, bypassAuthorize: true },
         { ...REST, honorBypassAuthorize: false },
         (o) => o,
      );
      expect(ignored().bypassAuthorize).toBe(false);
   });

   it("runs ad-hoc text alone, still stamping the answering version", async () => {
      const args = spyRun();
      await useCaseOver("1.0.0").run(
         { ...COMMAND, query: "run: s -> { group_by: a }" },
         {
            ...REST,
            shape: "compact",
            adHocRunsAlone: true,
         },
         (o) => o,
      );
      const seen = args();
      expect(seen.sourceName).toBeUndefined();
      expect(seen.shape).toBe("compact");
      expect(seen.metadata?.version).toBe("1.0.0");
      // The record still names the source the caller sent.
      expect(records()[0]).toMatchObject({ requestedSource: "s" });
   });

   it("takes the slot only when given a label, and holds it through present", async () => {
      await withSettings(
         { env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "4" } },
         async () => {
            const before = getActiveQueryCount();
            const heldWithout = await useCaseOver().run(COMMAND, REST, () =>
               getActiveQueryCount(),
            );
            expect(heldWithout).toBe(before);
            const heldWith = await useCaseOver().run(
               COMMAND,
               { ...REST, querySlotLabel: "spec" },
               () => getActiveQueryCount(),
            );
            expect(heldWith).toBe(before + 1);
            expect(getActiveQueryCount()).toBe(before);
         },
      );
   });

   it("records a failure to present as the request's outcome and releases the slot", async () => {
      await withSettings(
         { env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "4" } },
         async () => {
            const before = getActiveQueryCount();
            const err = await useCaseOver()
               .run(COMMAND, { ...REST, querySlotLabel: "spec" }, () => {
                  throw new Error("cannot shape");
               })
               .catch((e: unknown) => e);
            expect((err as Error).message).toBe("cannot shape");
            expect(getActiveQueryCount()).toBe(before);
            expect(records()[0]).toMatchObject({ status: 500 });
         },
      );
   });

   it("records a refusal made before run", async () => {
      await useCaseOver().recordRefusal(
         COMMAND,
         new BadRequestError("bad param"),
      );
      expect(records()[0]).toMatchObject({
         status: 400,
         environment: "env",
         adHoc: false,
      });
   });
});
