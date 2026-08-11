import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { BadRequestError } from "../errors";
import type { MaterializationService } from "../service/materialization_service";
import { MaterializationController } from "./materialization.controller";

/**
 * Unit tests for {@link MaterializationController.createMaterialization}'s body
 * validation. The service is stubbed so each test asserts the parsed options
 * the controller forwards (or the rejection for a malformed body).
 */
function build() {
   const createMaterialization = sinon.stub().resolves({ id: "m1" });
   const service = {
      createMaterialization,
   } as unknown as MaterializationService;
   const controller = new MaterializationController(service);
   return { controller, createMaterialization };
}

/** Parsed options the controller forwards for a given request body. */
async function parse(body: Record<string, unknown>) {
   const { controller, createMaterialization } = build();
   await controller.createMaterialization("env", "pkg", body);
   return createMaterialization.firstCall.args[2];
}

describe("MaterializationController.createMaterialization validation", () => {
   it("forwards an empty body as empty options", async () => {
      expect(await parse({})).toEqual({});
   });

   it("passes through forceRefresh and sourceNames", async () => {
      expect(
         await parse({ forceRefresh: true, sourceNames: ["a", "b"] }),
      ).toEqual({ forceRefresh: true, sourceNames: ["a", "b"] });
   });

   it("never forwards a client-supplied trigger (SCHEDULER cannot be forged)", async () => {
      // trigger is service-level-only: the controller must strip it so an API
      // caller cannot mint a run that reads as scheduler-driven. The service
      // then defaults it to ON_DEMAND.
      const parsed = await parse({ forceRefresh: true, trigger: "SCHEDULER" });
      expect(parsed).toEqual({ forceRefresh: true });
      expect("trigger" in (parsed as object)).toBe(false);
   });

   it("rejects a non-boolean forceRefresh", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            forceRefresh: "yes",
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("rejects sourceNames that is not an array of strings", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            sourceNames: [1, 2],
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("flattens buildInstructions.sources into the instruction list", async () => {
      const parsed = await parse({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b1",
                  sourceID: "orders@m",
                  materializedTableId: "mt-1",
                  physicalTableName: "orders_v1",
                  realization: "COPY",
               },
            ],
         },
      });
      expect(parsed).toEqual({
         buildInstructions: [
            {
               sourceEntityId: "b1",
               sourceID: "orders@m",
               materializedTableId: "mt-1",
               physicalTableName: "orders_v1",
               realization: "COPY",
            },
         ],
      });
   });

   it("treats null buildInstructions as absent (auto-run)", async () => {
      expect(await parse({ buildInstructions: null })).toEqual({});
   });

   it("parses referenceManifest and strictUpstreams alongside sources", async () => {
      const parsed = await parse({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b2",
                  materializedTableId: "mt-2",
                  physicalTableName: "downstream_v1",
                  realization: "COPY",
               },
            ],
            referenceManifest: [
               { sourceEntityId: "b1", physicalTableName: "upstream_table" },
            ],
            strictUpstreams: true,
         },
      });
      expect(parsed).toEqual({
         buildInstructions: [
            {
               sourceEntityId: "b2",
               sourceID: undefined,
               materializedTableId: "mt-2",
               physicalTableName: "downstream_v1",
               realization: "COPY",
            },
         ],
         referenceManifest: [
            { sourceEntityId: "b1", physicalTableName: "upstream_table" },
         ],
         strictUpstreams: true,
      });
   });

   it("preserves the storage `destination` on a build instruction", async () => {
      // Regression: `destination` is the orchestrated `storage=` axis. Dropping it
      // here silently downgrades an orchestrated build to a colocated
      // build, so it never materializes into the storage destination.
      const parsed = await parse({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b2",
                  materializedTableId: "mt-2",
                  physicalTableName: "downstream_v1",
                  realization: "COPY",
                  destination: "lake",
               },
            ],
         },
      });
      expect(parsed.buildInstructions).toEqual([
         {
            sourceEntityId: "b2",
            sourceID: undefined,
            materializedTableId: "mt-2",
            physicalTableName: "downstream_v1",
            realization: "COPY",
            destination: "lake",
         },
      ]);
   });

   it("preserves `reseed` on a build instruction, per source", async () => {
      // Regression, and the same drift as `destination` above: reseed is the ONLY
      // way an orchestrated caller can ask for a full rebuild (request-level
      // forceRefresh deliberately does not mean re-seed), so dropping it here
      // would leave the host's escape hatch inert — an incremental source it no
      // longer trusts would keep advancing by delta. Per source, so one can
      // rebuild while the rest advance in the same run.
      const parsed = await parse({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b1",
                  materializedTableId: "mt-1",
                  physicalTableName: "orders_v1",
                  realization: "COPY",
                  reseed: true,
               },
               {
                  sourceEntityId: "b2",
                  materializedTableId: "mt-2",
                  physicalTableName: "events_v1",
                  realization: "COPY",
               },
            ],
         },
      });
      expect(parsed.buildInstructions?.[0]).toMatchObject({ reseed: true });
      // Absent stays absent rather than becoming an explicit false.
      expect("reseed" in (parsed.buildInstructions?.[1] as object)).toBe(false);
   });

   it("passes through a run-level `reseed`, separately from forceRefresh", async () => {
      // Two flags, two questions: forceRefresh decides whether an unchanged table
      // is reused, reseed decides whether an incremental source rebuilds. Folding
      // them together would make every scheduled fire a full rebuild.
      expect(await parse({ reseed: true })).toEqual({ reseed: true });
      expect(await parse({ forceRefresh: true, reseed: false })).toEqual({
         forceRefresh: true,
         reseed: false,
      });
   });

   it("rejects a non-boolean run-level `reseed`", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", { reseed: "yes" }),
      ).rejects.toThrow("reseed must be a boolean");
   });

   it("rejects a non-boolean `reseed`", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: "b1",
                     materializedTableId: "mt-1",
                     physicalTableName: "orders_v1",
                     realization: "COPY",
                     reseed: "yes",
                  },
               ],
            },
         }),
      ).rejects.toThrow("'reseed' must be a boolean");
   });

   it("carries the caller's `ledger` through, empty or not", async () => {
      // Same carry-through hazard as `destination` and `reseed`: a silently
      // dropped ledger reads as "use the local store", which on an
      // interchangeable worker means a full rebuild that reports success.
      const entry = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-06-20",
         coveredThroughType: "date",
         watermark: "order_date",
         mergeKeys: ["order_id"],
         strategy: "range_replace",
         sourceEntityId: "content-addr",
      };
      const sources = [
         {
            sourceEntityId: "b1",
            materializedTableId: "mt-1",
            physicalTableName: "orders_v1",
            realization: "COPY",
         },
      ];
      const parsed = await parse({
         buildInstructions: { sources, ledger: [entry] },
      });
      expect(parsed.ledger).toEqual([entry]);

      // Empty and absent mean OPPOSITE things — "I own the ledger and nothing
      // is recorded" vs "use your local store" — so both must survive as sent.
      expect(
         (await parse({ buildInstructions: { sources } })).ledger,
      ).toBeUndefined();
      expect(
         (await parse({ buildInstructions: { sources, ledger: [] } })).ledger,
      ).toEqual([]);
      // And null reads as absent, not as an empty ledger.
      expect(
         (await parse({ buildInstructions: { sources, ledger: null } })).ledger,
      ).toBeUndefined();
   });

   it("rejects a malformed ledger entry", async () => {
      // Every field is an echo of what the publisher reported, so a wrong shape
      // is a caller bug — refused here, before it can misdescribe a boundary.
      const valid = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-06-20",
         coveredThroughType: "date",
         watermark: "order_date",
         strategy: "range_replace",
         sourceEntityId: "content-addr",
      };
      const withLedger = (ledger: unknown) => ({
         buildInstructions: {
            ledger,
            sources: [
               {
                  sourceEntityId: "b1",
                  materializedTableId: "mt-1",
                  physicalTableName: "orders_v1",
                  realization: "COPY",
               },
            ],
         },
      });
      const { controller } = build();
      for (const bad of [
         valid, // not wrapped in an array
         ["2024-06-20"],
         [{ ...valid, coveredThrough: "" }],
         [{ ...valid, coveredThrough: 20240620 }],
         [{ ...valid, watermark: undefined }],
         [{ ...valid, physicalTableName: undefined }],
         [{ ...valid, connectionName: "" }],
         [{ ...valid, strategy: "upsert" }],
         [{ ...valid, mergeKeys: "order_id" }],
      ]) {
         await expect(
            controller.createMaterialization("env", "pkg", withLedger(bad)),
         ).rejects.toThrow(BadRequestError);
      }
      // And the valid shape survives without a merge key list, which is what a
      // keyless (range-replace) source reports.
      const parsed = await parse(withLedger([valid]));
      expect(parsed.ledger).toEqual([valid]);
   });

   it("preserves the optional `connectionName` on a manifest reference", async () => {
      // Regression: `connectionName` (added by #904) lets the seed loop dialect-
      // quote the referenced upstream for a case-folding engine. Dropping it here
      // silently reverts to an unquoted seed — the same manual-copy drift that
      // dropped BuildInstruction.destination.
      const parsed = await parse({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b2",
                  materializedTableId: "mt-2",
                  physicalTableName: "downstream_v1",
                  realization: "COPY",
               },
            ],
            referenceManifest: [
               {
                  sourceEntityId: "b1",
                  physicalTableName: "upstream_table",
                  connectionName: "sf",
               },
            ],
         },
      });
      expect(parsed.referenceManifest).toEqual([
         {
            sourceEntityId: "b1",
            physicalTableName: "upstream_table",
            connectionName: "sf",
         },
      ]);
   });

   it("rejects a referenceManifest entry missing a required field", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: "b2",
                     materializedTableId: "mt-2",
                     physicalTableName: "downstream_v1",
                     realization: "COPY",
                  },
               ],
               referenceManifest: [{ sourceEntityId: "b1" }],
            },
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("rejects a non-boolean strictUpstreams", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: "b2",
                     materializedTableId: "mt-2",
                     physicalTableName: "downstream_v1",
                     realization: "COPY",
                  },
               ],
               strictUpstreams: "yes",
            },
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("rejects buildInstructions without a non-empty sources array", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            buildInstructions: { sources: [] },
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("rejects an instruction missing a required field", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            buildInstructions: {
               sources: [{ sourceEntityId: "b1", materializedTableId: "mt-1" }],
            },
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("rejects an unrecognized realization", async () => {
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: "b1",
                     materializedTableId: "mt-1",
                     physicalTableName: "orders_v1",
                     realization: "MERGE",
                  },
               ],
            },
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("rejects a runId the metadata contract cannot carry", async () => {
      // runId becomes the `run_id` property on every statement of the build, so
      // an over-long or unrenderable value is silently truncated and rewritten
      // at dispatch — leaving the caller holding an id that joins to nothing.
      // The neighbouring `trigger` is enum-validated; this is the same kind of
      // boundary and gets the same treatment.
      const { controller } = build();
      await expect(
         controller.createMaterialization("env", "pkg", {
            runContext: { runId: "x".repeat(300) },
         }),
      ).rejects.toThrow(BadRequestError);
      await expect(
         controller.createMaterialization("env", "pkg", {
            runContext: { runId: 'has "quotes"' },
         }),
      ).rejects.toThrow(BadRequestError);
   });

   it("still accepts a conforming runId", async () => {
      expect(
         await parse({ runContext: { trigger: "publish", runId: "run-42" } }),
      ).toEqual({ runContext: { trigger: "publish", runId: "run-42" } });
   });
});
