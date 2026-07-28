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
});
