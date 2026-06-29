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
                  buildId: "b1",
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
               buildId: "b1",
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
               sources: [{ buildId: "b1", materializedTableId: "mt-1" }],
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
                     buildId: "b1",
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
