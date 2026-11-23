// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import * as sinon from "sinon";
import { BadRequestError } from "../errors";
import { logger } from "../logger";
import type { MaterializationService } from "../service/materialization/run/materialization_service";
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
      // then defaults it to on_demand.
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

   describe("a host-supplied physicalTableName, held to the bind's rule", () => {
      const source = (physicalTableName: string) => ({
         sourceEntityId: "b1",
         materializedTableId: "mt-1",
         physicalTableName,
      });
      const bodies: [string, (name: string) => Record<string, unknown>][] = [
         [
            "Build instruction",
            (n) => ({ buildInstructions: { sources: [source(n)] } }),
         ],
         [
            "Manifest reference",
            (n) => ({
               buildInstructions: {
                  sources: [source("ok_v1")],
                  referenceManifest: [
                     { sourceEntityId: "b0", physicalTableName: n },
                  ],
               },
            }),
         ],
      ];
      for (const [what, body] of bodies) {
         for (const bad of [
            "t\\'; DROP TABLE x --",
            '"s.t',
            'a"b.t',
            "a..b",
            ".t",
         ]) {
            it(`refuses ${what} ${JSON.stringify(bad)} with a 400 naming the field`, async () => {
               const { controller } = build();
               const err = await controller
                  .createMaterialization("env", "pkg", body(bad))
                  .catch((e) => e);
               expect(err).toBeInstanceOf(BadRequestError);
               expect(String(err.message)).toContain(
                  `${what} 'physicalTableName'`,
               );
            });
         }
         for (const good of ["s.orders_v1", '"My Schema"."T x"']) {
            it(`accepts ${what} ${JSON.stringify(good)}`, async () => {
               const { controller } = build();
               await controller.createMaterialization("env", "pkg", body(good));
            });
         }
      }
   });

   // A ledger entry is an echo of what the publisher reported, so a bad table
   // name in one is a corrupted entry, not a wrong request: it is dropped (its
   // source seeds) rather than refused, and never reaches SQL.
   describe("a ledger entry's physicalTableName, held to the bind's rule", () => {
      const ledger = (physicalTableName: string) => ({
         connectionName: "c",
         physicalTableName,
         coveredThrough: "1",
         coveredThroughType: "number",
         watermark: "w",
         strategy: "merge",
         sourceEntityId: "b1",
      });
      const body = (n: string) => ({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b1",
                  materializedTableId: "mt-1",
                  physicalTableName: "ok_v1",
               },
            ],
            ledger: [ledger(n)],
         },
      });
      for (const bad of ["t\\'; DROP TABLE x --", '"s.t', "a..b", ".t"]) {
         it(`drops ${JSON.stringify(bad)}, naming the field`, async () => {
            const warn = spyOn(logger, "warn");
            try {
               expect((await parse(body(bad))).ledger).toEqual([]);
               const dropped = warn.mock.calls.find((c) =>
                  String(c[0]).includes("malformed buildInstructions.ledger"),
               );
               expect(
                  String(
                     ((dropped as unknown[])[1] as { defect: string }).defect,
                  ),
               ).toContain("Ledger entry 'physicalTableName'");
            } finally {
               warn.mockRestore();
            }
         });
      }
      for (const good of ["s.orders_v1", '"My Schema"."T x"']) {
         it(`keeps ${JSON.stringify(good)}`, async () => {
            expect((await parse(body(good))).ledger).toEqual([ledger(good)]);
         });
      }
   });

   it("treats null buildInstructions as absent (auto-run)", async () => {
      expect(await parse({ buildInstructions: null })).toEqual({});
   });

   it("parses referenceManifest alongside sources, and drops the deprecated strictUpstreams", async () => {
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
      });
   });

   it("carries a reference's nativeSchema, and refuses a malformed one", async () => {
      const nativeSchema = [
         { name: "cat", type: "VARCHAR" },
         { name: "amt", type: "DECIMAL(10,1)" },
      ];
      const body = (schema: unknown) => ({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b2",
                  materializedTableId: "mt-2",
                  physicalTableName: "downstream_v1",
               },
            ],
            referenceManifest: [
               {
                  sourceEntityId: "b1",
                  physicalTableName: "upstream_table",
                  nativeSchema: schema,
               },
            ],
         },
      });
      const parsed = (await parse(body(nativeSchema))) as {
         referenceManifest: unknown[];
      };
      expect(parsed.referenceManifest).toEqual([
         {
            sourceEntityId: "b1",
            physicalTableName: "upstream_table",
            nativeSchema,
         },
      ]);
      for (const bad of [
         "DECIMAL",
         [{ name: "amt" }],
         [{ name: "", type: "INT" }],
         [{ name: "amt", type: 7 }],
         [null],
      ]) {
         const { controller } = build();
         let err: unknown;
         try {
            await controller.createMaterialization("env", "pkg", body(bad));
         } catch (e) {
            err = e;
         }
         expect(err).toBeInstanceOf(BadRequestError);
         expect(String((err as Error).message)).toContain(
            "Manifest reference 'nativeSchema'",
         );
      }
   });

   it("carries a reference's schema, dataAsOf, builtAt and contract, and refuses a malformed schema or instant", async () => {
      const body = (ref: Record<string, unknown>) => ({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b2",
                  materializedTableId: "mt-2",
                  physicalTableName: "downstream_v1",
               },
            ],
            referenceManifest: [
               {
                  sourceEntityId: "b1",
                  physicalTableName: "upstream_table",
                  storageDestinationName: "lake",
                  ...ref,
               },
            ],
         },
      });
      const schema = [{ name: "cat", type: "VARCHAR" }];
      // A host's contract, as it stored it: carried unparsed, unknown keys too.
      const contract = { v: 1, columns: [], later: { x: 1 } };
      const parsed = (await parse(
         body({
            schema,
            dataAsOf: "2026-11-20T00:00:00.000Z",
            builtAt: "2026-11-20T00:05:00.000Z",
            contract,
         }),
      )) as { referenceManifest: unknown[] };
      expect(parsed.referenceManifest).toEqual([
         {
            sourceEntityId: "b1",
            physicalTableName: "upstream_table",
            storageDestinationName: "lake",
            schema,
            dataAsOf: "2026-11-20T00:00:00.000Z",
            builtAt: "2026-11-20T00:05:00.000Z",
            contract,
         },
      ]);
      for (const [bad, field] of [
         [{ schema: [{ name: "cat" }] }, "schema"],
         [{ dataAsOf: "yesterday" }, "dataAsOf"],
         [{ builtAt: 7 }, "builtAt"],
      ] as const) {
         const { controller } = build();
         let err: unknown;
         try {
            await controller.createMaterialization("env", "pkg", body(bad));
         } catch (e) {
            err = e;
         }
         expect(err).toBeInstanceOf(BadRequestError);
         expect(String((err as Error).message)).toContain(
            `Manifest reference '${field}'`,
         );
      }
   });

   it("keeps a reference's storageDestinationName, reads an empty nativeSchema as none, and refuses an empty destination", async () => {
      const body = (ref: Record<string, unknown>) => ({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b2",
                  materializedTableId: "mt-2",
                  physicalTableName: "downstream_v1",
               },
            ],
            referenceManifest: [
               { sourceEntityId: "b1", physicalTableName: "up_t", ...ref },
            ],
         },
      });
      const parsed = (await parse(
         body({ storageDestinationName: "lake", nativeSchema: [] }),
      )) as { referenceManifest: unknown[] };
      expect(parsed.referenceManifest).toEqual([
         {
            sourceEntityId: "b1",
            physicalTableName: "up_t",
            storageDestinationName: "lake",
         },
      ]);
      for (const bad of ["", 7]) {
         const { controller } = build();
         let err: unknown;
         try {
            await controller.createMaterialization(
               "env",
               "pkg",
               body({ storageDestinationName: bad }),
            );
         } catch (e) {
            err = e;
         }
         expect(err).toBeInstanceOf(BadRequestError);
         expect(String((err as Error).message)).toContain(
            "Manifest reference 'storageDestinationName'",
         );
      }
   });

   it("preserves the storage `destination` on a build instruction", async () => {
      // Regression: `destination` is the orchestrated `storage=` axis. Dropping it
      // here silently downgrades an orchestrated build to a database
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

      // A boundary on a table in a storage destination names that destination,
      // which is part of the table's identity. Carried with the same weight as
      // the table name: an entry indexed without it is never found by the source
      // that owns it, so the source rebuilds in full on every refresh.
      const stored = { ...entry, storageDestinationName: "lake" };
      expect(
         (await parse({ buildInstructions: { sources, ledger: [stored] } }))
            .ledger,
      ).toEqual([stored]);

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

   it("drops a malformed ledger entry, keeping the rest, and warns naming the table and defect", async () => {
      // Hosts courier entries whole, so a wrong shape is a corrupted entry: it
      // is dropped (its source seeds) rather than wedging every build of the
      // package with a 400.
      const valid = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-06-20",
         coveredThroughType: "date",
         watermark: "order_date",
         strategy: "range_replace",
         sourceEntityId: "content-addr",
      };
      const other = { ...valid, physicalTableName: "events_v1" };
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
      const cases: [unknown, string, string | undefined][] = [
         ["2024-06-20", "not an object", undefined],
         [null, "not an object", undefined],
         [["a"], "not an object", undefined],
         [{ ...valid, coveredThrough: "" }, "'coveredThrough'", "orders_v1"],
         [
            { ...valid, coveredThrough: 20240620 },
            "'coveredThrough'",
            "orders_v1",
         ],
         [{ ...valid, watermark: undefined }, "'watermark'", "orders_v1"],
         [
            { ...valid, physicalTableName: undefined },
            "'physicalTableName'",
            undefined,
         ],
         [{ ...valid, connectionName: "" }, "'connectionName'", "orders_v1"],
         [{ ...valid, strategy: "upsert" }, "'strategy'", "orders_v1"],
         [{ ...valid, mergeKeys: "order_id" }, "'mergeKeys'", "orders_v1"],
         [{ ...valid, mergeKeys: [1] }, "'mergeKeys'", "orders_v1"],
         [
            { ...valid, storageDestinationName: "" },
            "'storageDestinationName'",
            "orders_v1",
         ],
         [
            { ...valid, storageDestinationName: 7 },
            "'storageDestinationName'",
            "orders_v1",
         ],
      ];
      const warn = spyOn(logger, "warn");
      try {
         for (const [bad, defect, table] of cases) {
            warn.mockClear();
            const parsed = await parse(withLedger([bad, other]));
            expect(parsed.ledger).toEqual([other]);
            const dropped = warn.mock.calls.filter((c) =>
               String(c[0]).includes("malformed buildInstructions.ledger"),
            );
            expect(dropped).toHaveLength(1);
            const fields = (dropped[0] as unknown[])[1] as Record<
               string,
               unknown
            >;
            expect(fields).toMatchObject({
               environmentName: "env",
               packageName: "pkg",
            });
            expect(String(fields.defect)).toContain(defect);
            expect(fields.table).toBe(table);
         }
         // Every entry malformed still leaves a caller-owned (empty) ledger, not
         // "use the local store": the sources seed.
         expect(
            (await parse(withLedger([{ ...valid, strategy: "x" }]))).ledger,
         ).toEqual([]);
      } finally {
         warn.mockRestore();
      }
      // The valid shape survives without a merge key list, which is what a
      // keyless (range-replace) source reports.
      expect((await parse(withLedger([valid]))).ledger).toEqual([valid]);
   });

   it("still refuses a ledger that is not an array", async () => {
      const { controller } = build();
      for (const ledger of [{ a: 1 }, "x", 7]) {
         await expect(
            controller.createMaterialization("env", "pkg", {
               buildInstructions: {
                  ledger,
                  sources: [
                     {
                        sourceEntityId: "b1",
                        materializedTableId: "mt-1",
                        physicalTableName: "orders_v1",
                     },
                  ],
               },
            }),
         ).rejects.toThrow(BadRequestError);
      }
   });

   it("keeps keys it does not know on an echoed ledger entry, untouched", async () => {
      // The ledger is opaque to hosts and the publisher may add keys to it;
      // stripping one here would lose it to the next run.
      const entry = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         coveredThrough: "2024-06-20",
         coveredThroughType: "date",
         watermark: "order_date",
         strategy: "range_replace",
         sourceEntityId: "content-addr",
         parentGeneration: {
            sourceEntityId: "p1",
            coveredThrough: "2024-06-19",
         },
         futureFlag: true,
      };
      const parsed = await parse({
         buildInstructions: {
            sources: [
               {
                  sourceEntityId: "b1",
                  materializedTableId: "mt-1",
                  physicalTableName: "orders_v1",
               },
            ],
            ledger: [entry],
         },
      });
      expect(parsed.ledger).toEqual([entry]);
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

   // ms2data/service#6756.
   describe("warehouseReach", () => {
      const SOURCE = {
         sourceEntityId: "b2",
         materializedTableId: "mt-2",
         physicalTableName: "downstream_v1",
         realization: "COPY",
      };

      it("accepts recompute and refuse, ignores both, and warns once per request", async () => {
         const warn = spyOn(logger, "warn");
         try {
            for (const warehouseReach of ["recompute", "refuse"]) {
               warn.mockClear();
               const parsed = await parse({
                  buildInstructions: {
                     sources: [SOURCE],
                     strictUpstreams: true,
                     warehouseReach,
                  },
               });
               expect("warehouseReach" in parsed).toBe(false);
               expect("strictUpstreams" in parsed).toBe(false);
               const deprecations = warn.mock.calls.filter((c) =>
                  String(c[0]).includes("deprecated and ignored"),
               );
               expect(deprecations).toHaveLength(1);
               expect((deprecations[0] as unknown[])[1]).toMatchObject({
                  options: ["strictUpstreams", "warehouseReach"],
               });
            }
            warn.mockClear();
            await parse({ buildInstructions: { sources: [SOURCE] } });
            expect(
               warn.mock.calls.some((c) =>
                  String(c[0]).includes("deprecated and ignored"),
               ),
            ).toBe(false);
         } finally {
            warn.mockRestore();
         }
      });

      it("reads null and absent alike, as the default", async () => {
         for (const buildInstructions of [
            { sources: [SOURCE], warehouseReach: null },
            { sources: [SOURCE] },
         ]) {
            const parsed = await parse({ buildInstructions });
            expect("warehouseReach" in parsed).toBe(false);
         }
      });

      it("rejects any other value with a 400 naming the accepted ones", async () => {
         for (const warehouseReach of ["REFUSE", "skip", 1, true]) {
            const { controller } = build();
            await expect(
               controller.createMaterialization("env", "pkg", {
                  buildInstructions: { sources: [SOURCE], warehouseReach },
               }),
            ).rejects.toThrow(
               /warehouseReach must be one of recompute \| refuse/,
            );
         }
      });
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

describe("MaterializationController responses", () => {
   // The record keeps the version a run built; the wire Materialization has
   // no `version` field, so every route leaves it off and the version goes
   // out in metadata.versionId. With versioning off the record's version is
   // null, and the response must still match the schema.
   const run = (version: string | null) => ({
      id: "m1",
      environmentId: "env-1",
      packageName: "pkg",
      status: "MANIFEST_FILE_READY",
      manifest: null,
      metadata: version ? { versionId: version, scope: "version" } : {},
      startedAt: null,
      completedAt: null,
      error: null,
      version,
      createdAt: new Date(0),
      updatedAt: new Date(0),
   });

   it("never answers with the record's version, on any route", async () => {
      for (const version of ["1.0.0", null]) {
         const service = {
            createMaterialization: sinon.stub().resolves(run(version)),
            stopMaterialization: sinon.stub().resolves(run(version)),
            getMaterialization: sinon.stub().resolves(run(version)),
            listMaterializations: sinon.stub().resolves([run(version)]),
         } as unknown as MaterializationService;
         const controller = new MaterializationController(service);
         const answers = [
            await controller.createMaterialization("env", "pkg", {}),
            await controller.stopMaterialization("env", "pkg", "m1"),
            await controller.getMaterialization("env", "pkg", "m1"),
            ...(await controller.listMaterializations("env", "pkg")),
         ];
         for (const answer of answers) {
            expect("version" in answer).toBe(false);
            expect(answer.metadata).toEqual(run(version).metadata);
         }
      }
   });
});
