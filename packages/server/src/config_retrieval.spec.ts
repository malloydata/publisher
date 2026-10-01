// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES,
   getPublisherConfig,
   getSemanticIndexMaxEntities,
   parseRetrievalConfig,
} from "./config";

describe("retrieval.indexing.maxEntities", () => {
   it("accepts a positive integer", () => {
      expect(
         parseRetrievalConfig({ indexing: { maxEntities: 20000 } }),
      ).toEqual({ indexing: { maxEntities: 20000 } });
   });

   it("accepts digit-only text, which is what ${VAR} substitution produces", () => {
      expect(
         parseRetrievalConfig({ indexing: { maxEntities: "20000" } }),
      ).toEqual({ indexing: { maxEntities: 20000 } });
   });

   it("leaves the cap unset when the key is absent", () => {
      expect(parseRetrievalConfig(undefined)).toBeUndefined();
      expect(parseRetrievalConfig({})).toEqual({});
      expect(parseRetrievalConfig({ indexing: {} })).toEqual({ indexing: {} });
   });

   it("ignores keys it does not know yet, so the block can grow", () => {
      expect(
         parseRetrievalConfig({
            futureSetting: true,
            indexing: { maxEntities: 7, futureKnob: 1 },
         }),
      ).toEqual({ indexing: { maxEntities: 7 } });
   });

   const bad: Array<[string, unknown, string]> = [
      ["zero", 0, "0"],
      ["negative", -5, "-5"],
      ["fractional", 1.5, "1.5"],
      ["text", "lots", '"lots"'],
      ["negative text", "-3", '"-3"'],
      ["boolean", true, "true"],
      ["an array", [1], "[1]"],
   ];
   for (const [label, value, shown] of bad) {
      it(`rejects ${label} with the key, the value and a fix`, () => {
         expect(() =>
            parseRetrievalConfig({ indexing: { maxEntities: value } }),
         ).toThrow(
            `Invalid retrieval.indexing.maxEntities: expected a positive integer, got ${shown}. Fix: set it to e.g. 20000`,
         );
      });
   }

   it("rejects a retrieval block that is not an object", () => {
      expect(() => parseRetrievalConfig("on")).toThrow(
         'Invalid retrieval: expected an object, got "on".',
      );
      expect(() => parseRetrievalConfig({ indexing: 5 })).toThrow(
         "Invalid retrieval.indexing: expected an object, got 5.",
      );
   });
});

describe("getSemanticIndexMaxEntities (the startup read)", () => {
   let root: string;
   beforeEach(() => {
      root = fs.mkdtempSync(path.join(os.tmpdir(), "retrieval-config-"));
   });
   afterEach(() => {
      fs.rmSync(root, { recursive: true, force: true });
   });
   const writeConfig = (body: unknown) =>
      fs.writeFileSync(
         path.join(root, "publisher.config.json"),
         JSON.stringify(body),
      );

   it("returns the configured value", () => {
      writeConfig({
         frozenConfig: false,
         retrieval: { indexing: { maxEntities: 20000 } },
         environments: [],
      });
      expect(getSemanticIndexMaxEntities(root)).toBe(20000);
      expect(getPublisherConfig(root).retrieval).toEqual({
         indexing: { maxEntities: 20000 },
      });
   });

   it("defaults to 5000 when the block is absent", () => {
      writeConfig({ frozenConfig: false, environments: [] });
      expect(getSemanticIndexMaxEntities(root)).toBe(
         DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES,
      );
      expect(DEFAULT_SEMANTIC_INDEX_MAX_ENTITIES).toBe(5000);
   });

   it("throws the actionable message for an invalid value", () => {
      writeConfig({
         frozenConfig: false,
         retrieval: { indexing: { maxEntities: -1 } },
         environments: [],
      });
      expect(() => getSemanticIndexMaxEntities(root)).toThrow(
         "Invalid retrieval.indexing.maxEntities: expected a positive integer, got -1. Fix: set it to e.g. 20000",
      );
   });
});
