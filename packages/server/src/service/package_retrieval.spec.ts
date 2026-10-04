// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { PackageManifestError } from "../errors";
import {
   DEFAULT_PACKAGE_RETRIEVAL,
   parsePackageRetrieval,
   readPackageRetrieval,
} from "./package_retrieval";

describe("publisher.json retrieval block", () => {
   it("defaults to the single representation", async () => {
      expect(await readPackageRetrieval("/nonexistent", undefined)).toEqual(
         DEFAULT_PACKAGE_RETRIEVAL,
      );
      expect(DEFAULT_PACKAGE_RETRIEVAL).toEqual({ representation: "single" });
   });

   it("accepts each valid value", () => {
      expect(parsePackageRetrieval({ representation: "facets" })).toEqual({
         representation: "facets",
      });
      expect(parsePackageRetrieval({ representation: "single" })).toEqual({
         representation: "single",
      });
   });

   it("rejects bad values with the valid list and a fix", () => {
      expect(() => parsePackageRetrieval({ representation: "multi" })).toThrow(
         'Invalid publisher.json retrieval.representation: expected one of single, facets, got "multi". Fix:',
      );
      expect(() => parsePackageRetrieval("on")).toThrow(
         "Invalid publisher.json retrieval: expected an object",
      );
   });

   it("an unknown key is an error that names the valid keys, so a typo or a later release's key is not silently ignored", () => {
      let error: unknown;
      try {
         parsePackageRetrieval({ refine: { enabled: true } });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(PackageManifestError);
      expect((error as Error).message).toBe(
         "Invalid publisher.json retrieval: unknown key 'refine'. Valid keys: representation.",
      );
   });
});
