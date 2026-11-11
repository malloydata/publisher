// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as path from "path";
import { pathToFileURL } from "url";
import { BadRequestError } from "../errors";
import {
   isManifestLocation,
   versionManifestLocation,
} from "./manifest_location";

// A file URL for an absolute path on the platform the test runs on. On Windows
// file:///srv/... has no drive letter, so it is not absolute there and the
// manifest loader cannot read it.
const fileUrl = (p: string) => pathToFileURL(path.resolve(p)).href;

describe("isManifestLocation", () => {
   it("takes what the manifest loader reads: object storage, a file URL, an absolute path", () => {
      for (const location of [
         "gs://bucket/pkg/manifest.json",
         "s3://bucket/pkg/manifest.json",
         fileUrl("/srv/manifests/pkg.json"),
         "/srv/manifests/pkg.json",
         "/tmp/gcs_mock/bucket/pkg___1.0.0.manifest.json",
      ]) {
         expect([location, isManifestLocation(location)]).toEqual([
            location,
            true,
         ]);
      }
   });

   it("refuses a relative path, another scheme, and a URI with no object", () => {
      // A relative path would resolve against the server's working directory,
      // which the caller cannot know.
      for (const location of [
         "manifest.json",
         "./manifests/pkg.json",
         "../manifests/pkg.json",
         "https://example.com/manifest.json",
         "gs://",
         "gs://bucket",
         "s3://bucket/",
         // A file URL without `//` resolves to an absolute path, but the
         // manifest loader reads only `file://` and would take this as a
         // relative path, binding nothing.
         "file:/srv/manifests/pkg.json",
         // On Windows a file URL with a host is a UNC share path, which the
         // manifest loader reads.
         ...(process.platform === "win32" ? [] : ["file://host/manifest.json"]),
         "",
      ]) {
         expect([location, isManifestLocation(location)]).toEqual([
            location,
            false,
         ]);
      }
   });
});

describe("versionManifestLocation", () => {
   it("passes a manifest location through, and reads null or empty as serve live", () => {
      expect(versionManifestLocation("/srv/m.json")).toBe("/srv/m.json");
      expect(versionManifestLocation(fileUrl("/srv/m.json"))).toBe(
         fileUrl("/srv/m.json"),
      );
      expect(versionManifestLocation(null)).toBeNull();
      expect(versionManifestLocation("")).toBeNull();
      expect(versionManifestLocation(undefined)).toBeUndefined();
   });

   it("refuses a relative path or a non-string with a 400 naming what it takes", () => {
      for (const value of ["m.json", 3, ["gs://b/k"]]) {
         let error: unknown;
         try {
            versionManifestLocation(value);
         } catch (err) {
            error = err;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         expect((error as Error).message).toContain("an absolute path");
      }
   });
});
