// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as path from "path";
import { pathToFileURL } from "url";
import { BadRequestError, PayloadTooLargeError } from "../errors";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { installVariables } from "../test_helpers/settings_env";
import {
   overlaidText,
   overlayByUrl,
   parseOverlayFiles,
} from "./request_overlay";

describe("parseOverlayFiles", () => {
   let harness: MetricsHarness;
   // Settings are read once at startup; a case installs the bound it needs.
   let restoreSettings: (() => void) | undefined;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      restoreSettings?.();
      restoreSettings = undefined;
      await harness.shutdown();
   });

   const counted = (outcome: string) =>
      harness.collectCounter("publisher_request_overlay_total", {
         route: "compile",
         outcome,
      });

   it("is undefined, and uncounted, when the request has no files", async () => {
      expect(parseOverlayFiles(undefined, "compile")).toBeUndefined();
      expect(parseOverlayFiles(null, "compile")).toBeUndefined();
      expect(await counted("accepted")).toBe(0);
   });

   it("keeps each entry's path and content, and counts the request accepted", async () => {
      const files = [
         { path: "a.malloy", content: "source: a is duckdb.sql('select 1')" },
         { path: "dir/b.malloynb", content: "" },
      ];
      expect(parseOverlayFiles(files, "compile")).toEqual(files);
      expect(await counted("accepted")).toBe(1);
   });

   it.each([
      ["not an array", { path: "a.malloy", content: "" }],
      ["an entry that is not an object", ["a.malloy"]],
      ["content that is not a string", [{ path: "a.malloy", content: 1 }]],
      ["a missing path", [{ content: "" }]],
      ["a parent segment", [{ path: "../a.malloy", content: "" }]],
      ["a nested parent segment", [{ path: "x/../../a.malloy", content: "" }]],
      ["an absolute path", [{ path: "/a.malloy", content: "" }]],
      ["a backslash", [{ path: "x\\a.malloy", content: "" }]],
      ["a NUL byte", [{ path: "a\0.malloy", content: "" }]],
      ["a dot segment", [{ path: "./a.malloy", content: "" }]],
      ["a file that is not a model", [{ path: "publisher.json", content: "" }]],
      [
         "a path given twice",
         [
            { path: "a.malloy", content: "" },
            { path: "a.malloy", content: "" },
         ],
      ],
   ])("refuses %s with 400, counted refused_entry", async (_why, input) => {
      expect(() => parseOverlayFiles(input, "compile")).toThrow(
         BadRequestError,
      );
      expect(await counted("refused_entry")).toBe(1);
   });

   it("treats an empty overlay as none", () => {
      expect(parseOverlayFiles([], "query")).toBeUndefined();
      expect(parseOverlayFiles([], "compile")).toBeUndefined();
   });

   it("refuses more files than PUBLISHER_MAX_OVERLAY_FILES with 413, counted refused_bounds", async () => {
      restoreSettings?.();
      restoreSettings = installVariables({
         PUBLISHER_MAX_OVERLAY_FILES: "1",
      });
      expect(() =>
         parseOverlayFiles(
            [
               { path: "a.malloy", content: "" },
               { path: "b.malloy", content: "" },
            ],
            "compile",
         ),
      ).toThrow(PayloadTooLargeError);
      expect(await counted("refused_bounds")).toBe(1);
   });

   it("bounds the summed UTF-8 bytes, not characters", () => {
      restoreSettings?.();
      restoreSettings = installVariables({
         PUBLISHER_MAX_OVERLAY_BYTES: "5",
      });
      // Three characters, six bytes.
      expect(() =>
         parseOverlayFiles([{ path: "a.malloy", content: "ééé" }], "query"),
      ).toThrow(PayloadTooLargeError);
      expect(
         parseOverlayFiles([{ path: "a.malloy", content: "ab" }], "query"),
      ).toHaveLength(1);
   });
});

describe("overlayByUrl and overlaidText", () => {
   it("keys each file by the file URL the compiler asks for under the package", () => {
      const root = path.resolve("/srv/pkg");
      const byUrl = overlayByUrl(root, [
         { path: "dir/a b.malloy", content: "A" },
      ]);
      expect(
         byUrl.get(pathToFileURL(path.join(root, "dir", "a b.malloy")).href),
      ).toBe("A");
      expect(overlayByUrl(root, undefined).size).toBe(0);
   });

   it("finds an overlaid file's text by its package path", () => {
      const files = [{ path: "a.malloy", content: "A" }];
      expect(overlaidText(files, "a.malloy")).toBe("A");
      expect(overlaidText(files, "b.malloy")).toBeUndefined();
      expect(overlaidText(undefined, "a.malloy")).toBeUndefined();
   });
});
