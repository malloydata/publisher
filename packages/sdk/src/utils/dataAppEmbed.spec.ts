// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   isPublisherColorSchemeApplied,
   isPublisherColorSchemeRequest,
   isPublisherResizeMessage,
   packageFileUrl,
   PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE,
   PUBLISHER_COLOR_SCHEME_MESSAGE_TYPE,
   PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE,
   PUBLISHER_RESIZE_MESSAGE_TYPE,
   serverBaseUrl,
} from "./dataAppEmbed";

describe("serverBaseUrl", () => {
   it("strips a trailing /api/v0", () => {
      expect(serverBaseUrl("https://pub.example.com/api/v0")).toBe(
         "https://pub.example.com",
      );
   });
   it("strips a trailing /api/v0/", () => {
      expect(serverBaseUrl("https://pub.example.com/api/v0/")).toBe(
         "https://pub.example.com",
      );
   });
   it("leaves a base without the API prefix untouched", () => {
      expect(serverBaseUrl("https://pub.example.com")).toBe(
         "https://pub.example.com",
      );
   });
   it("only strips the suffix, not an interior /api/v0", () => {
      expect(serverBaseUrl("https://host/api/v0/proxy/api/v0")).toBe(
         "https://host/api/v0/proxy",
      );
   });
});

describe("packageFileUrl", () => {
   const at = (path: string, versionId?: string) =>
      packageFileUrl({
         server: "https://pub.example.com/api/v0",
         environmentName: "env",
         packageName: "pkg",
         path,
         ...(versionId !== undefined ? { versionId } : {}),
      });
   it("names no version when given none, so the server serves latest", () => {
      expect(at("public/index.html")).toBe(
         "https://pub.example.com/environments/env/packages/pkg/index.html",
      );
   });
   it("pins a version in the query string, encoding build metadata", () => {
      expect(at("index.html", "1.2.0+build.5")).toBe(
         "https://pub.example.com/environments/env/packages/pkg/index.html?versionId=1.2.0%2Bbuild.5",
      );
   });
   it("adds the version to a path that already has a query", () => {
      expect(at("app.html?tab=2", "1.2.0")).toBe(
         "https://pub.example.com/environments/env/packages/pkg/app.html?tab=2&versionId=1.2.0",
      );
   });
});

describe("isPublisherResizeMessage", () => {
   it("accepts a well-formed resize message", () => {
      expect(
         isPublisherResizeMessage({
            type: PUBLISHER_RESIZE_MESSAGE_TYPE,
            height: 420,
         }),
      ).toBe(true);
   });
   it("rejects a wrong type", () => {
      expect(isPublisherResizeMessage({ type: "other", height: 1 })).toBe(
         false,
      );
   });
   it("rejects a missing/non-numeric height", () => {
      expect(
         isPublisherResizeMessage({ type: PUBLISHER_RESIZE_MESSAGE_TYPE }),
      ).toBe(false);
      expect(
         isPublisherResizeMessage({
            type: PUBLISHER_RESIZE_MESSAGE_TYPE,
            height: "420",
         }),
      ).toBe(false);
   });
   it("rejects null / non-objects", () => {
      expect(isPublisherResizeMessage(null)).toBe(false);
      expect(isPublisherResizeMessage("publisher:resize")).toBe(false);
   });
});

describe("color scheme messages", () => {
   it("accepts a page's request and its light or dark answer", () => {
      expect(
         isPublisherColorSchemeRequest({
            type: PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE,
         }),
      ).toBe(true);
      for (const mode of ["light", "dark"]) {
         expect(
            isPublisherColorSchemeApplied({
               type: PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE,
               mode,
            }),
         ).toBe(true);
      }
   });
   it("refuses another type, an unknown mode and a non-object", () => {
      expect(
         isPublisherColorSchemeApplied({
            type: PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE,
            mode: "sepia",
         }),
      ).toBe(false);
      expect(
         isPublisherColorSchemeApplied({
            type: PUBLISHER_COLOR_SCHEME_MESSAGE_TYPE,
            mode: "dark",
         }),
      ).toBe(false);
      expect(isPublisherColorSchemeRequest({ type: "publisher:resize" })).toBe(
         false,
      );
      expect(isPublisherColorSchemeRequest("publisher:color-scheme-request")).toBe(
         false,
      );
      expect(isPublisherColorSchemeApplied(null)).toBe(false);
   });
});
