import { describe, expect, it } from "bun:test";
import {
   isPublisherResizeMessage,
   PUBLISHER_RESIZE_MESSAGE_TYPE,
   serverBaseUrl,
} from "./pageEmbed";

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
