import { afterEach, describe, expect, it } from "bun:test";
import {
   getDimensionValueIndexCap,
   getDimensionValueIndexMode,
   getMcpTraceMode,
   getMcpTraceRetention,
} from "./config";

describe("eval and trace config", () => {
   afterEach(() => {
      delete process.env.PUBLISHER_MCP_TRACE;
      delete process.env.PUBLISHER_MCP_TRACE_RETENTION;
      delete process.env.PUBLISHER_DIMENSION_VALUE_INDEX;
      delete process.env.PUBLISHER_DIMENSION_VALUE_CAP;
   });

   it("defaults MCP trace to off", () => {
      expect(getMcpTraceMode()).toBe("off");
   });

   it("accepts metadata and retrieval", () => {
      process.env.PUBLISHER_MCP_TRACE = "metadata";
      expect(getMcpTraceMode()).toBe("metadata");
      process.env.PUBLISHER_MCP_TRACE = "retrieval";
      expect(getMcpTraceMode()).toBe("retrieval");
   });

   it("rejects an unrecognized trace mode", () => {
      process.env.PUBLISHER_MCP_TRACE = "debug";
      expect(() => getMcpTraceMode()).toThrow(/off \| metadata \| retrieval/);
   });

   it("defaults trace retention to 10000", () => {
      expect(getMcpTraceRetention()).toBe(10_000);
   });

   it("defaults dimension-value index to off", () => {
      expect(getDimensionValueIndexMode()).toBe("off");
      expect(getDimensionValueIndexCap()).toBe(500);
   });

   it("accepts lexical dimension-value indexing", () => {
      process.env.PUBLISHER_DIMENSION_VALUE_INDEX = "lexical";
      expect(getDimensionValueIndexMode()).toBe("lexical");
   });

   it("rejects an unrecognized dimension-value index mode", () => {
      process.env.PUBLISHER_DIMENSION_VALUE_INDEX = "embed";
      expect(() => getDimensionValueIndexMode()).toThrow(/off \| lexical/);
   });
});
