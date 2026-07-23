import { describe, expect, it } from "bun:test";
import type { VirtualMap } from "@malloydata/malloy";

// Prerequisite guard for the DuckLake materialization tier's build path.
//
// The tier resolves persist/virtual sources by handing the Malloy compile a
// `virtualMap` (connectionName → virtualName → tablePath). This asserts the
// pinned `@malloydata/malloy` still exports that public `VirtualMap` type, so a
// pin bump that drops or renames it fails CI typecheck here — turning a
// point-in-time manual confirm into a durable guard. Confirmed present in the
// shipped pin (^0.0.422): `VirtualMap = Map<string, Map<string, string>>` is
// exported from the package root and rides `PrepareResultOptions.virtualMap`.
describe("malloy pin prerequisites (DuckLake tier)", () => {
   it("exposes the VirtualMap substitution type in the pinned malloy", () => {
      // Compile-time guard: this type reference fails typecheck if `VirtualMap`
      // is removed/renamed in a future pin. The runtime assertions below double
      // as a shape check on the nested Map<string, Map<string, string>>.
      const virtualMap: VirtualMap = new Map([
         ["main_pg", new Map([["orders", "catalog.public.orders"]])],
      ]);
      expect(virtualMap.get("main_pg")?.get("orders")).toBe(
         "catalog.public.orders",
      );
   });
});
