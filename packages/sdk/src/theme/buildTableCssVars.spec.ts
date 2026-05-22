import { describe, expect, it } from "bun:test";
import { buildTableCssVars } from "./buildTableCssVars";
import { resolveTheme } from "./resolveTheme";

describe("buildTableCssVars", () => {
   it("emits a single --malloy-render--* namespace (no shadow set)", () => {
      const vars = buildTableCssVars(resolveTheme([], "light"));
      for (const key of Object.keys(vars)) {
         expect(key.startsWith("--malloy-render--")).toBe(true);
      }
   });

   it("includes header, body, border, pinned background, label, value", () => {
      const t = resolveTheme([], "light");
      const vars = buildTableCssVars(t);
      expect(vars["--malloy-render--table-header-color"]).toBe(t.tableHeader);
      expect(vars["--malloy-render--table-body-color"]).toBe(t.tableBody);
      expect(vars["--malloy-render--table-border"]).toBe(t.border);
      expect(vars["--malloy-render--table-pinned-background"]).toBe(t.tile);
      expect(vars["--malloy-render--label-color"]).toBe(t.tileTitle);
      expect(vars["--malloy-render--value-color"]).toBe(t.valueColor);
   });

   it("omits the dashboard-root background keys", () => {
      // background and table-background are deliberately not surfaced
      // here; the renderer paints the dashboard chrome with its own
      // neutral default and the operator's accent lands on viz
      // surfaces (charts via Vega, tables via the renderer prop).
      const vars = buildTableCssVars(resolveTheme([], "light"));
      expect(vars["--malloy-render--background"]).toBeUndefined();
      expect(vars["--malloy-render--table-background"]).toBeUndefined();
   });

   it("flips body color and pinned background in dark mode", () => {
      const dark = resolveTheme([], "dark");
      const vars = buildTableCssVars(dark);
      expect(vars["--malloy-render--table-body-color"]).toBe("#e2e8f0");
      expect(vars["--malloy-render--table-pinned-background"]).toBe("#0f172a");
   });

   it("honors a custom font size from the theme", () => {
      const t = resolveTheme([{ font: { size: 14 } }], "light");
      expect(buildTableCssVars(t)["--malloy-render--table-font-size"]).toBe(
         "14px",
      );
   });

   it("emits the font-family token consumed by the renderer", () => {
      const t = resolveTheme(
         [{ font: { family: "Roboto, sans-serif" } }],
         "light",
      );
      expect(buildTableCssVars(t)["--malloy-render--font-family"]).toBe(
         "Roboto, sans-serif",
      );
   });
});
