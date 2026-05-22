import { describe, expect, it } from "bun:test";
import { buildTableCssVars } from "./buildTableCssVars";
import { resolveTheme } from "./resolveTheme";

describe("buildTableCssVars", () => {
   it("emits the expected --malloy-render--* keys", () => {
      const t = resolveTheme([], "light");
      const vars = buildTableCssVars(t);
      expect(vars["--malloy-render--table-header-color"]).toBe("#5d626b");
      expect(vars["--malloy-render--table-font-size"]).toBe("12px");
   });

   it("intentionally omits the dashboard-root background keys", () => {
      // background and table-background are deliberately not surfaced
      // here so the renderer's neutral default paints the dashboard
      // chrome instead of the operator's accent colour. Charts get the
      // accent via vegaConfigOverride; tables get it via the renderer's
      // `theme.tableBackground` prop.
      const vars = buildTableCssVars(resolveTheme([], "light"));
      expect(vars["--malloy-render--background"]).toBeUndefined();
      expect(vars["--malloy-render--table-background"]).toBeUndefined();
      expect(vars["--malloy-theme--background"]).toBeUndefined();
      expect(vars["--malloy-theme--table-background"]).toBeUndefined();
   });

   it("flips body color and pinned background in dark mode", () => {
      const dark = resolveTheme([], "dark");
      const vars = buildTableCssVars(dark);
      expect(vars["--malloy-render--table-body-color"]).toBe("#e2e8f0");
      // Tiles are darker than the container so they read as recessed.
      expect(vars["--malloy-render--table-pinned-background"]).toBe("#0f172a");
   });

   it("honors a custom font size from the theme", () => {
      const t = resolveTheme([{ font: { size: { light: 14 } } }], "light");
      expect(buildTableCssVars(t)["--malloy-render--table-font-size"]).toBe(
         "14px",
      );
   });

   it("emits the font-family token consumed by the renderer", () => {
      const t = resolveTheme(
         [{ font: { family: { light: "Roboto, sans-serif" } } }],
         "light",
      );
      expect(buildTableCssVars(t)["--malloy-render--font-family"]).toBe(
         "Roboto, sans-serif",
      );
   });

   it("also emits the --malloy-theme--* shadow namespace for keys the renderer re-emits", () => {
      // The renderer writes its own `--malloy-render--*` inline style on
      // a deeper element using `var(--malloy-theme--<key>)` fallbacks.
      // We have to set the --malloy-theme--* source on our wrapper so
      // that downstream var() resolution lands on the operator's colour
      // and not the renderer's hardcoded default.
      const t = resolveTheme(
         [{ palette: { tableHeader: { light: "#ff0000" } } }],
         "light",
      );
      const vars = buildTableCssVars(t);
      expect(vars["--malloy-theme--table-header-color"]).toBe("#ff0000");
      expect(vars["--malloy-theme--table-border"]).toBeDefined();
   });
});
