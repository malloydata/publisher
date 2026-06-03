import { describe, test, expect } from "bun:test";
import { truncate } from "../utils/logger";

describe("truncate", () => {
  test("returns short strings unchanged", () => {
    expect(truncate("ok")).toBe("ok");
  });

  test("collapses newlines and whitespace runs to single spaces", () => {
    expect(truncate("line one\n   line two\t\tline three")).toBe(
      "line one line two line three",
    );
  });

  test("caps length with an ellipsis", () => {
    const long = "x".repeat(100);
    const out = truncate(long, 10);
    expect(out.length).toBe(10);
    expect(out.endsWith("…")).toBe(true);
  });

  test("collapses a long multi-line error and truncates it", () => {
    const err = "Compile error:\n  unexpected token\n  at line 5\n".repeat(5);
    const out = truncate(err);
    expect(out.includes("\n")).toBe(false);
    expect(out.length).toBeLessThanOrEqual(60);
  });
});
