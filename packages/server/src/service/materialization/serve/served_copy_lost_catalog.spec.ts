// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A served copy compiled while its destination's catalog is lost (and before
// the worker reopens it) fails, and the query answers live. That failure is
// the worker's, not the copy's text: it must not be contained to a source or
// remembered, or the copy would answer live for the memo's ten minutes after
// the reopen invalidated it. Malloy reports the loss as a
// `failed-to-fetch-table-schema` problem quoting the typed error.
import { describe, expect, it, spyOn } from "bun:test";
import { DuckLakeCatalogUnavailableError } from "../../ducklake_catalog_unavailable";
import { ServedCopyServe } from "./served_copy_serve";

type LoadCopy = (
   state: unknown,
   copy: unknown,
   overlay: ReadonlyMap<string, string>,
   load: () => Promise<never>,
   lift: unknown,
) => Promise<unknown>;

function servedCopy(): ServedCopyServe {
   const copy = new ServedCopyServe({
      modelPath: () => "m.malloy",
      modelDef: () => undefined,
      serveDestinationConfig: () => undefined,
   } as unknown as ConstructorParameters<typeof ServedCopyServe>[0]);
   copy.setContext("/pkg", { connections: {} } as never);
   return copy;
}

function attempt(copy: ServedCopyServe, error: unknown) {
   const state = {
      withoutKept: new Map(),
      problems: [] as { reason: string; detail?: string }[],
      candidates: [],
      asOf: Date.now(),
   };
   const loadCopy = (copy as unknown as { loadCopy: LoadCopy }).loadCopy.bind(
      copy,
   );
   return {
      state,
      result: loadCopy(
         state,
         { files: new Map() },
         new Map([["file:///pkg/m.malloy", "source: s is x"]]),
         async () => {
            throw error;
         },
         {},
      ),
   };
}

describe("a served copy compiled while its destination's catalog is lost", () => {
   it("fails the copy without containing or remembering the failure", async () => {
      const copy = servedCopy();
      const internals = copy as unknown as {
         contain: (...args: unknown[]) => boolean;
         rememberFailure: (...args: unknown[]) => void;
      };
      const contain = spyOn(internals, "contain");
      const remember = spyOn(internals, "rememberFailure");
      // As Malloy wraps the typed error a schema read raised.
      const wrapped = Object.assign(new Error("import reference failure"), {
         problems: [
            {
               code: "failed-to-fetch-table-schema",
               message: new DuckLakeCatalogUnavailableError("credible").message,
            },
         ],
      });
      for (const error of [
         wrapped,
         new DuckLakeCatalogUnavailableError("credible"),
      ]) {
         const { state, result } = attempt(copy, error);
         expect(await result).toBe("failed");
         expect(state.problems).toEqual([
            expect.objectContaining({ reason: "compile_failed" }),
         ]);
      }
      expect(contain).not.toHaveBeenCalled();
      expect(remember).not.toHaveBeenCalled();
   });
});
