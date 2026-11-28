// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, spyOn } from "bun:test";
import { FCJ_MAX_CHARS } from "../constants";
import * as replyBudget from "../mcp/reply_budget";
import type { PackageLookup } from "../service/environment_ports";
import { fakeOf } from "./store_fakes";

export type Result = {
   isError?: boolean;
   content: Array<{
      type: string;
      text?: string;
      resource?: { uri: string; text: string };
   }>;
};
export type Handler = (
   params: Record<string, unknown>,
   extra?: { requestId?: string | number },
) => Promise<Result>;

export const scopes = [{ environment: "env", package: "pkg" }];

export const parse = (r: Result) => JSON.parse(r.content[0]!.resource!.text);

/** The JSON-RPC response's length, which is what a host measures against the tool-result limit. */
export const wire = (r: Result, id: string | number = "i".repeat(62)) =>
   JSON.stringify({ jsonrpc: "2.0", id, result: r }).length;

/** The handler a skill tool registers, over a store whose one package is `getPackage()`. */
export function handlerOf(
   register: (server: never, store: PackageLookup) => void,
   getPackage: () => Promise<unknown>,
): Handler {
   let handler: Handler | undefined;
   register(
      {
         registerTool: (_n: string, _config: unknown, h: Handler) => {
            handler = h;
         },
      } as never,
      fakeOf<PackageLookup>({
         getEnvironment: async () => ({ getPackage }),
      }),
   );
   return handler!;
}

/** Each payload `wireChars` is asked to measure while `run` runs, as parsed JSON, in order. */
export async function measuredWhile(
   run: () => Promise<unknown>,
): Promise<Array<Record<string, unknown>>> {
   const probes = spyOn(replyBudget, "wireChars");
   try {
      await run();
      return probes.mock.calls.map(
         ([, payload]) => JSON.parse(JSON.stringify(payload)) as never,
      );
   } finally {
      probes.mockRestore();
   }
}

/** The part size at which `handlerFor`'s answer, with the list beside it, just fills the limit. */
const edgeOf = async (
   handlerFor: (n: number) => Handler,
   params: Record<string, unknown>,
) => 1000 + FCJ_MAX_CHARS - wire(await handlerFor(1000)(params));

/** Every answer fits while one part of it grows through the limit; returns the shapes the answers took. */
export async function sweep(
   handlerFor: (n: number) => Handler,
   params: Record<string, unknown>,
   shape: (payload: ReturnType<typeof parse>) => string,
   past: number,
) {
   const edge = await edgeOf(handlerFor, params);
   const shapes = new Set<string>();
   for (let n = edge - 20; n <= edge + past; n++) {
      const result = await handlerFor(n)(params);
      expect(wire(result)).toBeLessThanOrEqual(FCJ_MAX_CHARS);
      // A message that names a size names one over the limit, never one at or under it.
      const text = JSON.stringify(parse(result));
      for (const [, size] of text.matchAll(/would be (\d+) characters/g)) {
         expect(Number(size)).toBeGreaterThan(FCJ_MAX_CHARS);
      }
      shapes.add(shape(parse(result)));
   }
   return shapes;
}
