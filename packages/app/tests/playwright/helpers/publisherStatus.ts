// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, request, test } from "@playwright/test";

export type PublisherStatus = {
   mutable: boolean;
   frozenConfig: boolean;
};

/**
 * Fetches /api/v0/status and returns the effective mutability flags the
 * ServerProvider uses. `frozenConfig: true` forces `mutable: false` in the UI
 * regardless of the server's own setting.
 */
export async function getPublisherStatus(
   baseURL: string,
): Promise<PublisherStatus> {
   const ctx = await request.newContext({ baseURL });
   try {
      const res = await ctx.get("/api/v0/status");
      if (!res.ok()) {
         return { mutable: false, frozenConfig: true };
      }
      const body = (await res.json()) as {
         frozenConfig?: boolean;
         mutable?: boolean;
      };
      const frozenConfig = Boolean(body.frozenConfig);
      const mutable = frozenConfig ? false : body.mutable !== false;
      return { mutable, frozenConfig };
   } finally {
      await ctx.dispose();
   }
}

/**
 * Skips the calling spec unless the server shows the model viewer, its
 * `PUBLISHER_MODEL_PAGE` set to `viewer`; call it first in a `beforeAll`. The
 * suite runs against a server on the default, the Explorer, and CI runs the
 * viewer's specs once more against a server it starts with the setting. That
 * run sets `EXPECT_MODEL_PAGE=viewer`, so there a server reporting anything
 * else fails the spec rather than skipping it.
 */
export async function requireModelViewer(baseURL: string): Promise<void> {
   const ctx = await request.newContext({ baseURL });
   try {
      const res = await ctx.get("/api/v0/status");
      const body = res.ok()
         ? ((await res.json()) as { modelPage?: string })
         : {};
      if (process.env.EXPECT_MODEL_PAGE === "viewer") {
         expect(body.modelPage).toBe("viewer");
      }
      test.skip(
         body.modelPage !== "viewer",
         "the server shows the Explorer (PUBLISHER_MODEL_PAGE is not viewer)",
      );
   } finally {
      await ctx.dispose();
   }
}
