import { request } from "@playwright/test";

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
